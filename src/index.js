/**
 * WeSense OrbitDB Service
 *
 * Helia (libp2p) + OrbitDB + Express HTTP API for distributed
 * node registration, trust list sync, and store scope tracking.
 *
 * This is a WeSense-only P2P network on port 4002 for sharing live
 * state between stations. It is NOT connected to the public IPFS
 * network — that role belongs to Kubo on port 4001.
 *
 * Peer discovery:
 *   - LAN: mDNS (zero config, requires network_mode: host in Docker)
 *   - WAN: Direct dial via ORBITDB_BOOTSTRAP_PEERS env var as initial seed
 *   - Propagation: gossipsub Peer Exchange (doPX) — once connected to any
 *     peer, stations learn about the wider mesh automatically via graft/prune
 *     PX messages. This is why every station only needs a handful of seed
 *     addresses in ORBITDB_BOOTSTRAP_PEERS, not the full network roster.
 */

// Must be imported before any helia/libp2p code to patch stream prototypes.
import { patchRegistrarForLegacyHandlers } from "./libp2p-stream-compat.js";
import { installTeardownTrace, wrapYamux } from "./teardown-trace.js";

// Patch AbstractMessageStream.abort/close before any connections are
// constructed. No-op unless TEARDOWN_TRACE env var is set. See
// StreamResetInvestigation.md.
installTeardownTrace();

import { createHelia } from "helia";
import { createLibp2p } from "libp2p";
import { noise } from "@chainsafe/libp2p-noise";
import { yamux as rawYamux } from "@chainsafe/libp2p-yamux";
// Lazy-patches YamuxMuxer.prototype on first muxer instance. Identity
// function when TEARDOWN_TRACE is unset.
const yamux = wrapYamux(rawYamux);
import { tcp } from "@libp2p/tcp";
import { mdns } from "@libp2p/mdns";
import { gossipsub } from "@chainsafe/libp2p-gossipsub";
import { identify } from "@libp2p/identify";
import { ping } from "@libp2p/ping";
import { FsBlockstore } from "blockstore-fs";
import { FsDatastore } from "datastore-fs";
import { createOrbitDB } from "@orbitdb/core";
import express from "express";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { existsSync, readFileSync } from "node:fs";
import { createServer as createHttpsServer } from "node:https";

import { openDatabases } from "./databases.js";
import { wrapHeliaForOrbitDB, setDiskFull, getBlacklistStats } from "./helia-compat.js";
import { createNodesRouter } from "./routes/nodes.js";
import { createTrustRouter } from "./routes/trust.js";
import { createStoresRouter } from "./routes/stores.js";
import { createHealthRouter } from "./routes/health.js";

// GC pause monitoring via perf_hooks (no NODE_OPTIONS flags required).
// Logs any GC event longer than GC_PAUSE_WARN_MS so we can correlate with
// connection deaths. The hypothesis (Phase2Plan §4.4): long Mark-Compact
// pauses stall the event loop, queued writes flush on unblock, then a
// timeout callback fires and destroys the connection — producing the
// stream-reset churn we observe in production.
//
// Set GC_PAUSE_WARN_MS=0 to log ALL GC events (very noisy).
// Set GC_PAUSE_WARN_MS=99999 to effectively disable (silent).
const GC_PAUSE_WARN_MS = parseInt(process.env.GC_PAUSE_WARN_MS || "100", 10);
try {
  const { PerformanceObserver } = await import("node:perf_hooks");
  const gcKindNames = { 1: "Scavenge", 2: "Mark-Compact", 4: "Incremental", 8: "Weak-Phantom" };
  const obs = new PerformanceObserver((list) => {
    for (const entry of list.getEntries()) {
      if (entry.duration >= GC_PAUSE_WARN_MS) {
        const kind = gcKindNames[entry.detail?.kind ?? entry.kind] || `kind-${entry.detail?.kind ?? entry.kind ?? "?"}`;
        console.warn(
          `GC PAUSE: ${kind} ${entry.duration.toFixed(0)}ms` +
          (entry.duration >= 1000 ? " ⚠️ >1s — event loop was stalled" : "")
        );
      }
    }
  });
  // entryTypes (plural) is the classic API; type (singular) is newer.
  // Using entryTypes for broader Node version compatibility.
  obs.observe({ entryTypes: ["gc"] });
  console.log(`GC pause monitoring active (threshold: ${GC_PAUSE_WARN_MS}ms)`);
} catch (err) {
  console.warn(`GC monitoring unavailable: ${err.message}`);
}

// OrbitDB's sync module emits errors via EventEmitter and also throws errors
// that escape pipe()/try-catch as uncaught exceptions or unhandled rejections.
// Known transient errors that should not crash the process:
//   - CBOR decode error: corrupt oplog entry from peer
//   - LoadBlockFailedError / Failed to load block: IPFS block not on any peer
//   - Want was aborted: bitswap request timed out
//   - StreamResetError / stream has been reset: yamux stream reset mid-sync
//   - UnexpectedEOFError / Unexpected EOF: peer disconnected during protocol negotiation
//   - connection reset by peer / ECONNRESET: TCP-level disconnect
// All are transient — sync retries automatically on next peer connection.
const KNOWN_SYNC_ERRORS = [
  "CBOR decode error",
  "Failed to load block",
  "LoadBlockFailedError",
  "Want was aborted",
  "stream has been reset",
  "Unexpected EOF",
  "connection reset by peer",
  "ECONNRESET",
  "The operation was aborted",
  "stream closed",
  // helia-compat failure-cache errors — expected when the cache has learned
  // a block is unreachable. Re-throwing these with full stack trace was
  // flooding the logs with no diagnostic value; the cache's own one-line
  // logging when it records the failure is sufficient.
  "permanently blacklisted",
  "unreachable (attempt",
];

function isKnownSyncError(err) {
  const msg = err?.message || String(err);
  return KNOWN_SYNC_ERRORS.some((pattern) => msg.includes(pattern));
}

// Some OrbitDB / libp2p / helia internal paths (p-queue task rejections,
// sync-module event emitters) call `console.error(err)` directly with
// Error objects, bypassing both our process.on('unhandledRejection') and
// process.on('uncaughtException') handlers above. The result is raw Error
// stack traces in logs for errors we already know are transient and
// well-handled elsewhere.
//
// We intercept console.error to apply the KNOWN_SYNC_ERRORS filter, but
// with a windowed "first-seen + summary" policy rather than blanket
// suppression. This keeps log noise minimal while preserving diagnostic
// signal: an operator can always see (a) example instances of suppressed
// patterns, (b) the rate at which they're occurring, and (c) any novel
// patterns that happen to match the filter by accident — the first
// occurrence per pattern per window always prints.
//
// Window policy:
//   - First occurrence of a pattern in the current window: logs as
//     "OrbitDB sync error (first in window, non-fatal): <msg>"
//   - Subsequent occurrences of the same pattern in the window: silent,
//     counted
//   - End of window (every 5 min): if any patterns hit, emits a summary
//     line "OrbitDB sync error summary (last Ns): p1=C1, p2=C2, ..."
//   - Unrecognised errors: always pass through to original console.error
//
// If suppressed-error rates spike or new patterns appear, both are
// visible without needing to read through raw stack traces. If new
// failure modes emerge that DON'T match existing patterns, they print
// fully unchanged.
const FILTER_WINDOW_MS = 5 * 60 * 1000;
const errorFilterState = {
  windowStart: Date.now(),
  counts: new Map(), // pattern string → occurrences in current window
};

function emitFilterSummary() {
  const elapsedMs = Date.now() - errorFilterState.windowStart;
  if (errorFilterState.counts.size === 0) {
    errorFilterState.windowStart = Date.now();
    return;
  }
  const parts = [];
  for (const [pattern, count] of errorFilterState.counts) {
    parts.push(`"${pattern}"=${count}`);
  }
  console.warn(
    `OrbitDB sync error summary (last ${Math.round(elapsedMs / 1000)}s): ${parts.join(", ")}`
  );
  errorFilterState.counts.clear();
  errorFilterState.windowStart = Date.now();
}
setInterval(emitFilterSummary, FILTER_WINDOW_MS);

const origConsoleError = console.error.bind(console);
console.error = (...args) => {
  if (args.length > 0) {
    // Build a summary from all args so patterns can match regardless of
    // which positional argument carries the useful text. Many code paths
    // call console.error("[prefix]", errObj) where args[0] is a string
    // label and the real error is in args[1] — we want to match on the
    // combined content, and later we want to show the reader the same
    // combined content (not just args[0], which would often be the label).
    const argSummaries = args.map((a) => {
      if (a?.message) return a.message;
      if (a?.stack) return a.stack;
      if (typeof a === "string") return a;
      return String(a);
    });
    const combined = argSummaries.join(" ");
    const matchedPattern = KNOWN_SYNC_ERRORS.find((pattern) =>
      combined.includes(pattern)
    );
    if (matchedPattern) {
      const count = (errorFilterState.counts.get(matchedPattern) || 0) + 1;
      errorFilterState.counts.set(matchedPattern, count);
      if (count === 1) {
        // Compose a single-line message that preserves all the diagnostic
        // information. Truncate per-arg so pathological stacks don't blow
        // out a single log line.
        const MAX_PER_ARG = 400;
        const shown = argSummaries
          .map((s) => (s.length > MAX_PER_ARG ? s.slice(0, MAX_PER_ARG) + "…" : s))
          .join(" ");
        console.warn(
          `OrbitDB sync error (first in window, non-fatal): ${shown}`
        );
      }
      // Subsequent in-window occurrences: silent, counted for summary.
      return;
    }
  }
  return origConsoleError(...args);
};

process.on("uncaughtException", (err) => {
  if (isKnownSyncError(err)) {
    console.warn(`OrbitDB sync error (uncaught, non-fatal): ${err.message}`);
    return;
  }
  console.error("Uncaught exception:", err);
  process.exit(1);
});

process.on("unhandledRejection", (reason) => {
  if (isKnownSyncError(reason)) {
    console.warn(`OrbitDB sync error (unhandled rejection, non-fatal): ${reason?.message || reason}`);
    return;
  }
  console.error("Unhandled rejection:", reason);
  process.exit(1);
});

const PORT = parseInt(process.env.PORT || "5200", 10);
const LIBP2P_PORT = parseInt(process.env.LIBP2P_PORT || "4002", 10);
const DATA_DIR = process.env.DATA_DIR || "./data";
const ANNOUNCE_ADDRESS = process.env.ANNOUNCE_ADDRESS || "";
const BOOTSTRAP_PEERS = process.env.ORBITDB_BOOTSTRAP_PEERS || "";
const NODE_TTL_DAYS = parseInt(process.env.NODE_TTL_DAYS || "7", 10);

// libp2p's @libp2p/connection-monitor runs a /ipfs/ping/1.0.0 probe on every
// connection at pingIntervalMs (default 10s). If the probe exceeds its
// AdaptiveTimeout OR any step (newStream, write, read, close) fails, it calls
// conn.abort(err) — which cascades through muxer.abort → stream aborts →
// TCPSocketMultiaddrConnection.abort → socket.resetAndDestroy() → TCP RST.
//
// Under trans-continental RTT and/or transient event-loop contention this
// triggers ~30 disconnects/hr/host in production (see
// StreamResetInvestigation.md). Yamux keepalive (10s, enableKeepAlive:true
// in streamMuxers below) already detects genuinely-dead connections at the
// lower layer without tearing down the TCP socket on a single slow probe.
//
// CONNECTION_MONITOR_ABORT=false keeps the monitor running (so conn.rtt is
// still measured for metrics) but stops it aborting connections on ping
// failure. Left at libp2p's upstream default (true) unless explicitly
// overridden. Test rollout: set false on one host, compare teardown rate.
const CONNECTION_MONITOR_ABORT = process.env.CONNECTION_MONITOR_ABORT !== "false";

// Parse ORBITDB_BOOTSTRAP_PEERS — supports multiple formats:
//   Full multiaddr: /ip4/203.0.113.1/tcp/4002/p2p/12D3KooW...
//   IP:port:        203.0.113.1:4002
//   Just IP:        203.0.113.1  (uses LIBP2P_PORT)
//   Hostname:       bootstrap.wesense.earth  (uses /dns4/)
//   Hostname:port:  bootstrap.wesense.earth:4002
function parseBootstrapPeers(peersStr, defaultPort) {
  if (!peersStr) return [];
  return peersStr
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
    .map((addr) => {
      if (addr.startsWith("/")) return addr;
      let host, port;
      if (addr.includes(":")) {
        [host, port] = addr.split(":");
      } else {
        host = addr;
        port = defaultPort;
      }
      const proto = /^[\d.]+$/.test(host) ? "ip4" : "dns4";
      return `/${proto}/${host}/tcp/${port}`;
    });
}

const WESENSE_PEER_ADDRS = parseBootstrapPeers(BOOTSTRAP_PEERS, LIBP2P_PORT);

function uint8ArrayEquals(a, b) {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

async function main() {
  // Ensure data directories exist
  await mkdir(`${DATA_DIR}/blockstore`, { recursive: true });
  await mkdir(`${DATA_DIR}/datastore`, { recursive: true });
  await mkdir(`${DATA_DIR}/orbitdb`, { recursive: true });

  let blockstore = new FsBlockstore(`${DATA_DIR}/blockstore`);
  let datastore = new FsDatastore(`${DATA_DIR}/datastore`);

  // Self-healing: verify blockstore integrity BEFORE opening helia/OrbitDB.
  //
  // Every IPFS block's CID contains a hash of its content. If the stored bytes
  // don't match the CID's hash, the block is corrupt. The helia v6 streaming
  // blockstore bug wrote partial/garbled bytes that may still decode as valid
  // CBOR but whose content doesn't match the CID hash. These "silently corrupt"
  // blocks cause OrbitDB sync to fail during replication — peers exchange entries
  // that reference blocks whose content doesn't match, causing CBOR decode errors
  // on the receiving side, timeouts, disconnect/reconnect cycles, and memory leaks.
  //
  // Fix: for each block, hash the stored bytes and compare against the CID.
  // If ANY block fails verification, wipe all OrbitDB data (blockstore + oplog +
  // index + datastore). The node starts fresh and replicates clean data from peers.
  // OrbitDB only holds small state data (nodes, trust, stores — ~10 entries total)
  // so rebuilding is fast and cheap.
  {
    const { sha256 } = await import("multiformats/hashes/sha2");
    const { rm, readdir, stat } = await import("node:fs/promises");

    // Collect bytes from async iterable (FsBlockstore streaming API)
    const collectBytes = async (source) => {
      const chunks = [];
      let total = 0;
      for await (const chunk of source) {
        total += chunk.byteLength;
        if (total > 2 * 1024 * 1024) throw new Error("Block exceeds 2MB");
        chunks.push(chunk);
      }
      if (chunks.length === 0) return new Uint8Array(0);
      if (chunks.length === 1) return chunks[0];
      const result = new Uint8Array(total);
      let offset = 0;
      for (const chunk of chunks) { result.set(chunk, offset); offset += chunk.byteLength; }
      return result;
    };

    // Check if blockstore has any blocks
    const blockstorePath = `${DATA_DIR}/blockstore`;
    let hasBlocks = false;
    try {
      const shards = await readdir(blockstorePath);
      for (const shard of shards) {
        const st = await stat(`${blockstorePath}/${shard}`);
        if (st.isDirectory()) { hasBlocks = true; break; }
      }
    } catch {
      // No blockstore yet — first start
    }

    if (hasBlocks) {
      let corrupt = false;
      let checked = 0;
      let corruptCount = 0;

      try {
        for await (const { cid, bytes: blockStream } of blockstore.getAll()) {
          try {
            const bytes = await collectBytes(blockStream);

            // Verify the content hash matches the CID's multihash.
            // CIDs encode the hash algorithm + digest. OrbitDB uses sha2-256.
            // If the bytes were garbled by the streaming blockstore bug, the
            // hash won't match even if the bytes happen to be valid CBOR.
            const hash = await sha256.digest(bytes);
            if (!uint8ArrayEquals(hash.digest, cid.multihash.digest)) {
              if (corruptCount === 0) {
                console.warn(`Corrupt block: ${cid.toString().slice(0, 24)}... content hash mismatch`);
              }
              corruptCount++;
              corrupt = true;
              // Don't break — count total corrupt blocks for logging
              if (corruptCount >= 10) break; // but stop after 10 to avoid scanning forever
            } else {
              checked++;
            }
          } catch (err) {
            console.warn(`Block read error: ${cid.toString().slice(0, 24)}... (${err.message})`);
            corrupt = true;
            corruptCount++;
            if (corruptCount >= 10) break;
          }
        }
      } catch (err) {
        console.warn(`Blockstore scan error: ${err.message} — skipping integrity check`);
      }

      const totalBlocks = checked + corruptCount;

      // Wipe if corrupt blocks found OR if blockstore has excessive stale blocks.
      // OrbitDB with 3 small databases (nodes, trust, stores) should have <500 blocks.
      // Thousands of blocks indicate stale data from removed databases (e.g. attestations)
      // which overwhelms the sync protocol and triggers the helia streaming blockstore bug.
      if (corrupt) {
        console.warn(`Found ${corruptCount} corrupt block(s) out of ${totalBlocks} checked`);
        console.warn("Wiping OrbitDB data to self-heal");
        await blockstore.close();
        try { await rm(`${DATA_DIR}/blockstore`, { recursive: true, force: true }); } catch {}
        try { await rm(`${DATA_DIR}/orbitdb`, { recursive: true, force: true }); } catch {}
        try { await rm(`${DATA_DIR}/datastore`, { recursive: true, force: true }); } catch {}
        await mkdir(`${DATA_DIR}/blockstore`, { recursive: true });
        await mkdir(`${DATA_DIR}/datastore`, { recursive: true });
        await mkdir(`${DATA_DIR}/orbitdb`, { recursive: true });
        blockstore = new FsBlockstore(`${DATA_DIR}/blockstore`);
        datastore = new FsDatastore(`${DATA_DIR}/datastore`);
        console.log("OrbitDB data wiped — will replicate fresh from peers on connect");
      } else if (checked > 0) {
        console.log(`Blockstore integrity OK (${checked} blocks verified)`);
      }

    }

    // Clean up retired attestations database directory
    try {
      const attestationsDir = `${DATA_DIR}/orbitdb/orbitdb/zdpuAyzsJLK74DoVEQNzW9yyyL3Zfr8AEPc1dJZz2Kd8rvHX2`;
      await rm(attestationsDir, { recursive: true, force: true });
    } catch {}
  }

  // Announce the host's real IP/hostname so peers on other networks can reach us
  const announceProto = ANNOUNCE_ADDRESS && /^[\d.]+$/.test(ANNOUNCE_ADDRESS) ? "ip4" : "dns4";
  const announce = ANNOUNCE_ADDRESS
    ? [`/${announceProto}/${ANNOUNCE_ADDRESS}/tcp/${LIBP2P_PORT}`]
    : [];

  // Use a dedicated datastore for libp2p peer metadata
  await mkdir(`${DATA_DIR}/libp2p`, { recursive: true });
  const libp2pDatastore = new FsDatastore(`${DATA_DIR}/libp2p`);

  // Peer identity persistence — stable peer ID across restarts.
  const keyPath = `${DATA_DIR}/peer_key`;
  let privateKey;
  try {
    const keyBytes = await readFile(keyPath);
    const { privateKeyFromProtobuf } = await import("@libp2p/crypto/keys");
    privateKey = privateKeyFromProtobuf(keyBytes);
    console.log("Loaded persisted peer identity");
  } catch (err) {
    try {
      const { generateKeyPair, privateKeyToProtobuf } = await import("@libp2p/crypto/keys");
      privateKey = await generateKeyPair("Ed25519");
      await writeFile(keyPath, Buffer.from(privateKeyToProtobuf(privateKey)));
      console.log("Generated new peer identity (saved to disk)");
    } catch (genErr) {
      console.warn(`Could not generate/save peer key: ${genErr.message} — peer ID will change on restart`);
    }
  }

  // Create libp2p without auto-start so we can configure gossipsub before
  // services begin. This avoids the parallel startup race where mDNS discovers
  // peers before gossipsub registers its topology callbacks.
  const libp2p = await createLibp2p({
    start: false,
    ...(privateKey ? { privateKey } : {}),
    datastore: libp2pDatastore,
    addresses: {
      listen: [`/ip4/0.0.0.0/tcp/${LIBP2P_PORT}`],
      announce,
    },
    // NOTE: Hypothesis 2 (bumping TCP transport socket inactivity timeouts
    // to 30min) was tested 2026-04-17 and eliminated — no improvement on
    // 4/5 hosts. Yamux keepalive at 10s already prevents TCP-level idle
    // timeout from firing, so bumping it was a no-op. Default restored.
    // See Phase2Plan §4.4 Hypothesis 2.
    transports: [tcp()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux({
      maxInboundStreams: 256,
      maxOutboundStreams: 256,
      // Yamux connection-level keep-alive. Sends PING frames at the yamux
      // layer to prevent middleboxes from treating the TCP connection as
      // idle. Default keepAliveInterval is 30s; 10s is our tuned value.
      enableKeepAlive: true,
      keepAliveInterval: 10_000,
      // NOTE: Hypothesis 1 (bumping stream-level `inactivityTimeout` to
      // 30min) was tested 2026-04-17 and ELIMINATED — it made things
      // ~50% worse network-wide, consistently across all five hosts.
      // Stream-level default (120_000ms = 2 min) is now restored by not
      // passing streamOptions here. See Phase2Plan §4.4 Hypothesis 1.
    })],
    transportManager: {
      // Don't crash if a listen address is temporarily in use (e.g. previous
      // container still releasing port during restart).
      faultTolerance: 1, // NO_FATAL
    },
    connectionManager: {
      // WeSense-only network — every peer is another station running OrbitDB.
      // At scale, thousands of stations may participate (Tier 2 producers +
      // Tier 3 consumers). GossipSub handles fanout efficiently, but we need
      // enough connections for healthy mesh topology and replication speed.
      minConnections: 20,
      maxConnections: 300,
      // Limit inbound connections to prevent resource exhaustion from internet scans
      maxIncomingPendingConnections: 50,
    },
    connectionMonitor: {
      // See CONNECTION_MONITOR_ABORT comment above for the full rationale.
      // Env-gated so we can A/B a single host before rolling out.
      abortConnectionOnPingFailure: CONNECTION_MONITOR_ABORT,
    },
    peerDiscovery: [
      // mDNS for automatic LAN discovery. Non-standard port to avoid conflict
      // with avahi-daemon. All WeSense stations use the same port.
      mdns({ port: 5354 }),
    ],
    services: {
      identify: identify(),
      ping: ping(),
      pubsub: gossipsub({
        allowPublishToZeroTopicPeers: true,
        maxInboundDataLength: 2 * 1024 * 1024, // 2MB — reject oversized gossipsub messages
        // Enable Peer Exchange (PX). When a station grafts/prunes a peer out of
        // the mesh, PX messages include known-peer records (peer ID + addresses
        // learned via Identify). This is how stations discover each other
        // automatically without requiring every station to be listed in every
        // other station's ORBITDB_BOOTSTRAP_PEERS. Without doPX, the manual
        // bootstrap list is the ONLY way a station learns about peers.
        // See docs/architecture/p2p-network.md §Node Discovery.
        doPX: true,
        // Peer records to include per PX message. 16 is the library default;
        // declared explicitly so future readers know it's intentional.
        // At very large scale this may want tuning — see Phase2Plan §4.4 Tier 2.
        pxPeers: 16,
        // NOTE: Gossipsub peer scoring is not configured (all peers score 0).
        // PX still propagates correctly without scoring because every peer is
        // a trusted WeSense station. Enabling scoreParams/scoreThresholds is
        // deferred work — see Phase2Plan §4.4 Tier 2.
      }),
    },
  });

  // Patch registrar before helia starts libp2p — wraps handler signatures
  // for gossipsub and OrbitDB protocols (see libp2p-stream-compat.js).
  patchRegistrarForLegacyHandlers(libp2p);

  // Helia defaults add trustlessGateway(), httpGatewayRouting(), and
  // libp2pRouting() which connect to the public IPFS network. Disable all
  // of them — this is a private WeSense network. We keep bitswap() because
  // OrbitDB uses helia.blockstore for IPFS block storage/retrieval between peers.
  const { bitswap } = await import("@helia/block-brokers");
  const helia = await createHelia({
    libp2p,
    blockstore,
    datastore,
    blockBrokers: [bitswap()],
    routers: [],
  });
  console.log(`Helia peer ID: ${helia.libp2p.peerId.toString()}`);
  console.log(`Announced addresses: ${helia.libp2p.getMultiaddrs().map((a) => a.toString()).join(", ")}`);
  if (WESENSE_PEER_ADDRS.length > 0) {
    console.log(`Configured peer addresses: ${WESENSE_PEER_ADDRS.join(", ")}`);
  }

  // Dial peers discovered via mDNS. On this network every discovered peer
  // is another WeSense station — there are no IPFS bootstrap nodes.
  helia.libp2p.addEventListener("peer:discovery", (evt) => {
    const discoveredId = evt.detail.id;
    if (helia.libp2p.getPeers().some((p) => p.equals(discoveredId))) return;
    helia.libp2p.dial(discoveredId).then(
      () => console.log(`mDNS: Connected to ${discoveredId}`),
      (err) => {
        if (!err.message?.includes("dial self")) {
          console.warn(`mDNS: Failed to dial ${discoveredId}: ${err.message}`);
        }
      }
    );
  });

  // Local view of when we last saw each peer's libp2p activity. Used by the
  // node-cleanup loop (cleanupStaleNodes) to keep a peer's wesense.nodes
  // entry alive as long as we're still hearing from them on the network,
  // even if their entry's `updated_at` is older than NODE_TTL_DAYS.
  //
  // Why: a long-running OrbitDB peer self-registers once on startup. If it
  // runs for longer than NODE_TTL_DAYS without restart, the cleanup loop
  // would prune its entry — making it invisible to other peers in the
  // registry-driven dialer — even though it's still actively present and
  // reachable. Network presence (peer:connect events) is the truer signal
  // of "this node is still here" than the staleness of a registration
  // timestamp the node wrote weeks ago.
  //
  // Local-only state: NOT replicated via OrbitDB. Each station maintains
  // its own view of which peers it has personally seen.
  const lastSeenPeers = new Map(); // peerId (string) -> last-seen timestamp (ms)

  // Log peer connections and disconnections; record contact for the
  // presence-aware cleanup logic below.
  helia.libp2p.addEventListener("peer:connect", (evt) => {
    const peerId = evt.detail.toString();
    lastSeenPeers.set(peerId, Date.now());
    console.log(`Peer connected: ${peerId}`);
  });
  helia.libp2p.addEventListener("peer:disconnect", (evt) => {
    const peerId = evt.detail.toString();
    // Refresh on disconnect too — they were here right up until disconnect.
    lastSeenPeers.set(peerId, Date.now());
    console.log(`Peer disconnected: ${peerId}`);
  });

  // Helia v6 changed blockstore.get() to return AsyncGenerator<Uint8Array>
  // (streaming blockstores). OrbitDB @3.0.2 expects plain Uint8Array returns.
  // Wrap helia so OrbitDB gets the non-streaming API it needs.
  // See: https://github.com/orbitdb/orbitdb/issues/1244
  const heliaForOrbitDB = wrapHeliaForOrbitDB(helia);

  const orbitdb = await createOrbitDB({
    ipfs: heliaForOrbitDB,
    directory: `${DATA_DIR}/orbitdb`,
  });

  let dbs = await openDatabases(orbitdb);
  console.log(`Databases opened — nodes: ${dbs.nodes.address}, trust: ${dbs.trust.address}`);

  // Self-healing: verify all oplog heads reference blocks that exist locally.
  // If a head references a block that doesn't exist (orphaned from a previous
  // blockstore wipe, incomplete sync, or corrupt peer data), OrbitDB will
  // fire LoadBlockFailedError on every sync cycle — spamming logs and wasting
  // resources indefinitely.
  //
  // Strategy: try loading each database's heads. At startup (before peers
  // connect), bitswap has no peers to query so missing blocks fail immediately
  // rather than waiting for a timeout. If any heads fail to load, drop that
  // database and re-open it — it will replicate fresh from peers on connect.
  // OrbitDB holds ~10 entries across 3 small databases so rebuild is fast.
  {
    const dbsToReset = [];

    for (const [name, db] of Object.entries(dbs)) {
      try {
        // heads() loads each head entry from IPFS blocks via the oplog store.
        // If a block is missing locally and no peers are connected, this throws
        // LoadBlockFailedError almost immediately (bitswap has no one to ask).
        const heads = await Promise.race([
          db.log.heads(),
          new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), 5000)),
        ]);
        if (heads) {
          console.log(`[${name}] Oplog heads OK (${heads.length} heads)`);
        }
      } catch (err) {
        console.warn(`[${name}] Orphaned oplog heads detected: ${err.message}`);
        dbsToReset.push(name);
      }
    }

    if (dbsToReset.length > 0) {
      console.warn(`Dropping ${dbsToReset.length} database(s) with orphaned heads: ${dbsToReset.join(", ")}`);
      for (const name of dbsToReset) {
        try {
          await dbs[name].drop();
          console.log(`[${name}] Dropped — will replicate fresh from peers`);
        } catch (err) {
          console.warn(`[${name}] Drop failed: ${err.message}`);
        }
      }
      // Re-open dropped databases
      dbs = await openDatabases(orbitdb);
      console.log("Databases re-opened after self-heal");
    }
  }

  // Self-register this OrbitDB peer in wesense.nodes so other stations can
  // discover and dial us via the registry-driven dialer.
  //
  // Without this, wesense.nodes only contains records written by data
  // services (storage-broker, ingesters) which carry their own iroh / zenoh
  // endpoints, NOT libp2p peer addresses. The registry-driven dialer
  // (see further down) needs `announce_address` populated to dial peers,
  // so without self-registration it has nothing useful to act on.
  //
  // Written once on startup. This is intentional rather than periodic:
  //   1. Catches changes to ANNOUNCE_ADDRESS (env var changes take effect
  //      on the next restart, which is when we want the registry updated)
  //   2. Avoids continuous oplog growth at scale — at 1M peers, hourly
  //      re-registration would mean 1M extra oplog entries per hour
  //
  // Caveat: wesense-orbitdb has a node-cleanup loop (cleanupStaleNodes,
  // below) that prunes records whose updated_at is older than
  // NODE_TTL_DAYS (default 7 days). If a station runs continuously for
  // longer than NODE_TTL_DAYS without restart, its self-registration will
  // be pruned and other peers won't see it. Operators who expect long
  // uninterrupted uptime should set NODE_TTL_DAYS to comfortably cover
  // their typical maintenance interval (e.g. 30 days to align with the
  // OrbitDB oplog TTL).
  //
  // See wesense-general-docs Phase2Plan §4.4 for the registry-driven
  // dialer design and the discussion of why self-registration was missing.
  if (ANNOUNCE_ADDRESS) {
    try {
      const selfPeerId = helia.libp2p.peerId.toString();
      await dbs.nodes.put({
        _id: selfPeerId,
        ingester_id: selfPeerId,
        announce_address: ANNOUNCE_ADDRESS,
        type: "orbitdb-peer",
        updated_at: new Date().toISOString(),
      });
      console.log(
        `Self-registered as orbitdb-peer in wesense.nodes ` +
        `(peer_id=${selfPeerId}, announce_address=${ANNOUNCE_ADDRESS})`
      );
    } catch (err) {
      console.warn(`Self-registration in wesense.nodes failed: ${err.message}`);
    }
  } else {
    console.log(
      "ANNOUNCE_ADDRESS not set — skipping self-registration in wesense.nodes. " +
      "This peer will not be discoverable by other stations via the registry."
    );
  }

  // Patch access controllers to tolerate missing identity blocks.
  // Old oplog entries created before the helia-compat.js fix have identity
  // blocks that were corrupted by helia v6's streaming blockstore. These
  // entries replicate forever between peers, and every verification attempt
  // triggers a LoadBlockFailedError. Since all databases use write: ["*"]
  // (permissive access), identity verification is unnecessary — we accept
  // unverifiable entries rather than erroring.
  for (const [name, db] of Object.entries(dbs)) {
    const origCanAppend = db.access.canAppend.bind(db.access);
    db.access.canAppend = async (entry) => {
      try {
        return await origCanAppend(entry);
      } catch (err) {
        const msg = err?.message || "";
        if (msg.includes("Failed to load block") ||
            msg.includes("Want was aborted") ||
            msg.includes("CBOR decode error") ||
            msg.includes("permanently blacklisted")) {
          return true; // accept entry — write: ["*"] permits all writers
        }
        throw err;
      }
    };
  }

  // Gossipsub error logging — gossipsub@14 swallows stream creation errors
  // into debug-only logging (libp2p:gossipsub namespace). Monkey-patch the
  // log.error method to also write to console so we can see failures.
  const pubsub = helia.libp2p.services.pubsub;
  if (pubsub.log?.error) {
    const origLogError = pubsub.log.error;
    pubsub.log.error = (...args) => {
      console.error("[gossipsub]", ...args);
      return origLogError.apply(pubsub.log, args);
    };
  }

  // Ensure every connected peer has a healthy outbound gossipsub stream.
  //
  // Handles two failure modes:
  //
  // 1. libp2p@3 startup race: peers connect before gossipsub registers its
  //    topology. We detect this via `outboundStreams < connectedPeers` and
  //    re-identify the affected peers to trigger gossipsub's callback, then
  //    manually call `createOutboundStream` as a bypass if that doesn't help.
  //
  // 2. Zombie streams: a peer's outbound stream was reset remotely (yamux
  //    RESET frame) mid-flight. The `OutboundStream` entry still exists in
  //    `pubsub.streamsOutbound`, but the underlying raw libp2p stream is
  //    closed/reset and unusable. Gossipsub v14 has no independent stream-
  //    close listener and its error callback only logs (upstream bug: pipe
  //    error at @chainsafe/libp2p-gossipsub/dist/src/index.js:484 is a
  //    no-op aside from logging), so the zombie entry sits there until the
  //    TCP/yamux connection itself eventually dies. Between stream reset
  //    and connection teardown, sync silently fails on that peer.
  //
  //    We detect zombies by checking the `rawStream.status` and
  //    `rawStream.writeStatus` on each `streamsOutbound` entry. A healthy
  //    stream is `status === 'open'` AND `writeStatus === 'writable'` —
  //    anything else (closed, reset, aborted, closing) is a zombie. We
  //    delete zombies from the map so the fix logic below treats the peer
  //    as needing a fresh stream.
  //
  // See Phase2Plan §4.4 libp2p-stream-compat investigation for the full
  // diagnosis and upstream PR plan.
  const ensureGossipsubStreams = async () => {
    const connectedPeers = helia.libp2p.getPeers();

    // Zombie sweep — remove entries whose underlying stream is dead.
    let zombiesRemoved = 0;
    if (pubsub.streamsOutbound) {
      for (const [id, outbound] of pubsub.streamsOutbound.entries()) {
        const rawStream = outbound?.rawStream;
        const healthy =
          rawStream &&
          rawStream.status === "open" &&
          rawStream.writeStatus === "writable";
        if (!healthy) {
          console.warn(
            `Gossipsub: zombie stream for ${id} ` +
            `(status=${rawStream?.status ?? "no-stream"} ` +
            `writeStatus=${rawStream?.writeStatus ?? "no-stream"}) — removing`
          );
          pubsub.streamsOutbound.delete(id);
          zombiesRemoved++;
        }
      }
    }

    const outboundStreams = pubsub.streamsOutbound?.size ?? 0;
    if (connectedPeers.length === 0) return;
    if (outboundStreams >= connectedPeers.length && zombiesRemoved === 0) {
      return; // all peers have healthy streams
    }
    console.log(
      `Gossipsub streams: ${outboundStreams}/${connectedPeers.length} — attempting to fix` +
        (zombiesRemoved > 0 ? ` (${zombiesRemoved} zombie${zombiesRemoved > 1 ? "s" : ""} removed)` : "")
    );

    // Step 1: Re-identify peers to trigger topology callbacks
    const identifySvc = helia.libp2p.services.identify;
    for (const peerId of connectedPeers) {
      if (pubsub.streamsOutbound?.has(peerId.toString())) continue;
      const conns = helia.libp2p.getConnections(peerId);
      for (const conn of conns) {
        if (conn.status !== "open") continue;
        try {
          await identifySvc.identify(conn);
        } catch (err) {
          console.warn(`Re-identify failed for ${peerId}: ${err.message}`);
        }
      }
    }

    // Wait a moment for the outbound inflight queue to process
    await new Promise((r) => setTimeout(r, 2000));

    // Step 2: If streams still missing, directly call createOutboundStream
    // bypassing the internal queue (which may be stuck or not processing).
    for (const peerId of connectedPeers) {
      const id = peerId.toString();
      if (pubsub.streamsOutbound?.has(id)) {
        console.log(`Gossipsub: stream already exists for ${id}`);
        continue;
      }
      // Ensure peer is in gossipsub's peers map
      if (!pubsub.peers?.has(id)) {
        const conns = helia.libp2p.getConnections(peerId);
        if (conns.length > 0) {
          try {
            pubsub.addPeer?.(peerId, conns[0].direction, conns[0].remoteAddr);
            console.log(`Gossipsub: manually added peer ${id}`);
          } catch (err) {
            console.warn(`Gossipsub: addPeer failed for ${id}: ${err.message}`);
          }
        }
      }
      const conns = helia.libp2p.getConnections(peerId);
      if (conns.length === 0) continue;
      console.log(`Gossipsub: directly calling createOutboundStream for ${id}`);
      try {
        await pubsub.createOutboundStream(peerId, conns[0]);
        console.log(`Gossipsub: after createOutboundStream for ${id} — has stream: ${pubsub.streamsOutbound?.has(id)}`);
      } catch (err) {
        console.error(`Gossipsub: createOutboundStream threw for ${id}: ${err.message}`);
      }
    }

    const finalStreams = pubsub.streamsOutbound?.size ?? 0;
    console.log(`Gossipsub streams after fix: ${finalStreams}/${connectedPeers.length}`);
  };
  setTimeout(ensureGossipsubStreams, 5_000);
  setTimeout(ensureGossipsubStreams, 15_000);
  setTimeout(ensureGossipsubStreams, 30_000);

  // Also run gossipsub fix when a peer (re)connects — handles bootstrap
  // disconnect/reconnect race where the initial fix attempts have already fired.
  helia.libp2p.addEventListener("peer:connect", () => {
    setTimeout(ensureGossipsubStreams, 3_000);
  });

  // Periodic zombie sweep. Stream resets on still-live connections don't
  // fire peer:connect (the connection stays up even after a stream dies),
  // so they'd otherwise sit as zombies until the connection eventually
  // tears down. 10s is a reasonable detection window — short enough that
  // sync loss is measured in seconds, not minutes; long enough not to burn
  // CPU checking a steady-state mesh.
  setInterval(ensureGossipsubStreams, 10_000);

  // Database replication event logging + error handling.
  // OrbitDB's sync module emits 'error' events when blocks can't be loaded
  // (peer offline, bitswap timeout) or when received data fails CBOR decode
  // (corrupt oplog entry, incompatible peer). Without a handler these crash
  // the process as unhandled EventEmitter errors.
  for (const [name, db] of Object.entries(dbs)) {
    db.events.on("join", (peerId, heads) => {
      console.log(`[${name}] Peer joined DB: ${peerId} (${heads?.length || 0} heads)`);
    });
    db.events.on("error", (err) => {
      console.warn(`[${name}] Sync error (non-fatal): ${err.message}`);
    });
  }

  // Replication trigger — when a WeSense peer connects, write a sync marker
  // to force HEAD re-publication so the new peer gets current data.
  let lastSyncTrigger = 0;
  const SYNC_DEBOUNCE = 60_000;

  const getWesensePeerCount = () => {
    const pubsub = helia.libp2p.services.pubsub;
    const topics = pubsub.getTopics ? pubsub.getTopics() : [];
    const peerSet = new Set();
    for (const topic of topics) {
      const subs = pubsub.getSubscribers ? pubsub.getSubscribers(topic) : [];
      for (const p of subs) peerSet.add(p.toString());
    }
    return peerSet.size;
  };

  const triggerSync = async (reason) => {
    try {
      const marker = {
        _id: "__sync__",
        type: "replication_trigger",
        peer_id: helia.libp2p.peerId.toString(),
        timestamp: new Date().toISOString(),
      };
      await dbs.nodes.put(marker);
      await dbs.trust.put(marker);
      console.log(`Replication sync triggered (${reason})`);
    } catch (err) {
      console.warn(`Sync trigger error: ${err.message}`);
    }
  };

  let syncPending = false;
  helia.libp2p.addEventListener("peer:connect", () => {
    if (syncPending) return;
    const now = Date.now();
    if (now - lastSyncTrigger < SYNC_DEBOUNCE) return;
    syncPending = true;
    // Delay to let gossipsub subscriptions propagate
    setTimeout(() => {
      syncPending = false;
      const wesensePeers = getWesensePeerCount();
      if (wesensePeers === 0) return;
      lastSyncTrigger = Date.now();
      triggerSync(`peer:connect, ${wesensePeers} WeSense peers`);
    }, 10_000);
  });

  // Periodic fallback — re-trigger every 10 minutes if WeSense peers are connected
  setInterval(async () => {
    const wesensePeers = getWesensePeerCount();
    if (wesensePeers === 0) return;
    await triggerSync(`periodic, ${wesensePeers} WeSense peers`);
  }, 10 * 60_000);

  // Periodic resource telemetry — logs memory, connections, and stream counts
  // so we can diagnose resource pressure from container logs after a crash.
  const logResourceStats = () => {
    const mem = process.memoryUsage();
    const peers = helia.libp2p.getPeers();
    const conns = helia.libp2p.getConnections();
    const pubsub = helia.libp2p.services.pubsub;
    const outbound = pubsub.streamsOutbound?.size ?? 0;
    const inbound = pubsub.streamsInbound?.size ?? 0;
    console.log(
      `RESOURCES | rss=${(mem.rss / 1048576).toFixed(0)}MB ` +
      `heap=${(mem.heapUsed / 1048576).toFixed(0)}/${(mem.heapTotal / 1048576).toFixed(0)}MB ` +
      `external=${(mem.external / 1048576).toFixed(1)}MB | ` +
      `peers=${peers.length} conns=${conns.length} ` +
      `streams_out=${outbound} streams_in=${inbound}`
    );
  };
  setInterval(logResourceStats, 60_000);
  setTimeout(logResourceStats, 10_000); // first report shortly after startup

  // Disk space monitoring — prevent orphaned block references from disk-full
  // partial writes. Checks the filesystem containing DATA_DIR every 5 minutes.
  // At 90% capacity: warn. At 95%: block writes. Below 90%: resume writes.
  {
    const { statfs } = await import("node:fs/promises");
    let diskWritesBlocked = false;

    const checkDiskSpace = async () => {
      try {
        const stats = await statfs(DATA_DIR);
        const totalBlocks = stats.blocks;
        const availableBlocks = stats.bavail; // blocks available to unprivileged users
        if (totalBlocks === 0n && typeof totalBlocks === "bigint") return;
        // statfs returns BigInt on some platforms, Number on others
        const total = Number(totalBlocks);
        const available = Number(availableBlocks);
        if (total === 0) return;
        const usagePercent = Math.round(((total - available) / total) * 100);

        if (usagePercent >= 95) {
          if (!diskWritesBlocked) {
            console.error(`DISK SPACE CRITICAL: ${usagePercent}% used on ${DATA_DIR} — blocking blockstore writes`);
            setDiskFull(true);
            diskWritesBlocked = true;
          }
        } else if (usagePercent >= 90) {
          console.warn(`DISK SPACE WARNING: ${usagePercent}% used on ${DATA_DIR}`);
          if (diskWritesBlocked) {
            console.log(`Disk usage dropped below 95% (${usagePercent}%) — resuming blockstore writes`);
            setDiskFull(false);
            diskWritesBlocked = false;
          }
        } else {
          if (diskWritesBlocked) {
            console.log(`Disk usage recovered to ${usagePercent}% — resuming blockstore writes`);
            setDiskFull(false);
            diskWritesBlocked = false;
          }
        }
      } catch (err) {
        console.warn(`Disk space check failed: ${err.message}`);
      }
    };

    // Check shortly after startup, then every 5 minutes
    setTimeout(checkDiskSpace, 10_000);
    setInterval(checkDiskSpace, 5 * 60_000);
  }

  // Oplog compaction — periodically remove expired entries from storage.
  // TTL filtering happens at read time, but compact() reclaims disk space
  // by deleting entries that have passed their TTL from the blockstore.
  {
    const compactAll = async () => {
      let total = 0;
      for (const [name, db] of Object.entries(dbs)) {
        try {
          if (db.log && typeof db.log.compact === 'function') {
            const removed = await db.log.compact();
            if (removed > 0) {
              console.log(`[${name}] Compacted: ${removed} expired oplog entries removed`);
            }
            total += removed;
          }
        } catch (err) {
          console.warn(`[${name}] Compact failed: ${err.message}`);
        }
      }
      if (total > 0) {
        console.log(`Oplog compaction complete: ${total} total entries removed`);
      }
    };

    // Compact once on startup (after 2 minutes to let sync settle), then daily
    setTimeout(compactAll, 2 * 60_000);
    setInterval(compactAll, 24 * 60 * 60_000);
  }

  // Node registry cleanup — remove entries we genuinely haven't heard from
  // within NODE_TTL_DAYS.
  //
  // "Heard from" = either:
  //   1. The entry's own updated_at is recent (the writer has refreshed it), OR
  //   2. The entry's ingester_id matches a peer we've seen on the libp2p
  //      network within the TTL window (lastSeenPeers map).
  //
  // (1) covers data services like storage-broker / ingesters that
  // periodically rewrite their own records to keep them fresh. (2) covers
  // long-running OrbitDB peers that self-register on startup only — they
  // remain visibly present on the network even if their registration
  // record's timestamp is old.
  //
  // Without (2), a peer that ran continuously past NODE_TTL_DAYS would
  // have its entry pruned and would disappear from other peers' views of
  // the registry — even though the peer itself is still actively reachable.
  // That's the wrong semantic. TTL should mean "we haven't seen this node",
  // not "this node hasn't bothered to re-write its registration".
  const cleanupStaleNodes = async () => {
    try {
      const cutoff = Date.now() - NODE_TTL_DAYS * 24 * 60 * 60 * 1000;
      const allEntries = await dbs.nodes.all();
      let removed = 0;
      let kept_via_presence = 0;
      for (const entry of allEntries) {
        const doc = entry.value;
        if (!doc || doc._id?.startsWith("__")) continue;
        const updatedAt = doc.updated_at ? new Date(doc.updated_at).getTime() : 0;
        if (updatedAt >= cutoff) continue; // (1) recent updated_at — keep

        // (2) check whether we've seen this peer on the network recently.
        // Applies primarily to orbitdb-peer entries (whose ingester_id is
        // a libp2p peer ID), but the map lookup is safe for any value —
        // non-peer ids simply won't be present in lastSeenPeers.
        const candidatePeerId = doc.ingester_id;
        if (candidatePeerId && lastSeenPeers.has(candidatePeerId)) {
          const lastSeen = lastSeenPeers.get(candidatePeerId);
          if (lastSeen >= cutoff) {
            kept_via_presence++;
            continue; // recently seen — keep entry alive
          }
        }

        await dbs.nodes.del(doc._id);
        removed++;
      }
      if (removed > 0 || kept_via_presence > 0) {
        console.log(
          `Node cleanup: removed ${removed} stale entries, ` +
          `kept ${kept_via_presence} via recent network presence ` +
          `(TTL: ${NODE_TTL_DAYS}d)`
        );
      }
    } catch (err) {
      console.warn(`Node cleanup error: ${err.message}`);
    }
  };
  setTimeout(cleanupStaleNodes, 30_000);
  setInterval(cleanupStaleNodes, 60 * 60_000);

  // Garbage-collect the lastSeenPeers map. The Map grows with every unique
  // peer connection; without GC it would accumulate indefinitely. At 1M+
  // peers seen over months of uptime this becomes meaningful memory.
  //
  // Entries older than NODE_TTL_DAYS no longer serve any purpose: they
  // can't save a registry entry from cleanup (cleanupStaleNodes uses the
  // same cutoff), so removing them changes nothing operationally.
  //
  // Bounded by NODE_TTL_DAYS regardless of network size — the Map size
  // tracks "peers seen in the last N days", not all-time peers.
  const cleanupLastSeenPeers = () => {
    const cutoff = Date.now() - NODE_TTL_DAYS * 24 * 60 * 60 * 1000;
    let removed = 0;
    for (const [peerId, lastSeen] of lastSeenPeers) {
      if (lastSeen < cutoff) {
        lastSeenPeers.delete(peerId);
        removed++;
      }
    }
    if (removed > 0) {
      console.log(`lastSeenPeers GC: removed ${removed} stale entries (now ${lastSeenPeers.size} tracked)`);
    }
  };
  // Hourly is fine — the Map only matters at cleanupStaleNodes time, which
  // also runs hourly. No urgency for tighter cadence.
  setInterval(cleanupLastSeenPeers, 60 * 60_000);

  // Peer dialing has two modes that share the same safety/self-check logic:
  //
  //   1. Bootstrap seed (ORBITDB_BOOTSTRAP_PEERS) — periodic dial of the manually
  //      configured peer list. Used for cold-start (when wesense.nodes is empty)
  //      and for recovery if the registry-driven dialer hasn't re-established yet.
  //
  //   2. Registry-driven (wesense.nodes) — reacts to OrbitDB update events on the
  //      node registry. When a station registers (or re-registers) with an
  //      announce_address, we dial it. This is WeSense's native peer-discovery
  //      mechanism — see wesense-docs/architecture/p2p-network.md §Node Discovery.
  //
  // Both modes converge on the same `tryDial()` which handles:
  //   - self-check (don't dial our own announce address)
  //   - already-connected check (don't re-dial an existing connection)
  //   - penalty check (future hook for the trust/quality prioritisation framework
  //     — see Phase2Plan §4.4; currently a no-op)

  const ownAddresses = new Set();
  if (ANNOUNCE_ADDRESS) {
    ownAddresses.add(ANNOUNCE_ADDRESS.toLowerCase());
    // Also resolve DNS to IP for comparison (e.g. bootstrap.wesense.earth → 74.208.250.27)
    try {
      const { lookup } = await import("node:dns/promises");
      const { address: resolvedIp } = await lookup(ANNOUNCE_ADDRESS);
      ownAddresses.add(resolvedIp);
    } catch {
      // Not a resolvable hostname or DNS failed — IP comparison still works
    }
  }

  const filteredPeerAddrs = WESENSE_PEER_ADDRS.filter((addr) => {
    const host = addr.match(/\/(?:ip4|dns4)\/([^/]+)\//)?.[1];
    if (host && ownAddresses.has(host.toLowerCase())) {
      console.log(`Direct dial: Skipping self-address ${addr}`);
      return false;
    }
    return true;
  });

  const { multiaddr: createMa } = await import("@multiformats/multiaddr");
  const { lookup: dnsLookup } = await import("node:dns/promises");

  // Resolve dns4 addresses and filter out self after resolution.
  // Cache resolved self-addresses so we only log once per address.
  const resolvedSelfCache = new Set();
  const resolvedSelfCheck = async (addr) => {
    const host = addr.match(/\/dns4\/([^/]+)\//)?.[1];
    if (host) {
      try {
        const { address: resolvedIp } = await dnsLookup(host);
        if (ownAddresses.has(resolvedIp)) {
          if (!resolvedSelfCache.has(addr)) {
            resolvedSelfCache.add(addr);
            console.log(`Dial: Skipping self-address ${addr} (resolved to ${resolvedIp})`);
          }
          return true;
        }
      } catch {}
    }
    return false;
  };

  const alreadyConnectedTo = (addr) => {
    const targetHost = addr.match(/\/(?:ip4|dns4)\/([^/]+)\//)?.[1];
    if (!targetHost) return false;
    return helia.libp2p
      .getConnections()
      .some((c) => c.remoteAddr.toString().includes(targetHost));
  };

  // Placeholder for the future trust/quality prioritisation framework.
  // See Phase2Plan §4.4 "Trust / quality prioritisation framework (deferred design)".
  // When implemented, this should consult whatever state we end up using —
  // likely fields on wesense.nodes records, or a separate wesense.peer_scores DB —
  // and return true for peers that should not be dialed (revoked, misbehaving, etc.).
  const isPenalised = (/* announceAddr, peerId */) => false;

  const tryDial = async (addr, source) => {
    if (await resolvedSelfCheck(addr)) return;
    if (alreadyConnectedTo(addr)) return;
    if (isPenalised(addr)) return;
    try {
      const ma = createMa(addr);
      await helia.libp2p.dial(ma);
      console.log(`Dial [${source}]: Connected to ${addr}`);
    } catch (err) {
      if (!err.message?.includes("dial self")) {
        console.warn(`Dial [${source}]: Failed ${addr}: ${err.message}`);
      }
    }
  };

  // --- 1. Bootstrap-seed dialer ---

  if (filteredPeerAddrs.length > 0) {
    const dialConfiguredPeers = async () => {
      for (const addr of filteredPeerAddrs) {
        await tryDial(addr, "bootstrap-seed");
      }
    };
    setTimeout(dialConfiguredPeers, 5_000);
    setInterval(dialConfiguredPeers, 60_000);
    console.log(`Bootstrap-seed dialing enabled for: ${filteredPeerAddrs.join(", ")}`);
  } else if (WESENSE_PEER_ADDRS.length > 0) {
    console.log("All configured bootstrap peers are self-addresses — no outbound seeding");
  }

  // --- 2. Registry-driven dialer (wesense.nodes) ---

  // Parse an announce_address field (as stored in wesense.nodes) into a
  // full libp2p multiaddr string. Supports the same formats as
  // ORBITDB_BOOTSTRAP_PEERS — full multiaddr, host:port, or bare host/IP.
  const toMultiaddrFromAnnounce = (announceAddr) => {
    if (!announceAddr || typeof announceAddr !== "string") return null;
    if (announceAddr.startsWith("/")) return announceAddr; // already a multiaddr
    let host, port;
    if (announceAddr.includes(":")) {
      [host, port] = announceAddr.split(":");
    } else {
      host = announceAddr;
      port = LIBP2P_PORT;
    }
    if (!host) return null;
    const proto = /^[\d.]+$/.test(host) ? "ip4" : "dns4";
    return `/${proto}/${host}/tcp/${port}`;
  };

  // Dial a single node record from the registry if it has an announce_address.
  const dialFromNodeDoc = async (doc, source) => {
    if (!doc || doc._id?.startsWith("__")) return;
    if (!doc.announce_address) return;
    const ma = toMultiaddrFromAnnounce(doc.announce_address);
    if (!ma) return;
    const tag = doc.ingester_id || doc._id || "unknown";
    await tryDial(ma, `${source}:${tag}`);
  };

  // Startup walk: dial every peer already known in the local replica of
  // wesense.nodes. Delayed slightly so the bootstrap seed has a chance to
  // connect first and start sync before we start the walk.
  //
  // Uses the Documents iterator() (lazy yield, one entry at a time) rather
  // than all() (collects everything before returning). This matters when a
  // station has accumulated many poisoned oplog entries: the inner
  // traverse()+safeFetchEntry() yields healthy entries immediately and
  // skips unreachable ones at 2s each. With iterator(), we process each
  // yielded doc as it arrives; with all(), we'd wait for the full set to
  // materialise (potentially minutes, exceeding any reasonable deadline).
  //
  // We enforce a soft deadline between yielded entries so the walk can't
  // run forever on extremely poisoned stations. If we hit the deadline,
  // we stop cleanly with whatever we processed — "partial results" rather
  // than "no results". The event-driven dialer catches anything we missed
  // when the remaining entries eventually arrive via sync updates.
  const REGISTRY_WALK_TIMEOUT_MS = 30000;
  const dialFromRegistryWalk = async () => {
    const startedAt = Date.now();
    const deadline = startedAt + REGISTRY_WALK_TIMEOUT_MS;
    let entriesSeen = 0;
    let considered = 0;
    let skippedNoAddr = 0;
    let skippedInternal = 0;
    let partial = false;

    try {
      // NB: do NOT pass { amount: -1 } here — the Documents iterator checks
      // `if (count >= amount) break`, and `1 >= -1` is true, so passing -1
      // causes the walk to stop after the first entry. With no argument,
      // `amount` is undefined and the comparison is always false, so we
      // iterate everything. (The Log iterator uses amount=-1 as "unlimited"
      // semantics, but the Documents iterator on top of it does not.)
      const iter = dbs.nodes.iterator();
      for await (const entry of iter) {
        if (Date.now() > deadline) {
          partial = true;
          break;
        }
        // Yield to the event loop between entries. Without this, the walk
        // monopolises the event loop for its entire duration (13+ seconds
        // on poisoned stations). During that time, yamux keepalive PINGs
        // can't send, the remote sees silence, and connections get RST'd.
        // Investigation 2026-04-17 showed disconnects correlate exactly
        // with the walk's firing interval — the walk was starving yamux.
        //
        // setTimeout(0) defers to the next macrotask, letting any queued
        // I/O callbacks (including yamux PING responses and gossipsub
        // heartbeats) run before we process the next entry.
        await new Promise((resolve) => setTimeout(resolve, 0));
        entriesSeen++;
        const doc = entry.value;
        if (!doc || doc._id?.startsWith("__")) {
          skippedInternal++;
          continue;
        }
        if (!doc.announce_address) {
          skippedNoAddr++;
          continue;
        }
        considered++;
        await dialFromNodeDoc(doc, "registry-walk");
      }
      const elapsed = Date.now() - startedAt;
      const suffix = partial
        ? ` (partial — deadline ${REGISTRY_WALK_TIMEOUT_MS}ms reached; ` +
          `event-driven path handles the rest)`
        : "";
      console.log(
        `Registry walk: ${entriesSeen} entries processed | ${considered} considered | ` +
        `${skippedNoAddr} no announce_address | ${skippedInternal} internal ` +
        `(${elapsed}ms)${suffix}`
      );
    } catch (err) {
      console.warn(`Registry walk error: ${err.message}`);
    }
  };
  setTimeout(dialFromRegistryWalk, 10_000);
  // Also re-walk periodically as a safety net — covers cases where an
  // event was missed (e.g. restart mid-sync). The event-driven path is
  // primary; this is just belt-and-braces.
  //
  // The walk now yields to the event loop between entries (setTimeout(0)
  // in the loop body), so it's safe at any frequency — yamux keepalive
  // PINGs and gossipsub heartbeats can fire between entry processing.
  setInterval(dialFromRegistryWalk, 5 * 60_000);

  // Event-driven: react to new or updated registry entries as soon as the
  // OrbitDB CRDT delivers them.
  dbs.nodes.events.on("update", async (entry) => {
    try {
      const op = entry?.payload?.op;
      if (op === "DEL") return; // deletion — nothing to dial
      await dialFromNodeDoc(entry?.payload?.value, "registry-event");
    } catch (err) {
      console.warn(`Registry-event dial error: ${err.message}`);
    }
  });

  console.log("Registry-driven peer dialing enabled (watching wesense.nodes)");

  // Keep this for the same else-if condition the legacy code had:
  if (filteredPeerAddrs.length === 0 && WESENSE_PEER_ADDRS.length === 0) {
    console.log("No bootstrap seed peers configured — relying on mDNS + registry-driven discovery");
  }

  // Express HTTP API — only accessible from Docker network (port 5200),
  // but harden anyway since network_mode: host exposes it on all interfaces.
  const app = express();
  app.use(express.json({ limit: "100kb" }));

  app.use("/nodes", createNodesRouter(dbs.nodes));
  app.use("/trust", createTrustRouter(dbs.trust));
  app.use("/stores", createStoresRouter(dbs.stores));
  app.use("/health", createHealthRouter({ helia, dbs }));

  // Block blacklist status (read-only)
  app.get("/blacklist", (req, res) => {
    const stats = getBlacklistStats();
    res.json(stats);
  });

  // Retry listen — with network_mode: host the previous container may not
  // have fully released the port yet during a restart.
  let httpServer = null;
  const startServer = (retries = 5, delay = 2000) => {
    let server;
    const TLS_ON = process.env.TLS_ENABLED === "true";
    const certFile = process.env.TLS_CERTFILE || "/app/certs/fullchain.pem";
    const keyFile = process.env.TLS_KEYFILE || "/app/certs/privkey.pem";

    if (TLS_ON && existsSync(certFile) && existsSync(keyFile)) {
      server = createHttpsServer({
        cert: readFileSync(certFile),
        key: readFileSync(keyFile),
      }, app).listen(PORT, () => {
        console.log(`HTTPS API listening on port ${PORT}`);
      });
    } else {
      server = app.listen(PORT, () => {
        console.log(`HTTP API listening on port ${PORT}`);
      });
    }
    server.on("error", (err) => {
      if (err.code === "EADDRINUSE" && retries > 0) {
        console.warn(`Port ${PORT} in use, retrying in ${delay / 1000}s (${retries} left)...`);
        setTimeout(() => startServer(retries - 1, delay), delay);
      } else {
        throw err;
      }
    });
    httpServer = server;
  };
  startServer();

  // Graceful shutdown
  let shuttingDown = false;
  const shutdown = async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log("Shutting down...");

    const forceExit = setTimeout(() => {
      console.warn("Graceful shutdown timed out, forcing exit");
      process.exit(1);
    }, 8000);
    forceExit.unref();

    try {
      if (httpServer) httpServer.close();
      await dbs.nodes.close();
      await dbs.trust.close();
      await dbs.stores.close();
      await orbitdb.stop();
      await helia.stop();
    } catch (err) {
      console.warn(`Shutdown error: ${err.message}`);
    }
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
