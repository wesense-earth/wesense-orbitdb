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
 *   - WAN: Direct dial via ORBITDB_BOOTSTRAP_PEERS env var
 */

// Must be imported before any helia/libp2p code to patch stream prototypes.
import { patchRegistrarForLegacyHandlers } from "./libp2p-stream-compat.js";

import { createHelia } from "helia";
import { createLibp2p } from "libp2p";
import { noise } from "@chainsafe/libp2p-noise";
import { yamux } from "@chainsafe/libp2p-yamux";
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
];

function isKnownSyncError(err) {
  const msg = err?.message || String(err);
  return KNOWN_SYNC_ERRORS.some((pattern) => msg.includes(pattern));
}

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
    transports: [tcp()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux({
      maxInboundStreams: 256,
      maxOutboundStreams: 256,
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

  // Log peer connections and disconnections
  helia.libp2p.addEventListener("peer:connect", (evt) => {
    console.log(`Peer connected: ${evt.detail.toString()}`);
  });
  helia.libp2p.addEventListener("peer:disconnect", (evt) => {
    console.log(`Peer disconnected: ${evt.detail.toString()}`);
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
            msg.includes("CBOR decode error")) {
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

  // Workaround for libp2p@3 parallel startup race: if peers connected before
  // gossipsub registered its topology, re-identify them so the peer:identify
  // event fires again. If that still doesn't create streams, bypass the
  // internal queue and call createOutboundStream directly.
  const ensureGossipsubStreams = async () => {
    const connectedPeers = helia.libp2p.getPeers();
    const outboundStreams = pubsub.streamsOutbound?.size ?? 0;
    if (connectedPeers.length === 0 || outboundStreams >= connectedPeers.length) {
      return; // all peers already have gossipsub streams
    }
    console.log(`Gossipsub streams: ${outboundStreams}/${connectedPeers.length} — attempting to fix`);

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
    const { execSync } = await import("node:child_process");
    let diskWritesBlocked = false;

    const checkDiskSpace = () => {
      try {
        // Use df to get usage percentage for the filesystem containing DATA_DIR.
        // -P forces POSIX output (single line per filesystem, consistent columns).
        const output = execSync(`df -P "${DATA_DIR}"`, { encoding: "utf-8", timeout: 5000 });
        const lines = output.trim().split("\n");
        if (lines.length < 2) return;
        // POSIX df columns: Filesystem 1024-blocks Used Available Capacity Mounted-on
        const fields = lines[1].split(/\s+/);
        const capacityStr = fields[4]; // e.g. "92%"
        if (!capacityStr) return;
        const usagePercent = parseInt(capacityStr.replace("%", ""), 10);
        if (isNaN(usagePercent)) return;

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

  // Node registry cleanup — remove entries not updated within NODE_TTL_DAYS.
  const cleanupStaleNodes = async () => {
    try {
      const cutoff = Date.now() - NODE_TTL_DAYS * 24 * 60 * 60 * 1000;
      const allEntries = await dbs.nodes.all();
      let removed = 0;
      for (const entry of allEntries) {
        const doc = entry.value;
        if (!doc || doc._id?.startsWith("__")) continue;
        const updatedAt = doc.updated_at ? new Date(doc.updated_at).getTime() : 0;
        if (updatedAt < cutoff) {
          await dbs.nodes.del(doc._id);
          removed++;
        }
      }
      if (removed > 0) {
        console.log(`Node cleanup: removed ${removed} stale entries (TTL: ${NODE_TTL_DAYS}d)`);
      }
    } catch (err) {
      console.warn(`Node cleanup error: ${err.message}`);
    }
  };
  setTimeout(cleanupStaleNodes, 30_000);
  setInterval(cleanupStaleNodes, 60 * 60_000);

  // Direct peer dialing — for configured WeSense station addresses.
  // Handles WAN discovery where mDNS can't reach (different networks, VPS).
  // Filter out self-addresses: if ANNOUNCE_ADDRESS matches a configured peer's
  // host, skip it — the node would be dialing itself. This is common when all
  // nodes share the same ORBITDB_BOOTSTRAP_PEERS list that includes the bootstrap.
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
    // Also try resolving dns4 addresses to check against our IP
    return true;
  });

  if (filteredPeerAddrs.length > 0) {
    const { multiaddr: createMa } = await import("@multiformats/multiaddr");

    // Resolve any dns4 addresses and filter out self after resolution
    // Cache resolved self-addresses so we only log once per address
    const resolvedSelfCache = new Set();
    const resolvedSelfCheck = async (addr) => {
      const host = addr.match(/\/dns4\/([^/]+)\//)?.[1];
      if (host) {
        try {
          const { lookup } = await import("node:dns/promises");
          const { address: resolvedIp } = await lookup(host);
          if (ownAddresses.has(resolvedIp)) {
            if (!resolvedSelfCache.has(addr)) {
              resolvedSelfCache.add(addr);
              console.log(`Direct dial: Skipping self-address ${addr} (resolved to ${resolvedIp})`);
            }
            return true;
          }
        } catch {}
      }
      return false;
    };

    const dialConfiguredPeers = async () => {
      for (const addr of filteredPeerAddrs) {
        if (await resolvedSelfCheck(addr)) continue;

        const targetHost = addr.match(/\/(?:ip4|dns4)\/([^/]+)\//)?.[1];
        if (targetHost) {
          const alreadyConnected = helia.libp2p
            .getConnections()
            .some((c) => c.remoteAddr.toString().includes(targetHost));
          if (alreadyConnected) continue;
        }

        try {
          const ma = createMa(addr);
          await helia.libp2p.dial(ma);
          console.log(`Direct dial: Connected to ${addr}`);
        } catch (err) {
          if (!err.message?.includes("dial self")) {
            console.warn(`Direct dial: Failed ${addr}: ${err.message}`);
          }
        }
      }
    };

    setTimeout(dialConfiguredPeers, 5_000);
    setInterval(dialConfiguredPeers, 60_000);
    console.log(`Direct peer dialing enabled for: ${filteredPeerAddrs.join(", ")}`);
  } else if (WESENSE_PEER_ADDRS.length > 0) {
    console.log("All configured bootstrap peers are self-addresses — no outbound dialing");
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
