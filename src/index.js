/**
 * WeSense OrbitDB Service
 *
 * Helia (libp2p) + OrbitDB + Express HTTP API for distributed
 * node registration, trust list sync, and archive attestations.
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

import { openDatabases } from "./databases.js";
import { wrapHeliaForOrbitDB } from "./helia-compat.js";
import { createNodesRouter } from "./routes/nodes.js";
import { createTrustRouter } from "./routes/trust.js";
import { createAttestationsRouter } from "./routes/attestations.js";
import { createHealthRouter } from "./routes/health.js";
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

async function main() {
  // Ensure data directories exist
  await mkdir(`${DATA_DIR}/blockstore`, { recursive: true });
  await mkdir(`${DATA_DIR}/datastore`, { recursive: true });
  await mkdir(`${DATA_DIR}/orbitdb`, { recursive: true });

  const blockstore = new FsBlockstore(`${DATA_DIR}/blockstore`);
  const datastore = new FsDatastore(`${DATA_DIR}/datastore`);

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
    streamMuxers: [yamux()],
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
    },
    peerDiscovery: [
      // mDNS for automatic LAN discovery. Non-standard port to avoid conflict
      // with avahi-daemon. All WeSense stations use the same port.
      mdns({ port: 5354 }),
    ],
    services: {
      identify: identify(),
      ping: ping(),
      pubsub: gossipsub({ allowPublishToZeroTopicPeers: true }),
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

  const dbs = await openDatabases(orbitdb);
  console.log(`Databases opened — nodes: ${dbs.nodes.address}, trust: ${dbs.trust.address}`);

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

  // Database replication event logging
  for (const [name, db] of Object.entries(dbs)) {
    db.events.on("join", (peerId, heads) => {
      console.log(`[${name}] Peer joined DB: ${peerId} (${heads?.length || 0} heads)`);
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
  if (WESENSE_PEER_ADDRS.length > 0) {
    const { multiaddr: createMa } = await import("@multiformats/multiaddr");

    const dialConfiguredPeers = async () => {
      for (const addr of WESENSE_PEER_ADDRS) {
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
    console.log(`Direct peer dialing enabled for: ${WESENSE_PEER_ADDRS.join(", ")}`);
  }

  // Express HTTP API
  const app = express();
  app.use(express.json());

  app.use("/nodes", createNodesRouter(dbs.nodes));
  app.use("/trust", createTrustRouter(dbs.trust));
  app.use("/attestations", createAttestationsRouter(dbs.attestations));
  app.use("/health", createHealthRouter({ helia, dbs }));

  // Retry listen — with network_mode: host the previous container may not
  // have fully released the port yet during a restart.
  let httpServer = null;
  const startServer = (retries = 5, delay = 2000) => {
    const server = app.listen(PORT, () => {
      console.log(`HTTP API listening on port ${PORT}`);
    });
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
      await dbs.attestations.close();
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
