/**
 * WeSense OrbitDB Service
 *
 * Helia (IPFS) + OrbitDB + Express HTTP API for distributed
 * node registration, trust list sync, and archive attestations.
 *
 * Peer discovery is automatic:
 *   - LAN: mDNS (zero config, requires network_mode: host in Docker)
 *   - WAN: IPFS DHT provider records — each station "provides" its
 *     OrbitDB database CIDs to the global DHT and periodically searches
 *     for other providers, dialing them when found
 */

import { createHelia } from "helia";
import { createLibp2p } from "libp2p";
import { noise } from "@chainsafe/libp2p-noise";
import { yamux } from "@chainsafe/libp2p-yamux";
import { tcp } from "@libp2p/tcp";
import { mdns } from "@libp2p/mdns";
import { gossipsub } from "@chainsafe/libp2p-gossipsub";
import { identify } from "@libp2p/identify";
import { bootstrap } from "@libp2p/bootstrap";
import { kadDHT } from "@libp2p/kad-dht";
import { ping } from "@libp2p/ping";
import { FsBlockstore } from "blockstore-fs";
import { FsDatastore } from "datastore-fs";
import { createOrbitDB } from "@orbitdb/core";
import { CID } from "multiformats/cid";
import { base58btc } from "multiformats/bases/base58";
import express from "express";
import { mkdir } from "node:fs/promises";

import { openDatabases } from "./databases.js";
import { createNodesRouter } from "./routes/nodes.js";
import { createTrustRouter } from "./routes/trust.js";
import { createAttestationsRouter } from "./routes/attestations.js";
import { createHealthRouter } from "./routes/health.js";

const PORT = parseInt(process.env.PORT || "5200", 10);
const LIBP2P_PORT = parseInt(process.env.LIBP2P_PORT || "4002", 10);
const DATA_DIR = process.env.DATA_DIR || "./data";
const ANNOUNCE_ADDRESS = process.env.ANNOUNCE_ADDRESS || "";
const BOOTSTRAP_PEERS = process.env.ORBITDB_BOOTSTRAP_PEERS || "";

// Public IPFS bootstrap nodes (from Helia/kubo defaults).
// These are the entry points into the IPFS DHT.
const IPFS_BOOTSTRAP_NODES = [
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
  "/dnsaddr/va1.bootstrap.libp2p.io/p2p/12D3KooWKnDdG3iXw9eTFijk3EWSunZcFi54Zka4wmtqtt6rPxc8",
  "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
];

// Additional bootstrap peers — other WeSense OrbitDB instances to connect to
// directly. Comma-separated multiaddrs, e.g.:
//   /ip4/203.0.113.1/tcp/4002/p2p/12D3KooW...
const WESENSE_BOOTSTRAP_PEERS = BOOTSTRAP_PEERS
  ? BOOTSTRAP_PEERS.split(",").map((s) => s.trim()).filter(Boolean)
  : [];

const DISCOVERY_INTERVAL = 60_000; // Search for peers every 60 seconds
const PROVIDE_INTERVAL = 30 * 60_000; // Re-announce to DHT every 30 minutes

/**
 * DHT-based peer discovery for OrbitDB replication.
 *
 * OrbitDB replicates via GossipSub, which requires a direct libp2p
 * connection between peers. mDNS handles LAN discovery automatically
 * (when using network_mode: host). For WAN, we use the IPFS DHT as
 * a rendezvous: each station "provides" its OrbitDB database CIDs,
 * and periodically searches for other providers of the same CIDs.
 * When a new provider is found, we dial them — once connected,
 * GossipSub kicks in and OrbitDB replicates.
 */
function startPeerDiscovery(helia, dbs) {
  // Extract CIDs from OrbitDB database addresses (/orbitdb/zdpuAk...)
  const dbCids = [dbs.nodes, dbs.trust, dbs.attestations].map((db) => {
    const cidStr = db.address.toString().split("/").pop();
    return CID.parse(cidStr, base58btc);
  });

  const provide = async () => {
    for (const cid of dbCids) {
      try {
        await helia.routing.provide(cid, { signal: AbortSignal.timeout(30_000) });
      } catch (err) {
        console.warn(`DHT provide failed for ${cid}: ${err.message}`);
      }
    }
    console.log("DHT: Provided database CIDs to IPFS network");
  };

  const discover = async () => {
    const myPeerId = helia.libp2p.peerId;
    const connectedPeers = new Set(helia.libp2p.getPeers().map((p) => p.toString()));

    // Only need to search one database CID — all stations provide all three,
    // so finding a provider for one means we'll connect and replicate all.
    const cid = dbCids[0];
    try {
      for await (const provider of helia.routing.findProviders(cid, {
        signal: AbortSignal.timeout(15_000),
      })) {
        if (provider.id.equals(myPeerId)) continue;
        if (connectedPeers.has(provider.id.toString())) continue;

        const addrs = (provider.multiaddrs || []).map((a) => a.toString());
        console.log(`DHT: Discovered peer ${provider.id} addrs=[${addrs.join(", ")}]`);

        try {
          if (addrs.length > 0) {
            // Store the provider's addresses before dialing
            await helia.libp2p.peerStore.merge(provider.id, {
              multiaddrs: provider.multiaddrs,
            });
          }
          await helia.libp2p.dial(provider.id);
          console.log(`DHT: Connected to peer ${provider.id}`);
        } catch (err) {
          console.warn(`DHT: Failed to dial ${provider.id}: ${err.message}`);
        }
      }
    } catch {
      // Timeout or no providers found — normal, will retry
    }
  };

  // Run initial provide + discover after a short delay (let libp2p settle)
  setTimeout(async () => {
    await provide();
    await discover();
  }, 10_000);

  // Periodic re-provide (DHT records expire after ~24 hours)
  setInterval(provide, PROVIDE_INTERVAL);

  // Periodic discovery
  setInterval(discover, DISCOVERY_INTERVAL);

  console.log("DHT peer discovery started (provide + find every 60s)");
}

async function main() {
  // Ensure data directories exist
  await mkdir(`${DATA_DIR}/blockstore`, { recursive: true });
  await mkdir(`${DATA_DIR}/datastore`, { recursive: true });
  await mkdir(`${DATA_DIR}/orbitdb`, { recursive: true });

  const blockstore = new FsBlockstore(`${DATA_DIR}/blockstore`);
  const datastore = new FsDatastore(`${DATA_DIR}/datastore`);

  // Announce the host's real IP so peers on other Docker hosts can reach us
  const announce = ANNOUNCE_ADDRESS
    ? [`/ip4/${ANNOUNCE_ADDRESS}/tcp/${LIBP2P_PORT}`]
    : [];

  // Use a dedicated datastore for libp2p so identity keys are persisted
  // across restarts (stable PeerID is critical for DHT provider records).
  await mkdir(`${DATA_DIR}/libp2p`, { recursive: true });
  const libp2pDatastore = new FsDatastore(`${DATA_DIR}/libp2p`);

  const libp2p = await createLibp2p({
    datastore: libp2pDatastore,
    addresses: {
      listen: [`/ip4/0.0.0.0/tcp/${LIBP2P_PORT}`],
      announce,
    },
    transports: [tcp()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    peerDiscovery: [
      mdns(),
      bootstrap({ list: [...IPFS_BOOTSTRAP_NODES, ...WESENSE_BOOTSTRAP_PEERS] }),
    ],
    services: {
      identify: identify(),
      ping: ping(),
      pubsub: gossipsub({ allowPublishToZeroTopicPeers: true }),
      aminoDHT: kadDHT({ protocol: "/ipfs/kad/1.0.0" }),
    },
  });

  const helia = await createHelia({ libp2p, blockstore, datastore });
  console.log(`Helia peer ID: ${helia.libp2p.peerId.toString()}`);
  console.log(`Announced addresses: ${helia.libp2p.getMultiaddrs().map((a) => a.toString()).join(", ")}`);
  if (WESENSE_BOOTSTRAP_PEERS.length > 0) {
    console.log(`Bootstrap peers: ${WESENSE_BOOTSTRAP_PEERS.join(", ")}`);
  }

  // Log peer connections and disconnections
  helia.libp2p.addEventListener("peer:connect", (evt) => {
    console.log(`Peer connected: ${evt.detail.toString()}`);
  });
  helia.libp2p.addEventListener("peer:disconnect", (evt) => {
    console.log(`Peer disconnected: ${evt.detail.toString()}`);
  });

  const orbitdb = await createOrbitDB({
    ipfs: helia,
    directory: `${DATA_DIR}/orbitdb`,
  });

  const dbs = await openDatabases(orbitdb);
  console.log(`Databases opened — nodes: ${dbs.nodes.address}, trust: ${dbs.trust.address}`);

  // Database replication event logging
  for (const [name, db] of Object.entries(dbs)) {
    db.events.on("join", (peerId, heads) => {
      console.log(`[${name}] Peer joined DB: ${peerId} (${heads?.length || 0} heads)`);
    });
    db.events.on("update", (entry) => {
      console.log(`[${name}] Replicated update from peer`);
    });
  }

  // Replication trigger — when a new WeSense peer connects, write a sync
  // marker to force HEAD re-publication via gossipsub.  This ensures peers
  // that connect after the initial HEAD publish still receive all updates.
  let lastSyncTrigger = 0;
  const SYNC_DEBOUNCE = 30_000; // Max once per 30 seconds

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

  helia.libp2p.addEventListener("peer:connect", () => {
    const now = Date.now();
    if (now - lastSyncTrigger < SYNC_DEBOUNCE) return;
    lastSyncTrigger = now;
    // Delay to let gossipsub mesh establish for the OrbitDB topics
    setTimeout(() => triggerSync("peer:connect"), 5000);
  });

  // Periodic fallback — re-trigger every 5 minutes if peers are connected
  setInterval(async () => {
    const peerCount = helia.libp2p.getPeers().length;
    if (peerCount === 0) return;
    await triggerSync(`periodic, ${peerCount} peers`);
  }, 5 * 60_000);

  // Start DHT-based peer discovery for cross-station replication
  startPeerDiscovery(helia, dbs);

  // Express HTTP API
  const app = express();
  app.use(express.json());

  app.use("/nodes", createNodesRouter(dbs.nodes));
  app.use("/trust", createTrustRouter(dbs.trust));
  app.use("/attestations", createAttestationsRouter(dbs.attestations));
  app.use("/health", createHealthRouter({ helia, dbs }));

  app.listen(PORT, () => {
    console.log(`HTTP API listening on port ${PORT}`);
  });

  // Graceful shutdown
  const shutdown = async () => {
    console.log("Shutting down...");
    await dbs.nodes.close();
    await dbs.trust.close();
    await dbs.attestations.close();
    await orbitdb.stop();
    await helia.stop();
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
