/**
 * WeSense OrbitDB Service
 *
 * Helia (IPFS) + OrbitDB + Express HTTP API for distributed
 * node registration, trust list sync, and archive attestations.
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
import { FsBlockstore } from "blockstore-fs";
import { FsDatastore } from "datastore-fs";
import { createOrbitDB } from "@orbitdb/core";
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
const BOOTSTRAP_PEERS = process.env.ORBITDB_BOOTSTRAP_PEERS
  ? process.env.ORBITDB_BOOTSTRAP_PEERS.split(",").map((s) => s.trim()).filter(Boolean)
  : [];

async function main() {
  // Ensure data directories exist
  await mkdir(`${DATA_DIR}/blockstore`, { recursive: true });
  await mkdir(`${DATA_DIR}/datastore`, { recursive: true });
  await mkdir(`${DATA_DIR}/orbitdb`, { recursive: true });

  const blockstore = new FsBlockstore(`${DATA_DIR}/blockstore`);
  const datastore = new FsDatastore(`${DATA_DIR}/datastore`);

  // libp2p peer discovery: mDNS for LAN, optional bootstrap for WAN
  const peerDiscovery = [mdns()];
  if (BOOTSTRAP_PEERS.length > 0) {
    peerDiscovery.push(bootstrap({ list: BOOTSTRAP_PEERS }));
  }

  const libp2p = await createLibp2p({
    addresses: { listen: [`/ip4/0.0.0.0/tcp/${LIBP2P_PORT}`] },
    transports: [tcp()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    peerDiscovery,
    services: {
      identify: identify(),
      pubsub: gossipsub({ allowPublishToZeroTopicPeers: true }),
    },
  });

  const helia = await createHelia({ libp2p, blockstore, datastore });
  console.log(`Helia peer ID: ${helia.libp2p.peerId.toString()}`);

  const orbitdb = await createOrbitDB({
    ipfs: helia,
    directory: `${DATA_DIR}/orbitdb`,
  });

  const dbs = await openDatabases(orbitdb);
  console.log(`Databases opened — nodes: ${dbs.nodes.address}, trust: ${dbs.trust.address}`);

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
