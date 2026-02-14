/**
 * WeSense OrbitDB Service
 *
 * Helia (IPFS) + OrbitDB + Express HTTP API for distributed
 * node registration, trust list sync, and archive attestations.
 *
 * Peer discovery is automatic:
 *   - LAN: mDNS (zero config)
 *   - WAN: Kademlia DHT via public IPFS bootstrap nodes
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

// Public IPFS bootstrap nodes (from Helia/kubo defaults).
// These are the entry points into the IPFS DHT — through them,
// OrbitDB instances discover each other automatically across the internet.
const IPFS_BOOTSTRAP_NODES = [
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
  "/dnsaddr/va1.bootstrap.libp2p.io/p2p/12D3KooWKnDdG3iXw9eTFijk3EWSunZcFi54Zka4wmtqtt6rPxc8",
  "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
];

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

  const libp2p = await createLibp2p({
    addresses: {
      listen: [`/ip4/0.0.0.0/tcp/${LIBP2P_PORT}`],
      announce,
    },
    transports: [tcp()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    peerDiscovery: [
      mdns(),
      bootstrap({ list: IPFS_BOOTSTRAP_NODES }),
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
