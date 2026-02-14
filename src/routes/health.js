/**
 * Health check endpoint.
 *
 * GET /health — Service status, peer count, database sizes, libp2p peer ID.
 */

import { Router } from "express";

/**
 * @param {{helia: object, dbs: {nodes: object, trust: object, attestations: object}}} ctx
 */
export function createHealthRouter({ helia, dbs }) {
  const router = Router();

  router.get("/", async (req, res) => {
    try {
      const peers = helia.libp2p.getPeers();

      // Count documents in each database
      const nodesAll = await dbs.nodes.all();
      const trustAll = await dbs.trust.all();
      const attestAll = await dbs.attestations.all();

      // Get announced/listen addresses for debugging connectivity
      const addrs = helia.libp2p.getMultiaddrs().map((a) => a.toString());

      res.json({
        status: "ok",
        peer_count: peers.length,
        peers: peers.map((p) => p.toString()),
        libp2p_peer_id: helia.libp2p.peerId.toString(),
        addresses: addrs,
        db_sizes: {
          nodes: nodesAll.length,
          trust: trustAll.length,
          attestations: attestAll.length,
        },
        db_addresses: {
          nodes: dbs.nodes.address.toString(),
          trust: dbs.trust.address.toString(),
          attestations: dbs.attestations.address.toString(),
        },
      });
    } catch (err) {
      console.error("GET /health error:", err);
      res.status(500).json({ status: "error", error: err.message });
    }
  });

  return router;
}
