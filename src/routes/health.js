/**
 * Health check endpoint.
 *
 * GET /health — Service status, peer count, database sizes, libp2p peer ID.
 *
 * Database sizes are maintained via event listeners (not queried per-request)
 * to avoid blocking the event loop on large databases.
 */

import { Router } from "express";

/**
 * @param {{helia: object, dbs: {nodes: object, trust: object, attestations: object}}} ctx
 */
export function createHealthRouter({ helia, dbs }) {
  const router = Router();

  // Maintain document counts via events instead of calling .all() per request.
  // Initial counts are loaded once at startup; updates/joins increment live.
  const dbCounts = { nodes: 0, trust: 0, attestations: 0 };

  const refreshCounts = async () => {
    for (const [name, db] of Object.entries(dbs)) {
      try {
        const all = await db.all();
        dbCounts[name] = all.filter((e) => !e.value?._id?.startsWith("__")).length;
      } catch {
        // Leave at current count if query fails
      }
    }
  };

  // Load initial counts, then refresh every 5 minutes
  refreshCounts();
  setInterval(refreshCounts, 5 * 60_000);

  router.get("/", (req, res) => {
    try {
      const peers = helia.libp2p.getPeers();

      // Get announced/listen addresses for debugging connectivity
      const addrs = helia.libp2p.getMultiaddrs().map((a) => a.toString());

      // Gossipsub topic diagnostics — shows which peers share OrbitDB topics.
      // Peers subscribed to OrbitDB database topics are other WeSense stations
      // (not IPFS bootstrap relays or transient DHT peers).
      const pubsub = helia.libp2p.services.pubsub;
      const topics = pubsub.getTopics ? pubsub.getTopics() : [];
      const topicPeers = {};
      const wesensePeerSet = new Set();
      for (const topic of topics) {
        const subs = pubsub.getSubscribers ? pubsub.getSubscribers(topic) : [];
        if (subs.length > 0) {
          topicPeers[topic] = subs.map((p) => p.toString());
          for (const p of subs) wesensePeerSet.add(p.toString());
        }
      }
      const wesensePeers = [...wesensePeerSet];

      res.json({
        status: "ok",
        peer_count: peers.length,
        wesense_peer_count: wesensePeers.length,
        wesense_peers: wesensePeers,
        peers: peers.map((p) => p.toString()),
        libp2p_peer_id: helia.libp2p.peerId.toString(),
        addresses: addrs,
        db_sizes: {
          nodes: dbCounts.nodes,
          trust: dbCounts.trust,
          attestations: dbCounts.attestations,
        },
        db_addresses: {
          nodes: dbs.nodes.address.toString(),
          trust: dbs.trust.address.toString(),
          attestations: dbs.attestations.address.toString(),
        },
        gossipsub_topics: topicPeers,
      });
    } catch (err) {
      console.error("GET /health error:", err);
      res.status(500).json({ status: "error", error: err.message });
    }
  });

  // POST /health/dial — Manually connect to a peer by multiaddr (for debugging)
  router.post("/dial", async (req, res) => {
    try {
      const { multiaddr } = req.body;
      if (!multiaddr) {
        return res.status(400).json({ error: "multiaddr required" });
      }
      const { multiaddr: createMa } = await import("@multiformats/multiaddr");
      const addr = createMa(multiaddr);
      console.log(`Manual dial: ${multiaddr}`);
      const conn = await helia.libp2p.dial(addr);
      console.log(`Manual dial connected: ${conn.remotePeer.toString()}`);
      res.json({ status: "connected", remotePeer: conn.remotePeer.toString() });
    } catch (err) {
      console.error("POST /health/dial error:", err);
      res.status(500).json({ status: "error", error: err.message });
    }
  });

  return router;
}
