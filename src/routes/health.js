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

  refreshCounts();
  setInterval(refreshCounts, 5 * 60_000);

  router.get("/", (req, res) => {
    try {
      const peers = helia.libp2p.getPeers();
      const addrs = helia.libp2p.getMultiaddrs().map((a) => a.toString());

      // GossipSub topic subscribers — peers sharing OrbitDB database topics.
      // On this WeSense-only network, all connected peers should eventually
      // appear as topic subscribers once gossipsub establishes.
      const pubsub = helia.libp2p.services.pubsub;
      const allTopics = pubsub.getTopics ? pubsub.getTopics() : [];
      const topicPeers = {};
      const wesensePeerSet = new Set();
      for (const topic of allTopics) {
        const subs = pubsub.getSubscribers ? pubsub.getSubscribers(topic) : [];
        topicPeers[topic] = subs.map((p) => p.toString());
        for (const p of subs) wesensePeerSet.add(p.toString());
      }

      res.json({
        status: "ok",
        peer_count: peers.length,
        wesense_peer_count: wesensePeerSet.size,
        wesense_peers: [...wesensePeerSet],
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

  // Temporary diagnostic endpoint to debug gossipsub stream establishment.
  router.get("/debug", async (req, res) => {
    try {
      const pubsub = helia.libp2p.services.pubsub;
      const connectedPeers = helia.libp2p.getPeers();

      // Gossipsub internal state
      const gossipPeers = pubsub.peers ? [...pubsub.peers.keys()] : [];
      const outboundStreams = pubsub.streamsOutbound
        ? [...pubsub.streamsOutbound.keys()]
        : [];
      const inboundStreams = pubsub.streamsInbound
        ? [...pubsub.streamsInbound.keys()]
        : [];

      // What protocols each connected peer supports (from identify/peerstore)
      const peerProtocols = {};
      for (const peerId of connectedPeers) {
        try {
          const peer = await helia.libp2p.peerStore.get(peerId);
          peerProtocols[peerId.toString()] = peer.protocols || [];
        } catch {
          peerProtocols[peerId.toString()] = ["(not in peerstore)"];
        }
      }

      // Registered protocols on this node
      const registeredProtocols = helia.libp2p.getProtocols();

      res.json({
        connected_peers: connectedPeers.map((p) => p.toString()),
        gossipsub_peers: gossipPeers,
        gossipsub_outbound_streams: outboundStreams,
        gossipsub_inbound_streams: inboundStreams,
        peer_protocols: peerProtocols,
        registered_protocols: registeredProtocols,
        subscriptions: pubsub.getTopics ? pubsub.getTopics() : [],
      });
    } catch (err) {
      console.error("GET /health/debug error:", err);
      res.status(500).json({ error: err.message, stack: err.stack });
    }
  });

  return router;
}
