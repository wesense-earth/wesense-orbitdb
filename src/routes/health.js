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
      const allTopics = pubsub.getTopics ? pubsub.getTopics() : [];
      const topicPeers = {};
      const wesensePeerSet = new Set();
      for (const topic of allTopics) {
        const subs = pubsub.getSubscribers ? pubsub.getSubscribers(topic) : [];
        topicPeers[topic] = subs.map((p) => p.toString());
        for (const p of subs) wesensePeerSet.add(p.toString());
      }
      const wesensePeers = [...wesensePeerSet];

      // Database sync diagnostics — check if OrbitDB's Sync protocol started
      const syncState = {};
      for (const [name, db] of Object.entries(dbs)) {
        syncState[name] = {
          has_sync: !!db.sync,
          sync_peers: db.sync?.peers ? [...db.sync.peers] : [],
          sync_has_start: typeof db.sync?.start === "function",
          sync_has_stop: typeof db.sync?.stop === "function",
        };
      }

      // GossipSub internal peer count — peers that negotiated the gossipsub
      // protocol (not just raw libp2p connections). If this is 0, gossipsub
      // protocol streams aren't being established.
      const gossipsubPeerCount = pubsub.peers?.size ?? pubsub.peers?.length ?? "N/A";
      // Check mesh status per topic
      const meshState = {};
      for (const topic of allTopics) {
        const meshPeers = pubsub.getMeshPeers ? pubsub.getMeshPeers(topic) : [];
        meshState[topic] = { mesh_peers: meshPeers.length, subscribers: (topicPeers[topic] || []).length };
      }
      // Get the gossipsub multicodec/protocol IDs
      const gsProtocols = pubsub.multicodecs ?? pubsub.protocol ?? "unknown";

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
        gossipsub_subscribed_topics: allTopics,
        gossipsub_status: pubsub.status?.code ?? pubsub.isStarted?.() ?? "unknown",
        gossipsub_peer_count: gossipsubPeerCount,
        gossipsub_protocols: gsProtocols,
        gossipsub_mesh: meshState,
        sync_state: syncState,
      });
    } catch (err) {
      console.error("GET /health error:", err);
      res.status(500).json({ status: "error", error: err.message });
    }
  });

  // GET /health/pubsub-test — Diagnostic: test if gossipsub subscribe/getTopics works
  router.get("/pubsub-test", (req, res) => {
    try {
      const pubsub = helia.libp2p.services.pubsub;
      const testTopic = "__wesense_diag_test__";
      const beforeTopics = pubsub.getTopics ? pubsub.getTopics() : [];
      let subscribeError = null;
      try {
        pubsub.subscribe(testTopic);
      } catch (e) {
        subscribeError = e.message;
      }
      const afterTopics = pubsub.getTopics ? pubsub.getTopics() : [];
      // Clean up
      try { pubsub.unsubscribe(testTopic); } catch {}
      const afterCleanup = pubsub.getTopics ? pubsub.getTopics() : [];

      // Also check db addresses (what OrbitDB should subscribe to)
      const expectedTopics = Object.entries(dbs).map(([name, db]) => ({
        name,
        address: db.address.toString(),
      }));

      res.json({
        pubsub_type: pubsub.constructor?.name ?? typeof pubsub,
        pubsub_status: pubsub.status?.code ?? "no status code",
        subscribe_error: subscribeError,
        topics_before_test: beforeTopics,
        topics_after_subscribe: afterTopics,
        topics_after_cleanup: afterCleanup,
        test_topic_appeared: afterTopics.includes(testTopic),
        expected_db_topics: expectedTopics,
        has_getTopics: typeof pubsub.getTopics === "function",
        has_subscribe: typeof pubsub.subscribe === "function",
        has_getSubscribers: typeof pubsub.getSubscribers === "function",
      });
    } catch (err) {
      console.error("GET /health/pubsub-test error:", err);
      res.status(500).json({ status: "error", error: err.message, stack: err.stack });
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
