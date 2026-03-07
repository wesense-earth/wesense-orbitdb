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
 * @param {{helia: object, dbs: {nodes: object, trust: object, attestations: object, stores: object}}} ctx
 */
export function createHealthRouter({ helia, dbs }) {
  const router = Router();

  // Maintain document counts via events instead of calling .all() per request.
  const dbCounts = { nodes: 0, trust: 0, attestations: 0, stores: 0 };

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
          stores: dbCounts.stores,
        },
        db_addresses: {
          nodes: dbs.nodes.address.toString(),
          trust: dbs.trust.address.toString(),
          attestations: dbs.attestations.address.toString(),
          stores: dbs.stores.address.toString(),
        },
        gossipsub_topics: topicPeers,
      });
    } catch (err) {
      console.error("GET /health error:", err);
      res.status(500).json({ status: "error", error: err.message });
    }
  });

  // Diagnostic endpoints — only accessible from localhost/Docker network.
  // Block access from external IPs to prevent information disclosure.
  const isLocalRequest = (req) => {
    const ip = req.ip || req.connection?.remoteAddress || "";
    return ip === "127.0.0.1" || ip === "::1" || ip === "::ffff:127.0.0.1" ||
           ip.startsWith("172.") || ip.startsWith("10.") || ip.startsWith("192.168.");
  };

  router.get("/debug", async (req, res) => {
    if (!isLocalRequest(req)) {
      return res.status(403).json({ error: "Forbidden" });
    }
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

      // Try to manually open a gossipsub stream to the first peer.
      // Tests both single-protocol (early negotiation) and multi-protocol (MSS).
      let streamTest = null;
      const meshsub = "/meshsub/1.2.0";
      const allMeshsub = ["/meshsub/1.2.0", "/meshsub/1.1.0", "/meshsub/1.0.0"];
      for (const peerId of connectedPeers) {
        const peerIdStr = peerId.toString();
        const protocols = peerProtocols[peerIdStr] || [];
        if (!protocols.includes(meshsub)) continue;
        const connections = helia.libp2p.getConnections(peerId);
        if (connections.length === 0) continue;
        const conn = connections[0];
        streamTest = {
          peer: peerIdStr,
          connection_status: conn.status,
          connection_direction: conn.direction,
          connection_multiplexer: conn.multiplexer,
          connection_encryption: conn.encryption,
        };
        // Test 1: Single protocol (early negotiation, no MSS)
        try {
          const stream = await conn.newStream(meshsub, {
            signal: AbortSignal.timeout(5000),
          });
          streamTest.single_protocol_ok = true;
          streamTest.single_protocol = stream.protocol;
          await stream.close();
        } catch (err) {
          streamTest.single_protocol_ok = false;
          streamTest.single_protocol_error = err.message;
          streamTest.single_protocol_error_name = err.name;
        }
        // Test 2: Multi protocol (MSS negotiation)
        try {
          const stream = await conn.newStream(allMeshsub, {
            signal: AbortSignal.timeout(5000),
          });
          streamTest.multi_protocol_ok = true;
          streamTest.multi_protocol = stream.protocol;
          await stream.close();
        } catch (err) {
          streamTest.multi_protocol_ok = false;
          streamTest.multi_protocol_error = err.message;
          streamTest.multi_protocol_error_name = err.name;
        }
        break;
      }

      res.json({
        connected_peers: connectedPeers.map((p) => p.toString()),
        gossipsub_started: pubsub.isStarted ? pubsub.isStarted() : "unknown",
        gossipsub_multicodecs: pubsub.multicodecs || [],
        gossipsub_peers: gossipPeers,
        gossipsub_outbound_streams: outboundStreams,
        gossipsub_inbound_streams: inboundStreams,
        peer_protocols: peerProtocols,
        registered_protocols: registeredProtocols,
        subscriptions: pubsub.getTopics ? pubsub.getTopics() : [],
        stream_test: streamTest,
      });
    } catch (err) {
      console.error("GET /health/debug error:", err);
      res.status(500).json({ error: "Internal error" });
    }
  });

  // Diagnostic: directly attempt to create gossipsub outbound streams and
  // return step-by-step results. This bypasses gossipsub's internal queue.
  router.get("/fix-gossipsub", async (req, res) => {
    if (!isLocalRequest(req)) {
      return res.status(403).json({ error: "Forbidden" });
    }
    try {
      const pubsub = helia.libp2p.services.pubsub;
      const connectedPeers = helia.libp2p.getPeers();
      const results = [];

      for (const peerId of connectedPeers) {
        const id = peerId.toString();
        const result = { peer: id };

        // Check guards
        result.isStarted = pubsub.isStarted?.() ?? "unknown";
        result.inPeersMap = pubsub.peers?.has(id) ?? false;
        result.hasOutbound = pubsub.streamsOutbound?.has(id) ?? false;
        result.hasInbound = pubsub.streamsInbound?.has(id) ?? false;

        if (result.hasOutbound) {
          result.action = "already_has_stream";
          results.push(result);
          continue;
        }

        // Ensure peer is in gossipsub's peers map
        if (!result.inPeersMap && pubsub.addPeer) {
          const conns = helia.libp2p.getConnections(peerId);
          if (conns.length > 0) {
            pubsub.addPeer(peerId, conns[0].direction, conns[0].remoteAddr);
            result.addedToPeers = true;
            result.inPeersMap = pubsub.peers?.has(id) ?? false;
          }
        }

        // Try calling createOutboundStream directly
        const conns = helia.libp2p.getConnections(peerId);
        if (conns.length === 0) {
          result.action = "no_connections";
          results.push(result);
          continue;
        }

        result.connectionStatus = conns[0].status;
        result.connectionDirection = conns[0].direction;

        // Intercept gossipsub's internal error logging to capture the error
        let capturedErrors = [];
        const origLogError = pubsub.log?.error;
        if (origLogError) {
          pubsub.log.error = (...args) => {
            capturedErrors.push(args.map((a) => {
              if (a instanceof Error) return { message: a.message, name: a.name, code: a.code, stack: a.stack?.split("\n").slice(0, 3).join("\n") };
              return String(a);
            }));
            return origLogError.apply(pubsub.log, args);
          };
        }

        try {
          await pubsub.createOutboundStream(peerId, conns[0]);
          result.afterCreate_hasOutbound =
            pubsub.streamsOutbound?.has(id) ?? false;
          result.action = result.afterCreate_hasOutbound
            ? "stream_created"
            : "create_returned_but_no_stream";
        } catch (err) {
          result.action = "create_threw";
          result.error = err.message;
          result.errorName = err.name;
          result.errorCode = err.code;
        }

        // Restore original log.error
        if (origLogError) {
          pubsub.log.error = origLogError;
        }
        if (capturedErrors.length > 0) {
          result.gossipsub_errors = capturedErrors;
        }

        results.push(result);
      }

      res.json({
        gossipsub_started: pubsub.isStarted?.() ?? "unknown",
        gossipsub_multicodecs: pubsub.multicodecs || [],
        outbound_before: pubsub.streamsOutbound
          ? [...pubsub.streamsOutbound.keys()]
          : [],
        inbound_before: pubsub.streamsInbound
          ? [...pubsub.streamsInbound.keys()]
          : [],
        peer_results: results,
        outbound_after: pubsub.streamsOutbound
          ? [...pubsub.streamsOutbound.keys()]
          : [],
        inbound_after: pubsub.streamsInbound
          ? [...pubsub.streamsInbound.keys()]
          : [],
      });
    } catch (err) {
      res.status(500).json({ error: "Internal error" });
    }
  });

  return router;
}
