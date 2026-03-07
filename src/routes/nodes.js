/**
 * Node registry CRUD routes.
 *
 * PUT  /nodes/:id     — Register/update node
 * GET  /nodes         — List all nodes (optional ?country= filter)
 * GET  /nodes/:id     — Get specific node
 * DELETE /nodes/:id   — Remove node
 */

import { Router } from "express";

/**
 * @param {object} nodesDb - OrbitDB Documents database (indexBy: ingester_id)
 */
// Allowed fields for node registration — reject anything else
const NODE_ALLOWED_FIELDS = new Set([
  "ingester_id", "node_name", "regions", "version", "uptime",
  "zenoh_endpoint", "iroh_node_id", "iroh_endpoint", "announce_address",
  "store_scope", "capabilities", "source",
]);
const MAX_ID_LENGTH = 256;
const MAX_FIELD_VALUE_LENGTH = 4096;

function sanitizeBody(body, allowedFields) {
  if (!body || typeof body !== "object" || Array.isArray(body)) return {};
  const clean = {};
  for (const [key, value] of Object.entries(body)) {
    if (!allowedFields.has(key)) continue;
    if (typeof value === "string" && value.length > MAX_FIELD_VALUE_LENGTH) continue;
    clean[key] = value;
  }
  return clean;
}

export function createNodesRouter(nodesDb) {
  const router = Router();

  // Register or update a node
  router.put("/:id", async (req, res) => {
    try {
      const ingester_id = req.params.id;
      if (ingester_id.length > MAX_ID_LENGTH) {
        return res.status(400).json({ error: "ID too long" });
      }
      const sanitized = sanitizeBody(req.body, NODE_ALLOWED_FIELDS);
      const doc = {
        _id: ingester_id,
        ingester_id,
        ...sanitized,
        updated_at: new Date().toISOString(),
      };
      await nodesDb.put(doc);
      res.json({ ok: true, ingester_id });
    } catch (err) {
      console.error("PUT /nodes/:id error:", err);
      res.status(500).json({ error: "Internal error" });
    }
  });

  // List all nodes
  router.get("/", async (req, res) => {
    try {
      const all = await nodesDb.all();
      let nodes = all.map((entry) => entry.value)
        .filter((n) => !n._id.startsWith("__"));

      // Optional country filter
      if (req.query.country) {
        const country = req.query.country.toLowerCase();
        nodes = nodes.filter(
          (n) =>
            n.regions &&
            n.regions.some &&
            n.regions.some((r) => r.toLowerCase() === country)
        );
      }

      res.json({ nodes });
    } catch (err) {
      console.error("GET /nodes error:", err);
      res.status(500).json({ error: "Internal error" });
    }
  });

  // Get specific node
  router.get("/:id", async (req, res) => {
    try {
      if (req.params.id.length > MAX_ID_LENGTH) {
        return res.status(400).json({ error: "ID too long" });
      }
      const doc = await nodesDb.get(req.params.id);
      if (!doc || doc.length === 0) {
        return res.status(404).json({ error: "not found" });
      }
      res.json(doc[0].value);
    } catch (err) {
      console.error("GET /nodes/:id error:", err);
      res.status(500).json({ error: "Internal error" });
    }
  });

  // Remove node
  router.delete("/:id", async (req, res) => {
    try {
      if (req.params.id.length > MAX_ID_LENGTH) {
        return res.status(400).json({ error: "ID too long" });
      }
      await nodesDb.del(req.params.id);
      res.json({ ok: true, deleted: req.params.id });
    } catch (err) {
      console.error("DELETE /nodes/:id error:", err);
      res.status(500).json({ error: "Internal error" });
    }
  });

  return router;
}
