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
export function createNodesRouter(nodesDb) {
  const router = Router();

  // Register or update a node
  router.put("/:id", async (req, res) => {
    try {
      const ingester_id = req.params.id;
      const doc = {
        _id: ingester_id,
        ingester_id,
        ...req.body,
        updated_at: new Date().toISOString(),
      };
      await nodesDb.put(doc);
      res.json({ ok: true, ingester_id });
    } catch (err) {
      console.error("PUT /nodes/:id error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  // List all nodes
  router.get("/", async (req, res) => {
    try {
      const all = await nodesDb.all();
      let nodes = all.map((entry) => entry.value);

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
      res.status(500).json({ error: err.message });
    }
  });

  // Get specific node
  router.get("/:id", async (req, res) => {
    try {
      const doc = await nodesDb.get(req.params.id);
      if (!doc || doc.length === 0) {
        return res.status(404).json({ error: "not found" });
      }
      res.json(doc[0].value);
    } catch (err) {
      console.error("GET /nodes/:id error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  // Remove node
  router.delete("/:id", async (req, res) => {
    try {
      await nodesDb.del(req.params.id);
      res.json({ ok: true, deleted: req.params.id });
    } catch (err) {
      console.error("DELETE /nodes/:id error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  return router;
}
