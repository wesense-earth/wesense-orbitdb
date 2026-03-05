/**
 * Store scope registry CRUD routes + replication aggregation.
 *
 * PUT    /stores/:id        — Register/update store scope for a node
 * GET    /stores            — List all stores (optional ?country= or ?region= filter)
 * GET    /stores/replication — Replication factor per region
 * GET    /stores/:id        — Get specific store
 * DELETE /stores/:id        — Remove store
 */

import { Router } from "express";

/**
 * @param {object} storesDb - OrbitDB Documents database
 */
export function createStoresRouter(storesDb) {
  const router = Router();

  // Register or update store scope for a node
  router.put("/:id", async (req, res) => {
    try {
      const store_id = req.params.id;
      const doc = {
        _id: store_id,
        store_id,
        ...req.body,
        updated_at: new Date().toISOString(),
      };
      await storesDb.put(doc);
      res.json({ ok: true, store_id });
    } catch (err) {
      console.error("PUT /stores/:id error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  // Replication factor per region (must be before /:id to avoid route conflict)
  router.get("/replication", async (req, res) => {
    try {
      const all = await storesDb.all();
      const stores = all
        .map((entry) => entry.value)
        .filter((s) => !s._id.startsWith("__"));

      // Build a map of region → set of node IDs
      const regionMap = new Map();
      for (const store of stores) {
        const scopes = store.store_scope || [];
        const nodeId = store.iroh_node_id || store.store_id || store._id;
        for (const scope of scopes) {
          if (!regionMap.has(scope)) {
            regionMap.set(scope, new Set());
          }
          regionMap.get(scope).add(nodeId);
        }
      }

      // Convert to response format
      const regions = [];
      for (const [scope, nodeSet] of regionMap) {
        const parts = scope.split("/");
        regions.push({
          country: parts[0] || "",
          subdivision: parts[1] || "*",
          scope_pattern: scope,
          node_count: nodeSet.size,
          nodes: [...nodeSet],
        });
      }

      // Sort by node_count ascending (under-replicated first)
      regions.sort((a, b) => a.node_count - b.node_count);

      res.json({ regions });
    } catch (err) {
      console.error("GET /stores/replication error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  // List all stores
  router.get("/", async (req, res) => {
    try {
      const all = await storesDb.all();
      let stores = all
        .map((entry) => entry.value)
        .filter((s) => !s._id.startsWith("__"));

      // Optional country filter
      if (req.query.country) {
        const country = req.query.country.toLowerCase();
        stores = stores.filter(
          (s) =>
            s.store_scope &&
            s.store_scope.some((scope) =>
              scope.toLowerCase().startsWith(country + "/")
            )
        );
      }

      // Optional region filter (country/subdivision)
      if (req.query.region) {
        const region = req.query.region.toLowerCase();
        stores = stores.filter(
          (s) =>
            s.store_scope &&
            s.store_scope.some((scope) => {
              const scopeLower = scope.toLowerCase();
              // Exact match or wildcard match
              return (
                scopeLower === region ||
                (scopeLower.endsWith("/*") &&
                  region.startsWith(scopeLower.slice(0, -1)))
              );
            })
        );
      }

      res.json({ stores });
    } catch (err) {
      console.error("GET /stores error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  // Get specific store
  router.get("/:id", async (req, res) => {
    try {
      const doc = await storesDb.get(req.params.id);
      if (!doc || doc.length === 0) {
        return res.status(404).json({ error: "not found" });
      }
      res.json(doc[0].value);
    } catch (err) {
      console.error("GET /stores/:id error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  // Remove store
  router.delete("/:id", async (req, res) => {
    try {
      await storesDb.del(req.params.id);
      res.json({ ok: true, deleted: req.params.id });
    } catch (err) {
      console.error("DELETE /stores/:id error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  return router;
}
