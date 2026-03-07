/**
 * Trust list CRUD routes.
 *
 * PUT  /trust/:ingester_id  — Add/update trusted key
 * GET  /trust                — Full trust list (TrustStore-compatible format)
 * GET  /trust/:ingester_id   — Get specific entry
 * DELETE /trust/:ingester_id — Revoke key (sets status: "revoked")
 */

import { Router } from "express";

/**
 * @param {object} trustDb - OrbitDB Documents database (indexBy: ingester_id)
 */
const MAX_ID_LENGTH = 256;
const MAX_PUBLIC_KEY_LENGTH = 512;
const MAX_VERSIONS = 20;

export function createTrustRouter(trustDb) {
  const router = Router();

  // Add or update a trusted key
  router.put("/:ingester_id", async (req, res) => {
    try {
      const ingester_id = req.params.ingester_id;
      if (ingester_id.length > MAX_ID_LENGTH) {
        return res.status(400).json({ error: "ID too long" });
      }
      const { public_key, key_version, status } = req.body;

      if (!public_key || key_version === undefined) {
        return res.status(400).json({ error: "public_key and key_version are required" });
      }
      if (typeof public_key !== "string" || public_key.length > MAX_PUBLIC_KEY_LENGTH) {
        return res.status(400).json({ error: "Invalid public_key" });
      }
      if (typeof key_version !== "number" && typeof key_version !== "string") {
        return res.status(400).json({ error: "Invalid key_version" });
      }

      // Fetch existing entry to merge versions
      const existing = await trustDb.get(ingester_id);
      let versions = {};
      if (existing && existing.length > 0 && existing[0].value.versions) {
        versions = { ...existing[0].value.versions };
      }

      if (Object.keys(versions).length >= MAX_VERSIONS && !(String(key_version) in versions)) {
        return res.status(400).json({ error: "Too many key versions" });
      }

      versions[String(key_version)] = {
        public_key,
        status: status || "active",
        added: new Date().toISOString(),
      };

      const doc = {
        _id: ingester_id,
        ingester_id,
        versions,
        updated_at: new Date().toISOString(),
      };
      await trustDb.put(doc);
      res.json({ ok: true, ingester_id });
    } catch (err) {
      console.error("PUT /trust/:ingester_id error:", err);
      res.status(500).json({ error: "Internal error" });
    }
  });

  // Get full trust list in TrustStore-compatible format:
  // {"keys": {"wsi_abc12345": {"1": {"public_key": "base64...", "status": "active", ...}}}}
  router.get("/", async (req, res) => {
    try {
      const all = await trustDb.all();
      const keys = {};
      for (const entry of all) {
        const val = entry.value;
        if (val.ingester_id && val.versions) {
          keys[val.ingester_id] = val.versions;
        }
      }
      res.json({ keys });
    } catch (err) {
      console.error("GET /trust error:", err);
      res.status(500).json({ error: "Internal error" });
    }
  });

  // Get specific trust entry
  router.get("/:ingester_id", async (req, res) => {
    try {
      if (req.params.ingester_id.length > MAX_ID_LENGTH) {
        return res.status(400).json({ error: "ID too long" });
      }
      const doc = await trustDb.get(req.params.ingester_id);
      if (!doc || doc.length === 0) {
        return res.status(404).json({ error: "not found" });
      }
      res.json(doc[0].value);
    } catch (err) {
      console.error("GET /trust/:ingester_id error:", err);
      res.status(500).json({ error: "Internal error" });
    }
  });

  // Revoke: sets status to "revoked" on all versions for this ingester
  router.delete("/:ingester_id", async (req, res) => {
    try {
      const ingester_id = req.params.ingester_id;
      if (ingester_id.length > MAX_ID_LENGTH) {
        return res.status(400).json({ error: "ID too long" });
      }
      const existing = await trustDb.get(ingester_id);
      if (!existing || existing.length === 0) {
        return res.status(404).json({ error: "not found" });
      }

      const val = existing[0].value;
      const versions = val.versions || {};
      for (const ver of Object.keys(versions)) {
        versions[ver].status = "revoked";
        versions[ver].revoked_at = new Date().toISOString();
      }

      const doc = {
        _id: ingester_id,
        ingester_id,
        versions,
        updated_at: new Date().toISOString(),
      };
      await trustDb.put(doc);
      res.json({ ok: true, revoked: ingester_id });
    } catch (err) {
      console.error("DELETE /trust/:ingester_id error:", err);
      res.status(500).json({ error: "Internal error" });
    }
  });

  return router;
}
