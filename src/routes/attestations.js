/**
 * Archive attestation routes (scaffolded for Phase 6 IPFS archiver).
 *
 * PUT  /attestations/:manifest_hash — Submit attestation
 * GET  /attestations                 — List all (optional ?period= filter)
 * GET  /attestations/:manifest_hash  — Get attestations for specific archive
 * GET  /attestations/:manifest_hash/consensus — Consensus status
 */

import { Router } from "express";

/**
 * @param {object} attestationsDb - OrbitDB Documents database (indexBy: manifest_hash)
 */
export function createAttestationsRouter(attestationsDb) {
  const router = Router();

  // Submit an attestation for an archive
  router.put("/:manifest_hash", async (req, res) => {
    try {
      const manifest_hash = req.params.manifest_hash;
      const { ingester_id, signature, ...rest } = req.body;

      if (!ingester_id) {
        return res.status(400).json({ error: "ingester_id is required" });
      }

      // Fetch existing to append attestation
      const existing = await attestationsDb.get(manifest_hash);
      let attestations = [];
      if (existing && existing.length > 0 && existing[0].value.attestations) {
        attestations = [...existing[0].value.attestations];
      }

      // Avoid duplicate attestation from same ingester
      if (!attestations.some((a) => a.ingester_id === ingester_id)) {
        attestations.push({
          ingester_id,
          signature: signature || "",
          attested_at: new Date().toISOString(),
          ...rest,
        });
      }

      const doc = {
        _id: manifest_hash,
        manifest_hash,
        attestations,
        updated_at: new Date().toISOString(),
      };
      await attestationsDb.put(doc);
      res.json({ ok: true, manifest_hash, attestation_count: attestations.length });
    } catch (err) {
      console.error("PUT /attestations/:manifest_hash error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  // List all attestations
  router.get("/", async (req, res) => {
    try {
      const all = await attestationsDb.all();
      let items = all.map((entry) => entry.value);

      // Optional period filter (e.g., ?period=2025-01)
      if (req.query.period) {
        const period = req.query.period;
        items = items.filter(
          (a) => a.manifest_hash && a.manifest_hash.includes(period)
        );
      }

      res.json({ attestations: items });
    } catch (err) {
      console.error("GET /attestations error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  // Get attestations for specific archive
  router.get("/:manifest_hash", async (req, res) => {
    try {
      const doc = await attestationsDb.get(req.params.manifest_hash);
      if (!doc || doc.length === 0) {
        return res.status(404).json({ error: "not found" });
      }
      res.json(doc[0].value);
    } catch (err) {
      console.error("GET /attestations/:manifest_hash error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  // Consensus status for an archive
  router.get("/:manifest_hash/consensus", async (req, res) => {
    try {
      const doc = await attestationsDb.get(req.params.manifest_hash);
      if (!doc || doc.length === 0) {
        return res.status(404).json({ error: "not found" });
      }

      const val = doc[0].value;
      const attestations = val.attestations || [];
      const count = attestations.length;

      // Simple threshold-based consensus (configurable in Phase 6)
      const threshold = parseInt(process.env.CONSENSUS_THRESHOLD || "2", 10);
      const reached = count >= threshold;

      res.json({
        manifest_hash: req.params.manifest_hash,
        attestation_count: count,
        threshold,
        consensus_reached: reached,
        attesters: attestations.map((a) => a.ingester_id),
      });
    } catch (err) {
      console.error("GET /attestations/:manifest_hash/consensus error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  return router;
}
