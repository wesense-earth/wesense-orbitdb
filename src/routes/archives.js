/**
 * Archive management routes for IPFS storage.
 *
 * POST   /archives           — Ingest archives from staging directory, add to IPFS tree, publish IPNS
 * GET    /archives/tree       — List root of archive directory tree
 * GET    /archives/tree/*path — List contents at a specific path
 * GET    /archives/resolve    — Resolve IPNS to current root CID
 * DELETE /archives/*path      — Remove an archive path from the tree
 */

import { Router } from "express";

const STAGING_DIR = process.env.ARCHIVE_STAGING_DIR || "/app/data/staging";

/**
 * @param {{ipfsTree: object, helia: object, ipnsPublish: function, ipnsResolve: function}} ctx
 */
export function createArchivesRouter({ ipfsTree, helia, ipnsPublish, ipnsResolve }) {
  const router = Router();

  // POST /archives — Ingest from staging directory, add to IPFS tree, publish IPNS
  router.post("/", async (req, res) => {
    try {
      const stagingDir = req.body.staging_dir || STAGING_DIR;

      const result = await ipfsTree.ingestFromStaging(stagingDir);

      // Publish updated root CID to IPNS
      let ipnsName = null;
      if (ipnsPublish) {
        try {
          ipnsName = await ipnsPublish(result.rootCid);
        } catch (err) {
          console.warn(`IPNS publish failed: ${err.message}`);
        }
      }

      res.json({
        ok: true,
        root_cid: result.rootCid.toString(),
        archives_added: result.archives.length,
        archives: result.archives.map((a) => ({
          path: a.path,
          tree_cid: a.cid,
        })),
        ipns_name: ipnsName,
      });
    } catch (err) {
      console.error("POST /archives error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  // GET /archives/tree — List root of archive tree
  router.get("/tree", async (req, res) => {
    try {
      const entries = await ipfsTree.listTree("");
      const rootCid = ipfsTree.getRootCid();
      res.json({
        root_cid: rootCid ? rootCid.toString() : null,
        entries,
      });
    } catch (err) {
      console.error("GET /archives/tree error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  // GET /archives/tree/:path(*) — List contents at a specific path
  router.get("/tree/*", async (req, res) => {
    try {
      // Express captures the wildcard as req.params[0]
      const path = req.params[0];
      const entries = await ipfsTree.listTree(path);
      const rootCid = ipfsTree.getRootCid();
      res.json({
        root_cid: rootCid ? rootCid.toString() : null,
        path,
        entries,
      });
    } catch (err) {
      console.error("GET /archives/tree/* error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  // GET /archives/resolve — Resolve IPNS to current root CID
  router.get("/resolve", async (req, res) => {
    try {
      const rootCid = ipfsTree.getRootCid();
      let ipnsResult = null;

      if (ipnsResolve) {
        try {
          ipnsResult = await ipnsResolve();
        } catch (err) {
          console.warn(`IPNS resolve failed: ${err.message}`);
        }
      }

      res.json({
        root_cid: rootCid ? rootCid.toString() : null,
        ipns: ipnsResult,
      });
    } catch (err) {
      console.error("GET /archives/resolve error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  // DELETE /archives/:path(*) — Remove an archive path from the tree
  router.delete("/*", async (req, res) => {
    try {
      const path = req.params[0];
      if (!path) {
        return res.status(400).json({ error: "Path is required" });
      }

      const newRootCid = await ipfsTree.removePath(path);

      // Re-publish IPNS with updated root
      let ipnsName = null;
      if (ipnsPublish) {
        try {
          ipnsName = await ipnsPublish(newRootCid);
        } catch (err) {
          console.warn(`IPNS publish failed: ${err.message}`);
        }
      }

      res.json({
        ok: true,
        removed_path: path,
        root_cid: newRootCid.toString(),
        ipns_name: ipnsName,
      });
    } catch (err) {
      console.error("DELETE /archives/* error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  return router;
}
