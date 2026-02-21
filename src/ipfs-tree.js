/**
 * IPFS directory tree manager for WeSense archives.
 *
 * Maintains a UnixFS directory tree on IPFS:
 *   /{country}/{year}/{month}/{day}/
 *     readings.parquet
 *     trust_snapshot.json
 *     manifest.json
 *
 * The tree root CID changes on every modification (immutable content addressing).
 * IPNS is used to publish a stable pointer to the latest root.
 *
 * Uses @helia/unixfs for file/directory operations.
 */

import { unixfs } from "@helia/unixfs";
import { CID } from "multiformats/cid";
import { readFile, readdir, stat } from "node:fs/promises";
import { join } from "node:path";

/**
 * @param {import("helia").Helia} helia
 */
export function createIPFSTree(helia) {
  const fs = unixfs(helia);

  // In-memory root CID — null until first archive is added or loaded from disk.
  let rootCid = null;

  /**
   * Get or create an empty directory CID.
   * @returns {Promise<CID>}
   */
  async function getOrCreateRoot() {
    if (rootCid) return rootCid;
    // Create an empty UnixFS directory
    rootCid = await fs.addDirectory();
    return rootCid;
  }

  /**
   * Add a file to the tree at a given path, creating intermediate directories.
   *
   * @param {CID} dirCid - Current directory CID
   * @param {string[]} pathParts - Remaining path segments (last is filename)
   * @param {Uint8Array} content - File content
   * @returns {Promise<CID>} New root CID after insertion
   */
  async function addFileToTree(dirCid, pathParts, content) {
    if (pathParts.length === 1) {
      // Leaf: add file, then link into directory
      const fileCid = await fs.addBytes(content);
      try {
        dirCid = await fs.rm(dirCid, pathParts[0]);
      } catch {
        // Didn't exist — fine
      }
      return fs.cp(fileCid, dirCid, pathParts[0]);
    }

    // Intermediate directory: descend or create
    const [head, ...rest] = pathParts;
    let childCid;
    try {
      // Try to stat the existing child
      const childStat = await fs.stat(dirCid, { path: head });
      childCid = childStat.cid;
    } catch {
      // Child doesn't exist — create empty directory
      childCid = await fs.addDirectory();
    }

    // Recurse into child
    const newChildCid = await addFileToTree(childCid, rest, content);

    // Replace child in parent
    try {
      // Remove old entry if it exists
      dirCid = await fs.rm(dirCid, head);
    } catch {
      // Didn't exist — fine
    }
    return fs.cp(newChildCid, dirCid, head);
  }

  /**
   * Ingest archive files from a staging directory into the IPFS tree.
   *
   * Expects staging dir structure:
   *   {stagingDir}/{country}/{year}/{month}/{day}/
   *     readings.parquet
   *     trust_snapshot.json
   *     manifest.json
   *
   * @param {string} stagingDir - Path to staging directory
   * @returns {Promise<{rootCid: CID, archives: Array<{path: string, cid: string}>}>}
   */
  async function ingestFromStaging(stagingDir) {
    let root = await getOrCreateRoot();
    const archives = [];

    // Walk the staging directory recursively
    const files = await walkDir(stagingDir);

    for (const filePath of files) {
      // Get relative path from staging dir
      const relPath = filePath.slice(stagingDir.length).replace(/^\//, "");
      const parts = relPath.split("/");

      const content = await readFile(filePath);
      root = await addFileToTree(root, parts, content);
      archives.push({ path: relPath, cid: root.toString() });
    }

    rootCid = root;
    return { rootCid, archives };
  }

  /**
   * List contents at a path in the tree.
   *
   * @param {string} [path=""] - Path to list (empty = root)
   * @returns {Promise<Array<{name: string, type: string, cid: string, size: number}>>}
   */
  async function listTree(path) {
    if (!rootCid) return [];

    let targetCid = rootCid;
    if (path) {
      try {
        const s = await fs.stat(rootCid, { path });
        targetCid = s.cid;
      } catch {
        return [];
      }
    }

    const entries = [];
    try {
      for await (const entry of fs.ls(targetCid)) {
        entries.push({
          name: entry.name,
          type: entry.type === "directory" ? "directory" : "file",
          cid: entry.cid.toString(),
          size: entry.size || 0,
        });
      }
    } catch {
      // Not a directory or empty
    }
    return entries;
  }

  /**
   * Remove an entry from the tree at the given path.
   *
   * @param {string} path - Path to remove (e.g., "nz/2026/02/13")
   * @returns {Promise<CID>} New root CID after removal
   */
  async function removePath(path) {
    if (!rootCid) throw new Error("Tree is empty");

    const parts = path.split("/").filter(Boolean);
    if (parts.length === 0) throw new Error("Cannot remove root");

    rootCid = await removeFromTree(rootCid, parts);
    return rootCid;
  }

  /**
   * Recursively remove a path from the tree.
   */
  async function removeFromTree(dirCid, pathParts) {
    if (pathParts.length === 1) {
      return fs.rm(dirCid, pathParts[0]);
    }

    const [head, ...rest] = pathParts;
    let childStat;
    try {
      childStat = await fs.stat(dirCid, { path: head });
    } catch {
      throw new Error(`Path not found: ${head}`);
    }

    const newChildCid = await removeFromTree(childStat.cid, rest);
    dirCid = await fs.rm(dirCid, head);
    return fs.cp(newChildCid, dirCid, head);
  }

  /**
   * Get the current root CID.
   * @returns {CID|null}
   */
  function getRootCid() {
    return rootCid;
  }

  /**
   * Set the root CID (e.g., when loading from persisted state).
   * @param {CID|string} cid
   */
  function setRootCid(cid) {
    rootCid = typeof cid === "string" ? CID.parse(cid) : cid;
  }

  return {
    getOrCreateRoot,
    ingestFromStaging,
    listTree,
    removePath,
    getRootCid,
    setRootCid,
  };
}

/**
 * Recursively walk a directory and return all file paths.
 * @param {string} dir
 * @returns {Promise<string[]>}
 */
async function walkDir(dir) {
  const files = [];
  const entries = await readdir(dir, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await walkDir(fullPath)));
    } else if (entry.isFile()) {
      files.push(fullPath);
    }
  }
  return files;
}
