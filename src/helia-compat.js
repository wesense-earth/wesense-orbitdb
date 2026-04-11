/**
 * Helia v6 compatibility wrapper for OrbitDB.
 *
 * Helia v6 changed blockstore.get() from returning Promise<Uint8Array>
 * to returning AsyncGenerator<Uint8Array> (streaming blockstores, per
 * js-stores PR #358). OrbitDB @3.0.2 expects the old non-streaming API
 * and breaks silently when it receives an async generator instead of bytes.
 *
 * This wrapper adapts helia's streaming blockstore back to the
 * non-streaming interface that OrbitDB expects. It is applied ONLY to
 * the helia instance passed to createOrbitDB — helia's own internals
 * (bitswap, pins, GC) continue using the native streaming API.
 *
 * Additionally, this wrapper:
 *   - Tracks failed CID fetches with a cooldown + permanent blacklist
 *     to suppress repeated attempts on orphaned oplog entries.
 *   - Verifies blocks after put() by reading them back and checking
 *     the content hash matches the CID, catching partial writes from
 *     disk-full or I/O errors before they create orphaned references.
 *   - Provides a diskFull flag that external code can set to block
 *     all writes when disk space is critically low.
 *
 * See: https://github.com/orbitdb/orbitdb/issues/1244
 */

import { writeFile, readFile, rename } from "node:fs/promises";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";

/**
 * Collect an async iterable of Uint8Array chunks into a single Uint8Array.
 * Enforces a size limit to prevent OOM from malicious peers sending huge blocks.
 */
const MAX_BLOCK_SIZE = 16 * 1024 * 1024; // 16MB — OrbitDB entries are small; this is generous

async function collectBytes(source) {
  const chunks = [];
  let totalLength = 0;
  for await (const chunk of source) {
    totalLength += chunk.byteLength;
    if (totalLength > MAX_BLOCK_SIZE) {
      throw new Error(`Block exceeds maximum size of ${MAX_BLOCK_SIZE} bytes`);
    }
    chunks.push(chunk);
  }
  if (chunks.length === 0) return new Uint8Array(0);
  if (chunks.length === 1) return chunks[0];
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return result;
}

/**
 * Failed-CID cache: tracks CIDs whose block fetch failed.
 * Prevents OrbitDB from retrying the same unreachable block on every
 * sync cycle, which spams logs and wastes resources.
 *
 * After FAIL_COOLDOWN_MS, the CID is eligible for retry in case a peer
 * that has the block comes online. After MAX_ATTEMPTS cooldown cycles
 * (i.e. MAX_ATTEMPTS * FAIL_COOLDOWN_MS of trying), the CID is
 * permanently blacklisted and persisted to disk.
 */
const FAIL_COOLDOWN_MS = 15 * 60 * 1000; // 15 minutes before retrying a failed CID
const MAX_ATTEMPTS = 3; // permanently blacklist after this many failed cooldown cycles
const MAX_FAIL_CACHE_SIZE = 10000;
const failedCids = new Map(); // CID string -> { failedAt, attempts }

/** Permanent blacklist — CIDs that failed MAX_ATTEMPTS times are never retried. */
let permanentBlacklist = new Map(); // CID string -> { blacklistedAt, attempts }
let blacklistPath = null; // set by wrapHeliaForOrbitDB
let blacklistDirty = false;

/**
 * Load the permanent blacklist from disk.
 * Called once at wrapper creation time.
 */
async function loadBlacklist(filePath) {
  blacklistPath = filePath;
  try {
    if (existsSync(filePath)) {
      const raw = await readFile(filePath, "utf-8");
      const data = JSON.parse(raw);
      if (data && typeof data.entries === "object") {
        permanentBlacklist = new Map(Object.entries(data.entries));
        console.log(`Block blacklist loaded: ${permanentBlacklist.size} permanently blacklisted CIDs`);
      }
    }
  } catch (err) {
    console.warn(`Failed to load block blacklist from ${filePath}: ${err.message}`);
  }
}

/**
 * Persist the permanent blacklist to disk atomically (write tmp, then rename).
 */
async function saveBlacklist() {
  if (!blacklistPath || !blacklistDirty) return;
  const entries = Object.fromEntries(permanentBlacklist);
  const data = JSON.stringify({ count: permanentBlacklist.size, entries }, null, 2);
  const tmpPath = blacklistPath + ".tmp";
  try {
    await writeFile(tmpPath, data, "utf-8");
    await rename(tmpPath, blacklistPath);
    blacklistDirty = false;
  } catch (err) {
    console.warn(`Failed to save block blacklist: ${err.message}`);
  }
}

function cidToString(cid) {
  return typeof cid === "string" ? cid : cid.toString();
}

/** Flag set by disk space monitor in index.js to block writes. */
let diskFull = false;

/**
 * Set the disk-full flag. When true, blockstore put() calls will throw.
 * @param {boolean} full
 */
export function setDiskFull(full) {
  diskFull = full;
}

/**
 * Get blacklist stats for the /status HTTP endpoint.
 * @returns {{ blacklisted: number, pendingFailures: number }}
 */
export function getBlacklistStats() {
  return {
    blacklisted: permanentBlacklist.size,
    pendingFailures: failedCids.size,
  };
}

/**
 * Manually blacklist a CID immediately (e.g. from an admin API).
 * @param {string} cidStr The CID string to blacklist
 */
export function manuallyBlacklist(cidStr) {
  if (permanentBlacklist.has(cidStr)) return false;
  permanentBlacklist.set(cidStr, {
    blacklistedAt: new Date().toISOString(),
    attempts: 0,
    manual: true,
  });
  failedCids.delete(cidStr);
  blacklistDirty = true;
  saveBlacklist();
  console.warn(`Block ${cidStr.slice(0, 20)}... manually blacklisted`);
  return true;
}

/**
 * Wrap a Helia instance so its blockstore.get() returns a plain
 * Uint8Array (via Promise) instead of an AsyncGenerator.
 *
 * Also caches failed CID lookups to prevent repeated fetches of
 * unreachable blocks (orphaned oplog entries, corrupted identity blocks),
 * with a permanent blacklist for blocks that fail repeatedly.
 *
 * Wraps put() with write-ahead verification — after writing a block,
 * reads it back and verifies the content hash matches the CID.
 *
 * All other properties and methods are forwarded unchanged.
 *
 * @param {import("helia").Helia} helia
 * @param {object} [options]
 * @param {string} [options.blacklistPath] - Path for the persistent blacklist JSON file.
 *   Defaults to DATA_DIR/orbitdb/block-blacklist.json (derived from env var or ./data).
 * @returns {import("helia").Helia}
 */
export function wrapHeliaForOrbitDB(helia, options = {}) {
  const resolvedBlacklistPath = options.blacklistPath
    || process.env.BLOCK_BLACKLIST_PATH
    || join(process.env.DATA_DIR || "./data", "orbitdb", "block-blacklist.json");

  // Load blacklist synchronously at wrap time (returns a promise but we
  // don't block — the blacklist will be available shortly after startup).
  loadBlacklist(resolvedBlacklistPath);

  const origGet = helia.blockstore.get.bind(helia.blockstore);
  const origPut = helia.blockstore.put.bind(helia.blockstore);
  const origHas = helia.blockstore.has.bind(helia.blockstore);

  const blockstoreProxy = new Proxy(helia.blockstore, {
    get(target, prop) {
      if (prop === "get") {
        return async (cid, options) => {
          const key = cidToString(cid);

          // Check permanent blacklist first — no logging, no retry
          if (permanentBlacklist.has(key)) {
            throw new Error(`Block ${key.slice(0, 20)}... permanently blacklisted`);
          }

          const cached = failedCids.get(key);
          if (cached) {
            const elapsed = Date.now() - cached.failedAt;
            if (elapsed < FAIL_COOLDOWN_MS) {
              throw new Error(`Block ${key.slice(0, 20)}... unreachable (attempt ${cached.attempts}, retry in ${Math.ceil((FAIL_COOLDOWN_MS - elapsed) / 60000)}m)`);
            }
            // Cooldown expired — allow retry but keep the attempt count
          }

          try {
            const result = await collectBytes(origGet(cid, options));
            // Success — clear from failed cache if present
            if (failedCids.has(key)) {
              failedCids.delete(key);
            }
            return result;
          } catch (err) {
            // Record failure, preserving cumulative attempt count
            const prev = failedCids.get(key);
            const attempts = (prev?.attempts || 0) + 1;

            if (attempts >= MAX_ATTEMPTS) {
              // Permanently blacklist this CID
              permanentBlacklist.set(key, {
                blacklistedAt: new Date().toISOString(),
                attempts,
              });
              failedCids.delete(key);
              blacklistDirty = true;
              console.warn(`Block ${key.slice(0, 20)}... permanently blacklisted after ${attempts} failed attempts`);
              // Persist asynchronously — don't block the error path
              saveBlacklist();
            } else {
              failedCids.set(key, { failedAt: Date.now(), attempts });

              // Evict oldest entries if cache is full
              if (failedCids.size > MAX_FAIL_CACHE_SIZE) {
                const oldest = failedCids.keys().next().value;
                failedCids.delete(oldest);
              }

              if (attempts === 1) {
                console.warn(`Block ${key.slice(0, 20)}... fetch failed (attempt ${attempts}/${MAX_ATTEMPTS}), will not retry for ${FAIL_COOLDOWN_MS / 60000}m: ${err.message}`);
              }
            }
            throw err;
          }
        };
      }

      if (prop === "put") {
        return async (cid, bytes, options) => {
          // Block writes when disk is critically full
          if (diskFull) {
            throw new Error("Blockstore write rejected: disk space critically low");
          }

          // Write the block
          await origPut(cid, bytes, options);

          // Write-ahead verification: read back and verify hash matches CID
          try {
            const readBack = await collectBytes(origGet(cid));
            const key = cidToString(cid);

            // Verify length matches
            if (readBack.length !== bytes.length) {
              throw new Error(
                `Write verification failed for ${key.slice(0, 20)}...: ` +
                `wrote ${bytes.length} bytes but read back ${readBack.length}`
              );
            }

            // Verify content matches by comparing the hash against the CID's multihash.
            // Import sha256 lazily to avoid top-level await.
            const { sha256 } = await import("multiformats/hashes/sha2");
            const hash = await sha256.digest(readBack);
            const cidMultihashDigest = cid.multihash?.digest;
            if (cidMultihashDigest && !uint8ArrayEquals(hash.digest, cidMultihashDigest)) {
              throw new Error(
                `Write verification failed for ${key.slice(0, 20)}...: ` +
                `content hash does not match CID multihash after write`
              );
            }
          } catch (verifyErr) {
            // Verification failed — try to remove the corrupt block
            try {
              if (typeof target.delete === "function") {
                await target.delete(cid);
              }
            } catch {
              // Best effort cleanup
            }
            throw verifyErr;
          }
        };
      }

      const val = target[prop];
      return typeof val === "function" ? val.bind(target) : val;
    },
  });

  return new Proxy(helia, {
    get(target, prop) {
      if (prop === "blockstore") return blockstoreProxy;
      const val = target[prop];
      return typeof val === "function" ? val.bind(target) : val;
    },
  });
}

function uint8ArrayEquals(a, b) {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}
