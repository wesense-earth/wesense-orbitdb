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
 * the CID is moved to the long-term blacklist.
 */
const FAIL_COOLDOWN_MS = 15 * 60 * 1000; // 15 minutes before retrying a failed CID
const MAX_ATTEMPTS = 3; // move to long-term blacklist after this many failed cooldown cycles
const MAX_FAIL_CACHE_SIZE = 10000;
const failedCids = new Map(); // CID string -> { failedAt, attempts }

/**
 * Long-term blacklist — CIDs that reached MAX_ATTEMPTS failures are held
 * here for BLACKLIST_TTL_MS before becoming retryable again.
 *
 * Design rationale: "never retry" is wrong for a distributed system where
 * peers can legitimately be offline for days or weeks (hardware swap,
 * vacation, ISP outage, storage migration). A block we could not fetch
 * today might be fetchable next week when its holder comes back. At the
 * same time, we don't want to retry continuously — that's what the cache
 * is preventing.
 *
 * The TTL strikes a balance: blocks stay blacklisted long enough to avoid
 * repeated futile fetches, but short enough that recovery is possible.
 * 30 days matches the oplog TTL in the WeSense OrbitDB fork — entries
 * older than that are filtered at read-time anyway, so a blacklist entry
 * older than 30 days can't protect anything useful.
 *
 * On TTL expiry: the entry is removed from the blacklist, meaning the
 * next access attempt goes back through the normal fetch path. If the
 * block is still unreachable, it goes through the cooldown/blacklist
 * cycle again from scratch. If it's now fetchable, it is fetched normally
 * and the CID is effectively "unblacklisted".
 *
 * Also supports opt-out (set BLOCK_BLACKLIST_TTL_DAYS=0) for operators
 * who want the previous "never retry" behaviour.
 */
const BLACKLIST_TTL_DAYS = parseFloat(process.env.BLOCK_BLACKLIST_TTL_DAYS || "30");
const BLACKLIST_TTL_MS = BLACKLIST_TTL_DAYS > 0
  ? BLACKLIST_TTL_DAYS * 24 * 60 * 60 * 1000
  : Infinity; // 0 means "never expire"
const BLACKLIST_SWEEP_INTERVAL_MS = 60 * 60 * 1000; // hourly sweep for expired entries

let permanentBlacklist = new Map(); // CID string -> { blacklistedAt, attempts, manual? }
let blacklistPath = null; // set by wrapHeliaForOrbitDB
let blacklistDirty = false;

/**
 * Returns true if a blacklist entry is expired and should be removed on
 * next sweep. Entries with non-numeric or missing blacklistedAt default
 * to "old" (expired immediately) so malformed records don't stick.
 */
function isBlacklistEntryExpired(entry, now = Date.now()) {
  if (!Number.isFinite(BLACKLIST_TTL_MS)) return false; // TTL disabled
  if (entry?.manual) return false; // manually blacklisted entries never expire
  const blacklistedAt = entry?.blacklistedAt
    ? new Date(entry.blacklistedAt).getTime()
    : 0;
  if (!Number.isFinite(blacklistedAt) || blacklistedAt === 0) return true;
  return now - blacklistedAt > BLACKLIST_TTL_MS;
}

/**
 * Load the blacklist from disk. Filters out entries whose TTL has expired
 * since they were persisted — expired entries get a fresh retry chance on
 * the next access attempt, rather than being stuck in the blacklist forever.
 *
 * Called once at wrapper creation time.
 */
async function loadBlacklist(filePath) {
  blacklistPath = filePath;
  try {
    if (existsSync(filePath)) {
      const raw = await readFile(filePath, "utf-8");
      const data = JSON.parse(raw);
      if (data && typeof data.entries === "object") {
        const now = Date.now();
        const loaded = new Map(Object.entries(data.entries));
        let expiredOnLoad = 0;
        for (const [cid, entry] of loaded) {
          if (isBlacklistEntryExpired(entry, now)) {
            loaded.delete(cid);
            expiredOnLoad++;
          }
        }
        permanentBlacklist = loaded;
        if (expiredOnLoad > 0) {
          blacklistDirty = true;
          await saveBlacklist();
        }
        console.log(
          `Block blacklist loaded: ${permanentBlacklist.size} blacklisted CIDs` +
          (expiredOnLoad > 0 ? ` (${expiredOnLoad} expired on load and removed)` : "") +
          (Number.isFinite(BLACKLIST_TTL_MS) ? ` (TTL ${BLACKLIST_TTL_DAYS}d)` : " (no TTL — never expire)")
        );
      }
    }
  } catch (err) {
    console.warn(`Failed to load block blacklist from ${filePath}: ${err.message}`);
  }
}

/**
 * Remove expired blacklist entries. Expired CIDs become retryable on next
 * access. Run periodically (hourly) to keep the in-memory set bounded to
 * "still meaningful" entries.
 *
 * Started by wrapHeliaForOrbitDB; no need to invoke from outside.
 */
async function sweepExpiredBlacklist() {
  if (!Number.isFinite(BLACKLIST_TTL_MS)) return; // TTL disabled
  const now = Date.now();
  let removed = 0;
  for (const [cid, entry] of permanentBlacklist) {
    if (isBlacklistEntryExpired(entry, now)) {
      permanentBlacklist.delete(cid);
      removed++;
    }
  }
  if (removed > 0) {
    blacklistDirty = true;
    await saveBlacklist();
    console.log(
      `Block blacklist sweep: removed ${removed} expired entries ` +
      `(${permanentBlacklist.size} remain; TTL ${BLACKLIST_TTL_DAYS}d)`
    );
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

  // Periodically sweep expired blacklist entries. Makes previously-failed
  // blocks retryable once BLACKLIST_TTL_MS has elapsed — important for a
  // network where peers can legitimately be offline for extended periods
  // and come back holding blocks we gave up on.
  setInterval(sweepExpiredBlacklist, BLACKLIST_SWEEP_INTERVAL_MS);

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
              // Move to long-term blacklist. Entry will be eligible for
              // retry after BLACKLIST_TTL_MS — not truly "permanent", just
              // held long enough to avoid futile retries while letting
              // peers that were offline recover legitimately.
              permanentBlacklist.set(key, {
                blacklistedAt: new Date().toISOString(),
                attempts,
              });
              failedCids.delete(key);
              blacklistDirty = true;
              const ttlNote = Number.isFinite(BLACKLIST_TTL_MS)
                ? `TTL ${BLACKLIST_TTL_DAYS}d`
                : "no TTL";
              console.warn(`Block ${key.slice(0, 20)}... blacklisted after ${attempts} failed attempts (${ttlNote})`);
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

          // Write the block — catch disk-full errors and set the flag reactively
          try {
            await origPut(cid, bytes, options);
          } catch (writeErr) {
            const msg = (writeErr?.message || "").toLowerCase();
            if (msg.includes("no space") || msg.includes("enospc") || msg.includes("disk full") || msg.includes("quota")) {
              console.error("Blockstore write failed (disk full) — blocking all further writes");
              diskFull = true;
            }
            throw writeErr;
          }

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
