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
 * See: https://github.com/orbitdb/orbitdb/issues/1244
 */

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
 * that has the block comes online.
 */
const FAIL_COOLDOWN_MS = 15 * 60 * 1000; // 15 minutes before retrying a failed CID
const MAX_FAIL_CACHE_SIZE = 10000;
const failedCids = new Map(); // CID string → { failedAt, attempts }

function cidToString(cid) {
  return typeof cid === "string" ? cid : cid.toString();
}

/**
 * Wrap a Helia instance so its blockstore.get() returns a plain
 * Uint8Array (via Promise) instead of an AsyncGenerator.
 *
 * Also caches failed CID lookups to prevent repeated fetches of
 * unreachable blocks (orphaned oplog entries, corrupted identity blocks).
 *
 * All other properties and methods are forwarded unchanged.
 *
 * @param {import("helia").Helia} helia
 * @returns {import("helia").Helia}
 */
export function wrapHeliaForOrbitDB(helia) {
  const origGet = helia.blockstore.get.bind(helia.blockstore);
  const origHas = helia.blockstore.has.bind(helia.blockstore);

  const blockstoreProxy = new Proxy(helia.blockstore, {
    get(target, prop) {
      if (prop === "get") {
        return async (cid, options) => {
          const key = cidToString(cid);
          const cached = failedCids.get(key);
          if (cached) {
            const elapsed = Date.now() - cached.failedAt;
            if (elapsed < FAIL_COOLDOWN_MS) {
              throw new Error(`Block ${key.slice(0, 20)}... unreachable (attempt ${cached.attempts}, retry in ${Math.ceil((FAIL_COOLDOWN_MS - elapsed) / 60000)}m)`);
            }
            // Cooldown expired — allow retry
            failedCids.delete(key);
          }

          try {
            return await collectBytes(origGet(cid, options));
          } catch (err) {
            // Record failure
            const prev = failedCids.get(key);
            const attempts = (prev?.attempts || 0) + 1;
            failedCids.set(key, { failedAt: Date.now(), attempts });

            // Evict oldest entries if cache is full
            if (failedCids.size > MAX_FAIL_CACHE_SIZE) {
              const oldest = failedCids.keys().next().value;
              failedCids.delete(oldest);
            }

            if (attempts === 1) {
              console.warn(`Block ${key.slice(0, 20)}... fetch failed, will not retry for ${FAIL_COOLDOWN_MS / 60000}m: ${err.message}`);
            }
            throw err;
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
