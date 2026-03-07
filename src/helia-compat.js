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
 * Wrap a Helia instance so its blockstore.get() returns a plain
 * Uint8Array (via Promise) instead of an AsyncGenerator.
 *
 * All other properties and methods are forwarded unchanged.
 *
 * @param {import("helia").Helia} helia
 * @returns {import("helia").Helia}
 */
export function wrapHeliaForOrbitDB(helia) {
  const origGet = helia.blockstore.get.bind(helia.blockstore);

  const blockstoreProxy = new Proxy(helia.blockstore, {
    get(target, prop) {
      if (prop === "get") {
        return async (cid, options) => collectBytes(origGet(cid, options));
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
