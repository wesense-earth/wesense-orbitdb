/**
 * libp2p@3 compatibility shim for gossipsub@14 and OrbitDB@3.
 *
 * Two breaking changes in libp2p@3 (shipped with helia@6) affect our stack:
 *
 * 1. STREAM DUPLEX: Streams changed from duplex ({sink, source}) to event-based
 *    ({send(), [Symbol.asyncIterator]}). gossipsub@14 and OrbitDB@3 use it-pipe
 *    which requires .sink/.source, causing "fns.shift(...) is not a function".
 *    Fix: patch AbstractMessageStream.prototype with .sink/.source getters.
 *
 * 2. HANDLER SIGNATURE: Protocol stream handlers changed from
 *    handler({stream, connection}) to handler(stream, connection).
 *    @chainsafe/libp2p-gossipsub@14 still uses the old single-object form
 *    (see node_modules/@chainsafe/libp2p-gossipsub/dist/src/index.js:435
 *    `onIncomingStream({ stream, connection }) {...}`) so its /meshsub/ and
 *    /floodsub/ handlers need wrapping.
 *
 *    OrbitDB (our wesense-main fork) uses the NEW two-arg form — see
 *    orbitdb-fork/src/sync.js:173 `handleReceiveHeads = async (stream,
 *    connection) => { const peerId = String(connection.remotePeer) ... }`.
 *    Wrapping its `/orbitdb/*` handlers with `handler({stream, connection})`
 *    packs the two args into a single object, which the fork then receives as
 *    its first positional arg, leaving `connection` undefined and throwing
 *    `TypeError: Cannot read properties of undefined (reading 'remotePeer')`.
 *    Historic note: `/orbitdb/` used to be in the wrapped list based on the
 *    assumption that OrbitDB still used the old signature; removed 2026-04-17
 *    after the teardown-trace investigation surfaced 15–18 TypeErrors per
 *    30 min in production logs.
 *
 *    Fix: wrap gossipsub protocols; leave OrbitDB alone.
 *
 * Import this module BEFORE creating any Helia/libp2p instances.
 * Call patchRegistrarForLegacyHandlers(libp2p) after createLibp2p({start:false})
 * but before createHelia().
 */

import { AbstractMessageStream } from "@libp2p/utils";

// --- Fix 1: Stream duplex compatibility ---

if (
  !Object.getOwnPropertyDescriptor(AbstractMessageStream.prototype, "sink")
) {
  Object.defineProperty(AbstractMessageStream.prototype, "sink", {
    get() {
      const stream = this;
      return async (source) => {
        for await (const chunk of source) {
          stream.send(chunk);
        }
      };
    },
    configurable: true,
    enumerable: true,
  });
  console.log("libp2p-stream-compat: added .sink to AbstractMessageStream");
}

if (
  !Object.getOwnPropertyDescriptor(AbstractMessageStream.prototype, "source")
) {
  Object.defineProperty(AbstractMessageStream.prototype, "source", {
    get() {
      return this; // Already an AsyncIterable via [Symbol.asyncIterator]
    },
    configurable: true,
    enumerable: true,
  });
  console.log("libp2p-stream-compat: added .source to AbstractMessageStream");
}

// --- Fix 2: Handler signature compatibility ---

/**
 * Patch the libp2p registrar so that protocol handlers registered by
 * gossipsub@14 and OrbitDB@3 (which expect the @libp2p/interface@2.x
 * signature: handler({stream, connection})) work with libp2p@3 which
 * calls handler(stream, connection) as two separate arguments.
 *
 * Must be called after createLibp2p({start: false}) but before
 * createHelia() (which starts libp2p and triggers handler registration).
 *
 * @param {import("libp2p").Libp2p} libp2p
 */
export function patchRegistrarForLegacyHandlers(libp2p) {
  const registrar = libp2p.components.registrar;
  const origHandle = registrar.handle.bind(registrar);

  registrar.handle = async (protocol, handler, opts) => {
    // gossipsub@14 still uses the old IncomingStreamData signature:
    // handler({stream, connection}). Wrap its protocols to accept the
    // libp2p@3 signature: handler(stream, connection). Do NOT wrap
    // /orbitdb/* — the fork already uses the new two-arg signature.
    if (
      protocol.startsWith("/meshsub/") ||
      protocol.startsWith("/floodsub/")
    ) {
      const wrappedHandler = (stream, connection) => {
        return handler({ stream, connection });
      };
      return origHandle(protocol, wrappedHandler, opts);
    }
    return origHandle(protocol, handler, opts);
  };

  console.log(
    "libp2p-stream-compat: patched registrar for legacy handler signatures"
  );
}
