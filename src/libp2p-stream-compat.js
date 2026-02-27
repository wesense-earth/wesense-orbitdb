/**
 * libp2p@3 compatibility shim for gossipsub@14 and OrbitDB@3.
 *
 * Two breaking changes in libp2p@3 (shipped with helia@6) affect these modules:
 *
 * 1. STREAM DUPLEX: Streams changed from duplex ({sink, source}) to event-based
 *    ({send(), [Symbol.asyncIterator]}). gossipsub@14 and OrbitDB@3 use it-pipe
 *    which requires .sink/.source, causing "fns.shift(...) is not a function".
 *    Fix: patch AbstractMessageStream.prototype with .sink/.source getters.
 *
 * 2. HANDLER SIGNATURE: Protocol stream handlers changed from
 *    handler({stream, connection}) to handler(stream, connection).
 *    gossipsub@14 and OrbitDB@3 destructure a single object arg, receiving
 *    undefined for both stream and connection — silently breaking inbound streams.
 *    Fix: wrap the registrar.handle() to convert args for affected protocols.
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
    // gossipsub and OrbitDB protocols use the old IncomingStreamData
    // signature: handler({stream, connection}). Wrap them to accept
    // the libp2p@3 signature: handler(stream, connection).
    if (
      protocol.startsWith("/meshsub/") ||
      protocol.startsWith("/floodsub/") ||
      protocol.startsWith("/orbitdb/")
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
