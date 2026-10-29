/**
 * libp2p@3 compatibility shim for OrbitDB@3.
 *
 * libp2p@3 (shipped with helia@6) changed streams from duplex ({sink, source})
 * to event-based ({send(), [Symbol.asyncIterator]}). OrbitDB@3 uses it-pipe, which
 * requires .sink/.source, and fails with "fns.shift(...) is not a function".
 * Fix: add .sink/.source getters to AbstractMessageStream.prototype.
 *
 * There used to be a second shim here that wrapped the /meshsub/ and /floodsub/
 * handlers for @chainsafe/libp2p-gossipsub@14, which still used the old
 * handler({stream, connection}) signature. We now use @libp2p/gossipsub, which
 * uses the libp2p@3 signature natively, so that wrapper was removed (it would
 * break the new library). OrbitDB (the wesense-main fork) already uses the
 * two-argument form.
 *
 * Import this module BEFORE creating any Helia/libp2p instances.
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
