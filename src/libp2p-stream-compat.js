/**
 * libp2p@3 stream duplex compatibility shim.
 *
 * libp2p@3 (shipped with helia@6) changed the Stream interface from the
 * duplex pattern ({sink, source}) to an event-based pattern ({send(),
 * [Symbol.asyncIterator]}).  gossipsub@14 and OrbitDB@3 still use the
 * duplex pattern via it-pipe, causing "fns.shift(...) is not a function"
 * when they try to pipe data through streams that lack .sink/.source.
 *
 * This module patches AbstractMessageStream.prototype to add .sink and
 * .source getters, restoring duplex compatibility for all libp2p streams.
 * Import this BEFORE creating any Helia/libp2p instances.
 */

import { AbstractMessageStream } from "@libp2p/utils";

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
