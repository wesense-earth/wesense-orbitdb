/**
 * Teardown trace — diagnostic instrumentation for the stream-reset
 * investigation (wesense-general-docs/general/StreamResetInvestigation.md).
 *
 * The pcap evidence showed that the Node process itself sends TCP RSTs
 * mid-traffic — some code path inside wesense-orbitdb (or its deps) is
 * initiating connection teardowns. This module wraps the teardown
 * methods so every abort/close logs a single-line JSON event to stderr
 * with the originating stack trace, letting us histogram callers.
 *
 * What we wrap:
 *   - AbstractMessageStream.prototype.abort — fires for every
 *     MA-connection and yamux-stream abort. For TCP MA-connections this
 *     is the sole path to sendReset() → socket.resetAndDestroy() → wire
 *     RST, so its stack trace is the RST's origin.
 *   - AbstractMessageStream.prototype.close — graceful half-close, for
 *     contrast. Enable via TEARDOWN_TRACE=2.
 *   - YamuxMuxer.prototype.close + abort + sendGoAway — patched lazily
 *     via wrapYamux() because @chainsafe/libp2p-yamux doesn't export the
 *     muxer subpath.
 *
 * What we deliberately do NOT do:
 *   - No rate limiting: ~30 disconnects/hr means ~1 event every 2
 *     minutes, well under any log-volume concern.
 *   - No stack frame filtering: the whole point is to see the caller
 *     path, so we emit up to 25 frames verbatim.
 *   - No downstream effect: wrappers call through to the originals
 *     and return the same value. Enabling the trace should not change
 *     disconnect behaviour.
 *
 * Enable: TEARDOWN_TRACE=1 (aborts + yamux) or TEARDOWN_TRACE=2 (also
 * closes). Default unset = no patching, zero cost.
 *
 * Analysis: grep '"event":"abs-abort"' logs | jq -c '.stack' | sort | uniq -c | sort -rn
 */

import { AbstractMessageStream } from "@libp2p/utils";

const level = (() => {
  const raw = process.env.TEARDOWN_TRACE;
  if (raw === "1") return 1;
  if (raw === "2") return 2;
  return 0;
})();

export const TEARDOWN_TRACE_ENABLED = level > 0;

let nextId = 1;
const idByTarget = new WeakMap();

function targetId(target) {
  if (!target || typeof target !== "object") return null;
  let id = idByTarget.get(target);
  if (!id) {
    id = `o${nextId++}`;
    idByTarget.set(target, id);
  }
  return id;
}

function emit(event, fields) {
  try {
    process.stderr.write(
      JSON.stringify({ event, ts: Date.now(), ...fields }) + "\n"
    );
  } catch {
    // never throw out of instrumentation
  }
}

function captureStack(depthFrames = 25) {
  const holder = { stack: null };
  Error.captureStackTrace(holder, captureStack);
  return (holder.stack || "")
    .split("\n")
    .slice(1, 1 + depthFrames)
    .map((l) => l.trim())
    .join(" | ");
}

function describe(target) {
  if (!target || typeof target !== "object") return null;
  const out = {
    id: targetId(target),
    kind: target.constructor?.name,
  };
  if (target.status != null) out.status = target.status;
  if (target.direction != null) out.direction = target.direction;
  if (target.protocol != null) out.protocol = target.protocol;
  if (target.remoteAddr != null) {
    try {
      out.remoteAddr = target.remoteAddr.toString();
    } catch {}
  }
  // Yamux streams expose `.muxer`; the muxer has `.maConn.remoteAddr`.
  const maConn = target.muxer?.maConn;
  if (maConn?.remoteAddr) {
    try {
      out.viaConnId = targetId(maConn);
      out.viaRemoteAddr = maConn.remoteAddr.toString();
    } catch {}
  }
  // Yamux muxers expose `.maConn` directly.
  if (target.maConn?.remoteAddr) {
    try {
      out.viaConnId = targetId(target.maConn);
      out.viaRemoteAddr = target.maConn.remoteAddr.toString();
    } catch {}
  }
  return out;
}

function describeArgs(args) {
  return args.map((a) => {
    if (a instanceof Error) {
      return { err: a.message, name: a.name, code: a.code };
    }
    if (a && typeof a === "object") {
      return { keys: Object.keys(a) };
    }
    return a;
  });
}

function wrapMethod(proto, name, tag) {
  if (!proto || typeof proto[name] !== "function") {
    emit("teardown-trace-skip", { tag, reason: `missing ${name}` });
    return false;
  }
  if (proto[name].__teardownWrapped) return false;
  const orig = proto[name];
  function wrapped(...args) {
    emit(tag, {
      target: describe(this),
      args: describeArgs(args),
      stack: captureStack(),
    });
    return orig.apply(this, args);
  }
  wrapped.__teardownWrapped = true;
  proto[name] = wrapped;
  return true;
}

/**
 * Patch the base AbstractMessageStream prototype. Safe to call before
 * libp2p is constructed — this is the same pattern as libp2p-stream-compat.js.
 */
export function installTeardownTrace() {
  if (!TEARDOWN_TRACE_ENABLED) return;
  wrapMethod(AbstractMessageStream.prototype, "abort", "abs-abort");
  if (level >= 2) {
    wrapMethod(AbstractMessageStream.prototype, "close", "abs-close");
  }
  emit("teardown-trace-installed", { level, patched: ["abs-abort"] });
}

/**
 * Wrap the yamux() factory so we can patch YamuxMuxer.prototype lazily on
 * the first muxer instance. The muxer subpath isn't in the package's
 * exports field, so direct import would fail.
 *
 * Usage (in src/index.js):
 *   import { yamux as rawYamux } from "@chainsafe/libp2p-yamux";
 *   const yamux = wrapYamux(rawYamux);
 *   // ...then use yamux({...}) as before.
 */
export function wrapYamux(yamuxFactory) {
  if (!TEARDOWN_TRACE_ENABLED) return yamuxFactory;
  let patched = false;
  return function instrumentedYamux(init) {
    const createMuxerComponent = yamuxFactory(init);
    return function instrumentedCreate(...componentArgs) {
      const muxer = createMuxerComponent(...componentArgs);
      if (muxer && !patched) {
        const origCreate = muxer.createStreamMuxer?.bind(muxer);
        if (origCreate) {
          muxer.createStreamMuxer = (maConn, ...rest) => {
            const instance = origCreate(maConn, ...rest);
            if (!patched && instance) {
              const proto = Object.getPrototypeOf(instance);
              wrapMethod(proto, "close", "yamux-close");
              wrapMethod(proto, "abort", "yamux-abort");
              wrapMethod(proto, "sendGoAway", "yamux-goaway");
              patched = true;
              emit("teardown-trace-yamux-patched", {
                kind: proto.constructor?.name,
              });
            }
            return instance;
          };
        }
      }
      return muxer;
    };
  };
}
