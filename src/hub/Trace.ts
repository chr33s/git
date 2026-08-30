/**
 * The trace namespace: detailed audit records nothing is allowed to decide on.
 *
 * ```text
 * refs/hub/session/<session-id>
 *   the distilled account — what was asked, what came of it
 *   policy may consult it
 *
 * refs/hub/trace/<session-id>
 *   context exposures, invocations, tools, workspace, lifecycle
 *   never consulted for authorization or merge policy
 * ```
 *
 * Two namespaces rather than one, and the split is the point (docs/context-pack.md
 * §3, docs/telemetry.md §2). A session record is small, bounded and worth
 * keeping forever, and the protected-branch fold reads it. A trace is none of
 * those: it is written per invocation, it is as large as a harness cares to
 * make it, and its volume must never be able to slow down — or influence — a
 * push. Kept on the session ref, an agent's ordinary afternoon would be a
 * quadratic fold on the receive-pack path, and every context exposure would
 * become an input to authorization decisions it has no business informing.
 *
 * Mechanically it is an ordinary hub ref: one per session, append-only,
 * hash-linked commits carrying a signed payload, walked by `Event.walk`. What
 * differs is who reads it. Nothing in `Policy.ts` folds this namespace, and
 * `Projection.ts` does not project it.
 */
import { Effect } from "effect";

import { NAMESPACE, type PrivateKey, sign } from "../crypto/SshSignature.ts";
import { Invalid } from "../git/Error.ts";
import type { TreeEntry } from "../git/Format.ts";
import { Repository } from "../git/Repository.ts";
import { checkRefName } from "../git/Store.ts";
import * as Event from "./Event.ts";

/** Where one session's trace records live. */
export const refOf = (session: string): string => `refs/hub/trace/${session}`;

/** The session a hub ref traces, or `null` for a ref that traces none. */
export const traceOf = (ref: string): string | null => {
  const prefix = "refs/hub/trace/";
  if (!ref.startsWith(prefix)) return null;
  const id = ref.slice(prefix.length);
  return id.length === 0 || id.includes("/") ? null : id;
};

/**
 * Whether an id can name a trace ref.
 *
 * Asked of the ref this would actually write, for the reason `isSessionId`
 * gives: a second list of what git refuses drifts from the first, and the
 * failure lands after the objects are already written.
 */
export const isTraceId = (id: string): boolean => {
  if (id.length === 0 || id.length > 128 || id.includes("/")) return false;
  return checkRefName(refOf(id)) === null;
};

/** Every session this repository holds trace records for. */
export const traces = Effect.fn("hub.Trace.traces")(function* () {
  const repository = yield* Repository;
  const ids: Array<string> = [];
  for (const [name] of yield* repository.refs) {
    const id = traceOf(name);
    if (id !== null) ids.push(id);
  }
  return ids.sort();
});

/**
 * How large one trace record may be.
 *
 * Larger than a session record, which is a distillation, and still bounded:
 * this namespace replicates like every other, so an unbounded record is one
 * every clone pays for. A render body that does not fit belongs outside the
 * payload — attached to the record's tree, where retention can expire it
 * without rewriting the signed bytes.
 */
export const MAX_PAYLOAD = 512 * 1024;

/**
 * Sign one trace record and append it to its session's trace ref.
 *
 * `attach` is how a record carries the objects it must keep reachable —
 * a Context Exposure's `context/` subtree — rather than merely name in JSON.
 */
export const append = Effect.fn("hub.Trace.append")(function* (input: {
  readonly session: string;
  readonly type: string;
  readonly id: string;
  readonly payload: Uint8Array;
  readonly key: PrivateKey;
  readonly attach?: ReadonlyArray<TreeEntry>;
}) {
  if (!isTraceId(input.session)) {
    return yield* new Invalid({
      field: "session",
      reason: `'${input.session}' cannot name a trace; it must be one ref path component`,
    });
  }
  if (input.payload.length > MAX_PAYLOAD) {
    return yield* new Invalid({
      field: "trace",
      reason: `a trace record may not exceed ${MAX_PAYLOAD} bytes; this one is ${input.payload.length}`,
    });
  }

  const signature = yield* sign(input.key, input.payload, NAMESPACE);
  return yield* Event.appendTo({
    ref: refOf(input.session),
    message: `${input.type} ${input.id}\n`,
    payload: input.payload,
    signatures: [signature],
    attach: input.attach,
  });
});
