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
 * hash-linked commits carrying a signed payload. What differs is who reads it.
 * Nothing in `Policy.ts` folds this namespace, and `Projection.ts` does not
 * project it.
 *
 * The walk here hands back *bytes*, not decoded payloads, and that is the one
 * structural difference from `Event.walk`. One trace ref carries several
 * unrelated record kinds — a context exposure, a runtime invocation, a tool
 * operation, a workspace transition — and a walk that decoded with one
 * namespace's schema would report every other kind as unreadable, which is how
 * "this replica is missing a record" and "this record is somebody else's"
 * become the same finding. Each reader decodes what it recognises and steps
 * over the rest.
 */
import { Context, Effect, Layer, Option } from "effect";

import { NAMESPACE, type PrivateKey, sign } from "../crypto/SshSignature.ts";
import * as Dag from "../git/Dag.ts";
import { Invalid } from "../git/Error.ts";
import type { TreeEntry } from "../git/Format.ts";
import { Repository } from "../git/Repository.ts";
import { checkRefName, type Oid } from "../git/Store.ts";
import * as Record from "../trust/Record.ts";
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
 * How many records one session's trace ref may hold.
 *
 * Its own bound, deliberately, and four times the pull-request ceiling:
 * docs/telemetry.md §13 says trace storage must not inherit the
 * policy-critical session fold's budget, and the two are bounded for different
 * reasons. A pull request's ceiling exists because folding one is quadratic and
 * runs synchronously on the receive-pack path. Nothing folds a trace ref: the
 * boundary walks it once, linearly, to check containment, and the projection
 * that reads it runs where a person is waiting rather than where a push is.
 *
 * Set where an honest session stops rather than where a large one does. Two
 * records per invocation — an exposure and its runtime record — puts this at
 * some thousands of invocations for one session, which is far past any run a
 * person is still calling a session.
 */
export const MAX_RECORDS = 16_384;

/**
 * The ceiling in force, when a host wants a different one.
 *
 * A tuning number rather than an authority, like `Event.Ceiling`: the default
 * is what every caller gets, and a host that lowers it only narrows what it
 * will walk.
 */
export class Ceiling extends Context.Service<Ceiling, number>()("hub/Trace/Ceiling") {}

export const ceiling = (records: number): Layer.Layer<Ceiling> => Layer.succeed(Ceiling)(records);

export const ceilingOf = Effect.fnUntraced(function* () {
  return Option.getOrElse(yield* Effect.serviceOption(Ceiling), () => MAX_RECORDS);
});

/** Whether a trace ref's history is short enough for this host to walk. */
export const withinCeiling = Effect.fn("hub.Trace.withinCeiling")(function* (head: Oid) {
  return yield* Dag.reachable(head, null, Event.isHubCommit, yield* ceilingOf()).pipe(
    Effect.as(true),
    Effect.catchTag("Invalid", () => Effect.succeed(false)),
  );
});

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

// -- reading --------------------------------------------------------------------

/** One record on a trace ref, still in the bytes its signature covers. */
export interface Entry {
  readonly commit: Oid;
  /** The records this one follows; a join has several. */
  readonly parents: ReadonlyArray<Oid>;
  /**
   * `<type> <id>` off the commit message.
   *
   * Read from the message rather than the payload because it is the only part
   * of a redacted record that survives, and "a tool operation was removed here"
   * is a more useful thing for a projection to be able to say than a gap.
   */
  readonly type: string | null;
  readonly id: string | null;
  readonly payload: Uint8Array;
  readonly signatures: ReadonlyArray<string>;
}

export interface Unreadable {
  readonly commit: Oid;
  readonly type: string | null;
  readonly id: string | null;
}

export interface Walk {
  /** Oldest first, parents before children. */
  readonly records: ReadonlyArray<Entry>;
  /** The DAG the walk was taken over, so a reader can keep concurrent lanes. */
  readonly parents: Dag.Parents;
  /**
   * Commits carrying a record whose payload this replica could not read.
   *
   * The declared type comes along, because the commit message survives a
   * redaction when the payload does not — that is what `summaryOf` is for.
   * Without it a reader could not tell one of its own damaged records from a
   * different namespace's deliberately removed one, and every consumer had to
   * treat all of them as its own.
   */
  readonly unreadable: ReadonlyArray<Unreadable>;
  /** Every commit reached, joins and unreadable records included. */
  readonly walked: number;
}

const summaryOf = (message: string) => {
  const [type = "", id = ""] = message.split("\n")[0]?.split(" ") ?? [];
  return { type: type === "" ? null : type, id: id === "" ? null : id };
};

/**
 * Every record on one session's trace ref, oldest first, nothing verified.
 *
 * Parentage comes back with the records because docs/telemetry.md §15 forbids
 * manufacturing a single causal order out of timestamps when concurrent
 * parents exist. A topological order alone would hide that two records were
 * written by fibers that never saw each other; handing the edges back lets the
 * Flight Recorder show two lanes where there were two lanes.
 *
 * Signatures are returned rather than checked, for the reason `Event.walk`
 * returns them: how much verification a reader needs is what differs — an
 * audit checks every one, a projection listing what happened checks none.
 */
export const walk = Effect.fn("hub.Trace.walk")(function* (session: string) {
  const repository = yield* Repository;
  const head = yield* repository.resolve(refOf(session));
  if (head === null) {
    return { records: [], parents: new Map(), unreadable: [], walked: 0 } satisfies Walk;
  }

  // The ceiling actually in force, not the compile-time default: a host that
  // lowered it reported "more than 16384 records" for a ref holding far fewer,
  // which sends an operator looking for a problem that is not there.
  const ceiling = yield* ceilingOf();
  const parents = yield* Dag.reachable(head, null, Event.isHubCommit, ceiling).pipe(
    Effect.mapError((error) =>
      error._tag === "Invalid"
        ? new Invalid({
            field: "session",
            reason: `trace '${session}' holds more than ${ceiling} records and cannot be read`,
          })
        : error,
    ),
  );

  // A head the walk could not classify is a ref this replica cannot read, not
  // an empty one. `Dag.reachable` drops the head itself when `belongs` says no,
  // and `Event.isHubCommit` deliberately answers `false` for a commit whose
  // tree never arrived — the partial-replication state it is written for, since
  // refs are applied without a connectivity check. Read as empty, `session show
  // --audit` reported "No invocations recorded" for a session that has them and
  // `trace redact` reported "has no trace record", leaving the operator unable
  // to write the tombstone. Reported as `Invalid`, it lands where the
  // over-ceiling case already lands and every caller already handles it.
  //
  // `Redaction.tombstonesOn` makes the same check for the same reason.
  if (!parents.has(head)) {
    return yield* new Invalid({
      field: "session",
      reason: `trace '${session}' cannot be read here; its head is missing an object`,
    });
  }

  const records: Array<Entry> = [];
  const unreadable: Array<Unreadable> = [];

  for (const commit of Dag.topological(parents)) {
    // Joins carry nothing: they are how two lanes became one again.
    if (!(yield* Record.carries(commit, Event.RECORD))) continue;

    const info = yield* repository.readCommit(commit);
    const read = yield* Record.read(commit, Event.RECORD).pipe(
      Effect.catchTags({
        ObjectNotFound: () => Effect.succeed(null),
        Invalid: () => Effect.succeed(null),
      }),
    );
    // A redaction deletes the payload blob and leaves the tree entry naming
    // it, so the read fails where every other record's succeeds.
    if (read === null) {
      unreadable.push({ commit, ...summaryOf(info.message) });
      continue;
    }

    records.push({
      commit,
      parents: parents.get(commit) ?? [],
      ...summaryOf(info.message),
      payload: read.payload,
      signatures: read.signatures,
    });
  }

  return { records, parents, unreadable, walked: parents.size } satisfies Walk;
});

/**
 * Whether any two records on this ref were written without seeing each other.
 *
 * True when the DAG branches: two children of one commit are two lanes, and a
 * reader that printed them as a line would be asserting an order the history
 * does not contain (§15).
 */
export const concurrent = (parents: Dag.Parents): boolean => {
  const children = new Map<Oid, number>();
  for (const [, of] of parents) {
    for (const parent of of) children.set(parent, (children.get(parent) ?? 0) + 1);
  }
  for (const [, count] of children) if (count > 1) return true;
  return false;
};
