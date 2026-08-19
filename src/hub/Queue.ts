/**
 * The merge queue: which pull requests are landing on a branch, and in what
 * order.
 *
 * The boundary decides what may land — a candidate chain whose every merge it
 * re-derives (`server/Policy.ts`). This decides nothing. It is the *record* a
 * queue keeps so that several agents cooperating on one branch agree about who
 * is building what, and so a run that dies mid-batch leaves its successor
 * something to read rather than a branch and a guess.
 *
 * The separation is deliberate and load-bearing. **The policy boundary never
 * reads a queue ref.** Landing is judged from the trust log, the pull requests'
 * own events and the rules, exactly as a direct push is; the queue ref
 * authorizes nothing. A ref that gated pushes would be a ref whose corruption,
 * absence or oversize froze a branch — and this namespace is append-only, so
 * that freeze would be permanent. Two honest runners racing one landing are
 * already safe, because landing is compare-and-swap; a dishonest one gains
 * nothing by lying about an order it cannot land out of anyway.
 *
 * So an entry is **intent, not a lock** — the same reading a task claim gets
 * (`Task.ts`), for the same reason: it coordinates agents that want to
 * cooperate and restrains nobody. `queue.left` is undone by entering again, and
 * `queue.reset` by building again, which is what makes both safe to leave open
 * to any member holding the capability.
 *
 * There is no tombstone here, and that is not an omission. Every field a queue
 * event carries is structural — a pull request id, an object id, a ref name, a
 * reason from a fixed list — so there is nothing somebody typed for a redaction
 * to remove. `hub.queue` is therefore the only way onto these refs, and a
 * record claiming to be a tombstone reads here as one unreadable record rather
 * than as a removal that never happens.
 */
import { Effect, Schema } from "effect";

import {
  type Fingerprint,
  fingerprint,
  NAMESPACE,
  type PrivateKey,
  sign,
  verify,
} from "../crypto/SshSignature.ts";
import * as Dag from "../git/Dag.ts";
import { Invalid } from "../git/Error.ts";
import { TRUST_LOG } from "../git/Refspec.ts";
import { Repository } from "../git/Repository.ts";
import { checkRefName, type Oid } from "../git/Store.ts";
import * as Record from "../trust/Record.ts";
import * as Event from "./Event.ts";

export const refOf = (queue: string): string => `refs/hub/queue/${queue}`;

/** The queue a hub ref names, or `null` for a ref that is not one. */
export const queueOf = (ref: string): string | null => {
  if (!ref.startsWith("refs/hub/queue/")) return null;
  const id = ref.slice("refs/hub/queue/".length);
  return id.length === 0 || id.includes("/") ? null : id;
};

/** Asked of the ref this would actually write; see `Task.isTaskId`. */
export const isQueueId = (id: string): boolean => {
  if (id.length === 0 || id.length > 128 || id.includes("/")) return false;
  return checkRefName(refOf(id)) === null;
};

export const newId = Event.newId;

/** Every queue this repository holds, by id. */
export const queues = Effect.fn("hub.Queue.queues")(function* () {
  const repository = yield* Repository;
  const ids: Array<string> = [];
  for (const [name] of yield* repository.refs) {
    const id = queueOf(name);
    if (id !== null) ids.push(id);
  }
  return ids.sort();
});

const envelope = {
  version: Schema.Literal(1),
  repo: Schema.String,
  /** The queue this event belongs to. */
  queue: Schema.String,
  id: Schema.String,
  issuedAt: Schema.String,
  trustHead: Schema.NullOr(Schema.String),
};

/**
 * What this queue is for.
 *
 * The target is a full ref name for the reason a pull request's base is read as
 * one (hub.md §27): the protected-branch rules match on the ref being written,
 * and a bare branch name matches nothing while looking exactly like it named
 * something.
 */
export const QueueOpened = Schema.Struct({
  type: Schema.tag("queue.opened"),
  ...envelope,
  target: Schema.String,
});

/** A pull request offered for landing, at the revision it then proposed. */
export const QueueEntered = Schema.Struct({
  type: Schema.tag("queue.entered"),
  ...envelope,
  pr: Schema.String,
  head: Schema.String,
});

/**
 * A candidate built for one entry: the merge commit, and where it can be found.
 *
 * `branch` is an ordinary branch, deliberately. A candidate has to be fetchable
 * — CI must test it — and must not pin the object graph for ever, and both
 * point away from a new ref class: `refs/heads/queue/...` is deletable,
 * collectable, and needs nothing new from the advertisement or from `gc`. The
 * oid is what matters and the branch is where to find it, which is why the
 * branch is recorded beside the commit rather than instead of it.
 */
export const QueueCandidate = Schema.Struct({
  type: Schema.tag("queue.candidate"),
  ...envelope,
  pr: Schema.String,
  /** The candidate commit — `C_i` in docs/queue.md §3. */
  commit: Schema.String,
  /** What it was merged onto — the step before it, or the target's own tip. */
  onto: Schema.String,
  branch: Schema.String,
});

/**
 * An entry leaving the queue, and why.
 *
 * `landed` is the only outcome that says anything happened to the branch, and
 * even that is a report rather than a claim anybody acts on: what landed is
 * whatever the boundary accepted, and the branch is where to read it.
 */
export const QueueLeft = Schema.Struct({
  type: Schema.tag("queue.left"),
  ...envelope,
  pr: Schema.String,
  reason: Schema.Literals(["landed", "failed", "conflict", "stale", "withdrawn"]),
});

/**
 * The chain on record was not built on what the target now holds.
 *
 * Usually because the target moved outside this queue, and sometimes because
 * the chain lost a step from under the ones above it — an entry re-entering at
 * a new head clears its own candidate and leaves everything built on top of it
 * hanging. Either way the candidates are stale and the entries are not: what a
 * reset invalidates is the chain, never anybody's intention to land. `at` is
 * what the target was observed to hold, kept so a reader can tell one reset
 * from the next.
 */
export const QueueReset = Schema.Struct({
  type: Schema.tag("queue.reset"),
  ...envelope,
  at: Schema.String,
});

export type QueueLeft = (typeof QueueLeft)["Type"];

export const QueuePayload = Schema.Union([
  QueueOpened,
  QueueEntered,
  QueueCandidate,
  QueueLeft,
  QueueReset,
]);
export type QueuePayload = (typeof QueuePayload)["Type"];

const decodePayload = Schema.decodeUnknownEffect(QueuePayload);
const encoder = new TextEncoder();

export const encode = (payload: QueuePayload): Uint8Array => {
  const { id, issuedAt, queue, repo, trustHead, type, version, ...rest } = payload;
  return encoder.encode(
    `${JSON.stringify({ version, type, repo, queue, id, issuedAt, trustHead, ...rest }, null, 2)}\n`,
  );
};

/**
 * Far above any honest queue record.
 *
 * Smaller than a session's or a task's because there is no prose here to hold:
 * every field is an identifier, an object id or a ref name.
 */
export const MAX_PAYLOAD = 16 * 1024;

export const decode = Effect.fn("hub.Queue.decode")(function* (bytes: Uint8Array) {
  const json = yield* Effect.try({
    try: () => JSON.parse(new TextDecoder().decode(bytes)),
    catch: () => new Invalid({ field: "queue", reason: "queue event is not valid JSON" }),
  });
  return yield* decodePayload(json).pipe(
    Effect.mapError(
      (issue) => new Invalid({ field: "queue", reason: `malformed queue event: ${issue.message}` }),
    ),
  );
});

export const context = Effect.fn("hub.Queue.context")(function* (repo: string, queue: string) {
  const repository = yield* Repository;
  const trustHead = yield* repository.resolve(TRUST_LOG);
  return {
    version: 1,
    repo,
    queue,
    id: newId(),
    issuedAt: new Date().toISOString(),
    trustHead,
  } as const;
});

/** The pull request a record is about, where it names one. */
const prOf = (payload: QueuePayload): string | null =>
  payload.type === "queue.entered" ||
  payload.type === "queue.candidate" ||
  payload.type === "queue.left"
    ? payload.pr
    : null;

export const issue = Effect.fn("hub.Queue.issue")(function* (
  payload: QueuePayload,
  key: PrivateKey,
) {
  if (!isQueueId(payload.queue)) {
    return yield* new Invalid({
      field: "queue",
      reason: `'${payload.queue}' cannot name a queue; it must be one ref path component`,
    });
  }

  // Refused where it is written, which is the only place it can still be
  // refused: an append-only ref keeps whatever it is given, and a pull request
  // id that cannot name a ref is an entry no reader can ever resolve.
  const pr = prOf(payload);
  if (pr !== null && !Event.isPullRequestId(pr)) {
    return yield* new Invalid({
      field: "pr",
      reason: `'${pr}' cannot name a pull request; it must be one ref path component`,
    });
  }

  // Object ids are read as *names* by everything downstream, so a value that is
  // not one is refused here rather than left to fail as a lookup later; see
  // hub.md §23 on what a store does with a name it should not have been given.
  const oids: ReadonlyArray<readonly [string, string]> =
    payload.type === "queue.entered"
      ? [["head", payload.head]]
      : payload.type === "queue.candidate"
        ? [
            ["commit", payload.commit],
            ["onto", payload.onto],
          ]
        : payload.type === "queue.reset"
          ? [["at", payload.at]]
          : [];
  for (const [field, value] of oids) {
    if (Event.unqualify(value) === null) {
      return yield* new Invalid({
        field,
        reason: `'${value}' is not a hash-qualified object id`,
      });
    }
  }

  const refs: ReadonlyArray<readonly [string, string]> =
    payload.type === "queue.opened"
      ? [["target", payload.target]]
      : payload.type === "queue.candidate"
        ? [["branch", payload.branch]]
        : [];
  for (const [field, value] of refs) {
    if (checkRefName(value) !== null) {
      return yield* new Invalid({ field, reason: `'${value}' is not a ref name` });
    }
  }

  const bytes = encode(payload);
  if (bytes.length > MAX_PAYLOAD) {
    return yield* new Invalid({
      field: "queue",
      reason: `a queue record may not exceed ${MAX_PAYLOAD} bytes; this one is ${bytes.length}`,
    });
  }

  // No secret scan, unlike a session or a task: there is nothing here somebody
  // typed. Every field this record carries is an identifier, an object id, a
  // ref name or one of five reasons, all high-entropy or enumerated by
  // construction — the same argument `Task.ts` makes for not scanning its own
  // envelope, applied to a record that is entirely envelope.
  const signature = yield* sign(key, bytes, NAMESPACE);
  return yield* Event.appendTo({
    ref: refOf(payload.queue),
    message: `${payload.type} ${payload.id}\n`,
    payload: bytes,
    signatures: [signature],
  });
});

export const open = Effect.fn("hub.Queue.open")(function* (input: {
  readonly repo: string;
  readonly target: string;
  readonly key: PrivateKey;
  readonly queue?: string;
}) {
  const queue = input.queue ?? newId();
  const base = yield* context(input.repo, queue);
  const commit = yield* issue({ ...base, type: "queue.opened", target: input.target }, input.key);
  return { queue, commit };
});

export const enter = Effect.fn("hub.Queue.enter")(function* (input: {
  readonly repo: string;
  readonly queue: string;
  readonly pr: string;
  readonly head: Oid;
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.queue);
  return yield* issue(
    { ...base, type: "queue.entered", pr: input.pr, head: Event.qualify(input.head) },
    input.key,
  );
});

export const candidate = Effect.fn("hub.Queue.candidate")(function* (input: {
  readonly repo: string;
  readonly queue: string;
  readonly pr: string;
  readonly commit: Oid;
  readonly onto: Oid;
  readonly branch: string;
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.queue);
  return yield* issue(
    {
      ...base,
      type: "queue.candidate",
      pr: input.pr,
      commit: Event.qualify(input.commit),
      onto: Event.qualify(input.onto),
      branch: input.branch,
    },
    input.key,
  );
});

export const leave = Effect.fn("hub.Queue.leave")(function* (input: {
  readonly repo: string;
  readonly queue: string;
  readonly pr: string;
  readonly reason: QueueLeft["reason"];
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.queue);
  return yield* issue(
    { ...base, type: "queue.left", pr: input.pr, reason: input.reason },
    input.key,
  );
});

export const reset = Effect.fn("hub.Queue.reset")(function* (input: {
  readonly repo: string;
  readonly queue: string;
  readonly at: Oid;
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.queue);
  return yield* issue({ ...base, type: "queue.reset", at: Event.qualify(input.at) }, input.key);
});

/** Who signed a record, or `null`; see `Task.signerOf` for why it matters. */
const signerOf = Effect.fn("hub.Queue.signerOf")(function* (
  bytes: Uint8Array,
  signatures: ReadonlyArray<string>,
) {
  for (const armored of signatures) {
    const key = yield* verify(armored, bytes, NAMESPACE).pipe(
      Effect.catchTag("Invalid", () => Effect.succeed(null)),
    );
    if (key !== null) return yield* fingerprint(key);
  }
  return null;
});

/** One queue's events, oldest first; see `Task.entries` for the shape. */
export const entries = Effect.fn("hub.Queue.entries")(function* (queue: string) {
  const repository = yield* Repository;

  const head = yield* repository.resolve(refOf(queue));
  if (head === null) return { events: [], unreadable: [] } as const;

  const parents = yield* Dag.reachable(head, null, Event.isHubCommit, yield* Event.ceilingOf());
  const events: Array<{
    readonly commit: Oid;
    readonly payload: QueuePayload;
    /** `null` where no signature verified; such a record decides nothing. */
    readonly signer: Fingerprint | null;
  }> = [];
  const unreadable: Array<Oid> = [];

  for (const commit of Dag.topological(parents)) {
    if (!(yield* Record.carries(commit, Event.RECORD))) continue;
    const record = yield* Record.read(commit, Event.RECORD).pipe(
      Effect.catchTags({
        ObjectNotFound: () => Effect.succeed(null),
        Invalid: () => Effect.succeed(null),
      }),
    );
    if (record === null) {
      unreadable.push(commit);
      continue;
    }
    const payload = yield* decode(record.payload).pipe(Effect.orElseSucceed(() => null));
    if (payload === null) {
      unreadable.push(commit);
      continue;
    }
    events.push({ commit, payload, signer: yield* signerOf(record.payload, record.signatures) });
  }

  return { events, unreadable } as const;
});

/** One pull request's place in the queue. */
export interface Entry {
  readonly pr: string;
  /** The revision it was entered at, which a candidate is built from. */
  readonly head: Oid;
  readonly by: Fingerprint | null;
  /** The candidate last built for it, or `null` since the last reset. */
  readonly candidate: {
    readonly commit: Oid;
    readonly onto: Oid;
    readonly branch: string;
  } | null;
}

/** What `project` answers with, named so a caller can hold a list of them. */
export interface Projection {
  readonly queue: string;
  readonly exists: boolean;
  readonly target: string | null;
  readonly entries: ReadonlyArray<Entry>;
  readonly left: ReadonlyArray<{ readonly pr: string; readonly reason: string }>;
  readonly resets: number;
  readonly ignored: ReadonlyArray<Oid>;
  readonly unreadable: ReadonlyArray<Oid>;
}

/**
 * What a queue amounts to now: what it is for, and who is in it.
 *
 * Entries keep the order they were first entered in, which is the order a
 * runner builds them in — but it is a *suggestion* and not a rule, because
 * nothing downstream reads this to authorize anything. A runner that builds
 * them in another order produces a chain the boundary judges on its own terms.
 *
 * A record nobody signed decides nothing, here as everywhere. Beyond that the
 * rules are deliberately loose: entering, leaving and resetting are open to any
 * member holding `hub.queue`, because each is undone by saying the opposite and
 * none of them can move a branch.
 */
export const project = Effect.fn("hub.Queue.project")(function* (queue: string) {
  const walked = yield* entries(queue);

  let opened: { readonly target: string; readonly by: Fingerprint | null } | null = null;
  /** Insertion-ordered, which is what makes it the queue rather than a set. */
  const queued = new Map<string, Entry>();
  const left: Array<{ readonly pr: string; readonly reason: string }> = [];
  const ignored: Array<Oid> = [];
  let resets = 0;

  for (const { commit, payload, signer } of walked.events) {
    if (signer === null) {
      ignored.push(commit);
      continue;
    }

    switch (payload.type) {
      case "queue.opened":
        // The first opening wins, as a task's does: a ref that only grows has
        // exactly one beginning, and the boundary refuses a push that grafts a
        // second (hub.md §23).
        if (opened === null) opened = { target: payload.target, by: signer };
        else ignored.push(commit);
        break;

      case "queue.entered": {
        const head = Event.unqualify(payload.head);
        if (opened === null || head === null) {
          ignored.push(commit);
          break;
        }
        // Re-entering is how an entry's head moves, so the later record wins —
        // and it drops the candidate with it, because a candidate built from
        // the revision before it is exactly what a moved head invalidates.
        //
        // In place, keeping the position it already had. `Map.set` on a key it
        // already holds does not reorder, and deleting first would: a pull
        // request that pushed a fix while queued went to the back, which is not
        // what re-entering means and not what this function says it does.
        // Leaving and entering again is how something moves to the back.
        queued.set(payload.pr, { pr: payload.pr, head, by: signer, candidate: null });
        break;
      }

      case "queue.candidate": {
        const held = queued.get(payload.pr);
        const commitOid = Event.unqualify(payload.commit);
        const onto = Event.unqualify(payload.onto);
        if (held === undefined || commitOid === null || onto === null) {
          ignored.push(commit);
          break;
        }
        queued.set(payload.pr, {
          ...held,
          candidate: { commit: commitOid, onto, branch: payload.branch },
        });
        break;
      }

      case "queue.left":
        if (queued.delete(payload.pr)) left.push({ pr: payload.pr, reason: payload.reason });
        else ignored.push(commit);
        break;

      case "queue.reset":
        // The chain is stale; the intentions behind it are not.
        resets += 1;
        for (const [pr, held] of queued) queued.set(pr, { ...held, candidate: null });
        break;
    }
  }

  return {
    queue,
    exists: opened !== null,
    target: opened?.target ?? null,
    entries: [...queued.values()],
    left,
    resets,
    // Said out loud rather than swallowed: a record that did not count is
    // exactly what somebody will be looking for.
    ignored,
    unreadable: walked.unreadable,
  } satisfies Projection;
});

/**
 * The queue for a target branch, where the repository holds one.
 *
 * One queue this replica cannot walk is one candidate missing, not a failure of
 * the lookup — the same reading the policy boundary gives an oversized pull
 * request. A history that arrived by replication was never held to this host's
 * ceiling, so failing here would let whoever grew one queue break `queue run`
 * and `queue list` for every other target on the replica that copied it.
 */
export const forTarget = Effect.fn("hub.Queue.forTarget")(function* (target: string) {
  for (const id of yield* queues()) {
    const state = yield* project(id).pipe(
      Effect.catchTags({
        Invalid: () => Effect.succeed(null),
        ObjectNotFound: () => Effect.succeed(null),
        StorageFailure: () => Effect.succeed(null),
      }),
    );
    if (state !== null && state.exists && state.target === target) return state;
  }
  return null;
});
