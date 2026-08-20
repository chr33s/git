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
import * as Secrets from "./Secrets.ts";
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

/**
 * Where a candidate for one pull request is published.
 *
 * Derivable from the target and the pull request, and deliberately so: a caller
 * cleaning up after an entry cannot always read the record that named the
 * branch — a `queue.reset` clears the candidate while the branch it published
 * stays on disk — so a name that can only be looked up is one that leaks a ref
 * whenever the lookup comes back empty.
 */
export const candidateBranch = (target: string, pr: string): string =>
  `refs/heads/queue/${target.replace(/^refs\/heads\//, "")}/${pr}`;

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

/**
 * This queue is finished; open another for the branch if it still needs one.
 *
 * A queue ref grows for as long as the branch it serves does — a few records
 * per landing, for ever — and every fold of it is bounded by the same ceiling
 * every hub ref is (`Event.ceilingOf`). A pull request, a session and a task
 * are each about one finite piece of work, so their refs stop growing on their
 * own; a queue is about a *branch*, which does not. Without a way to end one,
 * a busy repository's queue would eventually pass the ceiling and become
 * unreadable and unremovable at once, taking `queue run` on that branch with
 * it — the exact failure this namespace's other rules exist to prevent.
 *
 * So a queue ends by saying so, and a fresh one takes over. Entries do not
 * migrate: what a closed queue holds is history, and whoever still wants to
 * land re-enters. Reopening is deliberately not offered — an ended queue is
 * ended, and the way back is another queue.
 */
export const QueueClosed = Schema.Struct({
  type: Schema.tag("queue.closed"),
  ...envelope,
  reason: Schema.String,
});

export const QueuePayload = Schema.Union([
  QueueOpened,
  QueueEntered,
  QueueCandidate,
  QueueLeft,
  QueueReset,
  QueueClosed,
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

/** The one field of a queue record somebody types. */
const prose = (payload: QueuePayload): string =>
  payload.type === "queue.closed" ? payload.reason : "";

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

  // Scanned over what somebody typed, and that is one field: a close's reason.
  // Everything else this record carries is an identifier, an object id, a ref
  // name or one of five enumerated words — high-entropy or fixed by
  // construction, which is the same argument `Task.ts` makes for not scanning
  // its own envelope.
  const leaked = Secrets.scan(prose(payload));
  if (leaked.length > 0) {
    return yield* new Invalid({
      field: "queue",
      reason: `this record looks like it carries ${leaked
        .map((finding) => `a ${finding.kind} (${finding.hint})`)
        .join(", ")}`,
    });
  }

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

export const close = Effect.fn("hub.Queue.close")(function* (input: {
  readonly repo: string;
  readonly queue: string;
  readonly reason: string;
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.queue);
  return yield* issue({ ...base, type: "queue.closed", reason: input.reason }, input.key);
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
  if (head === null) return { events: [], unreadable: [], walked: 0 } as const;

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

  // `walked`, not `events.length`: the ceiling bounds every commit this walk
  // reads — joins and records it cannot decode included — so a count of the
  // decodable ones is the wrong number to warn on. Undercounted, the warning
  // that a queue is filling up could never fire before the ref crossed the
  // ceiling and became unreadable and unremovable at once.
  return { events, unreadable, walked: parents.size } as const;
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
  /** Why this queue was ended, or `null` while it is still running. */
  readonly closed: string | null;
  /** Who opened it, for a reader that wants to know rather than to decide. */
  readonly openedBy: Fingerprint | null;
  /**
   * How many commits this queue's history holds.
   *
   * Every commit, not every readable record: joins and undecodable records are
   * walked too, and it is the walk the ceiling bounds. Surfaced because this is
   * the number that decides whether the ref is approaching that ceiling — the
   * one bound a queue, unlike every other hub ref, can reach just by doing its
   * job for long enough.
   */
  readonly records: number;
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

  let opened: { readonly target: string; readonly by: Fingerprint } | null = null;
  /** Insertion-ordered, which is what makes it the queue rather than a set. */
  const queued = new Map<string, Entry>();
  const left: Array<{ readonly pr: string; readonly reason: string }> = [];
  const ignored: Array<Oid> = [];
  let resets = 0;
  let closed: string | null = null;

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
        // Nothing joins a queue that has ended. Unreachable through the verbs,
        // which refuse a closed queue outright — but a holder of this
        // namespace's capability can append to any ref in it directly, and an
        // entry admitted after a close is one whose candidate branch nothing
        // will ever clean up.
        if (closed !== null) {
          ignored.push(commit);
          break;
        }
        // The id as well as the oid. `issue` refuses a record whose `pr` cannot
        // name a pull request, but the boundary charges this namespace by ref
        // name without decoding what lands on it — so a record written another
        // way could put an unusable id in `entries`, and every verb that later
        // tried to *act* on that entry refused it, aborting the run and every
        // run after it on a ref nothing can shorten. What this fold yields has
        // to be what the verbs can act on.
        if (opened === null || head === null || !Event.isPullRequestId(payload.pr)) {
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
        // Nothing is built for a queue that has ended, for the reason nothing
        // joins one: a candidate recorded after a close is one whose branch the
        // close has already swept past, and no verb will name it again.
        if (closed !== null) {
          ignored.push(commit);
          break;
        }
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

      case "queue.closed":
        // Any member holding the namespace's capability, deliberately — unlike
        // a task close, which is its opener's alone. The rule everywhere here
        // is that what comes back cheaply may be done by anybody and what does
        // not stays with whoever started it: a task close *ends the work*, and
        // a redaction destroys content. Ending a queue destroys nothing — the
        // pull requests are untouched, and a fresh queue plus a re-entry is one
        // command each.
        //
        // And the alternative is worse than the abuse it would prevent. This is
        // the one hub ref that grows without bound, so a queue nobody can end
        // is a queue that eventually crosses the ceiling and becomes unreadable
        // and unremovable at once — which is the state closing exists to
        // prevent. Held to the opening key, a lost or revoked one would strand
        // the ref there permanently.
        if (opened === null) ignored.push(commit);
        else closed ??= payload.reason;
        break;

      case "queue.reset":
        // Held to the same two conditions as everything else on this ref. A
        // reset landing after a close — the race `queue close` says it cannot
        // prevent — would otherwise clear `candidate` on every entry, hiding
        // the published branch names from a reader on a queue no verb can act
        // on any more, which is exactly when those names are what somebody
        // needs.
        if (opened === null || closed !== null) {
          ignored.push(commit);
          break;
        }
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
    closed,
    openedBy: opened?.by ?? null,
    records: walked.walked,
    // Said out loud rather than swallowed: a record that did not count is
    // exactly what somebody will be looking for.
    ignored,
    unreadable: walked.unreadable,
  } satisfies Projection;
});

/**
 * The queue for a target branch, and the queues this replica could not read.
 *
 * Both, because the two callers need opposite things from the same walk. Asking
 * "which queue do I run for this branch?", one queue this replica cannot walk
 * is one candidate missing — a history that arrived by replication was never
 * held to this host's ceiling, so failing would let whoever grew one queue
 * break every other target on the replica that copied it. Asking "may I open a
 * queue here?", the same silence is the wrong answer: opening a second queue
 * for one branch is permanent on a namespace nothing can delete, so a caller
 * about to do that has to be able to tell "there is none" from "I could not
 * tell", and fail closed on the second.
 */
/**
 * What a queue is for, and whether it is still running — and nothing else.
 *
 * Answering "which queue serves this branch?" needs two fields, and folding a
 * whole queue to get them verifies a signature per record. On the path a wake
 * fires per push, across every queue a rotating repository accumulates, that is
 * the cost — not the decoding. So this reads the same walk and verifies only
 * the two record kinds whose signer it actually consults.
 */
const summary = Effect.fn("hub.Queue.summary")(function* (queue: string) {
  const repository = yield* Repository;
  const head = yield* repository.resolve(refOf(queue));
  if (head === null) return { target: null, closed: null } as const;

  const parents = yield* Dag.reachable(head, null, Event.isHubCommit, yield* Event.ceilingOf());
  let target: string | null = null;
  let closed: string | null = null;
  for (const commit of Dag.topological(parents)) {
    if (!(yield* Record.carries(commit, Event.RECORD))) continue;
    const record = yield* Record.read(commit, Event.RECORD).pipe(
      Effect.catchTags({
        ObjectNotFound: () => Effect.succeed(null),
        Invalid: () => Effect.succeed(null),
      }),
    );
    if (record === null) continue;
    const payload = yield* decode(record.payload).pipe(Effect.orElseSucceed(() => null));
    if (payload === null) continue;
    if (payload.type !== "queue.opened" && payload.type !== "queue.closed") continue;
    // The two the fold reads a signer for, and only these two.
    if ((yield* signerOf(record.payload, record.signatures)) === null) continue;
    if (payload.type === "queue.opened") target ??= payload.target;
    else if (target !== null) closed ??= payload.reason;
  }
  return { target, closed } as const;
});

export const forTarget = Effect.fn("hub.Queue.forTarget")(function* (target: string) {
  const unreadable: Array<string> = [];
  let found: Projection | null = null;
  // Newest first, and stopping at the first live match. Folding a queue is a
  // signature verification per record, and this runs on the path a wake fires
  // per push — while the design *mandates* rotation, so the ended queues pile
  // up behind the live one. Ids are UUIDv7, so newest-first reaches the queue
  // in use immediately and leaves the history unread; scanning oldest-first
  // read every closed queue in full before finding the one that matters.
  //
  // Still a total order, so two live queues for one branch — only reachable
  // through the race `queue open` documents — resolve to the same one on every
  // replica: the newer, which is the one somebody meant.
  for (const id of [...(yield* queues())].reverse()) {
    // Summarised first: what this loop asks of a queue it does not want is two
    // fields, and folding one to find out verifies a signature per record it
    // will then discard. Only the queue that matches is folded.
    const brief = yield* summary(id).pipe(
      Effect.catchTags({
        Invalid: () => Effect.succeed(null),
        ObjectNotFound: () => Effect.succeed(null),
        StorageFailure: () => Effect.succeed(null),
      }),
    );
    if (brief === null) {
      unreadable.push(id);
      continue;
    }
    // A closed queue is history, and stepping aside is the whole point of
    // ending one: the next `queue open` for this branch has to be able to
    // succeed, and every caller asking "which queue serves this branch?" has to
    // get the live one.
    if (brief.target !== target || brief.closed !== null) continue;

    const state = yield* project(id).pipe(
      Effect.catchTags({
        Invalid: () => Effect.succeed(null),
        ObjectNotFound: () => Effect.succeed(null),
        StorageFailure: () => Effect.succeed(null),
      }),
    );
    if (state === null) {
      unreadable.push(id);
      continue;
    }
    found = state;
    break;
  }
  // `unreadable` is therefore complete exactly when nothing matched, which is
  // the case its one caller — the guard in `queue open` — is asked about.
  return { found, unreadable } as const;
});
