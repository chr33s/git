/**
 * Tasks: what needs doing, and who is on it.
 *
 * Sessions record work already chosen and pull requests record work already
 * done. Neither says what is *available*, so without this a fleet coordinates
 * off-repo — in exactly the platform database this design exists to remove.
 *
 * A claim is a **lease, and advisory**. It is live until its expiry or until
 * its holder lets go, so a sandbox that dies claiming work frees it by doing
 * nothing — the same aging-out that grant expiries give membership. Nothing at
 * the policy boundary enforces exclusivity: a claim coordinates honest agents
 * rather than restraining hostile ones, and saying so is better than
 * pretending a lease is a lock. What it does inherit is the namespace's
 * append-only rules — a task is closed by saying so, never by deletion.
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
import * as Tombstone from "./Tombstone.ts";

export const refOf = (task: string): string => `refs/hub/task/${task}`;

/** The task a hub ref names, or `null` for a ref that is not one. */
export const taskOf = (ref: string): string | null => {
  if (!ref.startsWith("refs/hub/task/")) return null;
  const id = ref.slice("refs/hub/task/".length);
  return id.length === 0 || id.includes("/") ? null : id;
};

/** Asked of the ref this would actually write; see `Session.isSessionId`. */
export const isTaskId = (id: string): boolean => {
  if (id.length === 0 || id.length > 128 || id.includes("/")) return false;
  return checkRefName(refOf(id)) === null;
};

export const newId = Event.newId;

/** Every task this repository holds, by id. */
export const tasks = Effect.fn("hub.Task.tasks")(function* () {
  const repository = yield* Repository;
  const ids: Array<string> = [];
  for (const [name] of yield* repository.refs) {
    const id = taskOf(name);
    if (id !== null) ids.push(id);
  }
  return ids.sort();
});

const envelope = {
  version: Schema.Literal(1),
  repo: Schema.String,
  /** The task this event belongs to. */
  task: Schema.String,
  id: Schema.String,
  issuedAt: Schema.String,
  trustHead: Schema.NullOr(Schema.String),
};

export const TaskOpened = Schema.Struct({
  type: Schema.tag("task.opened"),
  ...envelope,
  title: Schema.String,
  description: Schema.String,
  /** What it concerns: refs, commits, pull requests. */
  refs: Schema.Array(Schema.String),
  pulls: Schema.Array(Schema.String),
  /**
   * The task this one belongs to, if any.
   *
   * Optional rather than an empty string, so a task with no parent encodes
   * exactly the bytes it always did — these records are signed, and a field
   * that appeared in every payload would invalidate every existing signature.
   */
  parent: Schema.optional(Schema.String),
});

/**
 * The task this one belongs to, from now on.
 *
 * Work moves between the things it belongs to — a release slips, an epic is
 * split — and `task.opened` is issued once on a ref that cannot be rewound.
 * Without this, where a task sits would be decided for good by whoever opened
 * it, which is not how anyone plans. The empty string detaches it.
 *
 * This is deliberately the general edge and not a "milestone": the hub knows
 * that one task belongs to another, and what that means — a release, an epic,
 * a parent story — is the reader's to name.
 */
export const TaskReparented = Schema.Struct({
  type: Schema.tag("task.reparented"),
  ...envelope,
  parent: Schema.String,
});

/**
 * A member takes the work, until a stated time.
 *
 * `expiresAt` is required and not optional: a claim with no end is a lock, and
 * a lock held by a sandbox that died is work nobody can pick up again.
 */
export const TaskClaimed = Schema.Struct({
  type: Schema.tag("task.claimed"),
  ...envelope,
  expiresAt: Schema.String,
});

export const TaskReleased = Schema.Struct({ type: Schema.tag("task.released"), ...envelope });

/**
 * Undo a close.
 *
 * A task ref cannot be deleted or rewound, so without this a close is
 * permanent — and a close is the cheapest thing to get wrong. The namespace's
 * own answer to a mistake is another record saying so.
 */
export const TaskReopened = Schema.Struct({ type: Schema.tag("task.reopened"), ...envelope });

export const TaskClosed = Schema.Struct({
  type: Schema.tag("task.closed"),
  ...envelope,
  outcome: Schema.Literals(["completed", "abandoned", "superseded"]),
  /** What resolved it, so the outcome joins the provenance graph. */
  pulls: Schema.Array(Schema.String),
  sessions: Schema.Array(Schema.String),
});

/** A record removed from this task; see `Session`'s own and `Tombstone.ts`. */
export const RecordRedacted = Schema.Struct({
  type: Schema.tag("event.redacted"),
  ...envelope,
  ...Tombstone.fields,
});

export const TaskPayload = Schema.Union([
  TaskOpened,
  TaskClaimed,
  TaskReleased,
  TaskClosed,
  TaskReopened,
  TaskReparented,
  RecordRedacted,
]);
export type TaskPayload = (typeof TaskPayload)["Type"];

const decodePayload = Schema.decodeUnknownEffect(TaskPayload);
const encoder = new TextEncoder();

export const encode = (payload: TaskPayload): Uint8Array => {
  const { id, issuedAt, repo, task, trustHead, type, version, ...rest } = payload;
  return encoder.encode(
    `${JSON.stringify({ version, type, repo, task, id, issuedAt, trustHead, ...rest }, null, 2)}\n`,
  );
};

/** As a session's: far above any honest task, far below a transcript. */
export const MAX_PAYLOAD = 256 * 1024;

export const decode = Effect.fn("hub.Task.decode")(function* (bytes: Uint8Array) {
  const json = yield* Effect.try({
    try: () => JSON.parse(new TextDecoder().decode(bytes)),
    catch: () => new Invalid({ field: "task", reason: "task event is not valid JSON" }),
  });
  return yield* decodePayload(json).pipe(
    Effect.mapError(
      (issue) => new Invalid({ field: "task", reason: `malformed task event: ${issue.message}` }),
    ),
  );
});

export const context = Effect.fn("hub.Task.context")(function* (repo: string, task: string) {
  const repository = yield* Repository;
  const trustHead = yield* repository.resolve(TRUST_LOG);
  return {
    version: 1,
    repo,
    task,
    id: newId(),
    issuedAt: new Date().toISOString(),
    trustHead,
  } as const;
});

/** The parts of a record somebody wrote; see `Session`'s own. */
const prose = (payload: TaskPayload): string => {
  switch (payload.type) {
    case "task.opened":
      return [payload.title, payload.description].join("\n");
    case "task.claimed":
    case "task.released":
      return "";
    case "task.closed":
      return payload.outcome;
    case "task.reopened":
      return "";
    // A task id, not prose: high-entropy by construction, like the envelope's.
    case "task.reparented":
      return "";
    case "event.redacted":
      return payload.reason;
  }
};

/** The edge a record carries, or `undefined` where it carries none. */
const parentOf = (payload: TaskPayload): string | undefined =>
  payload.type === "task.opened" || payload.type === "task.reparented" ? payload.parent : undefined;

export const issue = Effect.fn("hub.Task.issue")(function* (payload: TaskPayload, key: PrivateKey) {
  if (!isTaskId(payload.task)) {
    return yield* new Invalid({
      field: "task",
      reason: `'${payload.task}' cannot name a task; it must be one ref path component`,
    });
  }

  // An expiry nothing can parse is the worst of both readings: `NaN` is
  // neither past nor future, so such a claim never expires *and* never counts
  // as live — it holds the slot for ever on an append-only ref while
  // `available` says the work is free, and two agents both take it with
  // neither recorded. Refused where it is written, which is the only place it
  // can still be refused.
  if (payload.type === "task.claimed" && !Number.isFinite(Date.parse(payload.expiresAt))) {
    return yield* new Invalid({
      field: "expiresAt",
      reason: `'${payload.expiresAt}' is not a time; a claim is a lease and has to say when it ends`,
    });
  }

  // Refused here for the reason the expiry above is: an append-only ref keeps
  // whatever it is given, and a parent that cannot name a task is an edge no
  // reader can follow.
  //
  // Not a guarantee, and not meant as one: `POST /hub/events` appends signed
  // bytes without coming through here, and two members filing A under B and B
  // under A at the same time each write a ref that is sound on its own. What
  // this catches is the mistake made in one place, early, with a reason
  // attached — `tasks()` is where a loop that got in anyway stops being one.
  const parent = parentOf(payload);
  if (parent !== undefined && parent !== "") {
    if (!isTaskId(parent)) {
      return yield* new Invalid({
        field: "parent",
        reason: `'${parent}' cannot name a task; it must be one ref path component`,
      });
    }
    if (yield* loops(payload.task, parent)) {
      return yield* new Invalid({
        field: "parent",
        reason:
          parent === payload.task
            ? `${payload.task} cannot belong to itself`
            : `filing ${payload.task} under ${parent} would close a loop`,
      });
    }
  }

  const bytes = encode(payload);
  if (bytes.length > MAX_PAYLOAD) {
    return yield* new Invalid({
      field: "task",
      reason: `a task record may not exceed ${MAX_PAYLOAD} bytes; this one is ${bytes.length}`,
    });
  }

  // Scanned as a session's prose is, and for the same reason: a task
  // description is somewhere a credential gets pasted, and this ref is
  // append-only and replicates. Over what somebody typed, not over the record,
  // whose own identifiers are high-entropy by construction.
  const leaked = Secrets.scan(prose(payload));
  if (leaked.length > 0) {
    return yield* new Invalid({
      field: "task",
      reason: `this record looks like it carries ${leaked
        .map((finding) => `a ${finding.kind} (${finding.hint})`)
        .join(", ")}`,
    });
  }

  const signature = yield* sign(key, bytes, NAMESPACE);
  return yield* Event.appendTo({
    ref: refOf(payload.task),
    message: `${payload.type} ${payload.id}\n`,
    payload: bytes,
    signatures: [signature],
  });
});

export const open = Effect.fn("hub.Task.open")(function* (input: {
  readonly repo: string;
  readonly title: string;
  readonly key: PrivateKey;
  readonly description?: string;
  readonly refs?: ReadonlyArray<string>;
  readonly pulls?: ReadonlyArray<string>;
  readonly task?: string;
  readonly parent?: string;
}) {
  const task = input.task ?? newId();
  const base = yield* context(input.repo, task);
  const opened: TaskPayload = {
    ...base,
    type: "task.opened",
    title: input.title,
    description: input.description ?? "",
    refs: input.refs ?? [],
    pulls: input.pulls ?? [],
  };
  // The field is left off entirely when there is no parent, so the encoded
  // bytes are the ones this record has always had; see `TaskOpened.parent`.
  const commit = yield* issue(
    input.parent === undefined || input.parent === ""
      ? opened
      : { ...opened, parent: input.parent },
    input.key,
  );
  return { task, commit };
});

/**
 * File a task under another one, or detach it with an empty `parent`.
 *
 * Written on the moving task's own ref rather than on the one it joins: an
 * agent already writes there, one ref per task keeps a busy parent from
 * becoming a ref every member contends on, and filing work under a release
 * does not need rights over the release.
 */
export const reparent = Effect.fn("hub.Task.reparent")(function* (input: {
  readonly repo: string;
  readonly task: string;
  readonly parent: string;
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.task);
  return yield* issue({ ...base, type: "task.reparented", parent: input.parent }, input.key);
});

export const claim = Effect.fn("hub.Task.claim")(function* (input: {
  readonly repo: string;
  readonly task: string;
  readonly key: PrivateKey;
  readonly ttlSeconds: number;
}) {
  if (input.ttlSeconds <= 0) {
    return yield* new Invalid({
      field: "ttl",
      reason: "a claim is a lease and needs a positive lifetime",
    });
  }
  const base = yield* context(input.repo, input.task);
  return yield* issue(
    {
      ...base,
      type: "task.claimed",
      expiresAt: new Date(Date.now() + input.ttlSeconds * 1000).toISOString(),
    },
    input.key,
  );
});

export const release = Effect.fn("hub.Task.release")(function* (input: {
  readonly repo: string;
  readonly task: string;
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.task);
  return yield* issue({ ...base, type: "task.released" }, input.key);
});

export const close = Effect.fn("hub.Task.close")(function* (input: {
  readonly repo: string;
  readonly task: string;
  readonly key: PrivateKey;
  readonly outcome?: "completed" | "abandoned" | "superseded";
  readonly pulls?: ReadonlyArray<string>;
  readonly sessions?: ReadonlyArray<string>;
}) {
  const base = yield* context(input.repo, input.task);
  return yield* issue(
    {
      ...base,
      type: "task.closed",
      outcome: input.outcome ?? "completed",
      pulls: input.pulls ?? [],
      sessions: input.sessions ?? [],
    },
    input.key,
  );
});

/**
 * Who signed a record, or `null`.
 *
 * A record nobody signed decides nothing here — not because a signature is
 * checked against the trust graph (the boundary does that when it accepts the
 * append), but because these events are *about* each other: which key closed a
 * task is what says whether the close counts.
 */
const signerOf = Effect.fn("hub.Task.signerOf")(function* (
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

export const reopen = Effect.fn("hub.Task.reopen")(function* (input: {
  readonly repo: string;
  readonly task: string;
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.task);
  return yield* issue({ ...base, type: "task.reopened" }, input.key);
});

/** One task's events, oldest first; see `Session.entries` for the shape. */
export const entries = Effect.fn("hub.Task.entries")(function* (task: string) {
  const repository = yield* Repository;

  const head = yield* repository.resolve(refOf(task));
  if (head === null) return { events: [], unreadable: [] } as const;

  const parents = yield* Dag.reachable(head, null, Event.isHubCommit, yield* Event.ceilingOf());
  const events: Array<{
    readonly commit: Oid;
    readonly payload: TaskPayload;
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

/**
 * Remove one record's content from this task; see `Session.redact`.
 *
 * A task's own records are titles and descriptions rather than prompts, which
 * makes this rarer here — and the namespace is the same append-only ref with
 * the same one way back, so the verb exists on both.
 */
export const redact = Effect.fn("hub.Task.redact")(function* (input: {
  readonly repo: string;
  readonly task: string;
  readonly target: string;
  readonly reason: string;
  readonly key: PrivateKey;
}) {
  yield* Tombstone.permitted(input.key);

  const walked = yield* entries(input.task);
  const claimants = walked.events.filter(({ payload }) => payload.id === input.target);
  if (claimants.length === 0) {
    return yield* new Invalid({
      field: "target",
      reason: `${input.task} has no record ${input.target}`,
    });
  }
  if (claimants.length > 1) {
    return yield* new Invalid({
      field: "target",
      reason: `${input.task} has ${claimants.length} records claiming ${input.target}`,
    });
  }
  const target = claimants[0]!;
  if (target.payload.type === "event.redacted") {
    return yield* new Invalid({
      field: "target",
      reason: "a tombstone is the record of a removal and is not itself removable",
    });
  }

  const base = yield* context(input.repo, input.task);
  return yield* issue(
    {
      ...base,
      type: "event.redacted",
      target: input.target,
      targetCommit: Event.qualify(target.commit),
      reason: input.reason,
    },
    input.key,
  );
});

/**
 * Whether filing `task` under `parent` would close a loop.
 *
 * Walked upwards from the proposed parent, which is the only direction the
 * edge is recorded in. A chain that is already circular above the proposed
 * parent ends the walk without blaming this record for it: the answer is
 * about the edge being written, not about the state it is being written into.
 */
const loops = Effect.fn("hub.Task.loops")(function* (task: string, parent: string) {
  const seen = new Set<string>();
  let at = parent;
  while (!seen.has(at)) {
    if (at === task) return true;
    seen.add(at);
    const above = (yield* project(at)).parent;
    if (above === null) return false;
    at = above;
  }
  return false;
});

/**
 * What a task amounts to now: is it open, is it claimed, and by whom.
 *
 * The lease is judged against a clock the caller supplies, because the events
 * carry no trustworthy one of their own: `expiresAt` is written by whoever
 * claimed, and a projection that believed it without a local clock would call
 * a claim live for as long as its holder cared to type.
 *
 * A release or a close ends the claim it follows. Concurrent claims are
 * ordinary divergence — the causally earliest one holds, and a later claimant
 * reads that here and backs off, which is all a lease can ask.
 */
export const project = Effect.fn("hub.Task.project")(function* (task: string, now?: Date) {
  const walked = yield* entries(task);
  const at = now ?? new Date();

  let opened: { readonly payload: TaskPayload; readonly by: Fingerprint | null } | null = null;
  let claim: {
    readonly by: Fingerprint | null;
    readonly commit: Oid;
    readonly expiresAt: string;
  } | null = null;
  let closed: { readonly outcome: string; readonly pulls: ReadonlyArray<string> } | null = null;
  let parent: string | null = null;
  const sessions: Array<string> = [];
  const ignored: Array<Oid> = [];
  const redacted: Array<string> = [];

  const live = (held: typeof claim): boolean =>
    held !== null && Date.parse(held.expiresAt) > at.getTime();

  for (const { commit, payload, signer } of walked.events) {
    switch (payload.type) {
      case "task.opened":
        if (opened === null) {
          opened = { payload, by: signer };
          parent = payload.parent === undefined || payload.parent === "" ? null : payload.parent;
        }
        break;

      case "task.claimed":
        // The earliest live claim holds: a second claimant appending while one
        // is still live is racing, not taking over.
        if (!live(claim)) claim = { by: signer, commit, expiresAt: payload.expiresAt };
        break;

      case "task.released":
        // A claim is its holder's to let go of. Anybody's release freed work
        // another agent was actively holding, on a ref that cannot be rewound
        // — which is a way to take a task off whoever has it, not a way to
        // coordinate with them.
        if (claim !== null && signer !== null && signer === claim.by) claim = null;
        else ignored.push(commit);
        break;

      case "event.redacted":
        // A removal says nothing about whether the task is open or claimed,
        // and the record it names already reads as unreadable once its payload
        // is gone. Whether the tombstone counted is the trust graph's answer,
        // asked where it decides something — `Tombstone.counts`, for `gc`.
        redacted.push(payload.targetCommit);
        break;

      case "task.reparented":
        // Any member's to re-file, unlike closing it — deliberately the looser
        // rule of the two. Filing work under a release is triage, it is undone
        // by another `task.reparented`, and the boundary already asks for a
        // `hub.*` capability before this ref can be appended to at all. A
        // close ends the work and a redaction destroys content; neither is
        // taken back by saying so again, which is why those stay the opener's.
        //
        // A record nobody signed still decides nothing, here as everywhere.
        if (opened === null || signer === null) {
          ignored.push(commit);
          break;
        }
        // The last one that counts is where the task sits — this is the event
        // that exists precisely so the answer can change.
        parent = payload.parent === "" ? null : payload.parent;
        break;

      case "task.closed":
      case "task.reopened":
        // And a task is its opener's to settle. Left open to any signer, a
        // member holding the lowest hub capability could close somebody else's
        // task for good — `hub/Projection.ts` charges `hub.merge` for exactly
        // this on a pull request, and this is the same denial.
        if (opened === null || signer === null || signer !== opened.by) {
          ignored.push(commit);
          break;
        }
        if (payload.type === "task.reopened") {
          closed = null;
          break;
        }
        claim = null;
        closed = { outcome: payload.outcome, pulls: payload.pulls };
        sessions.push(...payload.sessions);
        break;
    }
  }

  const holding = live(claim);
  const first = opened?.payload;
  return {
    task,
    exists: opened !== null,
    title: first?.type === "task.opened" ? first.title : "",
    description: first?.type === "task.opened" ? first.description : "",
    refs: first?.type === "task.opened" ? first.refs : [],
    /** The task this one belongs to; `children` is the lister's to derive. */
    parent,
    // `available` is the question an agent woken by a task actually asks.
    available: opened !== null && closed === null && !holding,
    claim: holding && claim !== null ? { by: claim.by, expiresAt: claim.expiresAt } : null,
    closed,
    sessions,
    // Said out loud rather than swallowed: a record that did not count is
    // exactly what somebody will be looking for.
    ignored,
    redacted,
    unreadable: walked.unreadable,
  };
});
