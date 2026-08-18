/**
 * Sessions: what an agent was told, and what came of it.
 *
 * A commit message records what changed; a session records what was *asked
 * for*. In a repository whose commits are increasingly agent-authored, that
 * answer otherwise lives in a hosting provider's session database, reachable
 * only through a URL in a trailer — the one dependency this design exists to
 * remove. Here it is a signed, append-only DAG per session, on exactly the
 * machinery a pull request already uses.
 *
 * Two event kinds and no more, deliberately (docs/agents.md §15). `opened`
 * carries the instruction and who was carrying it out; `produced` carries what
 * landed. A plan, a summary and a token count are fields on those rather than
 * events of their own: each additional kind is a schema, a projection case and
 * a verb, and none of the three would tell a reader anything the two do not.
 *
 * What is *not* here is the transcript. The canonical record is the distilled
 * minimum, bounded, and small enough to keep forever; tool output and file
 * dumps are the highest secret-leak content class in the system and belong
 * nowhere that replicates append-only (docs/agents.md §6).
 */
import { Effect, Schema } from "effect";

import { NAMESPACE, type PrivateKey, sign } from "../crypto/SshSignature.ts";
import { Repository } from "../git/Repository.ts";
import { TRUST_LOG } from "../git/Refspec.ts";
import { checkRefName } from "../git/Store.ts";
import { Invalid } from "../git/Error.ts";
import * as Dag from "../git/Dag.ts";
import type { Oid } from "../git/Store.ts";
import * as Record from "../trust/Record.ts";
import * as Event from "./Event.ts";

/** Where one session's events live. */
export const refOf = (session: string): string => `refs/hub/session/${session}`;

/** The session a hub ref names, or `null` for a ref that is not one. */
export const sessionOf = (ref: string): string | null => {
  if (!ref.startsWith("refs/hub/session/")) return null;
  const id = ref.slice("refs/hub/session/".length);
  return id.length === 0 || id.includes("/") ? null : id;
};

/**
 * Whether an id can name a session ref.
 *
 * Asked of the ref this would actually write, for the reason
 * `isPullRequestId` gives: a second list of what git refuses drifts from the
 * first, and the failure lands after the objects are already written.
 */
export const isSessionId = (id: string): boolean => {
  if (id.length === 0 || id.length > 128 || id.includes("/")) return false;
  return checkRefName(refOf(id)) === null;
};

export const newId = Event.newId;

/** Every session this repository holds, by id. */
export const sessions = Effect.fn("hub.Session.sessions")(function* () {
  const repository = yield* Repository;
  const ids: Array<string> = [];
  for (const [name] of yield* repository.refs) {
    const id = sessionOf(name);
    if (id !== null) ids.push(id);
  }
  return ids.sort();
});

/**
 * What every session event carries, whatever it says.
 *
 * The same shape a hub event carries, with `session` where a pull request has
 * `pr`: the repository so a record cannot be replayed into another one, an id
 * of its own, and the trust head its author signed against.
 */
const envelope = {
  version: Schema.Literal(1),
  repo: Schema.String,
  /** The session this event belongs to. */
  session: Schema.String,
  id: Schema.String,
  issuedAt: Schema.String,
  /** `null` means the author recorded none; see `Event`'s own envelope. */
  trustHead: Schema.NullOr(Schema.String),
};

/**
 * Who was working, under what instruction.
 *
 * `agent` is free-form and unverified — a signer says what harness and model
 * they were, and nothing can check it (docs/agents.md §13). The signing key is
 * the verified part. `context.instructions` pins the standing instructions in
 * force as a blob or tree id, which costs one oid because those files are
 * already objects in this repository, and turns "what was this agent told?"
 * from an inference into a lookup.
 */
export const SessionOpened = Schema.Struct({
  type: Schema.tag("session.opened"),
  ...envelope,
  agent: Schema.Struct({
    kind: Schema.String,
    model: Schema.String,
    harness: Schema.String,
  }),
  prompt: Schema.String,
  /** `user` for an operator's instruction, `system` for a standing one. */
  role: Schema.Literals(["user", "system"]),
  context: Schema.NullOr(Schema.Struct({ instructions: Schema.String })),
});

/**
 * What the session produced, and what it learned.
 *
 * `commits` is the binding a `Session:` trailer is checked against. `note` is
 * where the distillation lands — intent, outcome, what fought the tooling —
 * written by the harness's stop hook, which is the one place that holds the
 * transcript at the moment it is still worth summarizing. `usage` is an
 * operational label: cheap to carry, impossible to reconstruct later, and
 * self-reported like everything else the signer says about itself.
 */
export const SessionProduced = Schema.Struct({
  type: Schema.tag("session.produced"),
  ...envelope,
  commits: Schema.Array(Schema.String),
  refs: Schema.Array(Schema.String),
  pulls: Schema.Array(Schema.String),
  note: Schema.NullOr(Schema.String),
  usage: Schema.NullOr(
    Schema.Struct({
      inputTokens: Schema.Int,
      outputTokens: Schema.Int,
    }),
  ),
});

export type SessionOpened = (typeof SessionOpened)["Type"];

/**
 * A question only a person can answer.
 *
 * Agents block on judgement calls, and the only channel for one was a comment
 * nobody could act on mechanically. Recorded here, the human's word becomes a
 * signed, causally-placed part of the same account as the work it unblocked.
 */
export const DecisionRequested = Schema.Struct({
  type: Schema.tag("decision.requested"),
  ...envelope,
  question: Schema.String,
  options: Schema.Array(Schema.String),
  /** What the question is about: refs, commits, pull requests. */
  refs: Schema.Array(Schema.String),
});

/**
 * The answer — the one projected record a harness may treat as instruction.
 *
 * Everything else a session holds is somebody's account of their own work, and
 * docs/agents.md §8 says to read it as data. This is the carve-out, and it is
 * narrow on purpose: an answer to a question *this* session asked, from a key
 * the trust graph can name. What the answer authorizes is still judged
 * downstream, on the push it leads to.
 */
export const DecisionResolved = Schema.Struct({
  type: Schema.tag("decision.resolved"),
  ...envelope,
  /** The `decision.requested` this answers. */
  decision: Schema.String,
  chose: Schema.String,
  note: Schema.NullOr(Schema.String),
});

export const SessionPayload = Schema.Union([
  SessionOpened,
  SessionProduced,
  DecisionRequested,
  DecisionResolved,
]);
export type SessionPayload = (typeof SessionPayload)["Type"];

const decodePayload = Schema.decodeUnknownEffect(SessionPayload);
const encoder = new TextEncoder();

/**
 * The bytes that are signed and the bytes that are stored, in one encoding.
 *
 * Two encodings that agree today are two that can drift, and the failure would
 * be signatures that verify nowhere. Key order is fixed here for the same
 * reason: a signature covers bytes, not a value.
 */
export const encode = (payload: SessionPayload): Uint8Array => {
  const { id, issuedAt, repo, session, trustHead, type, version, ...rest } = payload;
  return encoder.encode(
    `${JSON.stringify({ version, type, repo, session, id, issuedAt, trustHead, ...rest }, null, 2)}\n`,
  );
};

/**
 * How large one session record may be.
 *
 * The canonical record is a distillation, and a bound is what keeps it one: a
 * caller with a transcript to store will otherwise store it here, where it
 * replicates to every clone and a redaction is the only way back out. Far
 * above any honest prompt and far below any transcript.
 */
export const MAX_PAYLOAD = 256 * 1024;

export const decode = Effect.fn("hub.Session.decode")(function* (bytes: Uint8Array) {
  const json = yield* Effect.try({
    try: () => JSON.parse(new TextDecoder().decode(bytes)),
    catch: () => new Invalid({ field: "session", reason: "session event is not valid JSON" }),
  });

  return yield* decodePayload(json).pipe(
    Effect.mapError(
      (issue) =>
        new Invalid({ field: "session", reason: `malformed session event: ${issue.message}` }),
    ),
  );
});

/**
 * The envelope every session event shares, filled in from the repository.
 *
 * `trustHead` is the log head this signer had seen. It is theirs to state and
 * the fold holds it to a floor rather than trusting it, but recording the real
 * one is what lets an honest event be judged against the membership its author
 * could actually see.
 */
export const context = Effect.fn("hub.Session.context")(function* (repo: string, session: string) {
  const repository = yield* Repository;
  const trustHead = yield* repository.resolve(TRUST_LOG);
  return {
    version: 1,
    repo,
    session,
    id: newId(),
    issuedAt: new Date().toISOString(),
    trustHead,
  } as const;
});

/** Open a session: who is working, and what they were asked for. */
export const open = Effect.fn("hub.Session.open")(function* (input: {
  readonly repo: string;
  readonly agent: { readonly kind: string; readonly model: string; readonly harness: string };
  readonly prompt: string;
  readonly key: PrivateKey;
  readonly role?: "user" | "system";
  /** `null` where the caller pinned no standing instructions. */
  readonly instructions?: string | null;
  /** Supplied only when a caller is reproducing a known session. */
  readonly session?: string;
}) {
  const session = input.session ?? newId();
  const base = yield* context(input.repo, session);
  const commit = yield* issue(
    {
      ...base,
      type: "session.opened",
      agent: input.agent,
      prompt: input.prompt,
      role: input.role ?? "user",
      context:
        input.instructions === undefined || input.instructions === null
          ? null
          : { instructions: input.instructions },
    },
    input.key,
  );
  return { session, commit };
});

/** Record what a session produced: the binding a `Session:` trailer names. */
export const produced = Effect.fn("hub.Session.produced")(function* (input: {
  readonly repo: string;
  readonly session: string;
  readonly key: PrivateKey;
  readonly commits?: ReadonlyArray<string>;
  readonly refs?: ReadonlyArray<string>;
  readonly pulls?: ReadonlyArray<string>;
  readonly note?: string | null;
  readonly usage?: { readonly inputTokens: number; readonly outputTokens: number } | null;
}) {
  const base = yield* context(input.repo, input.session);
  return yield* issue(
    {
      ...base,
      type: "session.produced",
      commits: input.commits ?? [],
      refs: input.refs ?? [],
      pulls: input.pulls ?? [],
      note: input.note ?? null,
      usage: input.usage ?? null,
    },
    input.key,
  );
});

/** Sign one session event and append it to its session's ref. */
export const issue = Effect.fn("hub.Session.issue")(function* (
  payload: SessionPayload,
  key: PrivateKey,
) {
  if (!isSessionId(payload.session)) {
    return yield* new Invalid({
      field: "session",
      reason: `'${payload.session}' cannot name a session; it must be one ref path component`,
    });
  }

  const bytes = encode(payload);
  // Refused before anything is written, rather than discovered as a ref that
  // will not replicate: an oversized record is a caller storing the wrong
  // thing, and telling them so is more useful than storing it.
  if (bytes.length > MAX_PAYLOAD) {
    return yield* new Invalid({
      field: "session",
      reason: `a session record may not exceed ${MAX_PAYLOAD} bytes; this one is ${bytes.length}`,
    });
  }

  const signature = yield* sign(key, bytes, NAMESPACE);
  return yield* Event.appendTo({
    ref: refOf(payload.session),
    message: `${payload.type} ${payload.id}\n`,
    payload: bytes,
    signatures: [signature],
  });
});

/** Ask a question this session cannot answer for itself. */
export const ask = Effect.fn("hub.Session.ask")(function* (input: {
  readonly repo: string;
  readonly session: string;
  readonly key: PrivateKey;
  readonly question: string;
  readonly options?: ReadonlyArray<string>;
  readonly refs?: ReadonlyArray<string>;
}) {
  const base = yield* context(input.repo, input.session);
  yield* issue(
    {
      ...base,
      type: "decision.requested",
      question: input.question,
      options: input.options ?? [],
      refs: input.refs ?? [],
    },
    input.key,
  );
  return base.id;
});

/** Answer one, which anybody holding a key of their own may do. */
export const answer = Effect.fn("hub.Session.answer")(function* (input: {
  readonly repo: string;
  readonly session: string;
  readonly key: PrivateKey;
  readonly decision: string;
  readonly chose: string;
  readonly note?: string | null;
}) {
  const base = yield* context(input.repo, input.session);
  return yield* issue(
    {
      ...base,
      type: "decision.resolved",
      decision: input.decision,
      chose: input.chose,
      note: input.note ?? null,
    },
    input.key,
  );
});

/**
 * One session's events, oldest first, with what could not be read named.
 *
 * The same walk a wake makes, and for the same reasons: bounded to this
 * namespace's own commits, stepping over joins, and treating an event this
 * version cannot decode as one event rather than a broken session. A
 * projection that failed on a single bad record would take the session's whole
 * account down with it.
 */
export const entries = Effect.fn("hub.Session.entries")(function* (session: string) {
  const repository = yield* Repository;

  const head = yield* repository.resolve(refOf(session));
  if (head === null) return { events: [], unreadable: [] } as const;

  const parents = yield* Dag.reachable(head, null, Event.isHubCommit, yield* Event.ceilingOf());
  const events: Array<{ readonly commit: Oid; readonly payload: SessionPayload }> = [];
  const unreadable: Array<Oid> = [];

  for (const commit of Dag.topological(parents)) {
    if (!(yield* Record.carries(commit, Event.RECORD))) continue;
    const record = yield* Record.read(commit, Event.RECORD).pipe(
      Effect.catchTags({
        ObjectNotFound: () => Effect.succeed(null),
        Invalid: () => Effect.succeed(null),
      }),
    );
    // A redaction deletes the payload and leaves the tree entry naming it, so
    // the read fails where every other event's succeeds. That absence is what
    // a tombstone looks like from here, and it is not a failure.
    if (record === null) {
      unreadable.push(commit);
      continue;
    }

    const payload = yield* decode(record.payload).pipe(Effect.orElseSucceed(() => null));
    if (payload === null) {
      unreadable.push(commit);
      continue;
    }
    events.push({ commit, payload });
  }

  return { events, unreadable } as const;
});

/**
 * What a session amounts to, for a reader about to continue it.
 *
 * Derived rather than stored: the events are the record, and a projection is a
 * convenience over them that any replica can rebuild. Concurrent appends are
 * ordinary DAG divergence — the walk orders them causally, and where two
 * events are concurrent the greater event id wins, which is the same rule a
 * pull request's projection settles ties with and is deterministic on every
 * replica holding the same events.
 */
export const project = Effect.fn("hub.Session.project")(function* (session: string) {
  const walked = yield* entries(session);

  let opened: SessionOpened | null = null;
  const prompts: Array<{ readonly role: "user" | "system"; readonly prompt: string }> = [];
  const commits: Array<string> = [];
  const refs: Array<string> = [];
  const pulls: Array<string> = [];
  const notes: Array<string> = [];
  let inputTokens = 0;
  let outputTokens = 0;

  const asked = new Map<
    string,
    { readonly question: string; readonly options: ReadonlyArray<string>; chose: string | null }
  >();

  for (const { payload } of walked.events) {
    if (payload.type === "decision.requested") {
      asked.set(payload.id, { question: payload.question, options: payload.options, chose: null });
      continue;
    }
    if (payload.type === "decision.resolved") {
      // An answer to a question this session never asked is somebody else's
      // record, and recording it here would put words in this session's mouth.
      const question = asked.get(payload.decision);
      if (question !== undefined) question.chose = payload.chose;
      continue;
    }
    if (payload.type === "session.opened") {
      // The opening is the first one this walk reaches; a second is somebody
      // else's claim on an id already in use, and the earlier one stands.
      if (opened === null || payload.id < opened.id) opened = payload;
      prompts.push({ role: payload.role, prompt: payload.prompt });
      continue;
    }
    commits.push(...payload.commits);
    refs.push(...payload.refs);
    pulls.push(...payload.pulls);
    if (payload.note !== null) notes.push(payload.note);
    if (payload.usage !== null) {
      inputTokens += payload.usage.inputTokens;
      outputTokens += payload.usage.outputTokens;
    }
  }

  return {
    session,
    exists: walked.events.length > 0,
    agent: opened?.agent ?? null,
    instructions: opened?.context?.instructions ?? null,
    prompts,
    commits,
    refs,
    pulls,
    notes,
    decisions: [...asked].map(([id, value]) => ({ id, ...value })),
    usage: { inputTokens, outputTokens },
    unreadable: walked.unreadable,
  };
});

/**
 * The session that last produced this branch.
 *
 * The question an agent actually has on checkout is "put me back in context
 * for this branch", not "for this id" — an id is what a caller has only if
 * they were the one who opened it. Answered by scanning the sessions that name
 * the ref and taking the newest, which UUIDv7 makes the greatest id.
 */
export const latestFor = Effect.fn("hub.Session.latestFor")(function* (ref: string) {
  let latest: string | null = null;
  for (const session of yield* sessions()) {
    const walked = yield* entries(session);
    const names = walked.events.some(
      ({ payload }) => payload.type === "session.produced" && payload.refs.includes(ref),
    );
    if (names && (latest === null || session > latest)) latest = session;
  }
  return latest;
});

/**
 * Every commit a session says it produced, walked from a given head.
 *
 * Takes the head rather than reading the ref, because the caller that needs
 * this most is the policy boundary judging a push that is *moving* that ref:
 * read from the ref, the check would ask about the session as it was before
 * the push, and a branch and its provenance arriving together — which is the
 * whole point of one receive-pack carrying both — would never pass.
 */
export const producedBy = Effect.fn("hub.Session.producedBy")(function* (head: Oid) {
  const parents = yield* Dag.reachable(head, null, Event.isHubCommit, yield* Event.ceilingOf());
  const commits = new Set<string>();

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
    if (payload === null || payload.type !== "session.produced") continue;
    for (const named of payload.commits) commits.add(named);
  }

  return commits;
});

/** The trailer a commit carries to name the session that made it. */
export const TRAILER = "Session";

/**
 * The session a commit message names, or why it names none.
 *
 * Exactly one, because two are two claims about the same commit and a rule
 * that took the last would let a second trailer overwrite the first — the
 * check exists to be answerable, not to be argued with.
 */
export const trailerOf = (
  message: string,
): { readonly session: string } | { readonly reason: string } => {
  const found = message
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.startsWith(`${TRAILER}:`))
    .map((line) => line.slice(TRAILER.length + 1).trim());

  if (found.length === 0) return { reason: `no ${TRAILER}: trailer` };
  if (found.length > 1) return { reason: `${found.length} ${TRAILER}: trailers` };
  const session = found[0]!;
  return isSessionId(session) ? { session } : { reason: `'${session}' cannot name a session` };
};

/**
 * What this repository has been told it cost, inside a window.
 *
 * Summed from what sessions report about themselves, which is the only place
 * the number exists here. A signer who under-reports is not caught by this and
 * is not meant to be: it is a budget over an honest fleet's own accounting,
 * and reading it as a spend limit would be reading it as something it cannot
 * be (docs/agents.md §23).
 */
export const usageSince = Effect.fn("hub.Session.usageSince")(function* (since: Date) {
  let inputTokens = 0;
  let outputTokens = 0;

  for (const session of yield* sessions()) {
    for (const { payload } of (yield* entries(session)).events) {
      if (payload.type !== "session.produced" || payload.usage === null) continue;
      const at = Date.parse(payload.issuedAt);
      if (!Number.isFinite(at) || at < since.getTime()) continue;
      inputTokens += payload.usage.inputTokens;
      outputTokens += payload.usage.outputTokens;
    }
  }

  return { inputTokens, outputTokens, total: inputTokens + outputTokens };
});
