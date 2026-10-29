/** Retry-safe output handoff. Persist Prepared locally before calling append. */
import { Effect, Schema } from "effect";
import type { PrivateKey } from "../crypto/SshSignature.ts";
import { Invalid } from "../git/Error.ts";
import { isOid, qualify, unqualify } from "../git/Oid.ts";
import { Repository } from "../git/Repository.ts";
import { checkRefName } from "../git/Store.ts";
import * as Secrets from "./Secrets.ts";
import * as Genesis from "../trust/Genesis.ts";
import * as Trust from "../trust/Projection.ts";
import * as Record from "../trust/Record.ts";
import * as Verify from "../trust/Verify.ts";
import { trustReach } from "./Projection.ts";
import * as Event from "./Event.ts";
import * as Session from "./Session.ts";

export const Prepared = Schema.Struct({
  version: Schema.Literal(1),
  payload: Schema.String,
  signatures: Schema.Array(Schema.String),
  expectedHead: Schema.NullOr(Schema.String),
  commit: Schema.String,
});
export interface Prepared extends Schema.Schema.Type<typeof Prepared> {}

export interface Output {
  readonly repo: string;
  readonly session: string;
  readonly key: PrivateKey;
  readonly note: string | null;
  readonly commits: ReadonlyArray<string>;
  readonly refs: ReadonlyArray<string>;
  readonly pulls: ReadonlyArray<string>;
  readonly usage: { readonly inputTokens: number; readonly outputTokens: number } | null;
}

export const prepare = Effect.fn("hub.SessionCapture.prepare")(function* (input: Output) {
  const repository = yield* Repository;
  for (const named of input.commits) {
    const oid = isOid(named) ? named : unqualify(named);
    if (oid === null)
      return yield* new Invalid({
        field: "commits",
        reason: "output links must name immutable commit OIDs",
      });
    yield* repository.readCommit(oid);
  }
  for (const ref of input.refs) {
    if (checkRefName(ref) !== null)
      return yield* new Invalid({
        field: "refs",
        reason: "output refs must be valid full ref names",
      });
  }
  for (const pr of input.pulls) {
    if (!Event.isPullRequestId(pr) || (yield* repository.resolve(Event.refOf(pr))) === null)
      return yield* new Invalid({
        field: "pulls",
        reason: "output PR links must name a local native PR",
      });
  }
  const expected = yield* repository.readRef(Session.refOf(input.session));
  const payload: typeof Session.SessionProduced.Type = {
    ...(yield* Session.context(input.repo, input.session)),
    type: "session.produced",
    note: input.note,
    commits: input.commits,
    refs: input.refs,
    pulls: input.pulls,
    usage: input.usage,
  };
  const signed = yield* Session.prepare(payload, input.key);
  const commit = yield* Record.write({
    name: Event.RECORD,
    ...signed,
    parents: expected === null ? [] : [expected],
    message: `${payload.type} ${payload.id}\n`,
    at: new Date(payload.issuedAt),
  });
  return {
    version: 1,
    payload: new TextDecoder().decode(signed.payload),
    signatures: signed.signatures,
    expectedHead: expected === null ? null : qualify(expected),
    commit: qualify(commit),
  } satisfies Prepared;
});

export const prepareOpen = Effect.fn("hub.SessionCapture.prepareOpen")(function* (input: {
  readonly repo: string;
  readonly session: string;
  readonly key: PrivateKey;
}) {
  const payload: Session.SessionOpened = {
    ...(yield* Session.context(input.repo, input.session)),
    type: "session.opened",
    agent: { kind: "knowledge-maintenance", model: "", harness: "node-worker" },
    role: "system",
    prompt: "Prepare a reviewable knowledge proposal from retained evidence.",
    context: null,
  };
  const signed = yield* Session.prepare(payload, input.key);
  const commit = yield* Record.write({
    name: Event.RECORD,
    ...signed,
    parents: [],
    message: `${payload.type} ${payload.id}\n`,
    at: new Date(payload.issuedAt),
  });
  return {
    version: 1,
    payload: new TextDecoder().decode(signed.payload),
    signatures: signed.signatures,
    expectedHead: null,
    commit: qualify(commit),
  } satisfies Prepared;
});

/** Reconcile the exact record before repeating any ref side effect. */
export const append = Effect.fn("hub.SessionCapture.append")(
  function* (prepared: Prepared, binding: { readonly repo: string; readonly session: string }) {
    const repository = yield* Repository;
    const bytes = new TextEncoder().encode(prepared.payload);
    if (bytes.length > Session.MAX_PAYLOAD)
      return yield* new Invalid({
        field: "handoff",
        reason: "prepared output exceeds the record byte limit",
      });
    const payload = yield* Session.decode(bytes);
    if (
      !Number.isFinite(Date.parse(payload.issuedAt)) ||
      (payload.trustHead !== null && !isOid(payload.trustHead)) ||
      !Session.isSessionId(payload.session)
    )
      return yield* new Invalid({ field: "handoff", reason: "invalid prepared output envelope" });
    if (payload.type === "session.produced" && Secrets.scan(payload.note ?? "").length > 0)
      return yield* new Invalid({ field: "handoff", reason: "prepared output contains a secret" });
    const expected = prepared.expectedHead === null ? null : unqualify(prepared.expectedHead);
    const commit = unqualify(prepared.commit);
    if (
      (payload.type !== "session.produced" && payload.type !== "session.opened") ||
      payload.repo !== binding.repo ||
      payload.session !== binding.session ||
      commit === null ||
      (prepared.expectedHead !== null && expected === null)
    ) {
      return yield* new Invalid({
        field: "handoff",
        reason:
          "prepared output does not match this repository/session or has invalid object identities",
      });
    }
    // Validate current authorization even when recovering an earlier attempt.
    const stored = yield* Genesis.readGenesis();
    if (stored === null || stored.genesis.repoId !== binding.repo) {
      return yield* new Invalid({ field: "handoff", reason: "no matching repository genesis" });
    }
    const reach = trustReach();
    const authorized = yield* Verify.authorize({
      projection: yield* Trust.project(stored.genesis),
      bytes,
      signatures: prepared.signatures,
      capability: "hub.session",
      made: {
        at: new Date(payload.issuedAt),
        trustHead:
          payload.trustHead !== null && isOid(payload.trustHead) ? payload.trustHead : null,
      },
      seen: reach.ancestry,
      contains: reach.contains,
    });
    if (!authorized.ok) return yield* new Invalid({ field: "handoff", reason: authorized.reason });
    const specification = {
      name: Event.RECORD,
      payload: bytes,
      signatures: prepared.signatures,
      parents: expected === null ? [] : [expected],
      message: `${payload.type} ${payload.id}\n`,
      at: new Date(payload.issuedAt),
    };
    if ((yield* Record.identify(specification)) !== commit)
      return yield* new Invalid({
        field: "handoff",
        reason: "prepared record identity does not match its retained bytes",
      });
    const walked = yield* Session.entries(binding.session);
    if (walked.parents.has(commit)) return { state: "recorded", commit, recovered: true } as const;
    if (payload.type === "session.opened" && walked.head !== null)
      return yield* new Invalid({
        field: "handoff",
        reason: "maintenance session identity is already occupied by another opening",
      });
    if (payload.type === "session.produced" && walked.head === null)
      return yield* new Invalid({
        field: "handoff",
        reason: "a produced record requires an existing session",
      });
    // Only an unaccepted record may need its prepared objects restored.
    yield* Record.write(specification);
    let destination = commit;
    if (walked.head !== expected) {
      // Preserve intervening output while keeping the prepared record immutable.
      if (walked.head === null)
        return yield* new Invalid({
          field: "handoff",
          reason: "the session disappeared after preparation",
        });
      destination = yield* repository.commitTree({
        tree: yield* repository.writeTree([]),
        parents: [walked.head, commit],
        message: "join\n",
        author: Record.identityAt(new Date(payload.issuedAt)),
      });
    }
    yield* repository.setRef({
      name: Session.refOf(binding.session),
      to: destination,
      expected: walked.head,
    });
    return { state: "recorded", commit, recovered: false } as const;
  },
  Effect.retry({ times: 3, while: (error) => error._tag === "RefConflict" }),
);
