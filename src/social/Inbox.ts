/** A host-local quarantine for contributions from callers without membership. */
import { Effect, Schema } from "effect";

import { fingerprint, type PrivateKey } from "../crypto/SshSignature.ts";
import { Invalid } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import { isOid, type Oid } from "../git/Store.ts";
import * as Event from "../hub/Event.ts";
import * as PullRequest from "../hub/PullRequest.ts";
import { isRepoId, readGenesis, type Genesis, type RepoId } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import type { Projection as TrustProjection } from "../trust/Projection.ts";
import * as Record from "../trust/Record.ts";
import * as Verify from "../trust/Verify.ts";

export const PENDING_PREFIX = "refs/quarantine/inbox/";
export const ADOPTED_PREFIX = "refs/quarantine/adopted/";
export const RECORD = "proposal";

const ProposalDocument = Schema.Struct({
  version: Schema.Literal(1),
  id: Schema.String,
  repo: Schema.String,
  head: Schema.String,
  base: Schema.String,
  title: Schema.String,
  description: Schema.String,
  submittedAt: Schema.String,
});

type ProposalDocument = (typeof ProposalDocument)["Type"];

export interface Proposal {
  readonly id: string;
  readonly repo: RepoId;
  readonly head: Oid;
  readonly base: string;
  readonly title: string;
  readonly description: string;
  readonly submittedAt: Date;
  readonly commit: Oid;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const decodeDocument = Schema.decodeUnknownEffect(ProposalDocument);

const refOf = (id: string): string => `${PENDING_PREFIX}${id}`;
const adoptedRefOf = (id: string): string => `${ADOPTED_PREFIX}${id}`;

/** UUIDv7 names keep unauthenticated submissions bounded to one flat ref component. */
export const isProposalId = (id: string): boolean =>
  /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(id) &&
  Event.isPullRequestId(id);

const validateId = (id: string): Effect.Effect<string, Invalid> =>
  isProposalId(id)
    ? Effect.succeed(id)
    : Effect.fail(new Invalid({ field: "id", reason: `'${id}' cannot name an inbox proposal` }));

const decode = Effect.fn("social.Inbox.decode")(function* (commit: Oid, fallbackId?: string) {
  const record = yield* Record.read(commit, RECORD).pipe(
    Effect.catchTags({
      Invalid: () => Effect.succeed(null),
      ObjectNotFound: () => Effect.succeed(null),
    }),
  );
  if (record === null) {
    if (fallbackId === undefined) {
      return yield* new Invalid({ field: "proposal", reason: "proposal metadata is missing" });
    }
    yield* validateId(fallbackId);
    const stored = yield* readGenesis();
    if (stored === null) {
      return yield* new Invalid({ field: "repo", reason: "the inbox has no repository identity" });
    }
    const repository = yield* Repository;
    const info = yield* repository.readCommit(commit);
    const configuredHead = yield* repository.head;
    const base = configuredHead.startsWith("refs/heads/") ? configuredHead : "refs/heads/main";
    const message = info.message.trim();
    const title = (message.split("\n")[0] ?? "Inbox proposal").slice(0, 256);
    return {
      id: fallbackId,
      repo: stored.genesis.repoId,
      head: commit,
      base,
      title,
      description: message.slice(0, 16_384),
      submittedAt: info.author.at,
      commit,
    } satisfies Proposal;
  }
  const json = yield* Effect.try({
    try: () => JSON.parse(decoder.decode(record.payload)),
    catch: () => new Invalid({ field: "proposal", reason: "proposal is not valid JSON" }),
  });
  const document = yield* decodeDocument(json).pipe(
    Effect.mapError(
      (issue) => new Invalid({ field: "proposal", reason: `malformed proposal: ${issue.message}` }),
    ),
  );
  yield* validateId(document.id);
  if (!isRepoId(document.repo)) {
    return yield* new Invalid({ field: "repo", reason: `'${document.repo}' is not a RepoID` });
  }
  if (!isOid(document.head)) {
    return yield* new Invalid({ field: "head", reason: `'${document.head}' is not an object id` });
  }
  if (!document.base.startsWith("refs/heads/")) {
    return yield* new Invalid({
      field: "base",
      reason: "proposal base must be a refs/heads/* ref",
    });
  }
  const submittedAt = new Date(document.submittedAt);
  if (Number.isNaN(submittedAt.getTime())) {
    return yield* new Invalid({ field: "submittedAt", reason: "proposal date is invalid" });
  }
  return { ...document, repo: document.repo, head: document.head, submittedAt, commit };
});

/** Store one proposal without adding anything to the hub projection. */
export const submit = Effect.fn("social.Inbox.submit")(function* (input: {
  readonly repo: RepoId;
  readonly head: Oid;
  readonly base: string;
  readonly title: string;
  readonly description?: string;
  readonly id?: string;
}) {
  const repository = yield* Repository;
  const genesis = yield* readGenesis();
  if (genesis === null || genesis.genesis.repoId !== input.repo) {
    return yield* new Invalid({ field: "repo", reason: "proposal targets another repository" });
  }
  yield* repository.readCommit(input.head);
  const id = yield* validateId(input.id ?? Log.newId());
  if ((yield* repository.resolve(adoptedRefOf(id))) !== null) {
    return yield* new Invalid({ field: "proposal", reason: `'${id}' was already adopted` });
  }
  const base = Event.branchRef(input.base);
  const document: ProposalDocument = {
    version: 1,
    id,
    repo: input.repo,
    head: input.head,
    base,
    title: input.title,
    description: input.description ?? "",
    submittedAt: new Date().toISOString(),
  };
  const commit = yield* Record.write({
    name: RECORD,
    payload: encoder.encode(`${JSON.stringify(document, null, 2)}\n`),
    signatures: [],
    parents: [input.head],
    message: `quarantined proposal ${id}\n`,
  });
  yield* repository.setRef({ name: refOf(id), to: commit, expected: null });
  return yield* decode(commit);
});

export const read = Effect.fn("social.Inbox.read")(function* (id: string) {
  yield* validateId(id);
  const repository = yield* Repository;
  const commit = yield* repository.resolve(refOf(id));
  if (commit === null) {
    return yield* new Invalid({ field: "proposal", reason: `no pending proposal '${id}'` });
  }
  return yield* decode(commit, id);
});

export const pending = Effect.fn("social.Inbox.pending")(function* () {
  const repository = yield* Repository;
  const proposals: Proposal[] = [];
  for (const [ref, commit] of yield* repository.refs) {
    if (!ref.startsWith(PENDING_PREFIX)) continue;
    const id = ref.slice(PENDING_PREFIX.length);
    if ((yield* repository.resolve(adoptedRefOf(id))) !== null) continue;
    proposals.push(yield* decode(commit, id));
  }
  return proposals.sort((left, right) => left.id.localeCompare(right.id));
});

/** Promote a quarantined proposal under a member's own signed opening event. */
export const adopt = Effect.fn("social.Inbox.adopt")(function* (input: {
  readonly genesis: Genesis;
  readonly trust: TrustProjection;
  readonly proposal: string;
  readonly key: PrivateKey;
}) {
  const proposal = yield* read(input.proposal);
  if (proposal.repo !== input.genesis.repoId || input.trust.repoId !== input.genesis.repoId) {
    return yield* new Invalid({ field: "repo", reason: "proposal and trust projection disagree" });
  }
  const signer = yield* fingerprint(input.key.publicKey);
  const authorized = yield* Verify.authorizeKey({
    projection: input.trust,
    signer,
    capability: "hub.create-pr",
  });
  if (!authorized.ok) {
    return yield* new Invalid({ field: "authorization", reason: authorized.reason });
  }

  const opened = yield* PullRequest.open({
    repo: input.genesis.repoId,
    title: proposal.title,
    description: proposal.description,
    base: proposal.base,
    head: proposal.head,
    key: input.key,
  });
  const repository = yield* Repository;
  yield* repository.setRef({ name: adoptedRefOf(proposal.id), to: opened.commit, expected: null });
  yield* repository.deleteRef(refOf(proposal.id));
  return opened;
});
