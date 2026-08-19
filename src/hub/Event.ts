/**
 * Hub events: pull requests, reviews, comments and checks as an append-only
 * history per pull request.
 *
 * ```text
 * refs/hub/pr/<pr-id> ──▶ event ──▶ event ──▶ event
 *                            ╲                  ╱
 *                             ╲── event ──── join
 * ```
 *
 * One ref per pull request, not one per event. The earlier design gave every
 * event its own immutable ref, which converges beautifully and then sends the
 * whole set to every client on every fetch: ref count grows with comments, and
 * a stock `git clone` pays for a year of review history it cannot read. Here
 * the ref count grows with pull requests, the events hash-link like commits
 * because they *are* commits, and two replicas that diverge merge the way two
 * branches merge — with a join, keeping both sides.
 *
 * Append-only is a property of the namespace, enforced at the policy boundary:
 * an update must contain what it replaces, and nothing here is ever deleted.
 * Redaction removes a payload's *content* (`Redaction.ts`) without removing
 * the event, because a hash chain with a hole in it is not a hash chain.
 *
 * Every event carries the trust head its author was writing against. That is
 * what makes revocation deterministic across replicas: "had this author
 * already seen that revocation?" is a question about ancestry, which everyone
 * computes identically, rather than about clocks, which nobody should trust.
 */
import { Context, Effect, Layer, Option, Schema } from "effect";

import { NAMESPACE, type PrivateKey, sign } from "../crypto/SshSignature.ts";
import * as Dag from "../git/Dag.ts";
import { Invalid, type ObjectNotFound, type StorageFailure } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import { checkRefName, isOid, type Oid } from "../git/Store.ts";
import { checkCapability } from "../trust/Certificate.ts";
import type { RepoId } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import * as Record from "../trust/Record.ts";

const decoder = new TextDecoder();
const encoder = new TextEncoder();

/** The record name inside each event commit — `event.json` and `event.sig`. */
export const RECORD = "event";

/**
 * How many commits one pull request's fold will walk.
 *
 * Not a limit on the feature — a pull request with four thousand events is not
 * a conversation — but a ceiling on what an append to a hub ref can make every
 * later protected-branch push pay for. The fold relates every commit to every
 * other, which is quadratic however it is stored, and it runs synchronously on
 * the receive-pack path inside a worker with a fixed memory ceiling. At this
 * bound the ancestor bitsets are 512 bytes per commit and two megabytes in
 * all; an order of magnitude higher and the push dies where it was meant to be
 * refused, which is the failure this number exists to make impossible.
 */
export const MAX_EVENTS = 4096;

/**
 * How many pull requests one repository may hold.
 *
 * The per-pull-request ceiling bounds one fold; this bounds how many folds a
 * single protected-branch push, collection or deepening fetch has to make.
 * `refs/hub/pr/*` is append-only, so the list only ever grows and a closed
 * pull request costs the same as an open one — which makes opening them the
 * cheapest way for anybody holding `hub.create-pr` to make every later push
 * slower for good.
 *
 * Set where a repository this size is already unusual rather than where it is
 * merely large, because reaching it means no new pull request can be opened.
 */
export const MAX_PULL_REQUESTS = 65_536;

/**
 * The ceiling in force, when a host wants a different one.
 *
 * A tuning number, not an authority: the default is what every caller gets,
 * and lowering it only narrows what this host will fold. It is a service so
 * that "how big may a pull request be here" is answerable per deployment —
 * a worker with less memory than the one this bound was chosen for wants a
 * smaller one — rather than being a constant a rebuild has to change.
 */
export class Ceiling extends Context.Service<Ceiling, number>()("hub/Event/Ceiling") {}

export const ceiling = (events: number): Layer.Layer<Ceiling> => Layer.succeed(Ceiling)(events);

/** The ceiling this fold is held to. */
export const ceilingOf = Effect.fnUntraced(function* () {
  return Option.getOrElse(yield* Effect.serviceOption(Ceiling), () => MAX_EVENTS);
});

/** How many pull requests this repository may hold; see `MAX_PULL_REQUESTS`. */
export class Population extends Context.Service<Population, number>()("hub/Event/Population") {}

export const population = (pulls: number): Layer.Layer<Population> =>
  Layer.succeed(Population)(pulls);

export const populationOf = Effect.fnUntraced(function* () {
  return Option.getOrElse(yield* Effect.serviceOption(Population), () => MAX_PULL_REQUESTS);
});

/** Where a pull request's history lives. */
export const refOf = (pr: string): string => `refs/hub/pr/${pr}`;

/**
 * Whether a string may name a pull request.
 *
 * One path component, and one this repository can find again: a `/` in an id
 * puts the ref under `refs/hub/pr/team/42`, which `prOf` refuses — so
 * `pullRequests()` never lists it, `Policy.protectedBranch` can never match
 * it (and the branch it targets becomes unpushable, since a pull request that
 * cannot be found is a pull request that approves nothing), and
 * `Redaction.excluded` never honours its tombstones.
 */
export const isPullRequestId = (id: string): boolean => {
  if (id.length === 0 || id.length > 128 || id.includes("/")) return false;
  // Asked of the ref this would actually write, rather than restated here. A
  // second list of the characters git refuses drifts from the first: this one
  // was missing `..`, a leading or trailing `.`, a `.lock` suffix and `@{`, so
  // `open` validated the id, wrote the payload blob, the tree and the commit,
  // and only then failed at `setRef` — leaving the objects behind and
  // reporting a ref-name error from a call that was given no ref.
  return checkRefName(refOf(id)) === null;
};

/** The pull request a hub ref names, or `null` when it names something else. */
export const prOf = (ref: string): string | null => {
  const prefix = "refs/hub/pr/";
  if (!ref.startsWith(prefix)) return null;
  const id = ref.slice(prefix.length);
  return id.length > 0 && !id.includes("/") ? id : null;
};

/** A new identifier, generated offline — the same UUIDv7 the trust log uses. */
export const newId = Log.newId;

/**
 * An object id as a payload spells one: `sha1:<hex>`.
 *
 * Qualified even though this version only writes SHA-1, because the point of
 * qualifying is that the payloads never have to change when it does not.
 */
/**
 * A pull request's base branch as a full ref name.
 *
 * `base` is a string a client writes, and both spellings are natural — `main`
 * from a UI, `refs/heads/main` from a script. `Policy.protectedBranch` matches
 * a pull request to the branch being pushed by comparing this against a fully
 * qualified ref, so an unqualified base matched nothing: the pull request
 * stopped counting toward its own branch's approvals and made that branch
 * permanently unpushable, with a message about missing approvals rather than
 * about the spelling. Normalised where it is read, so it holds for events
 * written by any client rather than only by this one.
 */
export const branchRef = (value: string): string =>
  value.startsWith("refs/") ? value : `refs/heads/${value}`;

export const qualify = (oid: Oid): string => `sha1:${oid}`;

export const unqualify = (value: string): Oid | null => {
  const [algorithm, hex] = value.split(":");
  if (algorithm !== "sha1" || hex === undefined || !/^[0-9a-f]{40}$/.test(hex)) return null;
  // SAFETY: the pattern above is exactly the forty lowercase hex characters
  // the `Oid` brand names.
  return hex as Oid;
};

// -- payloads -------------------------------------------------------------------

const envelope = {
  version: Schema.Literal(1),
  repo: Schema.String,
  /** The pull request this event belongs to. */
  pr: Schema.String,
  id: Schema.String,
  issuedAt: Schema.String,
  /**
   * The trust log head the author signed against.
   *
   * `null` means the author recorded none, and a verifier treats that as "they
   * had seen everything" — the safe reading, since an event that cannot show
   * it predates a revocation does not get the benefit of the doubt.
   */
  trustHead: Schema.NullOr(Schema.String),
};

export const PrOpened = Schema.Struct({
  type: Schema.tag("pr.opened"),
  ...envelope,
  title: Schema.String,
  description: Schema.String,
  /** The branch this asks to change. */
  base: Schema.String,
  /** The exact revision proposed, hash-qualified. */
  head: Schema.String,
});

export const PrUpdated = Schema.Struct({
  type: Schema.tag("pr.updated"),
  ...envelope,
  head: Schema.String,
});

export const PrClosed = Schema.Struct({ type: Schema.tag("pr.closed"), ...envelope });
export const PrReopened = Schema.Struct({ type: Schema.tag("pr.reopened"), ...envelope });

export const PrMerged = Schema.Struct({
  type: Schema.tag("pr.merged"),
  ...envelope,
  /** What was merged, and what it became. */
  head: Schema.String,
  mergeCommit: Schema.String,
});

/**
 * A review of one revision.
 *
 * `head` is not decoration: an approval means "Alice approved abc123", never
 * "Alice approved this pull request". When the head moves the approval stays
 * historically true and stops applying, which is the whole reason the field is
 * required rather than derived.
 */
export const ReviewSubmitted = Schema.Struct({
  type: Schema.tag("review.submitted"),
  ...envelope,
  head: Schema.String,
  decision: Schema.Literals(["approve", "reject", "comment"]),
  body: Schema.String,
});

export const ReviewDismissed = Schema.Struct({
  type: Schema.tag("review.dismissed"),
  ...envelope,
  /** The `review.submitted` event this dismisses. */
  review: Schema.String,
  reason: Schema.String,
});

export const CommentCreated = Schema.Struct({
  type: Schema.tag("comment.created"),
  ...envelope,
  body: Schema.String,
  /** Where the comment was made, for an inline one. */
  head: Schema.NullOr(Schema.String),
  path: Schema.NullOr(Schema.String),
  side: Schema.NullOr(Schema.Literals(["old", "new"])),
  line: Schema.NullOr(Schema.Int),
  /**
   * A digest of the lines around the comment.
   *
   * What lets a later revision decide whether the comment still points at the
   * code it was about, without re-reading the revision it was made against.
   */
  contextHash: Schema.NullOr(Schema.String),
});

export const CommentReplied = Schema.Struct({
  type: Schema.tag("comment.replied"),
  ...envelope,
  /** The `comment.created` event that opened the thread. */
  thread: Schema.String,
  body: Schema.String,
});

export const CommentResolved = Schema.Struct({
  type: Schema.tag("comment.resolved"),
  ...envelope,
  thread: Schema.String,
});

export const CommentReopened = Schema.Struct({
  type: Schema.tag("comment.reopened"),
  ...envelope,
  thread: Schema.String,
});

export const CheckStarted = Schema.Struct({
  type: Schema.tag("check.started"),
  ...envelope,
  head: Schema.String,
  name: Schema.String,
  provider: Schema.String,
});

export const CheckCompleted = Schema.Struct({
  type: Schema.tag("check.completed"),
  ...envelope,
  head: Schema.String,
  name: Schema.String,
  provider: Schema.String,
  status: Schema.Literals(["success", "failure", "neutral"]),
  url: Schema.NullOr(Schema.String),
});

/**
 * A tombstone.
 *
 * Immutable-forever collides with reality: a comment can carry a credential,
 * someone's address, or content that must legally go. Deleting the object does
 * not survive replication — it comes back from the first replica that still
 * has it — so removal has to be a signed event the union rule honours.
 */
export const EventRedacted = Schema.Struct({
  type: Schema.tag("event.redacted"),
  ...envelope,
  /**
   * The commit carrying the event being removed, hash-qualified.
   *
   * Named as well as `target`, and this is the field that resolves it. An id
   * alone stops resolving the moment the removal happens: the payload it was
   * read from is the payload that was deleted, so a projection rebuilt
   * afterwards could no longer tell which commit the tombstone meant — and
   * the blob quietly stopped being excluded from packs and collection, which
   * is the removal undoing itself. A commit is stable across exactly the
   * change the tombstone makes.
   */
  targetCommit: Schema.String,
  /** The event whose payload is to be removed. */
  target: Schema.String,
  reason: Schema.String,
});

export const HubPayload = Schema.Union([
  PrOpened,
  PrUpdated,
  PrClosed,
  PrReopened,
  PrMerged,
  ReviewSubmitted,
  ReviewDismissed,
  CommentCreated,
  CommentReplied,
  CommentResolved,
  CommentReopened,
  CheckStarted,
  CheckCompleted,
  EventRedacted,
]).pipe(Schema.toTaggedUnion("type"));

export type PrOpened = (typeof PrOpened)["Type"];
export type PrUpdated = (typeof PrUpdated)["Type"];
export type PrMerged = (typeof PrMerged)["Type"];
export type ReviewSubmitted = (typeof ReviewSubmitted)["Type"];
export type ReviewDismissed = (typeof ReviewDismissed)["Type"];
export type CommentCreated = (typeof CommentCreated)["Type"];
export type CheckCompleted = (typeof CheckCompleted)["Type"];
export type EventRedacted = (typeof EventRedacted)["Type"];
export type HubPayload = (typeof HubPayload)["Type"];

const decodePayload = Schema.decodeUnknownEffect(HubPayload);

/**
 * The canonical bytes for an event.
 *
 * `JSON.stringify` over the decoded object, with the envelope written first:
 * the field order is the canonical form, and it is what every signature covers.
 * Unlike the trust payloads this does not enumerate each variant's fields —
 * there are fourteen of them, the schema has already refused anything else, and
 * a fourteen-branch ladder is a place for a field to go missing quietly.
 */
export const encode = (payload: HubPayload): Uint8Array => {
  const { id, issuedAt, pr, repo, trustHead, type, version, ...rest } = payload;
  return encoder.encode(
    `${JSON.stringify({ version, type, repo, pr, id, issuedAt, trustHead, ...rest }, null, 2)}\n`,
  );
};

export const decode = Effect.fn("hub.Event.decode")(function* (bytes: Uint8Array) {
  const json = yield* Effect.try({
    try: () => JSON.parse(decoder.decode(bytes)),
    catch: () => new Invalid({ field: "event", reason: "hub event is not valid JSON" }),
  });

  return yield* decodePayload(json).pipe(
    Effect.mapError(
      (issue) => new Invalid({ field: "event", reason: `malformed hub event: ${issue.message}` }),
    ),
  );
});

/**
 * The capability an event's signer must hold.
 *
 * An approval costs more than a review, because "approve" is what a merge
 * policy counts. A check costs its own name — `hub.check:test` signs `test`
 * and nothing else — which is what stops the least trusted CI bot in a
 * repository from being able to satisfy every required check.
 */
export const capabilityFor = (payload: HubPayload): string => {
  switch (payload.type) {
    case "pr.opened":
    case "pr.updated":
    case "pr.closed":
    case "pr.reopened":
      return "hub.create-pr";
    case "pr.merged":
      return "hub.merge";
    case "review.submitted":
      return payload.decision === "approve" ? "hub.approve" : "hub.review";
    case "review.dismissed":
      return "hub.review";
    case "comment.created":
    case "comment.replied":
    case "comment.resolved":
    case "comment.reopened":
      return "hub.comment";
    case "check.started":
    case "check.completed":
      return checkCapability(payload.name);
    case "event.redacted":
      return "hub.redact";
  }
};

/** Structural checks, before an event is treated as a statement. */
export const validate = Effect.fn("hub.Event.validate")(function* (
  payload: HubPayload,
  repo: RepoId,
) {
  if (payload.repo !== repo) {
    return yield* new Invalid({
      field: "repo",
      reason: `event is for ${payload.repo}, not ${repo}`,
    });
  }
  if (Number.isNaN(Date.parse(payload.issuedAt))) {
    return yield* new Invalid({ field: "issuedAt", reason: `not a date: '${payload.issuedAt}'` });
  }

  // The trust head is an object id, and nothing else had ever checked that it
  // was one. It is written by the event's own signer, and it is *used* as a
  // name: the fold walks the log from it, which means reading an object by
  // that name. `../HEAD` is not an object id and joins into a path outside the
  // objects directory — a read oracle, and a failure that then took down every
  // fold, every protected-branch push and every collection touching the pull
  // request carrying it, on a ref that cannot be rewound. Unqualified rather
  // than hash-qualified, unlike the revisions below, because it names a commit
  // in this repository's own trust log rather than a revision an event is
  // about.
  if (payload.trustHead !== null && !isOid(payload.trustHead)) {
    return yield* new Invalid({
      field: "trustHead",
      reason: `'${payload.trustHead}' is not an object id`,
    });
  }

  // Every revision an event names has to be one this version can resolve;
  // an unqualified oid would be a silent mismatch the day a second hash
  // arrives, which is exactly what qualifying is for.
  const revisions =
    payload.type === "pr.merged"
      ? [payload.head, payload.mergeCommit]
      : "head" in payload && payload.head !== null
        ? [payload.head]
        : [];
  for (const revision of revisions) {
    if (unqualify(revision) === null) {
      return yield* new Invalid({
        field: "head",
        reason: `'${revision}' is not a hash-qualified object id`,
      });
    }
  }
});

// -- the DAG --------------------------------------------------------------------

export interface Entry {
  readonly commit: Oid;
  readonly parents: ReadonlyArray<Oid>;
  /**
   * The event, or `null` when its payload has been redacted away.
   *
   * A tombstoned event keeps its commit, its tree and its place in the chain —
   * removing those would break the hash links every later event depends on —
   * and loses only the blob. So the entry survives with nothing in it, which
   * is exactly what a redaction is meant to leave behind.
   */
  readonly payload: HubPayload | null;
  /**
   * What the event was, recovered from the commit message.
   *
   * The message is written as `<type> <id>`, which means an event's identity
   * outlives its content. Without it a redacted event would be an anonymous
   * commit, and a projection could not say *that* something was removed —
   * only that the history has a gap in it.
   */
  readonly summary: { readonly type: string; readonly id: string } | null;
  /** The stored bytes — what the signatures cover. Empty once redacted. */
  readonly bytes: Uint8Array;
  readonly signatures: ReadonlyArray<string>;
}

/**
 * Append an event to a pull request's history.
 *
 * The compare-and-swap is on the head the event was built against, so two
 * authors appending at once produce one winner and one retry rather than a
 * lost event. Retrying is safe: adding an event does not change what it says.
 */
export const append = Effect.fn("hub.Event.append")(
  function* (payload: HubPayload, bytes: Uint8Array, signatures: ReadonlyArray<string>) {
    const repository = yield* Repository;

    const ref = refOf(payload.pr);
    // `readRef`, not `resolve`: this value is both the parent and the
    // compare-and-swap, and a symbolic ref resolves to an oid the store never
    // wrote — so the swap would conflict against a value nobody holds, on
    // every append, with nothing to point at as the cause.
    const head = yield* repository.readRef(ref);

    const commit = yield* Record.write({
      name: RECORD,
      payload: bytes,
      signatures,
      parents: head === null ? [] : [head],
      message: `${payload.type} ${payload.id}\n`,
    });

    yield* repository.setRef({ name: ref, to: commit, expected: head });
    return commit;
  },
  Effect.retry({ times: 3, while: (error) => error._tag === "RefConflict" }),
);

/**
 * Sign an event and append it.
 *
 * The bytes are encoded once and both signed and stored: two encodings that
 * agree today are two that can drift, and the failure would be signatures that
 * verify nowhere.
 */
export const issue = Effect.fn("hub.Event.issue")(function* (payload: HubPayload, key: PrivateKey) {
  const bytes = encode(payload);
  const signature = yield* sign(key, bytes, NAMESPACE);
  return yield* append(payload, bytes, [signature]);
});

/**
 * Merge two divergent heads of one pull request.
 *
 * The join carries no payload — it is structure, not a statement — and the
 * walk steps through it. Resolving divergence by choosing a head instead would
 * silently drop whatever was said on the other side.
 */
export const join = Effect.fn("hub.Event.join")(function* (pr: string, heads: ReadonlyArray<Oid>) {
  const repository = yield* Repository;
  if (heads.length < 2) {
    return yield* new Invalid({ field: "heads", reason: "a join needs two heads or more" });
  }

  const tree = yield* repository.writeTree([]);
  const commit = yield* repository.commitTree({
    tree,
    parents: heads,
    message: "join\n",
    author: Record.identityAt(new Date()),
  });
  yield* repository.setRef({ name: refOf(pr), to: commit, expected: heads[0] ?? null });
  return commit;
});

/**
 * `<type> <id>` off the commit message.
 *
 * Written by `append`, and the reason it is written: it is the only part of a
 * redacted event that survives, and "something was removed here" is a more
 * useful thing for a projection to be able to say than a silent gap.
 */
const summaryOf = Effect.fn("hub.Event.summaryOf")(function* (commit: Oid) {
  const repository = yield* Repository;
  const info = yield* repository.readCommit(commit);
  const [type = "", id = ""] = info.message.split("\n")[0]?.split(" ") ?? [];
  return type === "" || id === "" ? null : { type, id };
});

/**
 * Every event in a pull request, oldest first, parents before children.
 *
 * Every payload-carrying commit, duplicates included. Two events claiming one
 * id is not a merge to resolve — it is a forgery or a corrupt replica — but
 * deciding *which* of them is the impostor needs the trust state this walk
 * does not have, so both are handed on and `hub/Projection.ts` rules on them.
 */
export const entries = Effect.fn("hub.Event.entries")(function* (pr: string) {
  const repository = yield* Repository;

  const head = yield* repository.resolve(refOf(pr));
  if (head === null) return { events: emptyEvents, parents: emptyParents, ordered: emptyOrder };

  // Bounded to the hub's own commits. A pull request's history is written
  // here and nowhere else, so a commit with a parent from the source history
  // is not a longer event chain — it is an attempt to make every projection
  // walk the whole repository, with a quadratic ancestor map on top.
  // Bounded as the walk runs, not once it has finished: bounding the result
  // means paying for the whole history before refusing it, which on the
  // receive-pack path is the cost the ceiling existed to refuse.
  const ceiling = yield* ceilingOf();
  const parents = yield* Dag.reachable(head, null, (commit) => isHubCommit(commit), ceiling).pipe(
    Effect.mapError((error) =>
      error._tag === "Invalid"
        ? new Invalid({
            field: "pr",
            reason: `pull request '${pr}' has more than ${ceiling} events and cannot be folded`,
          })
        : error,
    ),
  );
  // Bounded in *size* as well as in shape. Folding a pull request builds an
  // ancestor set per commit, which is quadratic, and how many commits a pull
  // request has is chosen by whoever may append to it — the lowest hub
  // capability there is. On the protected-branch path that fold is
  // synchronous and inside a worker with a fixed memory ceiling, so an
  // unbounded one is a push that never returns rather than a push that is
  // refused. The bound is far above any conversation: a pull request with
  // more events than this is not one a person is having.
  const ordered = Dag.topological(parents);

  const events: Entry[] = [];
  for (const oid of ordered) {
    // Joins carry nothing: they are how two histories became one.
    if (!(yield* Record.carries(oid, RECORD))) continue;

    const summary = yield* summaryOf(oid);

    // A redaction deletes the payload blob but leaves the tree entry naming
    // it, so the read fails where every other event's succeeds. That is the
    // one absence this walk expects, and it is what a tombstone looks like
    // from here.
    // `ObjectNotFound` is a redaction; `Invalid` is a malformed signature
    // blob. Both are one unreadable event, and neither may take down the
    // projection — which would fail `Policy.gate` and with it every
    // protected-branch push in the repository.
    const record = yield* Record.read(oid, RECORD).pipe(
      Effect.catchTags({
        ObjectNotFound: () => Effect.succeed(null),
        Invalid: () => Effect.succeed(null),
      }),
    );
    if (record === null) {
      events.push({
        commit: oid,
        parents: parents.get(oid) ?? [],
        payload: null,
        summary,
        bytes: new Uint8Array(),
        signatures: [],
      });
      continue;
    }

    // An event this version cannot read is one event, not a broken pull
    // request: failing here would take out the projection, and with it every
    // protected-branch push in the repository.
    const payload = yield* decode(record.payload).pipe(Effect.orElseSucceed(() => null));
    if (payload === null) {
      events.push({
        commit: oid,
        parents: parents.get(oid) ?? [],
        payload: null,
        summary,
        bytes: record.payload,
        signatures: record.signatures,
      });
      continue;
    }

    // Duplicate ids are *not* resolved here, deliberately. This walk has no
    // trust state, so the only tie-break available to it is commit order —
    // which breaks ties by oid, which anybody who can write a hub ref can
    // grind. Resolving here let a member holding only `hub.comment` forge an
    // event re-using an approval's id, sort it first, and divert the real,
    // authorized approval into the conflict list. `hub/Projection.ts` decides
    // it instead, after the signature and capability have been checked, so the
    // first *authorized* claim is the one that stands.
    events.push({
      commit: oid,
      parents: parents.get(oid) ?? [],
      payload,
      summary,
      bytes: record.payload,
      signatures: record.signatures,
    });
  }
  // The whole DAG rides along, join commits included. A projection computing
  // ancestry from the payload-carrying events alone would find its chains cut
  // at every join — which is exactly where two concurrent histories meet, and
  // exactly where knowing which event descends from which matters.
  return { events, parents, ordered };
});

/**
 * Whether a value stays inside the fold's ceiling.
 *
 * Asked by the policy boundary before a hub ref is allowed to move, so that a
 * pull request can never *become* unfoldable. Bounded only on the fold side,
 * the ceiling turned a slow push into a bricked pull request: anybody who may
 * append could take somebody else's approved one past the line and, with it,
 * every approval the protected branch behind it was counting on.
 */
export const withinCeiling = Effect.fn("hub.Event.withinCeiling")(function* (head: Oid) {
  const walked = yield* Dag.reachable(
    head,
    null,
    (commit) => isHubCommit(commit),
    yield* ceilingOf(),
  ).pipe(
    Effect.as(true),
    Effect.catchTag("Invalid", () => Effect.succeed(false)),
  );
  return walked;
});

/**
 * Whether a commit belongs to a pull request's history.
 *
 * An event carries `event.json`; a join carries an empty tree. Anything else
 * is a commit from somewhere that is not the hub, and the walk stops there.
 */
export const isHubCommit = Effect.fn("hub.Event.isHubCommit")(function* (commit: Oid) {
  const repository = yield* Repository;
  const info = yield* repository
    .readCommit(commit)
    .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
  if (info === null) return false;
  // The tree, not only the commit. `fetchRepository` applies refs without a
  // connectivity check, so a replica can hold a commit whose tree object never
  // arrived — and this walk is what every protected-branch push, collection
  // and deepening fetch runs first. Read as a failure, one missing tree took
  // all of them out; read as "not part of this history", it is one commit the
  // walk steps over.
  const found = yield* repository
    .findPath(info.tree, `${RECORD}.json`)
    .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
  if (found !== null) return true;
  // A join carries an empty tree; anything else is not part of this history.
  // A missing tree is one commit the walk steps over; a store that *failed* to
  // answer is not. `orElseSucceed` swallowed both, so a transient read error
  // read as "not part of this history" — and this walk is the boundary of the
  // history, so the whole ref went empty. Narrowed to the same absence the two
  // reads above tolerate, a failure propagates and every caller turns it into
  // its own conservative answer instead of a silently empty one.
  //
  // Swallowed, one blip on a join's tree emptied a pull request: no events, so
  // no tombstones, so nothing excluded — and `gc` re-protected and repacked a
  // payload a valid tombstone covered, leaving bytes the operator had been
  // told were gone still clonable.
  const tree = yield* repository
    .readTree(info.tree)
    .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
  return tree?.length === 0;
});

/** A pull request's events and the DAG they were walked from. */
export interface Walked {
  readonly events: ReadonlyArray<Entry>;
  readonly parents: Dag.Parents;
  /**
   * The topological order this walk was taken in.
   *
   * Carried rather than recomputed. The fold needs the same order to build its
   * ancestor rows, and a pull request carrying a tombstone is folded twice —
   * so discarding it here meant sorting the same DAG three times over on the
   * receive-pack path.
   */
  readonly ordered: ReadonlyArray<Oid>;
}

const emptyParents: Dag.Parents = new Map();
const emptyEvents: ReadonlyArray<Entry> = [];
const emptyOrder: ReadonlyArray<Oid> = [];

/** Every pull request this repository holds events for. */
export const pullRequests = Effect.fn("hub.Event.pullRequests")(function* () {
  const repository = yield* Repository;
  const refs = yield* repository.refs;

  const ids: string[] = [];
  for (const [name] of refs) {
    const pr = prOf(name);
    if (pr !== null) ids.push(pr);
  }
  return ids.sort();
});

export type EventError = Invalid | ObjectNotFound | StorageFailure;
