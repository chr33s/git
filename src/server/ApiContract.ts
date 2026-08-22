/**
 * The JSON wire contract consumed by the browser UI.
 *
 * These schemas are imported by both the HTTP declaration and the browser
 * client. A response therefore earns its TypeScript type only after runtime
 * decoding against the same schema the server uses to encode it.
 */
import { Schema } from "effect";

import { isOid } from "../git/Oid.ts";

export const OidString = Schema.String.pipe(Schema.refine(isOid));

export const Ref = Schema.Struct({ name: Schema.String, oid: OidString });
export type Ref = (typeof Ref)["Type"];

export const Page = <A extends Schema.Top>(item: A) =>
  Schema.Struct({
    items: Schema.Array(item),
    next_cursor: Schema.NullOr(Schema.String),
    has_more: Schema.Boolean,
  });

export const RefsResponse = Schema.Struct({ refs: Schema.Array(Ref) });
export const RefPage = Page(Ref);

export const FileEntry = Schema.Struct({
  path: Schema.String,
  mode: Schema.String,
  oid: OidString,
});
export type FileEntry = (typeof FileEntry)["Type"];

export const FilesResponse = Schema.Struct({ files: Schema.Array(FileEntry) });

export const Encoding = Schema.Literals(["utf8", "base64"]);

export const FileContent = Schema.Struct({
  path: Schema.String,
  mode: Schema.String,
  oid: OidString,
  content: Schema.String,
  encoding: Encoding,
  size: Schema.Finite,
});
export type FileContent = (typeof FileContent)["Type"];

/** A path to write, or — with `content: null` — to remove. */
export const FileWrite = Schema.Struct({
  path: Schema.String,
  content: Schema.NullOr(Schema.String),
  encoding: Schema.optional(Encoding),
  mode: Schema.optional(Schema.String),
});
export type FileWrite = (typeof FileWrite)["Type"];

/** What `POST /:repo/commit` answers: the commit written, and its tree. */
export const CommitCreated = Schema.Struct({ oid: OidString, tree: OidString });
export type CommitCreated = (typeof CommitCreated)["Type"];

export const DiffRequest = Schema.Struct({
  from: Schema.String,
  to: Schema.String,
  path: Schema.optional(Schema.String),
  context: Schema.optional(Schema.Finite),
});
export type DiffRequest = (typeof DiffRequest)["Type"];

export const DiffFile = Schema.Struct({
  path: Schema.String,
  status: Schema.Literals(["added", "removed", "modified"]),
  binary: Schema.Boolean,
  patch: Schema.String,
});
export type DiffFile = (typeof DiffFile)["Type"];

export const DiffResponse = Schema.Struct({ files: Schema.Array(DiffFile) });

export const CommitSummary = Schema.Struct({
  oid: OidString,
  message: Schema.String,
});
export type CommitSummary = (typeof CommitSummary)["Type"];

/** A commit's author on the wire; the timestamp lives on the commit itself. */
export const CommitAuthor = Schema.Struct({ name: Schema.String, email: Schema.String });
export type CommitAuthor = (typeof CommitAuthor)["Type"];

/**
 * The commit view `log`, `commits` and `commit` all answer: everything a
 * listing or a header needs, so no client parses a raw object for an author
 * line. `at` is an ISO timestamp.
 */
export const CommitView = Schema.Struct({
  oid: OidString,
  message: Schema.String,
  /** First line of the message, trimmed. */
  subject: Schema.String,
  author: CommitAuthor,
  at: Schema.String,
  parents: Schema.Array(OidString),
});
export type CommitView = (typeof CommitView)["Type"];

export const LogResponse = Schema.Struct({ commits: Schema.Array(CommitView) });
export const CommitPage = Page(CommitView);

/** `GET /:repo/commit/:oid` — the shared view, plus the tree the commit wrote. */
export const Commit = Schema.Struct({ ...CommitView.fields, tree: OidString });
export type Commit = (typeof Commit)["Type"];

export const HistoryEntry = Schema.Struct({
  oid: OidString,
  message: Schema.String,
  blob: Schema.NullOr(OidString),
});
export const HistoryPage = Page(HistoryEntry);

export const RawObject = Schema.Struct({
  oid: OidString,
  type: Schema.Literals(["blob", "tree", "commit", "tag"]),
  size: Schema.Finite,
  content: Schema.String,
  encoding: Schema.Literals(["base64"]),
});
export type RawObject = (typeof RawObject)["Type"];

/** `{ deleted }` — what every DELETE endpoint answers. */
export const Deleted = Schema.Struct({ deleted: Schema.Boolean });

export const BranchCreateRequest = Schema.Struct({ name: Schema.String, base: Schema.String });
export type BranchCreateRequest = (typeof BranchCreateRequest)["Type"];

export const ResetRequest = Schema.Struct({
  ref: Schema.String,
  to: Schema.String,
  /** Absent moves whatever it is now; stating it makes this a CAS. */
  expected: Schema.optional(Schema.NullOr(OidString)),
});
export type ResetRequest = (typeof ResetRequest)["Type"];

export const ResetResult = Schema.Struct({
  ref: Schema.String,
  oid: OidString,
  previous: Schema.NullOr(OidString),
});
export type ResetResult = (typeof ResetResult)["Type"];

export const TagCreateRequest = Schema.Struct({
  name: Schema.String,
  /** A ref or an oid. */
  target: Schema.String,
  /** Present makes it annotated; absent makes it lightweight. */
  message: Schema.optional(Schema.String),
  force: Schema.optional(Schema.Boolean),
});
export type TagCreateRequest = (typeof TagCreateRequest)["Type"];

export const TagCreated = Schema.Struct({
  ref: Schema.String,
  oid: OidString,
  target: OidString,
});
export type TagCreated = (typeof TagCreated)["Type"];

export const TagRead = Schema.Struct({
  object: OidString,
  type: Schema.Literals(["blob", "tree", "commit", "tag"]),
  tag: Schema.String,
  message: Schema.String,
});
export type TagRead = (typeof TagRead)["Type"];

export const MergeRequest = Schema.Struct({
  ours: Schema.String,
  theirs: Schema.String,
  message: Schema.optional(Schema.String),
  strategy: Schema.optional(Schema.Literals(["recursive", "ours", "theirs"])),
  /** The ref to move on success; absent computes and stops. */
  into: Schema.optional(Schema.String),
  no_fast_forward: Schema.optional(Schema.Boolean),
});
export type MergeRequest = (typeof MergeRequest)["Type"];

export const MergeResult = Schema.Struct({
  kind: Schema.Literals(["up-to-date", "fast-forward", "merged", "conflicted"]),
  commit: Schema.NullOr(OidString),
  tree: Schema.NullOr(OidString),
  base: Schema.NullOr(OidString),
  conflicts: Schema.Array(
    Schema.Struct({
      path: Schema.String,
      reason: Schema.Literals(["content", "add/add", "modify/delete", "binary"]),
    }),
  ),
});
export type MergeResult = (typeof MergeResult)["Type"];

export const GrepRequest = Schema.Struct({
  pattern: Schema.String,
  ref: Schema.optional(Schema.String),
  path: Schema.optional(Schema.String),
  ignore_case: Schema.optional(Schema.Boolean),
  fixed: Schema.optional(Schema.Boolean),
  /** Bounded by default: a grep over a big tree is a lot of lines. */
  max_matches: Schema.optional(Schema.Finite),
});
export type GrepRequest = (typeof GrepRequest)["Type"];

export const GrepMatch = Schema.Struct({
  path: Schema.String,
  line: Schema.Finite,
  text: Schema.String,
});
export type GrepMatch = (typeof GrepMatch)["Type"];

export const GrepResponse = Schema.Struct({
  matches: Schema.Array(GrepMatch),
  truncated: Schema.Boolean,
  /** Files too large to scan, named so the answer is not silently partial. */
  skipped: Schema.Array(Schema.String),
});
export type GrepResponse = (typeof GrepResponse)["Type"];

export const ReflogResponse = Schema.Struct({
  entries: Schema.Array(
    Schema.Struct({
      from: Schema.NullOr(OidString),
      to: Schema.NullOr(OidString),
      at: Schema.String,
      message: Schema.String,
    }),
  ),
});
export type ReflogResponse = (typeof ReflogResponse)["Type"];

export const FsckReport = Schema.Struct({
  checked: Schema.Finite,
  ok: Schema.Boolean,
  problems: Schema.Array(Schema.Struct({ oid: OidString, problem: Schema.String })),
  dangling_refs: Schema.Array(Schema.Struct({ ref: Schema.String, oid: OidString })),
});
export type FsckReport = (typeof FsckReport)["Type"];

export const GcRequest = Schema.Struct({
  dry_run: Schema.optional(Schema.Boolean),
  /** Also write what survives into one pack and drop the loose copies. */
  repack: Schema.optional(Schema.Boolean),
  reflog_grace_ms: Schema.optional(Schema.Finite),
});
export type GcRequest = (typeof GcRequest)["Type"];

export const GcReport = Schema.Struct({
  scanned: Schema.Finite,
  reachable: Schema.Finite,
  removed: Schema.Array(OidString),
  /** Unreachable, but inside a pack: `repack` is what collects these. */
  retained: Schema.Array(OidString),
  packed: Schema.NullOr(Schema.Struct({ name: Schema.String, objects: Schema.Finite })),
  repack_skipped: Schema.NullOr(Schema.String),
});
export type GcReport = (typeof GcReport)["Type"];

/** A registered webhook as a client may see it: no secret, ever. */
export const WebhookWire = Schema.Struct({
  id: Schema.String,
  url: Schema.String,
  created_at: Schema.String,
});
export type WebhookWire = (typeof WebhookWire)["Type"];

export const WebhookList = Schema.Struct({ webhooks: Schema.Array(WebhookWire) });

/** A registered remote as a client may see it: no credential, ever. */
export const RemoteWire = Schema.Struct({
  name: Schema.String,
  url: Schema.String,
  has_credential: Schema.Boolean,
  has_key: Schema.Boolean,
  /** The standing instruction, or `null` for a remote nothing happens to. */
  sync: Schema.NullOr(Schema.Struct({ mode: Schema.String, refs: Schema.Array(Schema.String) })),
  created_at: Schema.String,
});
export type RemoteWire = (typeof RemoteWire)["Type"];

export const RemoteList = Schema.Struct({ remotes: Schema.Array(RemoteWire) });

/** A ref after a fetch moved it, and where it was before. */
export const FetchedRef = Schema.Struct({
  name: Schema.String,
  oid: OidString,
  from: Schema.NullOr(OidString),
});

export const FetchResult = Schema.Struct({
  /** The namespace the branches landed in: `refs/remotes/<remote>/…`. */
  remote: Schema.String,
  refs: Schema.Array(FetchedRef),
  objects: Schema.Finite,
});
export type FetchResult = (typeof FetchResult)["Type"];

export const PushResult = Schema.Struct({
  refs: Schema.Array(
    Schema.Struct({
      ref: Schema.String,
      ok: Schema.Boolean,
      reason: Schema.NullOr(Schema.String),
    }),
  ),
});
export type PushResult = (typeof PushResult)["Type"];

export const PullResult = Schema.Struct({
  kind: Schema.Literals(["up-to-date", "created", "fast-forward", "non-fast-forward"]),
  branch: Schema.String,
  tracking: Schema.String,
  /** Where the branch was; `null` when it did not exist. */
  from: Schema.NullOr(OidString),
  /** What the remote had — where the branch is now, unless it diverged. */
  to: OidString,
  objects: Schema.Finite,
});
export type PullResult = (typeof PullResult)["Type"];

export const WhoamiVerdict = Schema.Struct({
  push: Schema.Literals(["allowed", "refused"]),
  why: Schema.Array(Schema.String),
});

export const WhoamiAnswer = Schema.Struct({
  repo: Schema.NullOr(Schema.String),
  subject: Schema.NullOr(Schema.String),
  member: Schema.Boolean,
  why: Schema.NullOr(Schema.String),
  capabilities: Schema.Array(Schema.String),
  expiresAt: Schema.NullOr(Schema.String),
  trust: Schema.NullOr(
    Schema.Struct({
      maxTrustAgeSeconds: Schema.Int,
      fresh: Schema.Boolean,
      reason: Schema.NullOr(Schema.String),
    }),
  ),
  /** Only where the repository bounds what it accepts being told it cost. */
  budget: Schema.NullOr(
    Schema.Struct({
      maxUsageTokens: Schema.Int,
      windowSeconds: Schema.Int,
      usedTokens: Schema.Int,
      remainingTokens: Schema.Int,
    }),
  ),
  branches: Schema.Record(Schema.String, WhoamiVerdict),
});
export type WhoamiAnswer = (typeof WhoamiAnswer)["Type"];

// -- hub ----------------------------------------------------------------------

/**
 * A live claim on a task: who holds the lease and until when. `by` is the
 * claimant's key fingerprint, `null` where the record's signer could not be
 * established. Advisory by design — see `hub/Task.ts`.
 */
export const HubClaim = Schema.Struct({
  by: Schema.NullOr(Schema.String),
  expiresAt: Schema.String,
});

/** One task, as `hub/Task.ts` projects it from `refs/hub/task/<id>`. */
export const HubTask = Schema.Struct({
  task: Schema.String,
  exists: Schema.Boolean,
  title: Schema.String,
  description: Schema.String,
  refs: Schema.Array(Schema.String),
  /** The task this one belongs to — a release, an epic, a parent story. */
  parent: Schema.NullOr(Schema.String),
  /**
   * The tasks that name this one as their parent.
   *
   * Derived by the listing rather than recorded: the edge lives on the child,
   * so the parent's own ref never learns of it, and a repository that answers
   * for every task can group them in one pass.
   */
  children: Schema.Array(Schema.String),
  /** Open, unclaimed, unexpired — the question an idle agent asks. */
  available: Schema.Boolean,
  claim: Schema.NullOr(HubClaim),
  closed: Schema.NullOr(
    Schema.Struct({ outcome: Schema.String, pulls: Schema.Array(Schema.String) }),
  ),
  sessions: Schema.Array(Schema.String),
});
export type HubTask = (typeof HubTask)["Type"];

export const HubReview = Schema.Struct({
  id: Schema.String,
  author: Schema.String,
  head: OidString,
  base: Schema.String,
  commit: OidString,
  decision: Schema.Literals(["approve", "reject", "comment"]),
  body: Schema.String,
  at: Schema.String,
  dismissed: Schema.Boolean,
  stale: Schema.Boolean,
});
export type HubReview = (typeof HubReview)["Type"];

export const HubComment = Schema.Struct({
  id: Schema.String,
  author: Schema.String,
  body: Schema.String,
  at: Schema.String,
});
export type HubComment = (typeof HubComment)["Type"];

export const HubThread = Schema.Struct({
  id: Schema.String,
  commit: OidString,
  path: Schema.NullOr(Schema.String),
  side: Schema.NullOr(Schema.Literals(["old", "new"])),
  line: Schema.NullOr(Schema.Finite),
  head: Schema.NullOr(OidString),
  resolved: Schema.Boolean,
  comments: Schema.Array(HubComment),
});
export type HubThread = (typeof HubThread)["Type"];

export const HubCheck = Schema.Struct({
  name: Schema.String,
  provider: Schema.String,
  head: OidString,
  status: Schema.Literals(["started", "success", "failure", "neutral"]),
  url: Schema.NullOr(Schema.String),
  at: Schema.String,
  author: Schema.String,
});
export type HubCheck = (typeof HubCheck)["Type"];

/**
 * Whether the repository would settle this pull request now, and — when it
 * would not — every rule still standing in the way, in the server's words.
 * The UI renders this instead of reconstructing branch policy from counts.
 */
export const HubMergeable = Schema.Struct({
  ok: Schema.Boolean,
  reasons: Schema.Array(Schema.String),
});
export type HubMergeable = (typeof HubMergeable)["Type"];

/** One pull request in a listing: the projection, minus its heavy members. */
export const HubPullSummary = Schema.Struct({
  id: Schema.String,
  title: Schema.String,
  base: Schema.String,
  head: Schema.NullOr(OidString),
  state: Schema.Literals(["open", "closed", "merged"]),
  author: Schema.NullOr(Schema.String),
  /** Fresh, undismissed approvals of the current head, for the current base. */
  approvals: Schema.Int,
  /** Checks for the *current* proposed revision only; history stays in the detail's `checkList`. */
  checks: Schema.Struct({ total: Schema.Int, passed: Schema.Boolean }),
  threads: Schema.Struct({ total: Schema.Int, unresolved: Schema.Int }),
  mergeable: HubMergeable,
  at: Schema.String,
});
export type HubPullSummary = (typeof HubPullSummary)["Type"];

export const HubPullDetail = Schema.Struct({
  ...HubPullSummary.fields,
  description: Schema.String,
  mergeCommit: Schema.NullOr(OidString),
  /** Commits the proposal carries beyond its base, capped at 250. */
  commits: Schema.Int,
  reviews: Schema.Array(HubReview),
  threadList: Schema.Array(HubThread),
  checkList: Schema.Array(HubCheck),
  /** Events that were present but did not count, with the fold's reason. */
  rejected: Schema.Array(Schema.Struct({ commit: OidString, reason: Schema.String })),
});
export type HubPullDetail = (typeof HubPullDetail)["Type"];

/**
 * A pre-signed hub event, exactly as `Event.issue` would have written it.
 *
 * The server never holds a member's key, so it cannot author events over
 * HTTP — but it can append one authored elsewhere: `payload` is the exact
 * signed bytes (base64), `signatures` the armored SSH signatures over them.
 * The projection remains the judge of authority; this endpoint only refuses
 * what could never count — bytes no offered signature covers.
 */
export const HubEventRequest = Schema.Struct({
  payload: Schema.String,
  signatures: Schema.Array(Schema.String),
});
export type HubEventRequest = (typeof HubEventRequest)["Type"];

export const HubEventAppended = Schema.Struct({
  ref: Schema.String,
  commit: OidString,
});
export type HubEventAppended = (typeof HubEventAppended)["Type"];

/**
 * Settle a pull request: advance its base to the exact approved revision and
 * append the pre-signed `pr.merged` event, as one judged transition.
 *
 * The two client requests this replaces — a generic `/merge`, then an event
 * append — could not keep the branch and the hub's account of it consistent
 * under interruption, and the generic route refuses protected branches
 * outright (it cannot see approvals). This one names everything the server
 * re-checks at execution time: the head that was reviewed, the base tip the
 * caller saw, and the signed record of what the transition means.
 */
export const HubMergeRequest = Schema.Struct({
  /** The approved revision; the base advances to exactly this, or not at all. */
  head: OidString,
  /** The base tip the caller decided against — the compare-and-swap; `null` for an unborn base. */
  expected: Schema.NullOr(OidString),
  /** The pre-signed `pr.merged` bytes (base64) and the signatures over them. */
  payload: Schema.String,
  signatures: Schema.Array(Schema.String),
});
export type HubMergeRequest = (typeof HubMergeRequest)["Type"];

export const HubMerged = Schema.Struct({
  pr: Schema.String,
  /** The base ref that advanced. */
  base: Schema.String,
  /** Where it now points — the approved head. */
  commit: OidString,
  /** The hub record commit carrying `pr.merged`. */
  event: OidString,
});
export type HubMerged = (typeof HubMerged)["Type"];

/** One page of tasks; the same `Page` discipline every git listing follows. */
export const HubTaskPage = Page(HubTask);
export type HubTaskPage = (typeof HubTaskPage)["Type"];

/** One page of pull requests, carrying the hub's availability beside it. */
export const HubPullPage = Schema.Struct({
  enabled: Schema.Boolean,
  reason: Schema.NullOr(Schema.String),
  ...Page(HubPullSummary).fields,
});
export type HubPullPage = (typeof HubPullPage)["Type"];

/** A question a session asked, and what — if anything — a person chose. */
export const HubSessionDecision = Schema.Struct({
  id: Schema.String,
  question: Schema.String,
  options: Schema.Array(Schema.String),
  chose: Schema.NullOr(Schema.String),
});
export type HubSessionDecision = (typeof HubSessionDecision)["Type"];

const HubSessionAgent = Schema.Struct({
  kind: Schema.String,
  model: Schema.String,
  harness: Schema.String,
});

const HubSessionUsage = Schema.Struct({
  inputTokens: Schema.Int,
  outputTokens: Schema.Int,
});

/** One session in a listing: the projection, minus its heavy members. */
export const HubSessionSummary = Schema.Struct({
  session: Schema.String,
  agent: Schema.NullOr(HubSessionAgent),
  refs: Schema.Array(Schema.String),
  pulls: Schema.Array(Schema.String),
  commits: Schema.Int,
  decisions: Schema.Struct({ total: Schema.Int, open: Schema.Int }),
  usage: HubSessionUsage,
});
export type HubSessionSummary = (typeof HubSessionSummary)["Type"];

export const HubSessionPage = Page(HubSessionSummary);
export type HubSessionPage = (typeof HubSessionPage)["Type"];

/** One session, whole: what it was told, produced, asked and learned. */
export const HubSessionDetail = Schema.Struct({
  session: Schema.String,
  exists: Schema.Boolean,
  agent: Schema.NullOr(HubSessionAgent),
  /** The standing instructions in force, as a blob or tree id. */
  instructions: Schema.NullOr(Schema.String),
  prompts: Schema.Array(
    Schema.Struct({ role: Schema.Literals(["user", "system"]), prompt: Schema.String }),
  ),
  commits: Schema.Array(Schema.String),
  refs: Schema.Array(Schema.String),
  pulls: Schema.Array(Schema.String),
  notes: Schema.Array(Schema.String),
  decisions: Schema.Array(HubSessionDecision),
  redacted: Schema.Array(Schema.String),
  usage: HubSessionUsage,
});
export type HubSessionDetail = (typeof HubSessionDetail)["Type"];

/** A member as the trust projection holds it — the public record only. */
export const HubMember = Schema.Struct({
  fingerprint: Schema.String,
  publicKey: Schema.String,
  capabilities: Schema.Array(Schema.String),
  grantedAt: Schema.String,
  expiresAt: Schema.NullOr(Schema.String),
});
export type HubMember = (typeof HubMember)["Type"];

export const HubMembersResponse = Schema.Struct({
  enabled: Schema.Boolean,
  reason: Schema.NullOr(Schema.String),
  members: Schema.Array(HubMember),
});
export type HubMembersResponse = (typeof HubMembersResponse)["Type"];

/** The branch rules, exactly as `server/Policy.ts` evaluates them. */
export const PolicyRules = Schema.Struct({
  protected: Schema.Array(Schema.String),
  requiredApprovals: Schema.Int,
  requiredChecks: Schema.Array(Schema.String),
  requireResolvedThreads: Schema.Boolean,
  requirePullRequest: Schema.Boolean,
  maxTrustAgeSeconds: Schema.Int,
  requireProvenance: Schema.Boolean,
  maxUsageTokens: Schema.Int,
  usageWindowSeconds: Schema.Int,
  queueCandidates: Schema.Boolean,
  queueDepth: Schema.Int,
});
export type PolicyRules = (typeof PolicyRules)["Type"];

/**
 * The same rules as a caller may *write* them, newer fields optional.
 *
 * A repository always answers with every field, because what it enforces is
 * never partly unknown. What a client sends is a different question: a client
 * built before a field existed does not know to send it, and requiring it turns
 * every such client's next policy write into a rejection — for a field it was
 * never going to have an opinion about.
 *
 * Absent therefore means **unchanged**, and never the default. This is a `PUT`
 * of the whole document, so reading an absent field as the default let exactly
 * that client silently turn off something somebody else had enabled, and be
 * told 200 for it. (The rules *file* is the other way round — an absent field
 * there is a document that never mentioned it — which is why the two readings
 * are written down separately rather than shared.)
 */
export const PolicyRulesWrite = Schema.Struct({
  ...PolicyRules.fields,
  queueCandidates: Schema.optional(Schema.Boolean),
  queueDepth: Schema.optional(Schema.Int),
});
export type PolicyRulesWrite = (typeof PolicyRulesWrite)["Type"];

/** What the repository enforces now; `ref` is `null` on unpublished defaults. */
export const PolicyAnswer = Schema.Struct({
  rules: PolicyRules,
  ref: Schema.NullOr(OidString),
});
export type PolicyAnswer = (typeof PolicyAnswer)["Type"];

export const PolicyWritten = Schema.Struct({
  rules: PolicyRules,
  commit: OidString,
});
export type PolicyWritten = (typeof PolicyWritten)["Type"];

/**
 * What a replay produced, for both `cherry-pick` and `rebase`.
 *
 * `commits` lists every commit considered, not just the ones that produced
 * something: a `replayed` of `null` is a commit `onto` already had, or one
 * that conflicted, and dropping those would leave the caller unable to tell
 * an empty pick from a skipped one.
 */
export const ReplayResult = Schema.Struct({
  kind: Schema.Literals(["replayed", "up-to-date", "conflicted"]),
  head: Schema.NullOr(OidString),
  commits: Schema.Array(
    Schema.Struct({
      original: OidString,
      replayed: Schema.NullOr(OidString),
      conflicts: Schema.Array(
        Schema.Struct({
          path: Schema.String,
          reason: Schema.Literals(["content", "add/add", "modify/delete", "binary"]),
        }),
      ),
    }),
  ),
});
export type ReplayResult = (typeof ReplayResult)["Type"];

/** One bisection step: the next revision to test, or the culprit found. */
export const BisectAnswer = Schema.Struct({
  kind: Schema.Literals(["test", "found"]),
  commit: OidString,
  remaining: Schema.Finite,
  steps: Schema.Finite,
});
export type BisectAnswer = (typeof BisectAnswer)["Type"];
