/**
 * A browser client for the repository's JSON API.
 *
 * The endpoints and paths mirror `src/server/Api.ts`; their shared response
 * shapes come from the contract that module uses.
 *
 * The browser-safe response schemas live in `src/server/ApiContract.ts` and
 * are also used by the HTTP declaration. Everything is decoded against those
 * schemas at this boundary and handed on as a domain value, so a server/client
 * drift is an explicit `InvalidResponse` rather than an unsafe assertion.
 */

import { Data, Option, Schema } from "effect";

import * as Contract from "../server/ApiContract.ts";

/** Thrown for any non-2xx answer, carrying the server's tagged error name. */
export class ApiError extends Data.TaggedError("ApiError")<{
  readonly tag: string;
  readonly status: number;
  readonly message: string;
}> {
  /**
   * Whether nothing answered, as opposed to answering badly.
   *
   * The two need telling apart because they call for opposite responses: a
   * server that is not running is something the reader can start, while a 400
   * naming a bad ref is a bug in this client. They looked identical while both
   * produced the same "the API did not answer" notice — which is how a client
   * sending unqualified refs went unnoticed for as long as it did.
   *
   * The dev proxy turns a refused connection into a 502, so the gateway codes
   * count here alongside the transport failures.
   */
  get unreachable(): boolean {
    return this.status === 0 || this.status === 502 || this.status === 503 || this.status === 504;
  }
}

/**
 * Why a screen has no data, in one line, for the fallback notice.
 *
 * The two cases a caller can be holding: an answer it did not like, or no answer
 * at all — `fetch` rejects with a `TypeError` when it cannot connect.
 */
export type Unavailable = ApiError | TypeError;

export const describe = (error: Unavailable): string =>
  error instanceof ApiError && !error.unreachable
    ? `the git+ API answered ${error.tag}: ${error.message}`
    : "the git+ API is not running";

export type Ref = Contract.Ref;
export type FileEntry = Contract.FileEntry;
export type FileWrite = Contract.FileWrite;
export type CommitCreated = Contract.CommitCreated;
export type GrepMatch = Contract.GrepMatch;
export type GrepResponse = Contract.GrepResponse;
export type MergeResult = Contract.MergeResult;
export type ResetResult = Contract.ResetResult;
export type TagCreated = Contract.TagCreated;
export type TagRead = Contract.TagRead;
export type FsckReport = Contract.FsckReport;
export type GcReport = Contract.GcReport;
export type ReflogEntry = Contract.ReflogResponse["entries"][number];
export type WebhookWire = Contract.WebhookWire;
export type RemoteWire = Contract.RemoteWire;
export type FetchResult = Contract.FetchResult;
export type PushResult = Contract.PushResult;
export type PullResult = Contract.PullResult;

/**
 * What `POST /:repo/commit` is asked for, in the fields this client uses.
 *
 * `files` are layered onto the branch's current tree, and `expected` — when
 * given — must equal the tip for the commit to land; see `commitFiles`.
 */
export interface CommitFilesRequest {
  branch: string;
  message: string;
  files: readonly FileWrite[];
  expected?: string;
}

/**
 * Client-side request slices, in plain strings.
 *
 * The contract's request types brand oids, which a browser holding oids as
 * strings cannot produce without asserting; the server decodes and re-brands
 * at its boundary either way, so these carry the same wire shape unbranded.
 */
interface ResetOptions {
  ref: string;
  to: string;
  expected?: string;
}

interface RemoteAddRequest {
  name: string;
  url: string;
  credential?: string;
}
export type FileContent = Contract.FileContent;
export type DiffFile = Contract.DiffFile;
export type CommitSummary = Contract.CommitSummary;
export type CommitView = Contract.CommitView;
export type Commit = Contract.Commit;

/**
 * A commit with its author and date, as the enriched JSON endpoints answer
 * it. The ISO `at` becomes a `Date` here, once, at the boundary — screens
 * never see the wire string.
 */
export interface CommitDetail {
  readonly oid: string;
  /** First line of the message. */
  readonly subject: string;
  readonly author: string;
  readonly email: string;
  readonly at: Date;
  readonly parents: readonly string[];
}

/**
 * Who the server thinks is asking, and what they may do.
 *
 * `subject` is null for an unauthenticated caller, and for a repository with no
 * genesis `member` is false with `why` explaining it — so the UI shows a real
 * identity when there is one and says nothing rather than inventing one.
 */
export type Whoami = Contract.WhoamiAnswer;
export type PolicyAnswer = Contract.PolicyAnswer;
export type PolicyRules = Contract.PolicyRules;

/** A tagged error body, as `git/Error.ts` puts it on the wire. */
const ErrorBody = Schema.Struct({
  _tag: Schema.optional(Schema.String),
  message: Schema.optional(Schema.String),
  /** `Invalid` carries the detail here rather than in `message`. */
  reason: Schema.optional(Schema.String),
});

const decoder = new TextDecoder();

/**
 * base64 → text.
 *
 * `atob` yields one code unit per byte, so the bytes are rebuilt before
 * decoding; going straight from `atob` to a string mangles anything non-ASCII.
 */
export const decodeContent = (content: string): string => {
  const binary = atob(content);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
  return decoder.decode(bytes);
};

/** Wire → client, once: the ISO timestamp becomes a `Date` here. */
const commitDetailOf = (commit: Contract.CommitView): CommitDetail => ({
  oid: commit.oid,
  subject: commit.subject,
  author: commit.author.name,
  email: commit.author.email,
  at: new Date(commit.at),
  parents: commit.parents,
});

export interface ClientOptions {
  /** Repository name — the first path segment the server routes on. */
  readonly repo: string;
  /** Origin the API is served from; same-origin by default. */
  readonly base?: string;
}

export class GitApi {
  readonly repo: string;
  readonly #base: string;

  constructor(options: ClientOptions) {
    this.repo = options.repo;
    this.#base = (options.base ?? "").replace(/\/$/, "");
  }

  /**
   * What `git clone` should be handed.
   *
   * The same base the JSON calls use: the smart-HTTP endpoints live beside
   * them, so an empty base means this very origin serves the repository.
   */
  get cloneUrl(): string {
    const base = this.#base === "" ? globalThis.location.origin : this.#base;
    return `${base}/${encodeURIComponent(this.repo)}`;
  }

  #url(path: string, query?: URLSearchParams): string {
    const search = query === undefined || query.size === 0 ? "" : `?${query.toString()}`;
    return `${this.#base}/${encodeURIComponent(this.repo)}${path}${search}`;
  }

  async #json<S extends Schema.ConstraintDecoder<unknown>>(
    url: string,
    schema: S,
    init?: RequestInit,
  ): Promise<S["Type"]> {
    let response = await fetch(url, init);
    if (response.status === 401) {
      // A repository that requires authentication challenges with a nonce;
      // the browser's signing key (`identity.ts`, loaded lazily so anonymous
      // pages never pay for it) answers once with a signed envelope. A key
      // the repository has not granted gets the same refusal back.
      const retried = await import("./identity.ts")
        .then((identity) => identity.retryAuthorized(url, init ?? {}, response))
        .catch((): Response | null => null);
      if (retried !== null) response = retried;
    }
    if (!response.ok) {
      const decoded = Schema.decodeUnknownOption(ErrorBody)(
        await response.json().catch((): undefined => undefined),
      );
      const body = Option.isSome(decoded) ? decoded.value : undefined;
      throw new ApiError({
        tag: body?._tag ?? "HttpError",
        status: response.status,
        message: body?.message ?? body?.reason ?? `${response.status} ${response.statusText}`,
      });
    }
    const body: unknown = await response.json();
    try {
      return Schema.decodeUnknownSync(schema)(body);
    } catch (cause) {
      throw new ApiError({
        tag: "InvalidResponse",
        status: response.status,
        message: cause instanceof Error ? cause.message : String(cause),
      });
    }
  }

  /** Every ref, unpaged — the shape the smart-HTTP advertisement needs. */
  async refs(): Promise<readonly Ref[]> {
    const body = await this.#json(this.#url("/refs"), Contract.RefsResponse);
    return body.refs;
  }

  /** Branches, paged; one page is enough for a branch picker. */
  async branches(limit = "100"): Promise<readonly Ref[]> {
    const body = await this.#json(
      this.#url("/branches", new URLSearchParams({ limit })),
      Contract.RefPage,
    );
    return body.items;
  }

  /**
   * Every blob reachable at `ref`, as tree paths.
   *
   * This is the endpoint the explorer is built on: `@pierre/trees` is
   * path-first — it takes `paths: string[]` and derives the folder structure
   * itself — so the answer needs no reshaping at all.
   */
  async files(ref: string, path?: string): Promise<readonly FileEntry[]> {
    const query = new URLSearchParams({ ref });
    if (path !== undefined) query.set("path", path);
    const body = await this.#json(this.#url("/files", query), Contract.FilesResponse);
    return body.files;
  }

  /** One blob's content — UTF-8 text straight from the JSON, no base64 hop. */
  async file(ref: string, path: string): Promise<string> {
    const body = await this.#json(
      this.#url("/file", new URLSearchParams({ ref, path, encoding: "utf8" })),
      Contract.FileContent,
    );
    return body.encoding === "utf8" ? body.content : decodeContent(body.content);
  }

  /**
   * The patch set between two revisions.
   *
   * The server runs the diff itself and answers unified patches, which is
   * exactly what `@pierre/diffs` renders — so no diffing happens in the
   * browser.
   */
  async diff(from: string, to: string, path?: string): Promise<readonly DiffFile[]> {
    const payload: Contract.DiffRequest = path === undefined ? { from, to } : { from, to, path };
    const body = await this.#json(this.#url("/diff"), Contract.DiffResponse, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });
    return body.files;
  }

  /**
   * Write files to a branch tip as one new commit.
   *
   * See {@link CommitFilesRequest} for the fields.
   *
   * The server layers `files` onto the branch's current tree — a write is an
   * addition to what is there, not a replacement of it — and `expected` is a
   * compare-and-swap on the tip: if someone else committed while the editor
   * was open, the answer is `409 RefConflict` rather than a silent overwrite.
   * A `content` of `null` removes the path.
   */
  async commitFiles(options: Readonly<CommitFilesRequest>): Promise<CommitCreated> {
    const payload: CommitFilesRequest = {
      branch: options.branch,
      message: options.message,
      files: options.files,
    };
    if (options.expected !== undefined) payload.expected = options.expected;
    return await this.#json(this.#url("/commit"), Contract.CommitCreated, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });
  }

  /** Who the server thinks is asking. */
  async whoami(): Promise<Whoami> {
    return await this.#json(this.#url("/whoami"), Contract.WhoamiAnswer);
  }

  /** One POST body, one decoded answer — every write endpoint's shape. */
  async #post<S extends Schema.ConstraintDecoder<unknown>, P extends object>(
    path: string,
    payload: P,
    schema: S,
  ): Promise<S["Type"]> {
    return await this.#json(this.#url(path), schema, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });
  }

  /** `DELETE`, answered `{ deleted }`. */
  async #delete(path: string): Promise<boolean> {
    const body = await this.#json(this.#url(path), Contract.Deleted, { method: "DELETE" });
    return body.deleted;
  }

  /**
   * Search blob contents at a ref.
   *
   * Literal and case-insensitive, because this backs the UI's search box —
   * a reader types text, not a regular expression.
   */
  async grep(pattern: string, ref: string, maxMatches = 50): Promise<GrepResponse> {
    const payload: Contract.GrepRequest = {
      pattern,
      ref,
      fixed: true,
      ignore_case: true,
      max_matches: maxMatches,
    };
    return await this.#post("/grep", payload, Contract.GrepResponse);
  }

  /** Create a branch at `base` (a ref or an oid). */
  async branchCreate(name: string, base: string): Promise<Ref> {
    const payload: Contract.BranchCreateRequest = { name, base };
    return await this.#post("/branches/create", payload, Contract.Ref);
  }

  async branchDelete(name: string): Promise<boolean> {
    return await this.#delete(`/branches/${encodeURIComponent(name)}`);
  }

  /** Move a ref to a commit — `reset --hard`, with an optional CAS. */
  async reset(ref: string, to: string, expected?: string): Promise<ResetResult> {
    const payload: ResetOptions = { ref, to };
    if (expected !== undefined) payload.expected = expected;
    return await this.#post("/reset", payload, Contract.ResetResult);
  }

  /** Tags, paged; one page is enough for a tag list. */
  async tags(limit = "100"): Promise<readonly Ref[]> {
    const body = await this.#json(
      this.#url("/tags", new URLSearchParams({ limit })),
      Contract.RefPage,
    );
    return body.items;
  }

  /** Create a tag; a `message` makes it annotated. */
  async tagCreate(options: Contract.TagCreateRequest): Promise<TagCreated> {
    return await this.#post("/tags", options, Contract.TagCreated);
  }

  async tagDelete(name: string): Promise<boolean> {
    return await this.#delete(`/tags/${encodeURIComponent(name)}`);
  }

  /** An annotated tag object's target, name and message. */
  async tagRead(oid: string): Promise<TagRead> {
    return await this.#json(this.#url(`/tag/${oid}`), Contract.TagRead);
  }

  /** Merge `theirs` into `ours`, moving `into` on success when named. */
  async merge(options: Contract.MergeRequest): Promise<MergeResult> {
    return await this.#post("/merge", options, Contract.MergeResult);
  }

  /** Where a ref has been: every move, newest first. */
  async reflog(ref: string): Promise<readonly ReflogEntry[]> {
    const body = await this.#json(
      this.#url("/reflog", new URLSearchParams({ ref })),
      Contract.ReflogResponse,
    );
    return body.entries;
  }

  /** Prove every object decodes and every ref resolves. */
  async fsck(): Promise<FsckReport> {
    return await this.#post("/fsck", {}, Contract.FsckReport);
  }

  /** Collect what nothing reaches. */
  async gc(options: Contract.GcRequest = {}): Promise<GcReport> {
    return await this.#post("/gc", options, Contract.GcReport);
  }

  async webhooks(): Promise<readonly WebhookWire[]> {
    const body = await this.#json(this.#url("/webhooks"), Contract.WebhookList);
    return body.webhooks;
  }

  /** Register a webhook. The secret goes in and never comes back out. */
  async webhookAdd(url: string, secret: string): Promise<WebhookWire> {
    return await this.#post("/webhooks", { url, secret }, Contract.WebhookWire);
  }

  async webhookDelete(id: string): Promise<boolean> {
    return await this.#delete(`/webhooks/${encodeURIComponent(id)}`);
  }

  async remotes(): Promise<readonly RemoteWire[]> {
    const body = await this.#json(this.#url("/remotes"), Contract.RemoteList);
    return body.remotes;
  }

  /** Register a remote. The credential goes in and never comes back out. */
  async remoteAdd(name: string, url: string, credential?: string): Promise<RemoteWire> {
    const payload: RemoteAddRequest = { name, url };
    if (credential !== undefined && credential !== "") payload.credential = credential;
    return await this.#post("/remotes", payload, Contract.RemoteWire);
  }

  async remoteDelete(name: string): Promise<boolean> {
    return await this.#delete(`/remotes/${encodeURIComponent(name)}`);
  }

  /** Fetch everything a stored remote has into `refs/remotes/<name>/…`. */
  async fetchRemote(name: string): Promise<FetchResult> {
    return await this.#post("/fetch", { name }, Contract.FetchResult);
  }

  /** Push one local branch to a stored remote, same name on the far side. */
  async pushRemote(name: string, branch: string): Promise<PushResult> {
    return await this.#post("/push", { name, refs: [{ local: branch }] }, Contract.PushResult);
  }

  /** Fast-forward one branch from a stored remote. */
  async pullRemote(name: string, branch: string): Promise<PullResult> {
    return await this.#post("/pull", { name, branch }, Contract.PullResult);
  }

  /** One commit, with author and date, straight from `/commit/:oid`. */
  async commitDetail(oid: string): Promise<CommitDetail> {
    return commitDetailOf(await this.#json(this.#url(`/commit/${oid}`), Contract.Commit));
  }

  /**
   * The newest `limit` commits reachable from `oid`, each with author and
   * date — one paged request, because the rows carry the metadata now.
   */
  async recentCommits(oid: string, limit = 30): Promise<readonly CommitDetail[]> {
    const page = await this.#json(
      this.#url(`/commits/${oid}`, new URLSearchParams({ limit: String(limit) })),
      Contract.CommitPage,
    );
    return page.items.map(commitDetailOf);
  }

  /** Commit history from a starting oid, newest first. */
  async log(oid: string): Promise<readonly CommitView[]> {
    const body = await this.#json(this.#url(`/log/${oid}`), Contract.LogResponse);
    return body.commits;
  }

  /** One commit's message, parents and tree. */
  async commit(oid: string): Promise<Commit> {
    return await this.#json(this.#url(`/commit/${oid}`), Contract.Commit);
  }

  /** Commits touching one path, newest first. */
  async history(oid: string, path: string, limit = "20"): Promise<readonly CommitSummary[]> {
    const body = await this.#json(
      this.#url(`/history/${oid}`, new URLSearchParams({ path, limit })),
      Contract.HistoryPage,
    );
    return body.items;
  }

  /** The branch rules in force, and the commit publishing them. */
  async policy(): Promise<Contract.PolicyAnswer> {
    return await this.#json(this.#url("/policy"), Contract.PolicyAnswer);
  }

  /** Publish new branch rules — `policy.write`'s door, over JSON. */
  async policyWrite(rules: Contract.PolicyRules): Promise<Contract.PolicyWritten> {
    return await this.#json(this.#url("/policy"), Contract.PolicyWritten, {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(rules),
    });
  }

  /** Replay one commit onto a branch, moving it on success. */
  async cherryPick(commit: string, onto: string): Promise<Contract.ReplayResult> {
    return await this.#post("/cherry-pick", { commit, onto, into: onto }, Contract.ReplayResult);
  }

  /** Replay a branch onto another, moving it on success. */
  async rebase(branch: string, onto: string): Promise<Contract.ReplayResult> {
    return await this.#post("/rebase", { branch, onto, into: branch }, Contract.ReplayResult);
  }

  /** The next revision to test, between known-good marks and a known-bad one. */
  async bisect(good: readonly string[], bad: string): Promise<Contract.BisectAnswer> {
    return await this.#post("/bisect", { good, bad }, Contract.BisectAnswer);
  }

  /** Where a revision's archive can be fetched — for a download link. */
  archiveUrl(ref: string, format: "tar" | "tar.gz" | "zip" = "tar.gz"): string {
    return this.#url("/archive", new URLSearchParams({ ref, format }));
  }
}

/**
 * Which repository to talk to, and where.
 *
 * Read from the page so a deployment can point the same bundle at any repo:
 *   <meta name="gp-repo" content="core">
 *   <meta name="gp-api-base" content="https://git.example.com">
 */
export const clientFromDocument = (): GitApi => {
  const repo = document.querySelector('meta[name="gp-repo"]')?.getAttribute("content");
  const base = document.querySelector('meta[name="gp-api-base"]')?.getAttribute("content");
  return new GitApi({ repo: repo ?? "core", base: base ?? undefined });
};

/**
 * What the Code screen needs from its client — satisfied structurally by
 * both this module's HTTP `GitApi` and the OPFS-backed `LocalGitApi`
 * (`local.ts`), so the screen never knows which it holds.
 */
export interface CodeApi {
  readonly repo: string;
  readonly cloneUrl: string;
  refs(): Promise<readonly Ref[]>;
  files(ref: string): Promise<readonly FileEntry[]>;
  file(ref: string, path: string): Promise<string>;
  commitFiles(options: Readonly<CommitFilesRequest>): Promise<CommitCreated>;
  branchCreate(name: string, base: string): Promise<Ref>;
  commitDetail(oid: string): Promise<CommitDetail>;
  recentCommits(oid: string, limit?: number): Promise<readonly CommitDetail[]>;
  history(oid: string, path: string, limit?: string): Promise<readonly CommitSummary[]>;
  cherryPick(commit: string, onto: string): Promise<Contract.ReplayResult>;
  rebase(branch: string, onto: string): Promise<Contract.ReplayResult>;
  bisect(good: readonly string[], bad: string): Promise<Contract.BisectAnswer>;
}

/** What the Search screen needs — the HTTP client and the local one both fit. */
export interface SearchApi {
  readonly repo: string;
  refs(): Promise<readonly Ref[]>;
  grep(pattern: string, ref: string, maxMatches?: number): Promise<GrepResponse>;
}

/** Where a branch stands against origin's copy of it; see `local.ts`. */
export interface SyncState {
  readonly branch: string;
  readonly ahead: number;
  readonly behind: number;
  readonly remote: string | null;
}

/** The sync verbs only the local client carries. */
export interface SyncCapable {
  sync(branch: string): Promise<SyncState>;
  push(
    branch: string,
  ): Promise<
    ReadonlyArray<{ readonly ref: string; readonly ok: boolean; readonly reason?: string }>
  >;
  fetchOrigin(): Promise<{ readonly updated: number; readonly rejected: number }>;
}

/**
 * Duck-typed on purpose: naming `LocalGitApi` here would pull the whole
 * OPFS/pack/Effect graph into the entry bundle that this check exists to
 * keep it out of.
 */
export const syncCapable = (api: CodeApi | null): (CodeApi & SyncCapable) | null => {
  if (api === null) return null;
  // SAFETY: the two implementations of `CodeApi` are this module's `GitApi`
  // (no `sync` member) and `LocalGitApi`, which declares the whole
  // `SyncCapable` contract — so a present `sync` implies the rest of it.
  const candidate = api as CodeApi & Partial<SyncCapable>;
  // SAFETY: same invariant — `sync` present means the local client, whole.
  return candidate.sync === undefined ? null : (candidate as CodeApi & SyncCapable);
};
