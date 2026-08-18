/**
 * A browser client for the repository's JSON API.
 *
 * The endpoints and paths mirror `src/server/Api.ts`; their shared response
 * shapes come from the contract that module uses.
 *
 * The browser-safe response schemas live in `src/server/UiApiContract.ts` and
 * are also used by the HTTP declaration. Everything is decoded against those
 * schemas at this boundary and handed on as a domain value, so a server/client
 * drift is an explicit `InvalidResponse` rather than an unsafe assertion.
 */

import { Option, Schema } from "effect";

import * as Contract from "../src/server/UiApiContract.ts";

/** Thrown for any non-2xx answer, carrying the server's tagged error name. */
export class ApiError extends Error {
  constructor(
    readonly tag: string,
    readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }

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
export type FileContent = Contract.FileContent;
export type DiffFile = Contract.DiffFile;
export type CommitSummary = Contract.CommitSummary;
export type Commit = Contract.Commit;

/**
 * A commit with its author and date.
 *
 * Assembled from `/object/:oid` rather than `/commit/:oid`, because **no JSON
 * endpoint carries a commit timestamp**: `/log`, `/commits` and `/history`
 * answer `{ oid, message }`, and `/commit/:oid` adds only parents and tree. The
 * raw object does carry it — `author NAME <EMAIL> UNIXTS TZ` — so the header is
 * parsed here, at the boundary, and every screen above sees a `Date`.
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

/** A raw object, decoded from base64 to text. */
export interface RawObject {
  readonly oid: string;
  readonly type: "blob" | "tree" | "commit" | "tag";
  readonly text: string;
}

/**
 * Who the server thinks is asking, and what they may do.
 *
 * `subject` is null for an unauthenticated caller, and for a repository with no
 * genesis `member` is false with `why` explaining it — so the UI shows a real
 * identity when there is one and says nothing rather than inventing one.
 */
export type Whoami = Contract.WhoamiAnswer;

/** A tagged error body, as `git/Error.ts` puts it on the wire. */
const ErrorBody = Schema.Struct({
  _tag: Schema.optional(Schema.String),
  message: Schema.optional(Schema.String),
  /** `Invalid` carries the detail here rather than in `message`. */
  reason: Schema.optional(Schema.String),
});

const OID = /^[0-9a-f]{40}$/;

/**
 * Qualify a ref the way the server insists on.
 *
 * `/files`, `/file` and `/diff` all resolve a ref strictly: an oid, `HEAD`, or a
 * full `refs/...` name. A bare branch name comes back
 * `400 Invalid { field: "ref", reason: "unknown ref 'main'" }` — so the friendly
 * name the UI shows is expanded here, at the boundary, rather than every screen
 * remembering to do it.
 */
export const qualify = (ref: string): string =>
  OID.test(ref) || ref.startsWith("refs/") || ref === "HEAD" ? ref : `refs/heads/${ref}`;

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

/**
 * `author NAME <EMAIL> UNIXTS TZ`, from a raw commit's header.
 *
 * Anchored to the start of a line so a message body mentioning "author" cannot
 * be mistaken for the header field.
 */
const AUTHOR = /^author (.+) <([^>]*)> (\d+) ([+-]\d{4})$/m;

const PARENT = /^parent ([0-9a-f]{40})$/gm;

/**
 * Split a raw commit into the parts the UI needs.
 *
 * The header ends at the first blank line; everything after it is the message,
 * whose first line is the subject. A commit with no parsable author line is not
 * an error worth failing a screen over — the date falls back to the epoch and
 * the caller can still show the subject.
 */
const parseCommit = (oid: string, raw: string): CommitDetail => {
  const blank = raw.indexOf("\n\n");
  const header = blank === -1 ? raw : raw.slice(0, blank);
  const message = blank === -1 ? "" : raw.slice(blank + 2);
  const author = AUTHOR.exec(header);
  const parents: string[] = [];
  for (const match of header.matchAll(PARENT)) {
    const parent = match[1];
    if (parent !== undefined) parents.push(parent);
  }
  const seconds = author?.[3];
  return {
    oid,
    subject: (message.split("\n", 1)[0] ?? "").trim(),
    author: author?.[1] ?? "unknown",
    email: author?.[2] ?? "",
    at: new Date(seconds === undefined ? 0 : Number(seconds) * 1000),
    parents,
  };
};

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

  #url(path: string, query?: URLSearchParams): string {
    const search = query === undefined || query.size === 0 ? "" : `?${query.toString()}`;
    return `${this.#base}/${encodeURIComponent(this.repo)}${path}${search}`;
  }

  async #json<S extends Schema.ConstraintDecoder<unknown>>(
    url: string,
    schema: S,
    init?: RequestInit,
  ): Promise<S["Type"]> {
    const response = await fetch(url, init);
    if (!response.ok) {
      const decoded = Schema.decodeUnknownOption(ErrorBody)(
        await response.json().catch((): undefined => undefined),
      );
      const body = Option.isSome(decoded) ? decoded.value : undefined;
      throw new ApiError(
        body?._tag ?? "HttpError",
        response.status,
        body?.message ?? body?.reason ?? `${response.status} ${response.statusText}`,
      );
    }
    const body: unknown = await response.json();
    try {
      return Schema.decodeUnknownSync(schema)(body);
    } catch (cause) {
      throw new ApiError(
        "InvalidResponse",
        response.status,
        cause instanceof Error ? cause.message : String(cause),
      );
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
    const query = new URLSearchParams({ ref: qualify(ref) });
    if (path !== undefined) query.set("path", path);
    const body = await this.#json(this.#url("/files", query), Contract.FilesResponse);
    return body.files;
  }

  /** One blob's content, already decoded from base64. */
  async file(ref: string, path: string): Promise<string> {
    const body = await this.#json(
      this.#url("/file", new URLSearchParams({ ref: qualify(ref), path })),
      Contract.FileContent,
    );
    return decodeContent(body.content);
  }

  /**
   * The patch set between two revisions.
   *
   * The server runs the diff itself and answers unified patches, which is
   * exactly what `@pierre/diffs` renders — so no diffing happens in the
   * browser.
   */
  async diff(from: string, to: string, path?: string): Promise<readonly DiffFile[]> {
    const from_ = qualify(from);
    const to_ = qualify(to);
    const payload: Contract.DiffRequest =
      path === undefined ? { from: from_, to: to_ } : { from: from_, to: to_, path };
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

  /** One raw object, decoded to text. */
  async object(oid: string): Promise<RawObject> {
    const body = await this.#json(this.#url(`/object/${oid}`), Contract.RawObject);
    return { oid: body.oid, type: body.type, text: decodeContent(body.content) };
  }

  /** One commit, with the author and date its raw header carries. */
  async commitDetail(oid: string): Promise<CommitDetail> {
    const object = await this.object(oid);
    return parseCommit(oid, object.text);
  }

  /**
   * The newest `limit` commits reachable from `oid`, each with author and date.
   *
   * One request for the list and one per commit for its header, because the
   * list endpoint carries no timestamp. Bounded by `limit` precisely because
   * that makes it N+1.
   */
  async recentCommits(oid: string, limit = 30): Promise<readonly CommitDetail[]> {
    const page = await this.#json(
      this.#url(`/commits/${oid}`, new URLSearchParams({ limit: String(limit) })),
      Contract.CommitPage,
    );
    return await Promise.all(page.items.map((item) => this.commitDetail(item.oid)));
  }

  /** Commit history from a starting oid, newest first. */
  async log(oid: string): Promise<readonly CommitSummary[]> {
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
