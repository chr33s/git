/**
 * A browser client for the repository's JSON API.
 *
 * The endpoints, paths and payload shapes here mirror `src/server/Api.ts`,
 * which is the source of truth: one `HttpApi` value carrying both the `repo`
 * and `remotes` groups, each prefixed `/:repo`. The types below are transcribed
 * from those `Schema.Struct`s.
 *
 * They are transcribed rather than derived on purpose. `HttpApiClient.make(api)`
 * would give a client that cannot drift — but `api` is declared in the same
 * module as its handlers, deliberately ("the declaration is one value on
 * purpose"), and those handlers reach `Repository`, `Policy`, `Auth`,
 * `FileSystem` and `Path`. Importing the declaration would pull the whole
 * server into the browser bundle. Until the declaration is separable, a hand
 * transcription is the cheaper half of the trade.
 *
 * Everything is decoded at this boundary and handed on as a domain value, so no
 * screen ever touches a raw response.
 */

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
}

/** A ref as `/refs` and `/branches` return it. */
export interface Ref {
  readonly name: string;
  readonly oid: string;
}

/** One entry of `/files`: a blob, with the tree path that reaches it. */
export interface FileEntry {
  readonly path: string;
  readonly mode: string;
  readonly oid: string;
}

/** `/file`: content always arrives base64, because the server cannot know a
 * blob is text and guessing would corrupt the bytes that are not. */
export interface FileContent {
  readonly path: string;
  readonly mode: string;
  readonly oid: string;
  readonly content: string;
  readonly encoding: "base64";
  readonly size: number;
}

/** One file of a `/diff` answer, carrying a real unified patch. */
export interface DiffFile {
  readonly path: string;
  readonly status: "added" | "removed" | "modified";
  readonly binary: boolean;
  readonly patch: string;
}

export interface CommitSummary {
  readonly oid: string;
  readonly message: string;
}

export interface Commit {
  readonly message: string;
  readonly parents: readonly string[];
  readonly tree: string;
}

interface RefsResponse {
  readonly refs: readonly Ref[];
}

interface FilesResponse {
  readonly files: readonly FileEntry[];
}

/** The `/diff` request payload, as `src/server/Api.ts` declares it. */
interface DiffRequest {
  readonly from: string;
  readonly to: string;
  readonly path?: string;
}

interface DiffResponse {
  readonly files: readonly DiffFile[];
}

interface LogResponse {
  readonly commits: readonly CommitSummary[];
}

interface PageResponse<A> {
  readonly items: readonly A[];
  readonly next_cursor: string | null;
  readonly has_more: boolean;
}

/** A tagged error body, as `git/Error.ts` puts it on the wire. */
interface ErrorBody {
  readonly _tag?: string;
  readonly message?: string;
}

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

  async #json<A>(url: string, init?: RequestInit): Promise<A> {
    const response = await fetch(url, init);
    if (!response.ok) {
      // SAFETY: an error body is JSON on every path that produces one; a
      // parse failure falls through to the status-only message below.
      const body = (await response.json().catch(() => ({}) as ErrorBody)) as ErrorBody;
      throw new ApiError(
        body._tag ?? "HttpError",
        response.status,
        body.message ?? `${response.status} ${response.statusText}`,
      );
    }
    // SAFETY: the endpoint's success schema, transcribed above. The server
    // encodes through that schema, so a well-formed 2xx matches `A`.
    return (await response.json()) as A;
  }

  /** Every ref, unpaged — the shape the smart-HTTP advertisement needs. */
  async refs(): Promise<readonly Ref[]> {
    const body = await this.#json<RefsResponse>(this.#url("/refs"));
    return body.refs;
  }

  /** Branches, paged; one page is enough for a branch picker. */
  async branches(limit = "100"): Promise<readonly Ref[]> {
    const body = await this.#json<PageResponse<Ref>>(
      this.#url("/branches", new URLSearchParams({ limit })),
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
    const body = await this.#json<FilesResponse>(this.#url("/files", query));
    return body.files;
  }

  /** One blob's content, already decoded from base64. */
  async file(ref: string, path: string): Promise<string> {
    const body = await this.#json<FileContent>(
      this.#url("/file", new URLSearchParams({ ref, path })),
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
    const payload: DiffRequest = path === undefined ? { from, to } : { from, to, path };
    const body = await this.#json<DiffResponse>(this.#url("/diff"), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });
    return body.files;
  }

  /** Commit history from a starting oid, newest first. */
  async log(oid: string): Promise<readonly CommitSummary[]> {
    const body = await this.#json<LogResponse>(this.#url(`/log/${oid}`));
    return body.commits;
  }

  /** One commit's message, parents and tree. */
  async commit(oid: string): Promise<Commit> {
    return await this.#json<Commit>(this.#url(`/commit/${oid}`));
  }

  /** Commits touching one path, newest first. */
  async history(oid: string, path: string, limit = "20"): Promise<readonly CommitSummary[]> {
    const body = await this.#json<PageResponse<CommitSummary>>(
      this.#url(`/history/${oid}`, new URLSearchParams({ path, limit })),
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
