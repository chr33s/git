/**
 * The client's half of the `Hub-SSH-v1` challenge, as one transport rule.
 *
 * Smart HTTP answers an unauthenticated request on a private repository with
 * a 401 carrying a nonce and the RepoID (`Auth.ts`'s `challenge`). Any client
 * holding a granted key can turn that refusal into access by signing an
 * `auth.request` envelope over exactly what the server will check: this
 * repository, this operation, these ref commands, that nonce. This module is
 * the retry discipline — one unauthenticated attempt, one signed retry —
 * shared by the fetch and push clients so no caller invents its own.
 *
 * The signing itself stays with the caller (an `Authorize` function): the
 * key lives in different places for different callers — the browser's OPFS,
 * a CLI's file — and this module never needs to see it.
 */

/** One ref command an envelope binds, in the server's own spelling. */
export interface SignedCommand {
  readonly ref: string;
  readonly from: string | null;
  readonly to: string | null;
}

/** What a refused request looked like, for whoever can sign the retry. */
export interface Challenged {
  /** The 401 itself; its `www-authenticate` carries the nonce and RepoID. */
  readonly response: Response;
  /** The operation the envelope must name — `Auth.ts`'s `operationOf`. */
  readonly operation: string;
  /** The ref commands the envelope must cover; empty for reads. */
  readonly commands: ReadonlyArray<SignedCommand>;
}

/**
 * Produce an `authorization` header for the retry, or `null` to give up —
 * a malformed challenge, or no key to sign with.
 */
export type Authorize = (challenged: Challenged) => Promise<string | null>;

/** The nonce a `www-authenticate` header offered, or `null`. */
export const nonceOf = (response: Response): string | null =>
  /nonce="([^"]+)"/.exec(response.headers.get("www-authenticate") ?? "")?.[1] ?? null;

/** The RepoID a `www-authenticate` header named, or `null`. */
export const repoOf = (response: Response): string | null =>
  /repo="([^"]+)"/.exec(response.headers.get("www-authenticate") ?? "")?.[1] ?? null;

/**
 * One request, retried exactly once under a signed envelope when challenged.
 *
 * Only a 401 is retried, and only when the caller can sign: repeated
 * refusals and malformed challenges come back as the response they are, so
 * a key the repository has not granted sees the refusal rather than a loop.
 */
export const fetchAuthorized = async (
  url: string,
  init: RequestInit,
  context: { readonly operation: string; readonly commands: ReadonlyArray<SignedCommand> },
  authorize: Authorize | undefined,
): Promise<Response> => {
  const first = await fetch(url, init);
  if (first.status !== 401 || authorize === undefined) return first;
  const header = await authorize({ response: first, ...context });
  if (header === null) return first;
  const headers = new Headers(init.headers);
  headers.set("authorization", header);
  return await fetch(url, { ...init, headers });
};

/** The operation a URL's request spells, matching the server's `operationOf`. */
export const operationOf = (method: string, url: string): string => {
  const pathname = new URL(url, "http://relative").pathname;
  const last = pathname.split("/").at(-1);
  return last === "git-receive-pack" || last === "git-upload-pack" ? last : `${method} ${pathname}`;
};
