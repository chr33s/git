/**
 * URL → repository name, in one place.
 *
 * `git clone http://host/repo.git` and `http://host/repo` name the same
 * repository — git appends the suffix on its own when the URL has none, so a
 * server that treats them as two names hands the same user two empty
 * repositories depending on how they typed it. The suffix is stripped here,
 * once, rather than in each host's router.
 *
 * The name is also the storage key (a directory on node, a Durable Object name
 * on Cloudflare), so validation belongs on the same seam: no traversal, no
 * leading dot, and nothing outside the character set git itself accepts.
 */

/** No traversal, no hidden files, no path separators. */
const REPO_NAME = /^[A-Za-z0-9][A-Za-z0-9._-]*$/;

export interface Route {
  /** Suffix stripped, validated. */
  readonly repo: string;
  /** The first path segment after the repository, `""` at the root. */
  readonly route: string;
  /** Everything after the repository, no leading slash. */
  readonly rest: string;
}

/**
 * `null` when the first segment is missing or is not a usable name — the
 * caller answers 400, rather than creating storage under an attacker's key.
 */
export const routeOf = (pathname: string): Route | null => {
  const segments = pathname.split("/").filter((segment) => segment !== "");
  const first = segments[0];
  if (first === undefined) return null;

  // Only the trailing `.git` is a suffix; `my.git.repo` keeps its name.
  const repo = first.endsWith(".git") ? first.slice(0, -4) : first;
  if (!REPO_NAME.test(repo) || repo.includes("..")) return null;

  return { repo, route: segments[1] ?? "", rest: segments.slice(1).join("/") };
};

/**
 * Whether this request is the one that deletes objects.
 *
 * A host serializes handlers, but a pack body keeps reading the store after
 * its handler returned, so collection is the single caller that has to wait
 * for the bodies still in flight. Recognising it is routing, which is why the
 * rule lives here instead of once per host.
 */
export const collects = (request: Request): boolean => {
  if (request.method !== "POST") return false;
  const segments = new URL(request.url).pathname.split("/").filter((segment) => segment !== "");
  return segments.at(-1) === "gc";
};

/**
 * How long collection waits for those bodies.
 *
 * Bounded because the wait is on a client's pace: a stalled clone must not
 * postpone maintenance forever. Exceeding it is rare enough to accept the
 * narrow window it reopens — and gc's reflog grace covers recent objects.
 */
export const DELIVERY_WAIT_MS = 30_000;

/**
 * Wait for those bodies, but not past the bound.
 *
 * The timer is cleared rather than left to fire: on node an armed `setTimeout`
 * keeps the event loop alive, so a server that answered one `gc` would refuse
 * to exit for the full wait afterwards.
 */
export const settledWithin = async (
  pending: Iterable<Promise<unknown>>,
  withinMs: number = DELIVERY_WAIT_MS,
): Promise<void> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const bound = new Promise<void>((resolve) => {
    timer = setTimeout(resolve, withinMs);
  });
  try {
    await Promise.race([Promise.allSettled(pending), bound]);
  } finally {
    clearTimeout(timer);
  }
};

/**
 * The same request with the repository segment normalised out of the path.
 *
 * Handlers match on the tail (`info/refs`, `git-upload-pack`), so a request
 * that arrived as `/repo.git/info/refs` has to reach them looking like
 * `/repo/info/refs` — otherwise the suffix leaks into every downstream match.
 */
export const normalize = (request: Request, route: Route): Request => {
  const url = new URL(request.url);
  const normalized = `/${route.repo}${route.rest === "" ? "" : `/${route.rest}`}`;
  if (url.pathname === normalized) return request;
  url.pathname = normalized;
  return new Request(url, request);
};
