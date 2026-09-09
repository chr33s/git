/**
 * Which client answers for the repository, and who holds it.
 *
 * The Model holds a lifecycle — `Unavailable`, `Opening`, `Ready`, `Failed` —
 * and this holds the handle behind it. That split is the rule the migration is
 * for: a handle has a lifetime, a lifetime is not a fact, and a value the
 * Model can serialise must not be one that has to be released.
 *
 * A service boundary rather than a Foldkit ManagedResource, and deliberately:
 * the handle's lifetime is the page's, not a Model condition's. It is opened
 * once, off the boot path, and the only thing that would release it is the
 * page going away. What a ManagedResource buys — release and reacquire when
 * the Model changes — is exactly what nothing here asks for.
 *
 * The import is dynamic for the same reason `highlight.ts`'s is: the local
 * client carries the pack machinery and the Effect runtime, and first paint
 * should not wait on either.
 */
import type { CodeApi, GitApi, SearchApi, SyncCapable } from "./api.ts";

/** The OPFS-backed client, once its first-load clone lands. */
let local: (CodeApi & SearchApi & SyncCapable) | null = null;

/** Opened once; a second caller joins the first rather than cloning twice. */
let opening: Promise<boolean> | null = null;

/**
 * The last subject `signAs` was told about.
 *
 * Remembered rather than passed, because the clone and `/whoami` race and
 * either can land first. Holding it here is what makes the two orderings the
 * same: whichever arrives second finds the other already recorded, and a
 * commit written through the clone carries the reader's name in both.
 */
let signer: string | null = null;

/**
 * Open — and on first load, clone — the repository in OPFS.
 *
 * `false` for a browser without OPFS, or with the remote unreachable on first
 * load. Nothing about the page changes in that case: the HTTP client keeps
 * answering, which is the documented behaviour rather than a failure.
 */
export const open = async (http: GitApi, author: string | null): Promise<boolean> => {
  opening ??= (async (): Promise<boolean> => {
    try {
      const { LocalGitApi } = await import("./local.ts");
      const opened = await LocalGitApi.open({ repo: http.repo, cloneUrl: http.cloneUrl });
      if (opened === null) return false;
      local = opened;
      signAs(author);
      return true;
    } catch {
      return false;
    }
  })();
  return await opening;
};

/**
 * Author local commits as whoever `/whoami` said is asking.
 *
 * Called from both sides of the race — when identity resolves, and when the
 * clone opens — and each call is a no-op until both have happened.
 */
export const signAs = (subject: string | null): void => {
  if (subject !== null) signer = subject;
  if (local === null || signer === null) return;
  // SAFETY: `author` belongs to `LocalGitApi` alone, which is the only thing
  // `open` above ever assigns to `local`.
  (local as { author?: { name: string; email: string } }).author = {
    name: signer,
    email: `${signer}@git-plus.local`,
  };
};

/**
 * The client the Code screen should read through.
 *
 * The local repository when it is open — which turns a hundred-commit history
 * read into local object reads rather than an N+1 of requests — and the HTTP
 * client otherwise.
 */
export const reading = (http: GitApi): CodeApi => local ?? http;

/**
 * The client the Search screen should read through.
 *
 * The same rule as `reading`, and for the same reason: once the clone is open
 * it holds work that origin has not seen. Searching the server instead would
 * answer over the older tree — a file committed in the browser and not yet
 * pushed is on screen in Code and missing from the search that is supposed to
 * find it — and would report the server's outage as a failure for a
 * repository the browser is holding.
 */
export const searching = (http: GitApi): SearchApi => local ?? http;

/** The sync verbs, or `null`: against the HTTP client there is no "against". */
export const syncing = (): (CodeApi & SyncCapable) | null => local;
