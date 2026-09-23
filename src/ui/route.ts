/**
 * The address strings every other routing module builds on.
 *
 * The UI serves from real paths under `/hub`, not from the fragment. Foldkit's
 * router parses `url.pathname` and its runtime listens for `popstate`, never
 * for `hashchange`, so a hash route would be invisible to it. The prefix
 * exists because the Worker resolves a bare first segment to a repository
 * (`src/server/Route.ts`); `hub` is reserved there so this page and
 * `/:repo/...` can share one origin without either shadowing the other.
 *
 * Parsing and printing an address is `app.route.ts`'s alone — it is the
 * router the application runs, and `route.test.ts` checks its round trip.
 * This holds only what that router and its neighbours share: the prefix, the
 * screen names `dev.ts` reads to tell a screen from a module Vite would
 * otherwise resolve for the same path, and the rewrite of the addresses this
 * page used before it moved. The prefix the *hosts* match on is
 * `src/server/Route.ts`'s `UI_PREFIX`, which `PREFIX` below is checked against.
 */
/**
 * The screens this UI can show.
 *
 * `detail` covers both a Task and a Change Request, because a Change Request
 * *is* a Task with a diff attached — the spec is explicit that they are not
 * parallel entity types — so one screen renders both and shows the extra
 * sections only when a diff exists.
 */
export type Screen = "activity" | "code" | "tasks" | "detail" | "settings" | "search";

/**
 * Every screen the UI can show.
 *
 * A record rather than a list, so the set is exhaustive in both directions:
 * a screen added to `Screen` and forgotten here is a compile error, and a key
 * here that is not a screen is one too. A list would only have caught the
 * second, and the first is the one that silently 404s a working route.
 */
const SCREENS = {
  activity: true,
  code: true,
  tasks: true,
  detail: true,
  settings: true,
  search: true,
} satisfies Record<Screen, true>;

export const isScreen = (value: string): value is Screen => Object.hasOwn(SCREENS, value);

/** The path this application is mounted at. Also Vite's `base`. */
export const PREFIX = "/hub";

/**
 * The `#/screen/id` address this page used before it moved to `/hub`, as a
 * path — or `null` when the fragment is not one of ours.
 *
 * Kept alongside the inline redirect in `index.html` so the two cannot drift:
 * that script runs before the bundle and handles a cold load, this handles a
 * link clicked into a page already open. Both go when the links age out.
 */
export const fromLegacyHash = (hash: string): string | null => {
  // With or without the "#": `location.hash` carries it and Foldkit's parsed
  // `Url.hash` does not, and both callers are real.
  const fragment = hash.startsWith("#") ? hash.slice(1) : hash;
  if (!fragment.startsWith("/")) return null;
  const [screen, ...rest] = fragment.slice(1).split("/");
  if (screen === undefined || !isScreen(screen)) return null;
  return rest.length === 0 ? `${PREFIX}/${screen}` : `${PREFIX}/${screen}/${rest.join("/")}`;
};
