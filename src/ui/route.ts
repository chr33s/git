/**
 * URL ↔ screen, in one place.
 *
 * The UI serves from real paths under `/hub`, not from the fragment. Foldkit's
 * router parses `url.pathname` and its runtime listens for `popstate`, never
 * for `hashchange` — so a hash route is invisible to it, and the framework
 * migration in `docs/hub.foldkit.md` cannot begin until the addresses move.
 * The prefix exists because the Worker resolves a bare first segment to a
 * repository (`src/server/Route.ts`); `hub` is reserved there so this page and
 * `/:repo/...` can share one origin without either shadowing the other.
 *
 * The Lit shell reads this module today and Foldkit's route parsers will read
 * it tomorrow, so both agree on one answer while the two frameworks overlap.
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

/** Where `/hub` alone lands, and what `/` redirects to. */
export const DEFAULT_SCREEN: Screen = "code";

export interface Route {
  readonly screen: Screen;
  /** A Task or Change Request id for `detail`, a file path for `code`. */
  readonly id: string | null;
  /** Set when the address was addressed to this app but could not be read. */
  readonly malformed: boolean;
}

/**
 * Route ids, encoded per segment. A file path keeps its `/` as route
 * structure while everything inside a segment — spaces, `%`, `#`, Unicode —
 * is component-encoded, so a copied URL survives the address bar and a
 * refresh instead of being truncated at the first character a URL parser
 * claims for itself.
 */
const encodeId = (id: string): string => id.split("/").map(encodeURIComponent).join("/");

/** One decoded segment, or `null` for a malformed escape — never a throw. */
const decodeSegment = (segment: string): string | null => {
  try {
    return decodeURIComponent(segment);
  } catch {
    return null;
  }
};

/** `/hub/detail/CR-14`, `/hub/code/src/server/Api.ts`, `/hub/tasks`. */
export const pathOf = (screen: Screen, id?: string): string =>
  id === undefined || id === "" ? `${PREFIX}/${screen}` : `${PREFIX}/${screen}/${encodeId(id)}`;

/**
 * Read an address, or `null` when it names no screen.
 *
 * `null` covers both a path outside `/hub` and `/hub` itself; the caller
 * decides whether that means "leave the current screen alone" (a stray
 * `popstate`) or "show the default" (first paint). A malformed escape is
 * *not* `null` — the screen is known, so the shell shows it with a visible
 * navigation error rather than silently ignoring the address.
 */
export const routeOf = (pathname: string): Route | null => {
  const segments = pathname.split("/").filter((segment) => segment !== "");
  if (segments[0] !== PREFIX.slice(1)) return null;

  const screen = segments[1];
  if (screen === undefined || !isScreen(screen)) return null;

  const rest = segments.slice(2).map(decodeSegment);
  if (rest.some((segment) => segment === null)) return { screen, id: null, malformed: true };

  const id = rest.join("/");
  return { screen, id: id === "" ? null : id, malformed: false };
};

/**
 * The `#/screen/id` address this page used before it moved to `/hub`, as a
 * path — or `null` when the fragment is not one of ours.
 *
 * Kept alongside the inline redirect in `index.html` so the two cannot drift:
 * that script runs before the bundle and handles a cold load, this handles a
 * link clicked into a page already open. Both go when the links age out.
 */
export const fromLegacyHash = (hash: string): string | null => {
  if (!hash.startsWith("#/")) return null;
  const [screen, ...rest] = hash.slice(2).split("/");
  if (screen === undefined || !isScreen(screen)) return null;
  return rest.length === 0 ? `${PREFIX}/${screen}` : `${PREFIX}/${screen}/${rest.join("/")}`;
};
