/**
 * The addresses, as a parsed value.
 *
 * `route.ts` is the string half of this — the prefix, the encoding, the shape
 * the Lit shell reads — and this is the same contract expressed the way
 * Foldkit's router wants it: a union of what an address *means*, plus parsers
 * that turn a path into one and print one back into a path.
 *
 * The two agree by construction. Every router here is built from `PREFIX`, and
 * the round trip is checked in `app.route.test.ts` against `pathOf`, so the
 * shell and Foldkit cannot drift apart while both are in the page.
 *
 * `restString` on the Code route is what makes a file path work as one route
 * parameter: `src/server/Api.ts` carries slashes that are structure to the URL
 * and content to the screen, and a `string` segment would stop at the first.
 */
import { Route } from "foldkit";
import { Schema } from "effect";
import { pipe } from "effect";

import { PREFIX } from "./route.ts";

const prefix = PREFIX.slice(1);

export const AppRoute = Route.defineRouteUnion({
  Activity: {},
  Code: { path: Schema.String },
  Tasks: {},
  Detail: { id: Schema.String },
  Settings: {},
  Search: {},
  /** An address this application does not have. `path` is what was asked for. */
  NotFound: { path: Schema.String },
});
export type AppRoute = typeof AppRoute.Type;

/**
 * The Code screen with nothing open.
 *
 * `mapTo` takes a constructor rather than a function, so a route that supplies
 * a constant is spelled as one — this is the only place two addresses (`/hub`
 * and `/hub/code`) mean the same route, and they share it.
 */
const bareCode = { make: (): AppRoute => AppRoute.Code({ path: "" }) };

/** `/hub` alone, which is the Code screen: the shell's default. */
const home = pipe(Route.literal(prefix), Route.mapTo(bareCode));

const activity = pipe(
  Route.literal(prefix),
  Route.slash(Route.literal("activity")),
  Route.mapTo(AppRoute.Activity),
);

/**
 * The entry file, by name.
 *
 * `/hub/index.html` is this page — a reader who types it means the
 * application, not an address it does not have — so it opens the default
 * screen rather than the not-found one.
 */
const entryFile = pipe(
  Route.literal(prefix),
  Route.slash(Route.literal("index.html")),
  Route.mapTo(bareCode),
);

const code = pipe(Route.literal(prefix), Route.slash(Route.literal("code")), Route.mapTo(bareCode));

/** `/hub/code/src/server/Api.ts` — the tail is the file, slashes and all. */
const codePath = pipe(
  Route.literal(prefix),
  Route.slash(Route.literal("code")),
  Route.slash(Route.restString("path")),
  Route.mapTo(AppRoute.Code),
);

const tasks = pipe(
  Route.literal(prefix),
  Route.slash(Route.literal("tasks")),
  Route.mapTo(AppRoute.Tasks),
);

const detail = pipe(
  Route.literal(prefix),
  Route.slash(Route.literal("detail")),
  Route.slash(Route.string("id")),
  Route.mapTo(AppRoute.Detail),
);

const settings = pipe(
  Route.literal(prefix),
  Route.slash(Route.literal("settings")),
  Route.mapTo(AppRoute.Settings),
);

const search = pipe(
  Route.literal(prefix),
  Route.slash(Route.literal("search")),
  Route.mapTo(AppRoute.Search),
);

const parse = Route.parseUrlWithFallback(
  Route.oneOf(codePath, code, entryFile, activity, tasks, detail, settings, search, home),
  AppRoute.NotFound,
);

/** One decoded segment, or `null` for a malformed escape — never a throw. */
const decodeSegment = (segment: string): string | null => {
  try {
    return decodeURIComponent(segment);
  } catch {
    return null;
  }
};

/**
 * Read an address.
 *
 * The parser matches segments literally, so the decoding is here: a file path
 * keeps its `/` as route structure while everything inside a segment — spaces,
 * `%`, `#`, Unicode — is component-encoded, and `urlOf` writes exactly what
 * this reads back.
 *
 * `NotFound` rather than a failure, in both directions. An address the
 * application does not have is something a reader can type, and so is a
 * malformed escape: the screen says so with the shell around it rather than
 * the page failing to start.
 */
export const routeOfUrl = (url: Parameters<typeof parse>[0]): AppRoute => {
  const route = parse(url);
  if (route._tag === "Code") {
    if (route.path === "") return route;
    const decoded = route.path.split("/").map(decodeSegment);
    if (decoded.some((segment) => segment === null)) {
      return AppRoute.NotFound({ path: url.pathname });
    }
    return AppRoute.Code({ path: decoded.join("/") });
  }
  if (route._tag === "Detail") {
    const decoded = decodeSegment(route.id);
    return decoded === null
      ? AppRoute.NotFound({ path: url.pathname })
      : AppRoute.Detail({ id: decoded });
  }
  return route;
};

/** The address a route is at, for `pushUrl` and for the rail's links. */
export const urlOf = (route: AppRoute): string =>
  AppRoute.match(route, {
    Activity: () => `${PREFIX}/activity`,
    Code: ({ path }) => (path === "" ? `${PREFIX}/code` : `${PREFIX}/code/${encodeSegments(path)}`),
    Tasks: () => `${PREFIX}/tasks`,
    Detail: ({ id }) => `${PREFIX}/detail/${encodeURIComponent(id)}`,
    Settings: () => `${PREFIX}/settings`,
    Search: () => `${PREFIX}/search`,
    NotFound: ({ path }) => path,
  });

/**
 * Route ids, encoded per segment.
 *
 * A file path keeps its `/` as route structure while everything inside a
 * segment — spaces, `%`, `#`, Unicode — is component-encoded, so a copied URL
 * survives the address bar and a refresh.
 */
const encodeSegments = (path: string): string => path.split("/").map(encodeURIComponent).join("/");

/** Which rail item is current. `detail` is reached from Tasks, so it counts. */
export const railScreen = (route: AppRoute): string =>
  AppRoute.match(route, {
    Activity: () => "activity",
    Code: () => "code",
    Tasks: () => "tasks",
    Detail: () => "tasks",
    Settings: () => "settings",
    Search: () => "search",
    NotFound: () => "",
  });
