/**
 * The address bar is the one piece of state a user can type, bookmark and
 * send to someone else, so what these pin is the round trip: a path the
 * application writes is a path it reads back the same way, including the file
 * paths and Unicode ids that made the old fragment routes brittle.
 *
 * Against `app.route.ts` — the router the application runs — rather than a
 * copy of it kept for testing, so a regression in the real parser fails here.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Option } from "effect";
import { Url } from "foldkit";

import { UI_HOME, UI_PREFIX } from "../server/Route.ts";
import { AppRoute, routeOfUrl, urlOf } from "./app.route.ts";
import { fromLegacyHash, PREFIX } from "./route.ts";

const at = (pathname: string): AppRoute =>
  routeOfUrl(Option.getOrThrow(Url.fromString(`http://localhost${pathname}`)));

describe("the prefix", () => {
  it.effect("is the one the hosts reserve and match on", () =>
    Effect.sync(() => {
      // Two literals, one boundary: `src/server/Route.ts` refuses `hub` as a
      // repository name and both hosts answer under it, while this module is
      // what the page's own addresses are built from. They cannot be allowed
      // to drift apart in silence.
      assert.equal(PREFIX, UI_PREFIX);
    }),
  );

  it.effect("sends the hosts' home to the screen the shell opens by default", () =>
    Effect.sync(() => {
      assert.deepEqual(at(UI_HOME), AppRoute.Code({ path: "" }));
      assert.equal(urlOf(AppRoute.Code({ path: "" })), UI_HOME);
    }),
  );
});

describe("routeOfUrl", () => {
  it.effect("reads the screen and what it opens", () =>
    Effect.sync(() => {
      assert.deepEqual(at("/hub/tasks"), AppRoute.Tasks());
      assert.deepEqual(at("/hub/detail/CR-14"), AppRoute.Detail({ id: "CR-14" }));
    }),
  );

  it.effect("keeps a file path whole", () =>
    Effect.sync(() => {
      // The slashes are route structure, so the path is everything after the
      // screen — not just the segment that follows it.
      assert.deepEqual(
        at("/hub/code/src/server/Api.ts"),
        AppRoute.Code({ path: "src/server/Api.ts" }),
      );
    }),
  );

  it.effect("opens the default screen for the prefix alone and the entry file", () =>
    Effect.sync(() => {
      assert.deepEqual(at("/hub"), AppRoute.Code({ path: "" }));
      assert.deepEqual(at("/hub/index.html"), AppRoute.Code({ path: "" }));
    }),
  );

  it.effect("says an address it does not have is not found", () =>
    Effect.sync(() => {
      assert.deepEqual(at("/hub/nope"), AppRoute.NotFound({ path: "/hub/nope" }));
      // Whole segments: `hubbub` is a repository, not this prefix.
      assert.deepEqual(at("/hubbub/tasks"), AppRoute.NotFound({ path: "/hubbub/tasks" }));
    }),
  );

  it.effect("reports a malformed escape rather than throwing on it", () =>
    Effect.sync(() => {
      // `decodeURIComponent` throws on a truncated escape; the shell shows a
      // visible error instead of failing during initialisation.
      assert.deepEqual(at("/hub/code/%E0%A4%A"), AppRoute.NotFound({ path: "/hub/code/%E0%A4%A" }));
      assert.deepEqual(
        at("/hub/detail/%E0%A4%A"),
        AppRoute.NotFound({ path: "/hub/detail/%E0%A4%A" }),
      );
    }),
  );
});

describe("urlOf", () => {
  it.effect("round-trips every id through the address bar", () =>
    Effect.sync(() => {
      for (const route of [
        AppRoute.Code({ path: "src/a b/#hash%.ts" }),
        AppRoute.Code({ path: "docs/ünïcode.md" }),
        AppRoute.Detail({ id: "CR-14" }),
        // One segment in the route, so a slash inside an id is escaped rather
        // than read back as structure.
        AppRoute.Detail({ id: "a/b" }),
        AppRoute.Detail({ id: "ünï code#1" }),
        AppRoute.Activity(),
        AppRoute.Tasks(),
        AppRoute.Settings(),
        AppRoute.Search(),
      ]) {
        assert.deepEqual(at(urlOf(route)), route);
      }
    }),
  );

  it.effect("encodes per segment, so structure survives and content escapes", () =>
    Effect.sync(() => {
      assert.equal(urlOf(AppRoute.Code({ path: "a/b c" })), "/hub/code/a/b%20c");
      assert.equal(urlOf(AppRoute.Detail({ id: "a/b" })), "/hub/detail/a%2Fb");
    }),
  );
});

describe("fromLegacyHash", () => {
  it.effect("rewrites the addresses this UI used before /hub", () =>
    Effect.sync(() => {
      assert.equal(fromLegacyHash("#/tasks"), "/hub/tasks");
      assert.equal(fromLegacyHash("#/detail/CR-14"), "/hub/detail/CR-14");
      // Already encoded by whoever wrote the link; passed through untouched
      // so it decodes to the same id the old shell would have read.
      assert.equal(fromLegacyHash("#/code/src/a%20b.ts"), "/hub/code/src/a%20b.ts");
    }),
  );

  it.effect("leaves a fragment that is not a route alone", () =>
    Effect.sync(() => {
      assert.equal(fromLegacyHash(""), null);
      assert.equal(fromLegacyHash("#section"), null);
      assert.equal(fromLegacyHash("#/nope"), null);
    }),
  );
});
