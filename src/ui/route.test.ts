/**
 * The address bar is the one piece of state a user can type, bookmark and
 * send to someone else, so what these pin is the round trip: a path this
 * module writes is a path it reads back the same way, including the file
 * paths and Unicode ids that made the old fragment routes brittle.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

import { fromLegacyHash, pathOf, PREFIX, routeOf } from "./route.ts";

describe("routeOf", () => {
  it.effect("reads the screen and what it opens", () =>
    Effect.sync(() => {
      assert.deepEqual(routeOf("/hub/tasks"), { screen: "tasks", id: null, malformed: false });
      assert.deepEqual(routeOf("/hub/detail/CR-14"), {
        screen: "detail",
        id: "CR-14",
        malformed: false,
      });
    }),
  );

  it.effect("keeps a file path whole", () =>
    Effect.sync(() => {
      // The slashes are route structure, so the id is everything after the
      // screen — not just the segment that follows it.
      assert.equal(routeOf("/hub/code/src/server/Api.ts")?.id, "src/server/Api.ts");
    }),
  );

  it.effect("declines an address that is not this application's", () =>
    Effect.sync(() => {
      assert.equal(routeOf("/"), null);
      assert.equal(routeOf("/core/info/refs"), null);
      // Whole segments: `hubbub` is a repository, not this prefix.
      assert.equal(routeOf("/hubbub/tasks"), null);
      // Under the prefix, but naming no screen this UI has.
      assert.equal(routeOf("/hub"), null);
      assert.equal(routeOf("/hub/nope"), null);
    }),
  );

  it.effect("reports a malformed escape rather than throwing on it", () =>
    Effect.sync(() => {
      // `decodeURIComponent` throws on a truncated escape. The screen is still
      // known, so the shell shows it with a visible error instead of failing
      // during initialisation — which is why this is not simply `null`.
      assert.deepEqual(routeOf("/hub/code/%E0%A4%A"), {
        screen: "code",
        id: null,
        malformed: true,
      });
    }),
  );
});

describe("pathOf", () => {
  it.effect("round-trips every id through the address bar", () =>
    Effect.sync(() => {
      const trip = (screen: "code" | "detail", id: string): string | null =>
        routeOf(pathOf(screen, id))?.id ?? null;
      assert.equal(trip("code", "src/a b/#hash%.ts"), "src/a b/#hash%.ts");
      assert.equal(trip("code", "docs/ünïcode.md"), "docs/ünïcode.md");
      assert.equal(trip("detail", "CR-14"), "CR-14");
    }),
  );

  it.effect("writes a bare screen when nothing is opened", () =>
    Effect.sync(() => {
      assert.equal(pathOf("tasks"), `${PREFIX}/tasks`);
      assert.equal(pathOf("tasks", ""), `${PREFIX}/tasks`);
    }),
  );

  it.effect("encodes per segment, so structure survives and content escapes", () =>
    Effect.sync(() => {
      // The separators stay separators; everything inside them is escaped.
      assert.equal(pathOf("code", "a/b c"), "/hub/code/a/b%20c");
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
