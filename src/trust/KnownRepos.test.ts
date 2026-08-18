import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";

import { Effect } from "effect";

import type { RepoId } from "./Genesis.ts";
import {
  canonicalUrl,
  decide,
  formatFile,
  KnownRepos,
  mismatchMessage,
  parseFile,
} from "./KnownRepos.ts";
import { defaultPath, file } from "./KnownRepos.node.ts";

/** SAFETY: forty-three base64 characters after `SHA256:`, which is the shape. */
const repoId = (seed: string): RepoId => `SHA256:${seed.repeat(43).slice(0, 43)}` as RepoId;

const alpha = repoId("a");
const beta = repoId("b");

describe("KnownRepos", () => {
  let directory = "";
  let location = "";

  beforeEach(() => {
    directory = mkdtempSync(join(tmpdir(), "known-repos-"));
    location = join(directory, "known_repos");
  });

  afterEach(() => {
    rmSync(directory, { recursive: true, force: true });
  });

  const run = <A, E>(effect: Effect.Effect<A, E, KnownRepos>) =>
    Effect.runPromise(effect.pipe(Effect.provide(file(location))));

  describe("the file", () => {
    it("is empty before anything has been written", async () => {
      assert.deepEqual(await run(Effect.flatMap(KnownRepos, (store) => store.list)), []);
    });

    it("records a pin and reads it back", async () => {
      const pinned = await run(
        Effect.gen(function* () {
          const store = yield* KnownRepos;
          yield* store.remember({ url: "https://git.example.com/acme", repoId: alpha });
          return yield* store.lookup("https://git.example.com/acme");
        }),
      );
      assert.equal(pinned, alpha);
    });

    it("writes one entry per line, url then identity", async () => {
      await run(
        Effect.flatMap(KnownRepos, (store) =>
          store.remember({ url: "https://git.example.com/acme", repoId: alpha }),
        ),
      );
      assert.equal(readFileSync(location, "utf8"), `https://git.example.com/acme ${alpha}\n`);
    });

    it("leaves alone every line it does not recognise", async () => {
      // The file is a user's, and a user's file has comments in it and the
      // occasional typo. Rewritten by reformatting what parsed, an edit to one
      // pin quietly deleted the rest — and a deleted pin is not a cosmetic
      // loss: the next connection to that repository reads as *first use*, so
      // the identity-changed warning the pin existed to raise never comes.
      writeFileSync(
        location,
        [
          "# repositories I trust",
          "",
          `https://git.example.com/acme ${alpha}`,
          "https://git.example.com/typo NOTANIDENTITY",
          `https://git.example.com/other ${beta}`,
          "",
        ].join("\n"),
      );

      await run(
        Effect.gen(function* () {
          const store = yield* KnownRepos;
          yield* store.remember({ url: "https://git.example.com/acme", repoId: beta });
          yield* store.remember({ url: "https://git.example.com/fresh", repoId: alpha });
          return yield* store.forget("https://git.example.com/other");
        }),
      );

      assert.deepEqual(readFileSync(location, "utf8").split("\n"), [
        "# repositories I trust",
        "",
        `https://git.example.com/acme ${beta}`,
        "https://git.example.com/typo NOTANIDENTITY",
        `https://git.example.com/fresh ${alpha}`,
        "",
      ]);
    });

    it("replaces the entry for a url rather than appending a second", async () => {
      const entries = await run(
        Effect.gen(function* () {
          const store = yield* KnownRepos;
          yield* store.remember({ url: "https://git.example.com/acme", repoId: alpha });
          yield* store.remember({ url: "https://git.example.com/acme", repoId: beta });
          return yield* store.list;
        }),
      );
      assert.deepEqual(entries, [{ url: "https://git.example.com/acme", repoId: beta }]);
    });

    it("forgets a url, and says when there was nothing to forget", async () => {
      const outcome = await run(
        Effect.gen(function* () {
          const store = yield* KnownRepos;
          yield* store.remember({ url: "https://git.example.com/acme", repoId: alpha });
          return {
            removed: yield* store.forget("https://git.example.com/acme"),
            again: yield* store.forget("https://git.example.com/acme"),
            left: yield* store.list,
          };
        }),
      );
      assert.equal(outcome.removed, true);
      assert.equal(outcome.again, false);
      assert.deepEqual(outcome.left, []);
    });

    it("keeps reading a hand-edited file that has a bad line in it", async () => {
      // The store is documented as hand-editable; one typo must not take out
      // every other repository the user trusts.
      writeFileSync(
        location,
        [
          "# repositories I trust",
          `https://git.example.com/one ${alpha}`,
          "this line is nonsense",
          "https://git.example.com/two NOT-A-FINGERPRINT",
          "",
          `https://git.example.com/three ${beta}`,
        ].join("\n"),
      );

      const entries = await run(Effect.flatMap(KnownRepos, (store) => store.list));
      assert.deepEqual(entries, [
        { url: "https://git.example.com/one", repoId: alpha },
        { url: "https://git.example.com/three", repoId: beta },
      ]);
    });

    it("round-trips through the line format", () => {
      const entries = [
        { url: "https://git.example.com/one", repoId: alpha },
        { url: "https://git.example.com/two", repoId: beta },
      ];
      assert.deepEqual(parseFile(formatFile(entries)), entries);
    });
  });

  describe("the decision", () => {
    it("trusts a repository presenting the identity it presented before", async () => {
      const decision = await run(
        Effect.gen(function* () {
          const store = yield* KnownRepos;
          yield* store.remember({ url: "https://git.example.com/acme", repoId: alpha });
          return yield* decide("https://git.example.com/acme", alpha);
        }),
      );
      assert.equal(decision.kind, "trusted");
    });

    it("treats a url nobody has seen as first use", async () => {
      const decision = await run(decide("https://git.example.com/new", alpha));
      assert.equal(decision.kind, "new");
      assert.equal(decision.kind === "new" ? decision.alias : "", null);
    });

    it("recognises a repository that moved to another url", async () => {
      const decision = await run(
        Effect.gen(function* () {
          const store = yield* KnownRepos;
          yield* store.remember({ url: "https://old.example.com/acme", repoId: alpha });
          return yield* decide("https://new.example.com/acme", alpha);
        }),
      );
      assert.equal(decision.kind, "new");
      assert.equal(
        decision.kind === "new" ? decision.alias : null,
        "https://old.example.com/acme",
        "a move should be recognised, not a fresh trust decision",
      );
    });

    it("reports a changed identity with both sides of the mismatch", async () => {
      const decision = await run(
        Effect.gen(function* () {
          const store = yield* KnownRepos;
          yield* store.remember({ url: "https://git.example.com/acme", repoId: alpha });
          return yield* decide("https://git.example.com/acme", beta);
        }),
      );

      assert.equal(decision.kind, "changed");
      if (decision.kind !== "changed") return;
      assert.equal(decision.expected, alpha);
      assert.equal(decision.presented, beta);

      const warning = mismatchMessage("https://git.example.com/acme", alpha, beta);
      assert.match(warning, /REPOSITORY IDENTITY HAS CHANGED/);
      assert.ok(warning.includes(alpha) && warning.includes(beta));
    });
  });

  describe("url canonicalisation", () => {
    it("treats .git, a trailing slash and the bare path as one repository", async () => {
      const forms = await Effect.runPromise(
        Effect.all([
          canonicalUrl("https://git.example.com/acme/project"),
          canonicalUrl("https://git.example.com/acme/project/"),
          canonicalUrl("https://git.example.com/acme/project.git"),
        ]),
      );
      assert.equal(new Set(forms).size, 1, `expected one form, got ${JSON.stringify(forms)}`);
    });

    it("keeps different hosts apart", async () => {
      const [one, two] = await Effect.runPromise(
        Effect.all([
          canonicalUrl("https://a.example.com/acme"),
          canonicalUrl("https://b.example.com/acme"),
        ]),
      );
      assert.notEqual(one, two);
    });

    it("rejects something that is not a url", async () => {
      const failure = await Effect.runPromise(canonicalUrl("not a url").pipe(Effect.flip));
      assert.equal(failure._tag, "Invalid");
    });
  });

  describe("the default location", () => {
    it("follows XDG_CONFIG_HOME when it is absolute", () => {
      const previous = process.env["XDG_CONFIG_HOME"];
      process.env["XDG_CONFIG_HOME"] = "/xdg";
      try {
        assert.equal(defaultPath(), "/xdg/chr33s-git/known_repos");
      } finally {
        if (previous === undefined) delete process.env["XDG_CONFIG_HOME"];
        else process.env["XDG_CONFIG_HOME"] = previous;
      }
    });

    it("ignores a relative XDG_CONFIG_HOME, as the spec says to", () => {
      const previous = process.env["XDG_CONFIG_HOME"];
      process.env["XDG_CONFIG_HOME"] = "relative/path";
      try {
        const resolved = defaultPath();
        assert.ok(
          resolved === undefined || !resolved.startsWith("relative"),
          `relative XDG_CONFIG_HOME must not be used: ${resolved}`,
        );
      } finally {
        if (previous === undefined) delete process.env["XDG_CONFIG_HOME"];
        else process.env["XDG_CONFIG_HOME"] = previous;
      }
    });
  });
});
