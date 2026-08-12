/**
 * The registry and token contract, as one suite every backend has to pass.
 *
 * "Scoped, TTL'd, revocable" and "cursor-paged list" mean the same thing on
 * in-memory maps, on JSON files, and on Durable Object SQLite, or they mean
 * nothing. The suite is parameterised over the backend *and* the runner, so
 * the same assertions run under `node:test` out here and inside workerd via
 * `Conformance.ts`'s collector.
 *
 * Not a `*.test.ts` file: it is imported by `Registry.test.ts` and by the
 * conformance route in `git/Durable.ts`.
 */
import { Effect } from "effect";

import { Registry, type RepoMeta, Tokens } from "./Namespace.ts";

export interface Backend {
  /** Run one effect against a fresh, empty registry and token store. */
  readonly run: <A, E>(effect: Effect.Effect<A, E, Registry | Tokens>) => Promise<A>;
}

/** Just enough of a runner for this suite; `node:test` and the collector fit. */
export interface Runner {
  readonly describe: (name: string, body: () => void) => void;
  readonly it: (name: string, body: () => Promise<void> | void) => void;
}

const meta = (overrides?: Partial<RepoMeta>): RepoMeta => ({
  description: null,
  defaultBranch: "main",
  readOnly: false,
  source: null,
  ...overrides,
});

/** `node:assert` is not available inside workerd; the suite throws its own. */
const check = (condition: boolean, message: string): void => {
  if (!condition) throw new Error(message);
};

const equal = (actual: unknown, expected: unknown, what: string): void => {
  check(
    actual === expected,
    `${what}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`,
  );
};

export const registryContract = (label: string, backend: Backend, runner: Runner): void => {
  const { describe, it } = runner;
  const { run } = backend;

  describe(`${label}: Registry contract`, () => {
    it("creates, reads back every metadata field, and refuses duplicates", async () => {
      await run(
        Effect.gen(function* () {
          const registry = yield* Registry;
          const created = yield* registry.create(
            "alpha",
            meta({ description: "first", defaultBranch: "trunk", readOnly: true, source: "x:y" }),
          );
          equal(created.name, "alpha", "name");
          equal(created.description, "first", "description");
          equal(created.defaultBranch, "trunk", "defaultBranch");
          equal(created.readOnly, true, "readOnly");
          equal(created.source, "x:y", "source");
          equal(created.lastPushAt, null, "lastPushAt starts null");
          check(created.id.length > 0, "id is assigned");

          const fetched = yield* registry.get("alpha");
          equal(fetched?.id, created.id, "get returns the same row");
          equal(fetched?.readOnly, true, "readOnly survives the round trip");

          const duplicate = yield* registry.create("alpha", meta()).pipe(Effect.flip);
          check(/ALREADY_EXISTS/.test(duplicate.message), "duplicate is ALREADY_EXISTS");
        }),
      );
    });

    it("returns null for an unknown repository", async () => {
      await run(
        Effect.gen(function* () {
          const registry = yield* Registry;
          equal(yield* registry.get("nope"), null, "unknown repo");
        }),
      );
    });

    it("pages by cursor, in name order, with a stable total", async () => {
      await run(
        Effect.gen(function* () {
          const registry = yield* Registry;
          for (const name of ["delta", "alpha", "charlie", "bravo"]) {
            yield* registry.create(name, meta());
          }

          const first = yield* registry.list({ limit: 2 });
          equal(first.total, 4, "total");
          equal(first.repos.map((repo) => repo.name).join(","), "alpha,bravo", "first page");
          check(first.cursor !== undefined, "a cursor when more remain");

          const second = yield* registry.list({ limit: 2, cursor: first.cursor! });
          equal(second.total, 4, "total is the whole set, not the page");
          equal(second.repos.map((repo) => repo.name).join(","), "charlie,delta", "second page");
          equal(second.cursor, undefined, "no cursor on the last page");
        }),
      );
    });

    it("deletes idempotently and reports whether anything went", async () => {
      await run(
        Effect.gen(function* () {
          const registry = yield* Registry;
          yield* registry.create("gone", meta());
          equal(yield* registry.delete("gone"), true, "first delete");
          equal(yield* registry.delete("gone"), false, "second delete");
          equal(yield* registry.get("gone"), null, "row is gone");
        }),
      );
    });

    it("touch moves updatedAt and lastPushAt", async () => {
      await run(
        Effect.gen(function* () {
          const registry = yield* Registry;
          yield* registry.create("touched", meta());
          const at = new Date(1_700_000_000_000);
          yield* registry.touch("touched", at);
          const record = yield* registry.get("touched");
          equal(record?.lastPushAt?.getTime(), at.getTime(), "lastPushAt");
          equal(record?.updatedAt.getTime(), at.getTime(), "updatedAt");
        }),
      );
    });
  });

  describe(`${label}: Tokens contract`, () => {
    it("issues plaintext once and verifies it, scoped to its repository", async () => {
      await run(
        Effect.gen(function* () {
          const tokens = yield* Tokens;
          const issued = yield* tokens.issue("repo", "write", 300);
          check(issued.plaintext.length > 0, "plaintext is returned at creation");
          equal(issued.scope, "write", "scope");

          equal(yield* tokens.verify("repo", issued.plaintext), "write", "verifies");
          equal(yield* tokens.verify("other", issued.plaintext), null, "wrong repo");
          equal(yield* tokens.verify("repo", "art_nonsense"), null, "unknown token");

          // Listing must never expose anything that verifies.
          const listed = yield* tokens.list("repo");
          equal(listed.total, 1, "one token");
          equal(listed.tokens[0]?.state, "active", "state");
          check(
            !JSON.stringify(listed).includes(issued.plaintext),
            "plaintext must not survive into `list`",
          );
        }),
      );
    });

    it("rejects a non-positive ttl", async () => {
      await run(
        Effect.gen(function* () {
          const tokens = yield* Tokens;
          const failed = yield* tokens.issue("repo", "read", 0).pipe(Effect.flip);
          check(/INVALID_TTL/.test(failed.message), "INVALID_TTL");
        }),
      );
    });

    it("stops verifying once expired", async () => {
      await run(
        Effect.gen(function* () {
          const tokens = yield* Tokens;
          // The shortest ttl the API accepts, then outlive it: expiry has to
          // be enforced at verification, not merely recorded at issuance.
          const issued = yield* tokens.issue("repo", "read", 0.005);
          yield* Effect.sleep("50 millis");
          equal(yield* tokens.verify("repo", issued.plaintext), null, "expired token");
          const listed = yield* tokens.list("repo");
          equal(listed.tokens[0]?.state, "expired", "listed as expired");
        }),
      );
    });

    it("revokes by plaintext or id, once", async () => {
      await run(
        Effect.gen(function* () {
          const tokens = yield* Tokens;
          const byText = yield* tokens.issue("repo", "read", 300);
          equal(yield* tokens.revoke("repo", byText.plaintext), true, "revoke by plaintext");
          equal(yield* tokens.revoke("repo", byText.plaintext), false, "already revoked");
          equal(yield* tokens.verify("repo", byText.plaintext), null, "no longer verifies");

          const byId = yield* tokens.issue("repo", "write", 300);
          equal(yield* tokens.revoke("repo", byId.id), true, "revoke by id");
          equal(yield* tokens.verify("repo", byId.plaintext), null, "no longer verifies");

          const listed = yield* tokens.list("repo");
          equal(
            listed.tokens.filter((token) => token.state === "revoked").length,
            2,
            "both listed as revoked",
          );
        }),
      );
    });

    it("keeps tokens of different repositories apart", async () => {
      await run(
        Effect.gen(function* () {
          const tokens = yield* Tokens;
          yield* tokens.issue("left", "write", 300);
          const right = yield* tokens.issue("right", "read", 300);
          equal((yield* tokens.list("left")).total, 1, "left has one");
          equal((yield* tokens.list("right")).total, 1, "right has one");
          equal(yield* tokens.verify("left", right.plaintext), null, "no cross-repo verify");
        }),
      );
    });
  });
};
