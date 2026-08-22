/**
 * The ref-name rules, on their own.
 *
 * `Store.contract.ts` proves each backend enforces them; this pins down what
 * "them" is, including the one asymmetry that is easy to get wrong — a name
 * this version refuses to *create* may already exist, and refusing to delete
 * it would leave a ref nothing can remove and objects nothing can collect.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

import { checkRefAddress, checkRefName, checkRefNames, type Oid } from "./Store.ts";

// SAFETY: forty lowercase hex characters by construction, which is exactly
// what the Oid brand stands for.
const oid = "a".repeat(40) as Oid;

describe("ref names", () => {
  it.effect("accepts the names git accepts", () =>
    Effect.sync(() => {
      for (const name of [
        "refs/heads/main",
        "refs/heads/feature/nested/deep",
        "refs/tags/v1.0.0",
        "refs/remotes/origin/main",
        "refs/heads/release-1.2",
      ]) {
        assert.equal(checkRefName(name), null, name);
      }
    }),
  );

  it.effect("refuses names that address something other than a ref", () =>
    Effect.sync(() => {
      for (const name of [
        "",
        "HEAD",
        "config",
        "objects/ab/cdef",
        "refs",
        "refs/",
        "refs/heads/../../escape",
        "/refs/heads/absolute",
        "refs/heads//double",
        "refs/heads/.hidden",
        "refs/heads/nul\0byte",
      ]) {
        assert.notEqual(checkRefAddress(name), null, name);
        assert.notEqual(checkRefName(name), null, name);
      }
    }),
  );

  it.effect("refuses names git's own check-ref-format refuses", () =>
    Effect.sync(() => {
      for (const name of [
        "refs/heads/main.lock",
        "refs/heads/main@{0}",
        "refs/heads/has space",
        "refs/heads/tilde~1",
        "refs/heads/caret^2",
        "refs/heads/colon:name",
        "refs/heads/question?",
        "refs/heads/star*",
        "refs/heads/bracket[1]",
        "refs/heads/back\\slash",
        "refs/heads/trailing.",
      ]) {
        assert.notEqual(checkRefName(name), null, name);
        // Spelling only: these are all safely inside the ref namespace.
        assert.equal(checkRefAddress(name), null, name);
      }
    }),
  );

  it.effect("lets a name it would not create still be deleted", () =>
    Effect.promise(async () => {
      const create = await Effect.runPromise(
        Effect.flip(checkRefNames([{ name: "refs/heads/build.lock", value: oid }])),
      );
      assert.equal(create._tag, "Invalid");

      // The same name, deleted: a ref written by an older build has to be
      // removable, or it pins every object it reaches forever.
      await Effect.runPromise(checkRefNames([{ name: "refs/heads/build.lock", value: null }]));

      // A delete is still held to the addressing rules.
      const escape = await Effect.runPromise(
        Effect.flip(checkRefNames([{ name: "refs/../../etc/passwd", value: null }])),
      );
      assert.equal(escape._tag, "Invalid");
    }),
  );
});
