/**
 * Generated bundles must be what `git bundle verify` accepts.
 */
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it } from "@effect/vitest";

import { Effect, Layer, Stream } from "effect";

import { type Signature } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { gitIn, hasGit } from "../testing/Git.ts";
import * as BundleBuilder from "./BundleBuilder.ts";

const alice: Signature = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const live = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provideMerge(stores),
);

describe.skipIf(!hasGit)("BundleBuilder interop", () => {
  it.live("produces a bundle git bundle verify accepts", () =>
    Effect.gen(function* () {
      const bytes = yield* Effect.gen(function* () {
        const repository = yield* Repository;
        const blob = yield* repository.writeBlob(new TextEncoder().encode("hello from bundle\n"));
        const tree = yield* repository.writeTree([
          { mode: "100644", name: "hello.txt", oid: blob },
        ]);
        const commit = yield* repository.commit({
          branch: "main",
          tree,
          message: "hello",
          author: alice,
        });
        const built = yield* BundleBuilder.build({
          snapshot: {
            createdAt: new Date(),
            refs: { "refs/heads/main": commit },
            filter: null,
          },
          kind: "full",
        });
        const chunks = yield* Stream.runCollect(built.stream);
        let total = 0;
        for (const chunk of chunks) total += chunk.length;
        const out = new Uint8Array(total);
        let offset = 0;
        for (const chunk of chunks) {
          out.set(chunk, offset);
          offset += chunk.length;
        }
        yield* BundleBuilder.verifyBundle(out, {
          refs: { "refs/heads/main": commit },
          prerequisites: [],
          filter: null,
        });
        return out;
      }).pipe(Effect.provide(live));

      const root = yield* Effect.promise(() => mkdtemp(join(tmpdir(), "bundle-verify-")));
      try {
        const file = join(root, "repo.bundle");
        yield* Effect.promise(() => writeFile(file, bytes));
        const git = gitIn(root);
        git("init", "--bare", "--initial-branch=main", "verify.git");
        const output = git("-C", "verify.git", "bundle", "verify", file);
        assert.match(output, /The bundle contains|refs\/heads\/main/i);
      } finally {
        yield* Effect.promise(() => rm(root, { recursive: true, force: true }));
      }
    }),
  );
});
