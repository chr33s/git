/**
 * The objects an exposure writes, judged by the real `git` binary.
 *
 * A Context Exposure attaches a `context/` subtree to a signed record commit,
 * and that record's oid is the exposure's canonical identity on a ref this
 * version cannot delete. So "does git agree these are the objects it would
 * have written?" is not a stylistic question: a tree written with a mode git
 * merely *tolerates* hashes differently from the canonical one, which renames
 * the record — permanently, and only for records already in somebody's
 * history by the time anyone notices.
 *
 * The bug this exists to catch shipped: the subtree entries were written as
 * `040000`, git's `zeroPaddedFilemode`, which `git fsck --strict` reports and
 * no assertion in this repository's own suites could see, because both sides
 * of the round-trip were ours.
 */
import { execFileSync } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { generate } from "../crypto/SshSignature.ts";
import { type Signature } from "../git/Format.ts";
import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { indexMemory, workTreeMemory, WorkTree } from "../git/Work.ts";
import * as Checkout from "../git/Checkout.ts";
import { gitEnv, hasGit } from "../testing/Git.ts";
import * as Exposure from "./Exposure.ts";
import * as Pack from "./Pack.ts";
import * as Select from "./Select.ts";

const REPO = "SHA256:test";
const SESSION = "0192f000-0000-7000-8000-000000000000";

const author: Signature = {
  name: "Runner",
  email: "runner@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const encode = (text: string) => new TextEncoder().encode(text);

describe.skipIf(!hasGit)("Context Exposure against git", () => {
  let root = "";

  beforeEach(() => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), "exposure-interop-"));
    // git's own `init`, so the layout under test is one git already agrees is
    // a repository — the point of the suite is what *we* then write into it.
    execFileSync("git", ["init", "--bare", "--quiet", root], { env: gitEnv });
  });

  afterEach(() => {
    fs.rmSync(root, { recursive: true, force: true });
  });

  const git = (...args: ReadonlyArray<string>) =>
    execFileSync("git", ["--git-dir", root, ...args], { encoding: "utf8", env: gitEnv });

  it.effect("writes objects git accepts under fsck --strict", () =>
    Effect.promise(async () => {
      const written = await Effect.runPromise(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const work = yield* WorkTree;

          yield* work.write("AGENTS.md", encode("Do the authorize work.\n"), 0o100644);
          yield* work.write(
            "src/auth.ts",
            encode("export const authorize = () => true\n"),
            0o100644,
          );
          yield* Checkout.add(["."]);
          const made = yield* Checkout.commit({ message: "first\n", author });
          yield* repository.setRef({ name: "refs/heads/main", to: made.oid });

          // A dirty view, so the retained tree is an overlay no commit
          // reaches — the case where the record's own edge is the only thing
          // holding the object, and so the one git must be able to walk.
          yield* work.write(
            "src/auth.ts",
            encode("export const authorize = () => false\n"),
            0o100644,
          );
          const view = yield* Pack.capture(made.oid);
          const pack = yield* Select.select({ task: "authorize", view });

          return yield* Exposure.expose({
            repo: REPO,
            session: SESSION,
            key: yield* generate("runner@example.com"),
            pack,
            segments: yield* Select.render(pack, "authorize"),
          });
        }).pipe(
          Effect.provide(
            GitRepository.layer.pipe(
              Layer.provide(GitRepository.hooksNoop),
              Layer.provideMerge(stores(root)),
              Layer.provideMerge(indexMemory),
              Layer.provideMerge(workTreeMemory),
            ),
          ),
        ),
      );

      // The whole object graph, held to git's own strictness. `--strict` is
      // what turns `zeroPaddedFilemode` from a warning into an error, and it
      // is the only check that would have caught the mode this suite exists
      // for: nothing in a round-trip through our own codec can.
      git("fsck", "--strict", "--no-progress");

      // And the record is walkable: git resolves the ref, reads the commit,
      // and finds the `context/view` edge that makes the overlay reachable.
      assert.equal(git("rev-parse", `refs/hub/trace/${SESSION}`).trim(), written.commit);
      const listed = git("ls-tree", "-r", "--name-only", written.commit);
      assert.match(listed, /^context\/pack\.json$/m);
      assert.match(listed, /^context\/render\.bin$/m);
      assert.match(listed, /^context\/view\/src\/auth\.ts$/m);
      assert.match(listed, /^event\.json$/m);
    }),
  );

  it.effect("keeps view.tree alive through git's own reachability", () =>
    Effect.promise(async () => {
      const view = await Effect.runPromise(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const work = yield* WorkTree;
          yield* work.write(
            "src/auth.ts",
            encode("export const authorize = () => true\n"),
            0o100644,
          );
          yield* Checkout.add(["."]);
          const made = yield* Checkout.commit({ message: "first\n", author });
          yield* repository.setRef({ name: "refs/heads/main", to: made.oid });

          yield* work.write(
            "src/auth.ts",
            encode("export const authorize = () => false\n"),
            0o100644,
          );
          const captured = yield* Pack.capture(made.oid);
          const pack = yield* Select.select({ task: "authorize", view: captured });
          yield* Exposure.expose({
            repo: REPO,
            session: SESSION,
            key: yield* generate("runner@example.com"),
            pack,
            segments: yield* Select.render(pack, "authorize"),
          });
          return captured;
        }).pipe(
          Effect.provide(
            GitRepository.layer.pipe(
              Layer.provide(GitRepository.hooksNoop),
              Layer.provideMerge(stores(root)),
              Layer.provideMerge(indexMemory),
              Layer.provideMerge(workTreeMemory),
            ),
          ),
        ),
      );

      // git's collector, not ours: the overlay tree is referenced by no
      // commit, so only the record's `context/view` edge saves it — and this
      // is the implementation whose opinion actually decides that.
      git("gc", "--prune=now", "--quiet");
      const tree = Pack.unqualify(view.tree)!;
      assert.equal(git("cat-file", "-t", tree).trim(), "tree");
    }),
  );
});
