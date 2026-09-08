import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";
import { Effect, Layer } from "effect";

import { stores } from "../git/Node.ts";
import * as Repository from "../git/Repository.ts";
import { fastImport, gitIn, hasGit, importCommit } from "../testing/Git.ts";
import * as Archive from "./Archive.ts";

describe.skipIf(!hasGit)("named archives against Git", () => {
  it.live("exports short branches, exact refs, HEAD, and object IDs", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "archive-ref-"));
      const git = gitIn(root);
      try {
        git("init", "-q", "-b", "main");
        fastImport(
          root,
          importCommit({
            branch: "refs/heads/main",
            mark: 1,
            message: "base",
            files: [{ path: "readme.md", content: "archive this revision\n" }],
          }),
        );
        git("tag", "-am", "release", "release");
        git("-c", "advice.nestedTag=false", "tag", "-am", "nested", "nested", "release");
        git("tag", "-am", "tree", "tree", "HEAD^{tree}");
        let previous = "nested";
        for (let index = 0; index < 20; index++) {
          const name = `chain-${index}`;
          git("-c", "advice.nestedTag=false", "tag", "-am", name, name, previous);
          previous = name;
        }
        const head = git("rev-parse", "HEAD").trim();
        const layer = Repository.layer.pipe(
          Layer.provide(Repository.hooksNoop),
          Layer.provide(stores(path.join(root, ".git"))),
        );
        for (const ref of [
          "main",
          "refs/heads/main",
          "refs/tags/release",
          "HEAD",
          head,
          "refs/tags/nested",
          "refs/tags/tree",
          `refs/tags/${previous}`,
        ]) {
          git("archive", "--format=tar", "--output=git.tar", ref);
          const response = await Effect.runPromise(
            Archive.handle(new Request(`http://host/repo/archive/native.tar?ref=${ref}`), {
              prefix: "",
            }).pipe(Effect.provide(layer)),
          );
          assert.ok(response !== null);
          assert.equal(response.status, 200, ref);
          await fs.writeFile(
            path.join(root, "native.tar"),
            new Uint8Array(await response.arrayBuffer()),
          );
          const contents = (name: string) =>
            execFileSync("tar", ["-xOf", path.join(root, name), "readme.md"]);
          assert.deepEqual(contents("native.tar"), contents("git.tar"), ref);
        }
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );
});
