import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";
import { serve } from "../host/Node.ts";
import { fastImport, gitEnv, gitIn, hasGit, importCommit } from "../testing/Git.ts";

const execute = promisify(execFile);

describe.skipIf(!hasGit)("shallow push interoperability", () => {
  it.live("accepts established shallow history and reports refs requiring new boundaries", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "shallow-push-"));
      const origin = path.join(root, "origin");
      await fs.mkdir(origin);
      gitIn(origin)("init", "--bare", "-q", "-b", "main");
      fastImport(
        origin,
        [1, 2]
          .map((mark) =>
            importCommit({
              branch: "refs/heads/main",
              mark,
              message: `commit ${mark}`,
              from: mark === 1 ? undefined : mark - 1,
              files: [{ path: "file", content: String(mark) }],
            }),
          )
          .join(""),
      );
      const stock = path.join(root, "stock");
      await fs.cp(origin, stock, { recursive: true });
      const clone = path.join(root, "clone");
      await execute("git", ["clone", "--bare", "--depth", "1", pathToFileURL(origin).href, clone], {
        env: gitEnv,
      });
      const git = gitIn(clone);
      assert.equal(git("rev-list", "--count", "HEAD").trim(), "1");
      const tree = git("rev-parse", "HEAD^{tree}").trim();
      const next = git("commit-tree", tree, "-p", "HEAD", "-m", "from shallow clone").trim();
      git("update-ref", "refs/heads/main", next);
      // A stock receiver already holding the history needs no new boundary.
      await execute("git", ["push", stock, "main"], { cwd: clone, env: gitEnv });
      assert.equal(gitIn(stock)("rev-list", "--count", "main").trim(), "3");
      const server = await serve({ root, allowAnonymousWrites: true });
      try {
        await execute("git", ["push", `${server.url}/origin`, "main"], { cwd: clone, env: gitEnv });
        assert.equal(gitIn(origin)("rev-parse", "main").trim(), next);
        assert.equal(gitIn(origin)("rev-list", "--count", "main").trim(), "3");
        assert.equal(gitIn(origin)("rev-parse", "--is-shallow-repository").trim(), "false");

        for (const kind of ["stock", "native"]) {
          await fs.cp(clone, path.join(root, `${kind}-shallow`), { recursive: true });
        }
        const later = git("commit-tree", tree, "-p", "HEAD", "-m", "another shallow commit").trim();
        git("update-ref", "refs/heads/main", later);
        for (const [name, target, count, shallow] of [
          ["stock", stock, "4", "false"],
          ["origin", `${server.url}/origin`, "4", "false"],
          ["stock-shallow", path.join(root, "stock-shallow"), "3", "true"],
          ["native-shallow", `${server.url}/native-shallow`, "3", "true"],
        ] as const) {
          await execute("git", ["push", target, "main"], { cwd: clone, env: gitEnv });
          assert.equal(gitIn(path.join(root, name))("rev-list", "--count", "main").trim(), count);
          assert.equal(
            gitIn(path.join(root, name))("rev-parse", "--is-shallow-repository").trim(),
            shallow,
          );
        }

        const independent = git("commit-tree", tree, "-m", "independent root").trim();
        git("update-ref", "refs/heads/safe", independent);
        git("tag", "-a", "v1", "-m", "shallow tag", "main");
        for (const atomic of [false, true]) {
          for (const kind of ["stock", "native"]) {
            if (atomic && kind === "stock") continue;
            const name = `${kind}-${atomic ? "atomic" : "partial"}`;
            const directory = path.join(root, name);
            await fs.mkdir(directory);
            gitIn(directory)("init", "--bare", "-q", "-b", "main");
            const target = kind === "stock" ? directory : `${server.url}/${name}`;
            await assert.rejects(
              execute(
                "git",
                ["push", ...(atomic ? ["--atomic"] : []), target, "main", "safe", "refs/tags/v1"],
                { cwd: clone, env: gitEnv },
              ),
              (error) => error instanceof Error && /shallow update not allowed/.test(error.message),
            );
            const listed = gitIn(directory)("for-each-ref", "--format=%(refname)").trim();
            assert.equal(listed, atomic ? "" : "refs/heads/safe", name);
            if (listed !== "")
              assert.equal(gitIn(directory)("rev-parse", "safe").trim(), independent);
            // Retried packs must not make a refused boundary look like an
            // established root merely because its commit object now exists.
            await assert.rejects(
              execute("git", ["push", target, "main"], { cwd: clone, env: gitEnv }),
              (error) => error instanceof Error && /shallow update not allowed/.test(error.message),
            );
            if (!atomic) {
              await execute("git", ["push", target, ":refs/heads/safe"], {
                cwd: clone,
                env: gitEnv,
              });
              assert.equal(gitIn(directory)("for-each-ref", "--format=%(refname)").trim(), "");
            }
          }
        }

        // Stock Git rejects a new boundary even when its parents are already
        // here: the boundary must itself belong to established receiver history.
        for (const kind of ["stock", "native"]) {
          const name = `${kind}-ancestor`;
          const directory = path.join(root, name);
          await fs.mkdir(directory);
          gitIn(directory)("init", "--bare", "-q", "-b", "main");
          fastImport(
            directory,
            importCommit({
              branch: "refs/heads/main",
              mark: 1,
              message: "commit 1",
              files: [{ path: "file", content: "1" }],
            }),
          );
          await assert.rejects(
            execute(
              "git",
              [
                "push",
                kind === "stock" ? directory : `${server.url}/${name}`,
                "main:refs/heads/new",
              ],
              { cwd: clone, env: gitEnv },
            ),
            (error) => error instanceof Error && /shallow update not allowed/.test(error.message),
          );
          assert.equal(
            gitIn(directory)("for-each-ref", "--format=%(refname)").trim(),
            "refs/heads/main",
          );
          assert.equal(gitIn(directory)("rev-parse", "--is-shallow-repository").trim(), "false");
        }
      } finally {
        await server.close();
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );
});
