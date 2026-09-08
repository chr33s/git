import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { gitEnv, gitIn, hasGit } from "../testing/Git.ts";
import * as Checkout from "./Checkout.ts";
import { stores } from "./Node.ts";
import * as Repository from "./Repository.ts";
import { workspace } from "./Work.node.ts";

const standalone = path.resolve("dist/sea/git+");
const source = path.resolve("src/cli/main.ts");
const drivers = ["library", "source", ...(existsSync(standalone) ? ["standalone"] : [])];
const layerFor = (root: string) =>
  Repository.layer.pipe(
    Layer.provide(Repository.hooksNoop),
    Layer.provide(stores(path.join(root, ".git"))),
    Layer.provideMerge(workspace(root, path.join(root, ".git"))),
  );
const files = async (root: string, prefix = ""): Promise<Array<[string, string]>> => {
  const result: Array<[string, string]> = [];
  for (const entry of await fs.readdir(path.join(root, prefix), { withFileTypes: true })) {
    if (entry.name === ".git") continue;
    const relative = prefix === "" ? entry.name : `${prefix}/${entry.name}`;
    if (entry.isDirectory()) result.push(...(await files(root, relative)));
    else result.push([relative, await fs.readFile(path.join(root, relative), "utf8")]);
  }
  return result.sort(([left], [right]) => left.localeCompare(right));
};

describe.skipIf(!hasGit)("checkout path obstructions against Git", () => {
  for (const driver of drivers) {
    for (const sourceKind of ["directory", "file"]) {
      for (const mode of ["clean", "untracked", "ignored", "forced"]) {
        it(`${driver} replaces ${sourceKind}, mode=${mode}`, async () => {
          const blocked = mode !== "clean";
          const refused = mode === "untracked";
          const force = mode === "forced";
          const root = await fs.mkdtemp(path.join(os.tmpdir(), "checkout-obstruction-"));
          try {
            const native = path.join(root, "native"),
              reference = path.join(root, "reference");
            await fs.mkdir(native);
            gitIn(native)("init", "-q", "-b", "main");
            await fs.writeFile(path.join(native, "a"), "base\n");
            if (sourceKind === "directory") {
              await fs.mkdir(path.join(native, "dir"));
              await fs.writeFile(path.join(native, "dir/tracked"), "tracked\n");
            } else if (!blocked) await fs.writeFile(path.join(native, "dir"), "tracked file\n");
            gitIn(native)("add", ".");
            gitIn(native)("commit", "-qm", "base");
            gitIn(native)("checkout", "-qb", "target");
            await fs.rm(path.join(native, "dir"), { recursive: true, force: true });
            if (sourceKind === "directory")
              await fs.writeFile(path.join(native, "dir"), "replacement\n");
            else {
              await fs.mkdir(path.join(native, "dir"));
              await fs.writeFile(path.join(native, "dir/target"), "replacement\n");
            }
            await fs.writeFile(path.join(native, "a"), "target\n");
            gitIn(native)("add", ".");
            gitIn(native)("commit", "-qm", "target");
            gitIn(native)("checkout", "-q", "main");
            if (blocked)
              await fs.writeFile(
                path.join(native, sourceKind === "directory" ? "dir/extra" : "dir"),
                "untracked\n",
              );
            if (mode === "ignored")
              await fs.writeFile(
                path.join(native, ".git/info/exclude"),
                sourceKind === "directory" ? "dir/extra\n" : "dir\n",
              );
            await fs.cp(native, reference, { recursive: true });
            const before = await files(native);
            const index = await fs.readFile(path.join(native, ".git/index"));
            const expected = spawnSync(
              "git",
              ["-C", reference, "checkout", ...(force ? ["--force"] : []), "target"],
              {
                env: gitEnv,
                encoding: "utf8",
              },
            );
            assert.equal(expected.status === 0, !refused, expected.stderr);
            let succeeded: boolean;
            if (driver === "library") {
              const exit = await Effect.runPromise(
                Checkout.checkout("target", { force }).pipe(
                  Effect.provide(layerFor(native)),
                  Effect.exit,
                ),
              );
              succeeded = exit._tag === "Success";
            } else {
              const result = spawnSync(
                driver === "source" ? process.execPath : standalone,
                [
                  ...(driver === "source" ? [source] : []),
                  "switch",
                  "--work",
                  ".",
                  ...(force ? ["--force"] : []),
                  "target",
                ],
                { cwd: native, env: gitEnv, encoding: "utf8", timeout: 30_000 },
              );
              succeeded = result.status === 0;
            }
            assert.equal(succeeded, !refused);
            assert.deepEqual(await files(native), await files(reference));
            assert.equal(
              gitIn(native)("symbolic-ref", "HEAD"),
              gitIn(reference)("symbolic-ref", "HEAD"),
            );
            assert.equal(
              gitIn(native)("ls-files", "--stage"),
              gitIn(reference)("ls-files", "--stage"),
            );
            if (refused) {
              assert.deepEqual(await files(native), before);
              assert.deepEqual(await fs.readFile(path.join(native, ".git/index")), index);
            }
            assert.equal(existsSync(path.join(native, ".git/index.lock")), false);
          } finally {
            await fs.rm(root, { recursive: true, force: true });
          }
        });
      }
    }
  }
});
