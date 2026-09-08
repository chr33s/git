import assert from "node:assert/strict";
import { spawn, spawnSync, type Serializable } from "node:child_process";
import { existsSync } from "node:fs";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";
import { gitEnv, gitIn, hasGit } from "../testing/Git.ts";

const worker = `
  import fsp from "node:fs/promises";
  import { syncBuiltinESMExports } from "node:module";
  import { Effect, Layer } from "effect";
  import * as Checkout from ${JSON.stringify(new URL("./Checkout.ts", import.meta.url).href)};
  import * as Repository from ${JSON.stringify(new URL("./Repository.ts", import.meta.url).href)};
  import { stores } from ${JSON.stringify(new URL("./Node.ts", import.meta.url).href)};
  import { workspace } from ${JSON.stringify(new URL("./Work.node.ts", import.meta.url).href)};
  const [root, operation] = process.argv.slice(1);
  const controller = new AbortController();
  const gate = Promise.withResolvers();
  process.on("message", message => {
    if (message === "abort") { controller.abort(); process.send("aborted"); }
    if (message === "release") gate.resolve();
  });
  const write = fsp.writeFile;
  fsp.writeFile = async function (...args) {
    const target = String(args[0]);
    if (operation === "restore" ? target === root + "/a" : target.startsWith(root + "/.git/index.") && target.endsWith(".tmp")) {
      process.send("ready");
      await gate.promise;
    }
    return Reflect.apply(write, fsp, args);
  };
  const remove = fsp.rm;
  fsp.rm = async function (...args) {
    if (operation === "rm" && String(args[0]) === root + "/a") {
      process.send("ready");
      await gate.promise;
    }
    return Reflect.apply(remove, fsp, args);
  };
  syncBuiltinESMExports();
  const edit = operation === "restore" ? Checkout.restore(["a"])
    : operation === "rm" ? Checkout.remove(["a"], { force: true })
    : operation === "checkout" ? Checkout.checkout("side", { force: true }) : Checkout.add(["b"]);
  try {
    await Effect.runPromise(edit.pipe(Effect.provide(Repository.layer.pipe(
      Layer.provide(Repository.hooksNoop), Layer.provide(stores(root + "/.git")),
      Layer.provideMerge(workspace(root))
    ))), { signal: controller.signal });
  } catch (cause) {
    if (!controller.signal.aborted) throw cause;
  } finally { process.disconnect(); }
`;

describe.skipIf(!hasGit)("index lock during interrupted filesystem writes", () => {
  for (const operation of ["restore", "add", "rm", "checkout"]) {
    it(`${operation} finishes its pending write before releasing the index lock`, async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "index-cancel-"));
      try {
        const git = gitIn(root);
        git("init", "-q", "-b", "main");
        await fs.writeFile(path.join(root, "a"), "base\n");
        git("add", "a");
        git("commit", "-qm", "base");
        if (operation === "checkout") {
          git("checkout", "-qb", "side");
          await fs.writeFile(path.join(root, "a"), "side\n");
          git("commit", "-qam", "side");
          git("checkout", "-q", "main");
          await fs.writeFile(path.join(root, "a"), "main\n");
          git("commit", "-qam", "main");
          assert.equal(spawnSync("git", ["merge", "side"], { cwd: root, env: gitEnv }).status, 1);
        }
        await fs.writeFile(path.join(root, "a"), "local\n");
        await fs.writeFile(path.join(root, "b"), "new\n");
        const ready = Promise.withResolvers<void>();
        const aborted = Promise.withResolvers<void>();
        const child = spawn(
          process.execPath,
          ["--input-type=module", "-e", worker, root, operation],
          {
            stdio: ["ignore", "pipe", "pipe", "ipc"],
          },
        );
        assert.ok(child.stderr !== null);
        let stderr = "";
        child.stderr.on("data", (chunk: Buffer) => {
          stderr += chunk.toString();
        });
        child.on("message", (message: Serializable) => {
          if (message === "ready") ready.resolve();
          if (message === "aborted") aborted.resolve();
        });
        child.on("error", (cause: Error) => ready.reject(cause));
        const completed = new Promise<number | null>((resolve) => child.on("close", resolve));
        void completed.then(() => ready.reject(new Error(`writer exited: ${stderr}`)));
        try {
          await ready.promise;
          child.send("abort");
          await aborted.promise;
          assert.equal(existsSync(path.join(root, ".git/index.lock")), true);
          const contender = spawnSync("git", ["add", "b"], {
            cwd: root,
            env: gitEnv,
            encoding: "utf8",
          });
          assert.notEqual(contender.status, 0);
          assert.match(contender.stderr, /index\.lock/);
          child.send("release");
          assert.equal(await completed, 0, stderr);
          assert.equal(existsSync(path.join(root, ".git/index.lock")), false);
          if (operation === "restore")
            assert.equal(await fs.readFile(path.join(root, "a"), "utf8"), "base\n");
          else if (operation === "add") assert.equal(git("show", ":b"), "new\n");
          else if (operation === "checkout") {
            assert.equal(git("symbolic-ref", "HEAD").trim(), "refs/heads/side");
            assert.equal(git("show", ":a"), "side\n");
            assert.equal(existsSync(path.join(root, ".git/MERGE_HEAD")), false);
          } else assert.equal(existsSync(path.join(root, "a")), false);
        } finally {
          if (child.connected) child.send("release");
          await completed;
        }
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    });
  }
});
