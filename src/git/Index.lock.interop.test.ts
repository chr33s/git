import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";
import { gitEnv, gitIn, hasGit } from "../testing/Git.ts";

const source = path.resolve("src/cli/main.ts");
const standalone = path.resolve("dist/sea/git+");
const executables = [
  { name: "source", command: process.execPath, args: [source] },
  ...(existsSync(standalone) ? [{ name: "standalone", command: standalone, args: [] }] : []),
];
const commands = [
  { name: "add", native: ["add", "--work", ".", "c"], git: ["add", "c"] },
  { name: "rm", native: ["rm", "--work", ".", "--force", "a"], git: ["rm", "-f", "a"] },
  { name: "mv", native: ["mv", "--work", ".", "a", "renamed"], git: ["mv", "a", "renamed"] },
  {
    name: "restore",
    native: ["restore", "--work", ".", "--staged", "b"],
    git: ["restore", "--staged", "b"],
  },
  {
    name: "switch",
    native: ["switch", "--work", ".", "--force", "side"],
    git: ["switch", "--force", "side"],
  },
  {
    name: "commit",
    native: ["commit", "--work", ".", "--message", "staged"],
    git: ["commit", "-m", "staged"],
  },
];

const writer = `
  import fs from "node:fs";
  import fsp from "node:fs/promises";
  import { syncBuiltinESMExports } from "node:module";
  import { Effect, Layer } from "effect";
  import * as Checkout from ${JSON.stringify(new URL("./Checkout.ts", import.meta.url).href)};
  import * as Repository from ${JSON.stringify(new URL("./Repository.ts", import.meta.url).href)};
  import { stores } from ${JSON.stringify(new URL("./Node.ts", import.meta.url).href)};
  import { workspace } from ${JSON.stringify(new URL("./Work.node.ts", import.meta.url).href)};
  const root = process.argv[1];
  const read = fsp.readFile;
  let paused = false;
  fsp.readFile = async function (...args) {
    const data = await Reflect.apply(read, fsp, args);
    if (String(args[0]) === root + "/.git/index" && !paused) {
      paused = true;
      fs.writeSync(1, "ready\\n");
      fs.readSync(0, Buffer.alloc(1), 0, 1, null);
    }
    return data;
  };
  syncBuiltinESMExports();
  await Effect.runPromise(Checkout.add(["b"]).pipe(Effect.provide(
    Repository.layer.pipe(
      Layer.provide(Repository.hooksNoop),
      Layer.provide(stores(root + "/.git")),
      Layer.provideMerge(workspace(root))
    )
  )));
`;

describe.skipIf(!hasGit)("native index locking against Git", () => {
  let root: string;
  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "index-lock-"));
    const git = gitIn(root);
    git("init", "-q", "-b", "main");
    await fs.writeFile(path.join(root, "a"), "base\n");
    git("add", "a");
    git("commit", "-qm", "base");
    git("branch", "side");
    await fs.writeFile(path.join(root, "b"), "staged\n");
    await fs.writeFile(path.join(root, "c"), "new\n");
    git("add", "b");
  });
  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  for (const executable of executables) {
    for (const command of commands) {
      it(`${executable.name} ${command.name} respects an existing Git index lock before mutation`, async () => {
        const index = await fs.readFile(path.join(root, ".git/index"));
        const head = gitIn(root)("rev-parse", "HEAD");
        const lock = path.join(root, ".git/index.lock");
        await fs.writeFile(lock, "another writer\n");
        const git = spawnSync("git", command.git, { cwd: root, env: gitEnv, encoding: "utf8" });
        assert.notEqual(git.status, 0, git.stdout + git.stderr);
        assert.match(git.stderr, /index\.lock/);
        const native = spawnSync(executable.command, [...executable.args, ...command.native], {
          cwd: root,
          env: gitEnv,
          encoding: "utf8",
          timeout: 30_000,
        });
        assert.notEqual(native.status, 0, native.stdout + native.stderr);
        assert.match(native.stdout + native.stderr, /index\.lock|EEXIST/);
        assert.deepEqual(await fs.readFile(path.join(root, ".git/index")), index);
        assert.equal(await fs.readFile(lock, "utf8"), "another writer\n");
        assert.equal(gitIn(root)("rev-parse", "HEAD"), head);
        assert.equal(gitIn(root)("branch", "--show-current").trim(), "main");
        assert.equal(await fs.readFile(path.join(root, "a"), "utf8"), "base\n");
        assert.equal(existsSync(path.join(root, "renamed")), false);
      });
    }
  }

  for (const contender of [...executables, { name: "Git", command: "git", args: [] }]) {
    it(`preserves staging across processes with a ${contender.name} contender`, async () => {
      gitIn(root)("reset", "-q", "HEAD", "--", "b");
      const ready = Promise.withResolvers<void>();
      const child = spawn(process.execPath, ["--input-type=module", "-e", writer, root], {
        cwd: process.cwd(),
        stdio: ["pipe", "pipe", "pipe"],
      });
      let stderr = "";
      child.stderr.on("data", (chunk: Buffer) => {
        stderr += chunk.toString();
      });
      child.stdout.on("data", (chunk: Buffer) => {
        if (chunk.toString().includes("ready")) ready.resolve();
      });
      const completed = new Promise<void>((resolve, reject) => {
        child.on("error", reject);
        child.on("close", (code) => (code === 0 ? resolve() : reject(new Error(stderr))));
      });
      void completed.then(
        () => ready.reject(new Error("writer exited before readiness")),
        (cause: Error) => ready.reject(cause),
      );
      try {
        await ready.promise;
        const args = [
          ...contender.args,
          "add",
          ...(contender.name === "Git" ? [] : ["--work", "."]),
          "c",
        ];
        const second = spawnSync(contender.command, args, {
          cwd: root,
          env: gitEnv,
          encoding: "utf8",
          timeout: 30_000,
        });
        child.stdin.end("\n");
        await completed;
        if (second.status === 0) {
          assert.equal(
            gitIn(root)("diff", "--cached", "--name-only").trim(),
            "b\nc",
            "every acknowledged change must survive",
          );
        }
        assert.notEqual(second.status, 0);
        assert.match(second.stdout + second.stderr, /index\.lock|EEXIST/);
        assert.equal(existsSync(path.join(root, ".git/index.lock")), false);
        const retry = spawnSync(contender.command, args, {
          cwd: root,
          env: gitEnv,
          encoding: "utf8",
          timeout: 30_000,
        });
        assert.equal(retry.status, 0, retry.stdout + retry.stderr);
        assert.equal(gitIn(root)("diff", "--cached", "--name-only").trim(), "b\nc");
      } finally {
        child.stdin.end("\n");
        await completed;
      }
    });
  }
});
