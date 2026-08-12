/**
 * The CLI, black-box: every test spawns `node src/cli/main.ts …` and looks
 * only at exit codes and output — the same interface a user gets.
 */
import assert from "node:assert/strict";
import { type ChildProcess, execFile, spawn } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";
import { promisify } from "node:util";

import { Effect, Layer } from "effect";

import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { serve } from "../host/Node.ts";
import { hmacMint, hmacVerify } from "../server/Auth.ts";

const execFileAsync = promisify(execFile);
const entry = path.join(import.meta.dirname, "main.ts");

const cli = async (args: string[], env?: Record<string, string>) => {
  const result = await execFileAsync("node", [entry, ...args], {
    encoding: "utf8",
    env: { ...process.env, ...env },
  });
  return result.stdout;
};

const author = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const seed = (directory: string, message: string) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const repository = yield* Repository;
      const blob = yield* repository.writeBlob(new TextEncoder().encode(`${message}\n`));
      const tree = yield* repository.writeTree([{ mode: "100644", name: "f.txt", oid: blob }]);
      return yield* repository.commit({ branch: "main", tree, message, author });
    }).pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provide(stores(directory)),
        ),
      ),
    ) as unknown as Effect.Effect<string>,
  );

describe("cli", () => {
  it("init, refs and log against a seeded repository", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-basic-"));
    try {
      const initOut = await cli(["init", "--root", root, "basic"]);
      assert.match(initOut, /Initialized empty repository/);

      const first = await seed(path.join(root, "basic"), "first");
      const second = await seed(path.join(root, "basic"), "second");

      const refsOut = await cli(["refs", "--root", root, "basic"]);
      assert.match(refsOut, new RegExp(`${second}\trefs/heads/main`));

      const logOut = await cli(["log", "--root", root, "basic"]);
      assert.deepEqual(
        logOut
          .trim()
          .split("\n")
          .map((line) => line.split(" ").slice(1).join(" ")),
        ["second", "first"],
      );
      assert.ok(logOut.includes(first));

      await assert.rejects(cli(["log", "--root", root, "basic", "--ref", "bogus"]));
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("mints verifiable tokens, secret from flag or environment", async () => {
    const fromFlag = (await cli(["token", "repo-a", "--secret", "s3cret", "-s", "write"])).trim();
    assert.equal(await Effect.runPromise(hmacVerify("s3cret", "repo-a", fromFlag)), "write");

    const fromEnv = (await cli(["token", "repo-a"], { GIT_AUTH_SECRET: "s3cret" })).trim();
    assert.equal(await Effect.runPromise(hmacVerify("s3cret", "repo-a", fromEnv)), "read");
  });

  it("clones over smart HTTP, with and without a token", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-clone-"));
    const serverRoot = path.join(root, "server");
    const secret = "clone-secret";
    const server = await serve({
      root: serverRoot,
      verify: (repo, credential) => Effect.runPromise(hmacVerify(secret, repo, credential)),
    });
    try {
      await seed(path.join(serverRoot, "origin"), "published");
      const token = await Effect.runPromise(hmacMint(secret, "origin", "read", 300));

      const denied = await cli(["clone", "--root", root, `${server.url}/origin`, "denied"]).then(
        () => null,
        (error: { stderr?: string; stdout?: string }) => error,
      );
      assert.ok(denied !== null, "clone without a token must fail");
      assert.match(`${denied.stderr ?? ""}${denied.stdout ?? ""}`, /401/);

      const out = await cli([
        "clone",
        "--root",
        root,
        "--token",
        token,
        `${server.url}/origin`,
        "copy",
      ]);
      assert.match(out, /Cloned 1 ref\(s\)/);

      // The clone is a real git repository: the git binary reads it.
      const logOut = await execFileAsync(
        "git",
        [`--git-dir=${path.join(root, "copy")}`, "log", "--format=%s"],
        { encoding: "utf8" },
      );
      assert.equal(logOut.stdout.trim(), "published");
    } finally {
      await server.close();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("serves repositories via the serve command", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-serve-"));
    await seed(path.join(root, "hosted"), "served");

    let child: ChildProcess | null = null;
    try {
      child = spawn("node", [entry, "serve", "--root", root, "--port", "0"], {
        stdio: ["ignore", "pipe", "pipe"],
      });
      const url = await new Promise<string>((resolve, reject) => {
        let buffered = "";
        child!.stdout!.on("data", (chunk: Buffer) => {
          buffered += chunk.toString();
          const match = buffered.match(/server on (http:\/\/[^,]+),/);
          if (match) resolve(match[1]!);
        });
        child!.on("exit", (code) => reject(new Error(`serve exited early: ${code}`)));
        setTimeout(() => reject(new Error(`serve never announced: ${buffered}`)), 15_000);
      });

      const response = await fetch(`${url}/hosted/refs`);
      assert.equal(response.status, 200);
      const body = (await response.json()) as { refs: Array<{ name: string }> };
      assert.deepEqual(
        body.refs.map((ref) => ref.name),
        ["refs/heads/main"],
      );
    } finally {
      child?.kill();
      await fs.rm(root, { recursive: true, force: true });
    }
  });
});
