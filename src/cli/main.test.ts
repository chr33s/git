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
    ),
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

  it("inspects a repository: branch, tag, show, files, grep, diff", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-inspect-"));
    try {
      const directory = path.join(root, "repo");
      const first = await seed(directory, "first");
      const second = await seed(directory, "second");

      // A branch, listed with the checked-out one marked.
      await cli(["branch", "--root", root, "-c", "feature", "repo"]);
      const branches = await cli(["branch", "--root", root, "repo"]);
      assert.match(branches, /^\* [0-9a-f]{40}\tmain$/m);
      assert.match(branches, /^ {2}[0-9a-f]{40}\tfeature$/m);

      // Annotated tags and lightweight ones, listed together.
      await cli(["tag", "--root", root, "--name", "v1", "-m", "release", "repo"]);
      await cli(["tag", "--root", root, "--name", "latest", "repo"]);
      const tags = await cli(["tag", "--root", root, "repo"]);
      assert.match(tags, /^[0-9a-f]{40}\tv1$/m);
      assert.match(tags, /^[0-9a-f]{40}\tlatest$/m);

      const shown = await cli(["show", "--root", root, "repo", second]);
      assert.match(shown, /commit/);
      assert.match(shown, /^second$/m);

      const listed = await cli(["files", "--root", root, "repo"]);
      assert.match(listed, /100644 [0-9a-f]{40}\tf\.txt/);

      const found = await cli(["grep", "--root", root, "repo", "second"]);
      assert.equal(found.trim(), "f.txt:1:second");

      const patch = await cli(["diff", "--root", root, "repo", first, second]);
      assert.match(patch, /^--- a\/f\.txt$/m);
      assert.match(patch, /^-first$/m);
      assert.match(patch, /^\+second$/m);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("merges, and exits non-zero when the merge conflicts", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-merge-"));
    try {
      const directory = path.join(root, "repo");
      await seed(directory, "base");
      await cli(["branch", "--root", root, "-c", "side", "repo"]);

      // Both branches change the one file, on the same line.
      await Effect.runPromise(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const theirs = yield* repository.writeFiles({
            base: (yield* repository.readCommit((yield* repository.resolve("refs/heads/side"))!))
              .tree,
            changes: [{ path: "f.txt", content: new TextEncoder().encode("theirs\n") }],
          });
          yield* repository.commit({
            branch: "side",
            tree: theirs,
            message: "theirs",
            author,
          });
          const ours = yield* repository.writeFiles({
            base: (yield* repository.readCommit((yield* repository.resolve("refs/heads/main"))!))
              .tree,
            changes: [{ path: "f.txt", content: new TextEncoder().encode("ours\n") }],
          });
          yield* repository.commit({ branch: "main", tree: ours, message: "ours", author });
        }).pipe(
          Effect.provide(
            GitRepository.layer.pipe(
              Layer.provide(GitRepository.hooksNoop),
              Layer.provide(stores(directory)),
            ),
          ),
        ),
      );

      // A conflict is a failed merge to a shell, and the exit code says so.
      const failed = await cli([
        "merge",
        "--root",
        root,
        "repo",
        "refs/heads/main",
        "refs/heads/side",
      ]).then(
        () => null,
        (error: { code?: number; stdout?: string; stderr?: string }) => error,
      );
      assert.notEqual(failed, null);
      assert.equal(failed!.code, 1);
      assert.match(
        `${failed!.stdout ?? ""}${failed!.stderr ?? ""}`,
        /CONFLICT \(content\): f\.txt/,
      );

      // Choosing a side resolves it and moves the ref.
      const resolved = await cli([
        "merge",
        "--root",
        root,
        "-s",
        "theirs",
        "--into",
        "refs/heads/main",
        "repo",
        "refs/heads/main",
        "refs/heads/side",
      ]);
      assert.match(resolved, /^merged [0-9a-f]{40}$/m);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("pushes to a server and exports an archive", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-push-"));
    const serverRoot = path.join(root, "server");
    const server = await serve({ root: serverRoot });
    try {
      await seed(path.join(root, "local"), "pushed");

      const pushed = await cli(["push", "--root", root, "local", `${server.url}/remote`, "main"]);
      assert.match(pushed, /^ok refs\/heads\/main/m);

      // The server really has it: its own API says so.
      const refs = (await (await fetch(`${server.url}/remote/refs`)).json()) as {
        refs: Array<{ name: string }>;
      };
      assert.deepEqual(
        refs.refs.map((ref) => ref.name),
        ["refs/heads/main"],
      );

      // And the archive of what we pushed is a real tar.
      const tarball = path.join(root, "out.tar");
      await cli(["archive", "--root", root, "-o", tarball, "local"]);
      const listed = await execFileAsync("tar", ["-tf", tarball], { encoding: "utf8" });
      assert.match(listed.stdout, /f\.txt/);
    } finally {
      await server.close();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("checks integrity and collects garbage", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-maint-"));
    try {
      const directory = path.join(root, "repo");
      await seed(directory, "kept");

      const clean = await cli(["fsck", "--root", root, "repo"]);
      assert.match(clean, /checked \d+ object\(s\)/);

      // A blob nothing references is what gc is for.
      await Effect.runPromise(
        Effect.gen(function* () {
          yield* (yield* Repository).writeBlob(new TextEncoder().encode("orphan\n"));
        }).pipe(
          Effect.provide(
            GitRepository.layer.pipe(
              Layer.provide(GitRepository.hooksNoop),
              Layer.provide(stores(directory)),
            ),
          ),
        ),
      );

      const dry = await cli(["gc", "--root", root, "-n", "repo"]);
      assert.match(dry, /would remove 1 of \d+ object\(s\)/);
      const swept = await cli(["gc", "--root", root, "repo"]);
      assert.match(swept, /removed 1 of \d+ object\(s\)/);
      const again = await cli(["gc", "--root", root, "repo"]);
      assert.match(again, /removed 0 of \d+ object\(s\)/);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
});
