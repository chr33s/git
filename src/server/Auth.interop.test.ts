/**
 * Interop: stock `git` against an *authenticated* node host.
 *
 * The evaluation's compatibility warning, made executable: git sends the
 * token as the HTTP Basic password, so a clone URL like
 * `http://token@host/repo` must work, an anonymous clone must fail with 401,
 * and a read-scoped token must be able to fetch but not push.
 *
 * Skipped when `git` is not on PATH.
 */
import assert from "node:assert/strict";
import { execFile, execFileSync } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterAll, beforeAll, describe, it } from "@effect/vitest";
import { promisify } from "node:util";

import { Effect, Layer } from "effect";

import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { serve, type Server } from "../host/Node.ts";
import { hmacMint, hmacVerify } from "./Auth.ts";

const hasGit = (() => {
  try {
    execFileSync("git", ["--version"], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
})();

const execFileAsync = promisify(execFile);
const git = async (cwd: string, ...args: string[]): Promise<string> => {
  const result = await execFileAsync(
    "git",
    ["-c", "user.name=Test", "-c", "user.email=test@example.com", ...args],
    { cwd, encoding: "utf8", env: { ...process.env, GIT_TERMINAL_PROMPT: "0" } },
  );
  return result.stdout;
};

const author = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const SECRET = "interop-secret";

describe.skipIf(!hasGit)("Auth interop with git", () => {
  let root: string;
  let server: Server;
  let readToken: string;
  let writeToken: string;

  /** `http://<token>@host/…` — the token as the Basic credential. */
  const remote = (token: string | null, repo: string) => {
    const url = new URL(server.url);
    if (token !== null) url.username = token;
    return `${url.href}${repo}`;
  };

  beforeAll(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "git-auth-interop-"));
    server = await serve({
      root,
      verify: (repo, credential) => Effect.runPromise(hmacVerify(SECRET, repo, credential)),
    });
    readToken = await Effect.runPromise(hmacMint(SECRET, "authed", "read", 300));
    writeToken = await Effect.runPromise(hmacMint(SECRET, "authed", "write", 300));

    await Effect.runPromise(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const blob = yield* repository.writeBlob(new TextEncoder().encode("guarded\n"));
        const tree = yield* repository.writeTree([{ mode: "100644", name: "g.txt", oid: blob }]);
        return yield* repository.commit({ branch: "main", tree, message: "seed", author });
      }).pipe(
        Effect.provide(
          GitRepository.layer.pipe(
            Layer.provide(GitRepository.hooksNoop),
            Layer.provide(stores(path.join(root, "authed"))),
          ),
        ),
      ),
    );
  });

  afterAll(async () => {
    await server.close();
    await fs.rm(root, { recursive: true, force: true });
  });

  it("rejects an anonymous clone with 401", async () => {
    const work = path.join(root, "work-anon");
    // With prompts disabled, git surfaces the 401 challenge as a refusal to
    // ask for credentials — that refusal is the rejection.
    await assert.rejects(
      git(root, "clone", "--quiet", remote(null, "authed"), work),
      /Authentication failed|401|terminal prompts disabled/,
    );
  });

  it("clones with a read token, but cannot push with it", async () => {
    const work = path.join(root, "work-read");
    await git(root, "clone", "--quiet", remote(readToken, "authed"), work);
    assert.equal(await fs.readFile(path.join(work, "g.txt"), "utf8"), "guarded\n");

    await fs.writeFile(path.join(work, "nope.txt"), "denied\n");
    await git(work, "add", ".");
    await git(work, "commit", "--quiet", "-m", "denied");
    await assert.rejects(git(work, "push", "--quiet", "origin", "main"), /403|denied|forbidden/i);
  });

  it("pushes with a write token", async () => {
    const work = path.join(root, "work-write");
    await git(root, "clone", "--quiet", remote(writeToken, "authed"), work);

    await fs.writeFile(path.join(work, "ok.txt"), "allowed\n");
    await git(work, "add", ".");
    await git(work, "commit", "--quiet", "-m", "allowed");
    await git(work, "push", "--quiet", "origin", "main");

    const pushed = (await git(work, "rev-parse", "HEAD")).trim();
    const serverMain = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* (yield* Repository).resolve("refs/heads/main");
      }).pipe(
        Effect.provide(
          GitRepository.layer.pipe(
            Layer.provide(GitRepository.hooksNoop),
            Layer.provide(stores(path.join(root, "authed"))),
          ),
        ),
      ),
    );
    assert.equal(serverMain, pushed);
  });

  it("guards the JSON API with the same tokens", async () => {
    const anonymous = await fetch(`${server.url}/authed/refs`);
    assert.equal(anonymous.status, 401);

    const read = await fetch(`${server.url}/authed/refs`, {
      headers: { authorization: `Bearer ${readToken}` },
    });
    assert.equal(read.status, 200);

    const writeDenied = await fetch(`${server.url}/authed/commit`, {
      method: "POST",
      headers: { "content-type": "application/json", authorization: `Bearer ${readToken}` },
      body: JSON.stringify({ message: "nope" }),
    });
    assert.equal(writeDenied.status, 403);

    const written = await fetch(`${server.url}/authed/commit`, {
      method: "POST",
      headers: { "content-type": "application/json", authorization: `Bearer ${writeToken}` },
      body: JSON.stringify({ message: "yes" }),
    });
    assert.equal(written.status, 200);
  });
});
