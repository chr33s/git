/**
 * Interop: stock `git` against an *authenticated* node host.
 *
 * The evaluation's compatibility warning, made executable: git sends the
 * token as the HTTP Basic password, an anonymous clone must fail with 401,
 * and a read-scoped token must be able to fetch but not push.
 *
 * Skipped when `git` is not on PATH.
 */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
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
import { enableHubUnder, grantMemberUnder } from "../testing/Hub.ts";
import { hasGit } from "../testing/Git.ts";

const execFileAsync = promisify(execFile);
const run = async (cwd: string, args: string[], env: Record<string, string> = {}) => {
  const result = await execFileAsync(
    "git",
    ["-c", "user.name=Test", "-c", "user.email=test@example.com", ...args],
    { cwd, encoding: "utf8", env: { ...process.env, GIT_TERMINAL_PROMPT: "0", ...env } },
  );
  return result.stdout;
};
const git = (cwd: string, ...args: string[]): Promise<string> => run(cwd, args);

/**
 * `git` presenting `token` as the Basic password through a credential helper,
 * the way a stored credential arrives. Not `http://<token>@host/…`: git 2.56.0
 * with a curl that strips credentials from the effective URL (curl 8.23, and
 * distro backports) mistakes that for a redirect and drops the URL credential
 * after the first request (fixed upstream in git/git@76524c2604).
 */
const gitAs =
  (token: string) =>
  (cwd: string, ...args: string[]): Promise<string> =>
    run(
      cwd,
      [
        "-c",
        "credential.helper=",
        "-c",
        'credential.helper=!f() { echo username=git; echo "password=$GIT_AUTH_TOKEN"; }; f',
        ...args,
      ],
      { GIT_AUTH_TOKEN: token },
    );

const author = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

describe.skipIf(!hasGit)("Auth interop with git", () => {
  let root: string;
  let server: Server;
  let readToken: string;
  let writeToken: string;

  const remote = (repo: string) => `${new URL(server.url).href}${repo}`;

  beforeAll(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "git-auth-interop-"));
    server = await serve({ root, allowAnonymousWrites: true });

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

    // Membership is what makes the repository private: a reader who may only
    // read, and a writer who may push. Both credentials are signed by their
    // own holder's key — there is no server secret in this suite at all.
    const reader = await enableHubUnder(root, "authed", ["repo.read"]);
    const writer = await grantMemberUnder(root, "authed", reader.root, reader.repoId, [
      "repo.read",
      "source.push",
    ]);
    readToken = reader.credential;
    writeToken = writer.credential;
  });

  afterAll(async () => {
    await server.close();
    await fs.rm(root, { recursive: true, force: true });
  });

  it.effect("rejects an anonymous clone with 401", () =>
    Effect.promise(async () => {
      const work = path.join(root, "work-anon");
      // With prompts disabled, git surfaces the 401 challenge as a refusal to
      // ask for credentials — that refusal is the rejection.
      await assert.rejects(
        git(root, "clone", "--quiet", remote("authed"), work),
        /Authentication failed|401|terminal prompts disabled/,
      );
    }),
  );

  it.effect("clones with a read token, but cannot push with it", () =>
    Effect.promise(async () => {
      const work = path.join(root, "work-read");
      const reader = gitAs(readToken);
      await reader(root, "clone", "--quiet", remote("authed"), work);
      assert.equal(await fs.readFile(path.join(work, "g.txt"), "utf8"), "guarded\n");

      await fs.writeFile(path.join(work, "nope.txt"), "denied\n");
      await git(work, "add", ".");
      await git(work, "commit", "--quiet", "-m", "denied");
      await assert.rejects(
        reader(work, "push", "--quiet", "origin", "main"),
        /403|denied|forbidden/i,
      );
    }),
  );

  it.effect("pushes with a write token", () =>
    Effect.promise(async () => {
      const work = path.join(root, "work-write");
      const writer = gitAs(writeToken);
      await writer(root, "clone", "--quiet", remote("authed"), work);

      await fs.writeFile(path.join(work, "ok.txt"), "allowed\n");
      await git(work, "add", ".");
      await git(work, "commit", "--quiet", "-m", "allowed");
      await writer(work, "push", "--quiet", "origin", "main");

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
    }),
  );

  it.effect("guards the JSON API with the same tokens", () =>
    Effect.promise(async () => {
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
    }),
  );
});
