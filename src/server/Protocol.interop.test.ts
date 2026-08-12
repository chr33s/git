/**
 * Interop: the real `git` binary clones from and pushes to this server.
 *
 * The protocol handlers speak web `Request`/`Response`, so a ~30-line
 * `node:http` adapter is all it takes to put them behind a socket — the same
 * functions the Durable Object serves. Every test here is an end-to-end
 * conversation with stock git over smart HTTP.
 *
 * Skipped when `git` is not on PATH.
 */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { execFile, execFileSync } from "node:child_process";
import * as fs from "node:fs/promises";
import * as http from "node:http";
import type { AddressInfo } from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { after, before, describe, it } from "node:test";
import { promisify } from "node:util";

import { Effect, Layer } from "effect";

import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import * as Protocol from "./Protocol.ts";

const hasGit = (() => {
  try {
    execFileSync("git", ["--version"], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
})();

const execFileAsync = promisify(execFile);

/**
 * Async on purpose: the server under test runs on this process's event loop,
 * so a synchronous `git` invocation would deadlock — the client waiting on a
 * response the blocked loop can never produce.
 */
const git = async (cwd: string, ...args: string[]): Promise<string> => {
  const result = await execFileAsync(
    "git",
    [
      "-c",
      "user.name=Test",
      "-c",
      "user.email=test@example.com",
      "-c",
      "init.defaultBranch=main",
      ...args,
    ],
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

describe("Protocol interop with git", { skip: hasGit ? false : "git not installed" }, () => {
  let root: string;
  let base: string;
  let close: () => Promise<void>;

  const layerFor = (repo: string) =>
    GitRepository.layer.pipe(
      Layer.provide(GitRepository.hooksNoop),
      Layer.provide(stores(path.join(root, repo))),
    );

  const inRepo = <A, E>(repo: string, effect: Effect.Effect<A, E, Repository>) =>
    Effect.runPromise(effect.pipe(Effect.provide(layerFor(repo))) as Effect.Effect<A, E>);

  before(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "git-protocol-interop-"));

    const server = http.createServer((incoming, outgoing) => {
      void (async () => {
        const chunks: Buffer[] = [];
        for await (const chunk of incoming) chunks.push(chunk as Buffer);
        const repo = (incoming.url ?? "/").split("/")[1] ?? "";

        const headers = new Headers();
        for (const name of ["content-type", "content-encoding"]) {
          const value = incoming.headers[name];
          if (typeof value === "string") headers.set(name, value);
        }
        const request = new Request(`http://127.0.0.1${incoming.url ?? "/"}`, {
          method: incoming.method,
          headers,
          ...(chunks.length > 0 ? { body: new Uint8Array(Buffer.concat(chunks)) } : {}),
        });

        const response = await Effect.runPromise(
          Protocol.handle(request).pipe(
            Effect.map((matched) => matched ?? new Response("not found", { status: 404 })),
            Effect.catch((error) =>
              Effect.succeed(new Response(JSON.stringify(error), { status: 500 })),
            ),
            Effect.provide(layerFor(repo)),
          ) as Effect.Effect<Response>,
        );

        outgoing.writeHead(response.status, Object.fromEntries(response.headers.entries()));
        outgoing.end(Buffer.from(await response.arrayBuffer()));
      })().catch((error: unknown) => {
        outgoing.writeHead(500);
        outgoing.end(String(error));
      });
    });

    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    close = () =>
      new Promise((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
  });

  after(async () => {
    await close();
    await fs.rm(root, { recursive: true, force: true });
  });

  const seed = (repo: string) =>
    inRepo(
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;
        const blob = yield* repository.writeBlob(
          new TextEncoder().encode("hello from the server\n"),
        );
        const tree = yield* repository.writeTree([
          { mode: "100644", name: "hello.txt", oid: blob },
        ]);
        return yield* repository.commit({ branch: "main", tree, message: "seed", author });
      }),
    );

  it("clones a seeded repository", async () => {
    await seed("cloneme");
    const work = path.join(root, "work-clone");
    await git(root, "clone", "--quiet", `${base}/cloneme`, work);

    assert.equal(
      await fs.readFile(path.join(work, "hello.txt"), "utf8"),
      "hello from the server\n",
    );
    assert.match(await git(work, "log", "--format=%s"), /^seed$/m);
    await git(work, "fsck", "--strict");
  });

  it("accepts a push and serves it back to a second clone", async () => {
    await seed("roundtrip");
    const work = path.join(root, "work-push");
    await git(root, "clone", "--quiet", `${base}/roundtrip`, work);

    await fs.writeFile(path.join(work, "feature.txt"), "pushed content\n");
    await git(work, "add", ".");
    await git(work, "commit", "--quiet", "-m", "pushed from a client");
    await git(work, "push", "--quiet", "origin", "main");

    const pushed = (await git(work, "rev-parse", "HEAD")).trim();
    const serverMain = await inRepo(
      "roundtrip",
      Effect.gen(function* () {
        return yield* (yield* Repository).resolve("refs/heads/main");
      }),
    );
    assert.equal(serverMain, pushed);

    const verify = path.join(root, "work-verify");
    await git(root, "clone", "--quiet", `${base}/roundtrip`, verify);
    assert.equal(await fs.readFile(path.join(verify, "feature.txt"), "utf8"), "pushed content\n");
    await git(verify, "fsck", "--strict");
  });

  it("clones an empty repository, then receives its first push", async () => {
    const work = path.join(root, "work-empty");
    await git(root, "clone", "--quiet", `${base}/empty`, work);

    await fs.writeFile(path.join(work, "first.txt"), "first\n");
    await git(work, "add", ".");
    await git(work, "commit", "--quiet", "-m", "first");
    await git(work, "push", "--quiet", "-u", "origin", "main");

    const verify = path.join(root, "work-empty-verify");
    await git(root, "clone", "--quiet", `${base}/empty`, verify);
    assert.equal(await fs.readFile(path.join(verify, "first.txt"), "utf8"), "first\n");
  });

  it("creates and deletes a branch over push", async () => {
    await seed("branchy");
    const work = path.join(root, "work-branch");
    await git(root, "clone", "--quiet", `${base}/branchy`, work);

    await git(work, "push", "--quiet", "origin", "main:feature");
    assert.match(await git(work, "ls-remote", "origin"), /refs\/heads\/feature/);

    await git(work, "push", "--quiet", "origin", ":feature");
    assert.doesNotMatch(await git(work, "ls-remote", "origin"), /refs\/heads\/feature/);
  });

  it("rejects a push whose old-oid no longer matches the ref", async () => {
    // git itself can never present a stale old-oid over smart HTTP — every
    // push re-reads the advertisement first — so the compare-and-swap is
    // exercised the way a real race would: a crafted receive-pack request
    // whose old value disagrees with the ref.
    const head = await seed("contended");

    const encoderLocal = new TextEncoder();
    const pktLine = (line: string) =>
      encoderLocal.encode((line.length + 4).toString(16).padStart(4, "0") + line);
    const emptyPack = (() => {
      const header = new Uint8Array(12);
      header.set([0x50, 0x41, 0x43, 0x4b, 0, 0, 0, 2, 0, 0, 0, 0]);
      const digest = createHash("sha1").update(header).digest();
      return Buffer.concat([header, digest]);
    })();
    const zero = "0".repeat(40);
    const requestBody = Buffer.concat([
      // old = zero claims "ref must not exist" — but it does.
      pktLine(`${zero} ${head} refs/heads/main\0 report-status\n`),
      encoderLocal.encode("0000"),
      emptyPack,
    ]);

    const response = await fetch(`${base}/contended/git-receive-pack`, {
      method: "POST",
      headers: { "content-type": "application/x-git-receive-pack-request" },
      body: requestBody,
    });
    const reportText = await response.text();
    assert.match(reportText, /unpack ok/);
    assert.match(reportText, /ng refs\/heads\/main/);

    // The ref is untouched.
    const serverMain = await inRepo(
      "contended",
      Effect.gen(function* () {
        return yield* (yield* Repository).resolve("refs/heads/main");
      }),
    );
    assert.equal(serverMain, head);
  });

  it("fetches incrementally after the server moves ahead", async () => {
    await seed("increment");
    const work = path.join(root, "work-fetch");
    await git(root, "clone", "--quiet", `${base}/increment`, work);

    const publisher = path.join(root, "work-publisher");
    await git(root, "clone", "--quiet", `${base}/increment`, publisher);
    await fs.writeFile(path.join(publisher, "new.txt"), "new content\n");
    await git(publisher, "add", ".");
    await git(publisher, "commit", "--quiet", "-m", "ahead");
    await git(publisher, "push", "--quiet", "origin", "main");

    await git(work, "pull", "--quiet", "origin", "main");
    assert.equal(await fs.readFile(path.join(work, "new.txt"), "utf8"), "new content\n");
    await git(work, "fsck", "--strict");
  });
});
