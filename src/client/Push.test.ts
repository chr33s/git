/**
 * Push, proved against the server we serve and the git binary that consumes it.
 *
 * The local side is a real repository over `git/Node.ts`'s stores, the remote
 * is `host/Node.ts` — so a passing test is a full smart-HTTP receive-pack
 * conversation, and `git clone` afterwards is the independent reader that says
 * the objects and refs which arrived are the ones we meant to send.
 *
 * The git-dependent half is skipped when `git` is not on PATH; the client's own
 * decisions — the fast-forward refusal, the up-to-date no-op — need no binary
 * and always run.
 */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as http from "node:http";
import type { AddressInfo, Socket } from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { afterAll, beforeAll, describe, it } from "@effect/vitest";

import { Effect, Fiber, Layer } from "effect";

import { Invalid } from "../git/Error.ts";
import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import { serve, type Server } from "../host/Node.ts";
import { hasGit } from "../testing/Git.ts";
import { formatPublicKey, generate } from "../crypto/SshSignature.ts";
import * as Event from "../hub/Event.ts";
import * as PullRequest from "../hub/PullRequest.ts";
import * as Redaction from "../hub/Redaction.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import { push, type PushRef, type PushResult } from "./Push.ts";

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

const encoder = new TextEncoder();

let root: string;
let server: Server;

/**
 * Local repositories live outside the served root — anything under it is a
 * repository this server would hand out, and a client's own store is not that.
 */
const localOf = (name: string) =>
  GitRepository.layer.pipe(
    Layer.provide(GitRepository.hooksNoop),
    Layer.provide(stores(path.join(root, "local", name))),
  );

const inLocal = <A, E>(name: string, effect: Effect.Effect<A, E, Repository>): Promise<A> =>
  Effect.runPromise(effect.pipe(Effect.provide(localOf(name))));

/** One commit on `main` in a local repository, appended to whatever is there. */
const commitFile = (repo: string, file: string, content: string, message: string): Promise<Oid> =>
  inLocal(
    repo,
    Effect.gen(function* () {
      const repository = yield* Repository;
      const parent = yield* repository.resolve("refs/heads/main");
      const changes = [{ path: file, content: encoder.encode(content) }];
      const tree =
        parent === null
          ? yield* repository.writeFiles({ changes })
          : yield* repository.writeFiles({
              base: (yield* repository.readCommit(parent)).tree,
              changes,
            });
      return yield* repository.commit({ branch: "main", tree, message, author });
    }),
  );

const pushFrom = (
  repo: string,
  remote: string,
  refs: ReadonlyArray<PushRef>,
  options?: { readonly force?: boolean; readonly atomic?: boolean },
): Promise<ReadonlyArray<PushResult>> =>
  inLocal(repo, push({ url: `${server.url}/${remote}`, refs, ...options }));

/**
 * The object count in a packfile the client sent, or `null` when the request
 * carried no pack at all — which is how "a delete sends no pack" and "the
 * remote already had these objects" are told apart from each other.
 */
const packObjectCount = (body: Uint8Array): number | null => {
  for (let index = 0; index + 12 <= body.length; index++) {
    if (
      body[index] === 0x50 &&
      body[index + 1] === 0x41 &&
      body[index + 2] === 0x43 &&
      body[index + 3] === 0x4b
    ) {
      return new DataView(body.buffer, body.byteOffset + index, 12).getUint32(8);
    }
  }
  return null;
};

/** Every pack the client uploaded while `run` was in flight, by object count. */
const capturingPacks = async <A>(
  run: () => Promise<A>,
): Promise<{ readonly result: A; readonly packs: ReadonlyArray<number | null> }> => {
  const original = globalThis.fetch;
  const packs: Array<number | null> = [];
  const patched: typeof globalThis.fetch = (input, init) => {
    const body = init?.body;
    if (body instanceof Uint8Array) packs.push(packObjectCount(body));
    return original(input, init);
  };
  globalThis.fetch = patched;
  try {
    return { result: await run(), packs };
  } finally {
    globalThis.fetch = original;
  }
};

const serverRef = (repo: string, name: string): Promise<Oid | null> =>
  Effect.runPromise(
    Effect.gen(function* () {
      return yield* (yield* Repository).resolve(name);
    }).pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provide(stores(path.join(root, repo))),
        ),
      ),
    ),
  );

beforeAll(async () => {
  root = await fs.mkdtemp(path.join(os.tmpdir(), "client-push-"));
  server = await serve({ root, allowAnonymousWrites: true });
});

afterAll(async () => {
  await server.close();
  await fs.rm(root, { recursive: true, force: true });
});

describe.skipIf(!hasGit)("Push, read back by the git binary", () => {
  it("pushes a new branch the git binary then clones", async () => {
    const head = await commitFile("first", "hello.txt", "pushed by hand\n", "first");

    const { packs, result } = await capturingPacks(() =>
      pushFrom("first", "created", [{ local: "refs/heads/main", remote: "refs/heads/main" }]),
    );

    assert.deepEqual(result, [{ ref: "refs/heads/main", ok: true }]);
    // A commit, its tree, its blob — nothing the remote could already hold.
    assert.deepEqual(packs, [3]);
    assert.equal(await serverRef("created", "refs/heads/main"), head);

    const work = path.join(root, "work-created");
    await git(root, "clone", "--quiet", `${server.url}/created`, work);
    assert.equal(await fs.readFile(path.join(work, "hello.txt"), "utf8"), "pushed by hand\n");
    assert.match(await git(work, "log", "--format=%s"), /^first$/m);
    assert.equal((await git(work, "rev-parse", "HEAD")).trim(), head);
    await git(work, "fsck", "--strict");
  });

  it("sends only the new objects on a second push", async () => {
    await commitFile("incremental", "a.txt", "one\n", "one");
    await pushFrom("incremental", "incremental", [
      { local: "refs/heads/main", remote: "refs/heads/main" },
    ]);

    const second = await commitFile("incremental", "b.txt", "two\n", "two");
    const { packs, result } = await capturingPacks(() =>
      pushFrom("incremental", "incremental", [
        { local: "refs/heads/main", remote: "refs/heads/main" },
      ]),
    );

    assert.deepEqual(result, [{ ref: "refs/heads/main", ok: true }]);
    // The second commit, its tree and the one new blob: `a.txt` and the first
    // commit are already on the remote and are not sent again.
    assert.deepEqual(packs, [3]);

    const work = path.join(root, "work-incremental");
    await git(root, "clone", "--quiet", `${server.url}/incremental`, work);
    assert.equal((await git(work, "rev-list", "--count", "HEAD")).trim(), "2");
    assert.equal((await git(work, "rev-parse", "HEAD")).trim(), second);
    assert.equal(await fs.readFile(path.join(work, "a.txt"), "utf8"), "one\n");
    assert.equal(await fs.readFile(path.join(work, "b.txt"), "utf8"), "two\n");
    await git(work, "fsck", "--strict");
  });

  it("deletes a remote ref, without sending a pack", async () => {
    await commitFile("deletable", "x.txt", "x\n", "x");
    await pushFrom("deletable", "deletable", [
      { local: "refs/heads/main", remote: "refs/heads/main" },
      { local: "refs/heads/main", remote: "refs/heads/feature" },
    ]);
    assert.match(await git(root, "ls-remote", `${server.url}/deletable`), /refs\/heads\/feature/);

    const { packs, result } = await capturingPacks(() =>
      pushFrom("deletable", "deletable", [
        { local: "refs/heads/feature", remote: "refs/heads/feature", delete: true },
      ]),
    );

    assert.deepEqual(result, [{ ref: "refs/heads/feature", ok: true }]);
    // A delete-only push carries commands and a flush, and stops there.
    assert.deepEqual(packs, [null]);

    const remote = await git(root, "ls-remote", `${server.url}/deletable`);
    assert.doesNotMatch(remote, /refs\/heads\/feature/);
    assert.match(remote, /refs\/heads\/main/);
  });
});

describe("Push", () => {
  it("lets go of a peer that accepts and never answers", async () => {
    // The advertisement is a `fetch`, and a `fetch` given no signal survives
    // the interrupt of the effect around it: the promise is abandoned, the
    // socket is not, and the event loop stays alive holding it. That is how a
    // caller that bounded how long it would wait for a push waited on the
    // connection anyway — a mirror that black-holes holding the process that
    // pushed to it for as long as the peer cared to. The signal has to reach
    // the socket, and this is what says it does.
    const silent = http.createServer(() => {
      /* accept the connection, answer nothing, ever */
    });
    // The sockets themselves, because that is the claim. Interrupting the
    // effect always returns; what has to be true is that the connection went
    // with it, and only the peer's side can say so.
    const live = new Set<Socket>();
    silent.on("connection", (socket) => {
      live.add(socket);
      socket.on("close", () => live.delete(socket));
    });
    await new Promise<void>((resolve) => silent.listen(0, "127.0.0.1", resolve));
    // SAFETY: a server that has finished listening addresses an `AddressInfo`.
    const port = (silent.address() as AddressInfo).port;
    try {
      const oid = await commitFile("interruptible", "a.txt", "a", "one");
      const stopped = await inLocal(
        "interruptible",
        Effect.gen(function* () {
          const pushing = yield* Effect.forkDetach(
            push({
              url: `http://127.0.0.1:${String(port)}/silent`,
              refs: [{ local: oid, remote: "refs/heads/main" }],
            }),
          );
          // Long enough that the push is certainly waiting on the socket, and
          // far short of anything the peer will ever do about it.
          yield* Effect.sleep("100 millis");
          yield* Fiber.interrupt(pushing);
          return "let go";
        }).pipe(Effect.timeout("10000 millis")),
      );
      assert.equal(stopped, "let go");
      assert.equal(live.size > 0, true, "the push did connect, so there is something to close");
      // The close travels over the wire, and how fast is the HTTP client's
      // business, not this repository's: an abort that hands the connection
      // back to a pool closes it on the pool's own idle timer. So the ceiling
      // is generous and the loop leaves the moment it can — a passing run
      // costs whatever the client takes, and only a failing one waits it out.
      for (let waited = 0; waited < 500 && live.size > 0; waited += 1) {
        await new Promise<void>((resolve) => setTimeout(resolve, 20));
      }
      assert.equal(live.size, 0, "and the socket the push held is closed");
    } finally {
      silent.closeAllConnections();
      await new Promise<void>((resolve) => silent.close(() => resolve()));
    }
  });

  it("pushes a pull request whose payload a tombstone has already removed", async () => {
    // A redacted payload is absent by design while the tree naming it
    // survives, so a strict object closure over a hub ref fails the moment
    // anything in it has been redacted. The server's fetch retries against
    // what the tombstones account for; the push side did not, so a client
    // could not push back the pull requests it had itself redacted.
    const outcome = await inLocal(
      "redacted",
      Effect.gen(function* () {
        const repository = yield* Repository;
        const root = yield* generate("root@example.com");
        const author = yield* generate("author@example.com");

        const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
        yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
        yield* Log.issue(
          yield* Certificate.grant({
            repo: genesis.repoId,
            publicKey: formatPublicKey(author.publicKey),
            capabilities: ["hub.create-pr", "hub.comment", "hub.redact"],
            id: Log.newId(),
          }),
          [root],
        );

        const { pr } = yield* PullRequest.open({
          repo: genesis.repoId,
          title: "Add a thing",
          base: "refs/heads/main",
          // SAFETY: forty lowercase hex characters, which is what `Oid` brands.
          head: "a".repeat(40) as Oid,
          key: author,
        });
        const said = yield* PullRequest.comment({
          repo: genesis.repoId,
          pr,
          body: "the deploy key is hunter2",
          key: author,
        });
        const { events } = yield* Event.entries(pr);
        const target = events.find((entry) => entry.commit === said)?.payload?.id ?? "";
        yield* PullRequest.redact({
          repo: genesis.repoId,
          pr,
          target,
          reason: "sensitive-content",
          key: author,
        });

        // The payload really goes, which is what makes the closure strict-fail.
        yield* repository.gc({ repack: true, exclude: yield* Redaction.excluded() });
        return Event.refOf(pr);
      }),
    );

    const results = await pushFrom("redacted", "tombstoned", [{ local: outcome, remote: outcome }]);

    assert.deepEqual(
      results.map((result) => [result.ok, result.reason ?? ""]),
      [[true, ""]],
      "the pull request must push with its payload already gone",
    );
    assert.notEqual(await serverRef("tombstoned", outcome), null);
  });

  it("refuses a non-fast-forward, and accepts it with force", async () => {
    const first = await commitFile("diverged-a", "f.txt", "theirs\n", "theirs");
    assert.deepEqual(
      await pushFrom("diverged-a", "diverged", [
        { local: "refs/heads/main", remote: "refs/heads/main" },
      ]),
      [{ ref: "refs/heads/main", ok: true }],
    );

    // An unrelated history: the remote's commit is not among its ancestors,
    // so accepting it would drop what is already published.
    const second = await commitFile("diverged-b", "f.txt", "ours\n", "ours");

    const { packs, result } = await capturingPacks(() =>
      pushFrom("diverged-b", "diverged", [{ local: "refs/heads/main", remote: "refs/heads/main" }]),
    );
    assert.deepEqual(result, [{ ref: "refs/heads/main", ok: false, reason: "non-fast-forward" }]);
    // Refused before the upload, which is the point of checking here.
    assert.deepEqual(packs, []);
    assert.equal(await serverRef("diverged", "refs/heads/main"), first);

    assert.deepEqual(
      await pushFrom(
        "diverged-b",
        "diverged",
        [{ local: "refs/heads/main", remote: "refs/heads/main" }],
        { force: true },
      ),
      [{ ref: "refs/heads/main", ok: true }],
    );
    assert.equal(await serverRef("diverged", "refs/heads/main"), second);
  });

  it("reports ok when the remote already has everything", async () => {
    const head = await commitFile("noop", "n.txt", "n\n", "n");
    await pushFrom("noop", "noop", [{ local: "refs/heads/main", remote: "refs/heads/main" }]);

    // Same ref, same value: no commands to send, so no request at all.
    const repeated = await capturingPacks(() =>
      pushFrom("noop", "noop", [{ local: "refs/heads/main", remote: "refs/heads/main" }]),
    );
    assert.deepEqual(repeated.result, [{ ref: "refs/heads/main", ok: true, reason: "up to date" }]);
    assert.deepEqual(repeated.packs, []);

    // A new ref over objects the remote already holds: a command, and a pack
    // with nothing in it.
    const copied = await capturingPacks(() =>
      pushFrom("noop", "noop", [{ local: "refs/heads/main", remote: "refs/heads/copy" }]),
    );
    assert.deepEqual(copied.result, [{ ref: "refs/heads/copy", ok: true }]);
    assert.deepEqual(copied.packs, [0]);
    assert.equal(await serverRef("noop", "refs/heads/copy"), head);
  });

  it("reports a delete of a ref the remote does not have", async () => {
    await commitFile("gone", "g.txt", "g\n", "g");
    await pushFrom("gone", "gone", [{ local: "refs/heads/main", remote: "refs/heads/main" }]);

    const { packs, result } = await capturingPacks(() =>
      pushFrom("gone", "gone", [
        { local: "refs/heads/absent", remote: "refs/heads/absent", delete: true },
      ]),
    );
    assert.deepEqual(result, [
      { ref: "refs/heads/absent", ok: false, reason: "remote ref does not exist" },
    ]);
    assert.deepEqual(packs, []);
  });

  it("pushes an oid, and fails on a local ref that does not resolve", async () => {
    const head = await commitFile("byoid", "o.txt", "o\n", "o");

    assert.deepEqual(
      await pushFrom("byoid", "byoid", [{ local: head, remote: "refs/heads/main" }]),
      [{ ref: "refs/heads/main", ok: true }],
    );

    await assert.rejects(
      pushFrom("byoid", "byoid", [{ local: "refs/heads/nope", remote: "refs/heads/nope" }]),
      (error) => {
        assert.ok(error instanceof Invalid, "the rejection must be the push's own Invalid");
        assert.match(error.reason, /unknown ref 'refs\/heads\/nope'/);
        return true;
      },
    );
  });
});
