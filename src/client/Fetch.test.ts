/**
 * Fetch, measured in objects.
 *
 * The claim these tests exist to hold is that a second fetch is cheaper than
 * the first — so nothing here asserts on a proxy for that. Every request the
 * client makes is intercepted and the packfile header on the way back is read
 * for its object count, which is the number the negotiation is supposed to
 * move.
 *
 * Three independent readers: our own server (`host/Node.ts`), the real `git`
 * binary fetching from it, and — the other direction — our client negotiating
 * against stock `git-http-backend`, which is the only way to find out whether
 * we speak the protocol or merely speak to ourselves. The git halves are
 * skipped when the binary is not on PATH.
 */
import assert from "node:assert/strict";
import { execFile, execFileSync, spawn } from "node:child_process";
import { existsSync } from "node:fs";
import * as fs from "node:fs/promises";
import * as http from "node:http";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { afterAll, beforeAll, describe, it } from "@effect/vitest";

import { Effect, Layer, Predicate } from "effect";

import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { ObjectStore, type Oid, RefStore } from "../git/Store.ts";
import { HUB_FETCH } from "../git/Refspec.ts";
import { serve, type Server } from "../host/Node.ts";
import { hasGit } from "../testing/Git.ts";
import { fetchRepository, type FetchResult } from "./Fetch.ts";

const gitExecPath = hasGit ? execFileSync("git", ["--exec-path"], { encoding: "utf8" }).trim() : "";
const httpBackendPath = path.join(gitExecPath, "git-http-backend");
const hasHttpBackend = hasGit && existsSync(httpBackendPath);

const execFileAsync = promisify(execFile);

/**
 * Async on purpose: the server under test runs on this process's event loop,
 * so a synchronous `git` invocation would deadlock — the client waiting on a
 * response the blocked loop can never produce.
 */
const git = async (
  cwd: string,
  ...args: string[]
): Promise<{ readonly stdout: string; readonly stderr: string }> =>
  execFileAsync(
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

const author = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const encoder = new TextEncoder();
const decoder = new TextDecoder();

let root: string;
let server: Server;

const layerFor = (dir: string) =>
  GitRepository.layer.pipe(Layer.provide(GitRepository.hooksNoop), Layer.provide(stores(dir)));

const inRepo = <A, E>(dir: string, effect: Effect.Effect<A, E, Repository>): Promise<A> =>
  Effect.runPromise(effect.pipe(Effect.provide(layerFor(dir))));

/** One commit on `main`, appended to whatever is already there. */
const commitFile = (dir: string, file: string, content: string, message: string): Promise<Oid> =>
  inRepo(
    dir,
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

const fetchInto = (dir: string, url: string): Promise<FetchResult> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const target = { objects: yield* ObjectStore, refs: yield* RefStore };
      return yield* fetchRepository({ url, stores: target });
    }).pipe(Effect.provide(stores(dir))),
  );

const refsOf = (dir: string): Promise<ReadonlyArray<readonly [string, Oid]>> =>
  inRepo(
    dir,
    Effect.gen(function* () {
      return yield* (yield* Repository).refs;
    }),
  );

/**
 * `HEAD` is written by `setHead` and by nothing else, and a fetch has no
 * business guessing at one — but the `git` binary refuses a directory without
 * it, so a test that hands its output to git supplies the file itself.
 */
const setHead = (dir: string, ref: string): Promise<void> =>
  Effect.runPromise(
    Effect.gen(function* () {
      yield* (yield* RefStore).setHead(ref);
    }).pipe(Effect.provide(stores(dir))),
  );

/** The object count in a packfile, or `null` when the bytes carry no pack. */
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

interface Traffic {
  /** One entry per `POST …/git-upload-pack`: the oids its `have` lines named. */
  readonly rounds: ReadonlyArray<ReadonlyArray<string>>;
  /** Object counts of the packs that came back, negotiation rounds excluded. */
  readonly packs: ReadonlyArray<number>;
}

/**
 * Both halves of every upload-pack exchange `run` performs.
 *
 * The response is buffered rather than teed: a pack in these tests is a few
 * hundred bytes, and handing the client back a fresh `Response` keeps the
 * measurement out of the code being measured.
 */
const capturing = async <A>(
  run: () => Promise<A>,
): Promise<{ readonly result: A; readonly traffic: Traffic }> => {
  const original = globalThis.fetch;
  const rounds: Array<ReadonlyArray<string>> = [];
  const packs: number[] = [];

  const patched: typeof globalThis.fetch = async (input, init) => {
    const target = Predicate.isString(input)
      ? input
      : input instanceof URL
        ? input.href
        : input.url;
    if (!target.endsWith("/git-upload-pack")) return original(input, init);

    const body = init?.body;
    if (body instanceof Uint8Array) {
      rounds.push(
        (decoder.decode(body).match(/have [0-9a-f]{40}\n/g) ?? []).map((line) => line.slice(5, 45)),
      );
    }

    const response = await original(input, init);
    const bytes = new Uint8Array(await response.arrayBuffer());
    const count = packObjectCount(bytes);
    if (count !== null) packs.push(count);
    return new Response(bytes, { status: response.status, headers: response.headers });
  };

  globalThis.fetch = patched;
  try {
    return { result: await run(), traffic: { rounds, packs } };
  } finally {
    globalThis.fetch = original;
  }
};

beforeAll(async () => {
  root = await fs.mkdtemp(path.join(os.tmpdir(), "client-fetch-"));
  server = await serve({ root, allowAnonymousWrites: true });
});

afterAll(async () => {
  await server.close();
  await fs.rm(root, { recursive: true, force: true });
});

describe("Fetch", () => {
  it("reports a stripped Git-Protocol header rather than fetching nothing", async () => {
    // The hidden namespaces are reachable only through a v2 `ls-refs`, and the
    // version travels in a header. A proxy that drops unknown headers leaves
    // this server reading a v2 body as a v0 want-list and answering 400 — which
    // means "your request did not arrive", not "there is nothing here". Read as
    // the latter, a mirror reported a trust and hub replication it had not
    // performed, revocations included, which is the one failure this whole
    // path exists to make visible.
    const source = path.join(root, "stripped-source");
    await commitFile(source, "a.txt", "one\n", "one");

    const original = globalThis.fetch;
    const stripped: typeof globalThis.fetch = async (input, init) => {
      const headers = new Headers(init?.headers);
      headers.delete("git-protocol");
      return original(input, { ...init, headers });
    };

    globalThis.fetch = stripped;
    const outcome = await Effect.runPromise(
      Effect.gen(function* () {
        const target = { objects: yield* ObjectStore, refs: yield* RefStore };
        return yield* fetchRepository({
          url: `${server.url}/stripped-source`,
          stores: target,
          refspecs: HUB_FETCH,
        });
      }).pipe(
        Effect.provide(stores(path.join(root, "stripped-target"))),
        Effect.map(() => null),
        Effect.catchCause((cause: unknown) => Effect.succeed(String(cause))),
      ),
    ).finally(() => {
      globalThis.fetch = original;
    });

    assert.notEqual(outcome, null, "a replication that fetched nothing must not report success");
  });

  it("writes one update per local ref, whatever the refspecs overlap on", async () => {
    // Two refspecs can name one local ref from different remote ones. Both
    // updates then go into a single `apply` batch judged against the value the
    // ref held before either — so the store takes both, the second silently
    // wins, and nothing is reported as rejected. Whichever the caller listed
    // first is the one that lands.
    const source = path.join(root, "overlap-source");
    const head = await commitFile(source, "a.txt", "one\n", "one");
    await inRepo(
      source,
      Effect.flatMap(Repository, (repository) =>
        repository.setRef({ name: "refs/heads/other", to: head }),
      ),
    );

    const target = path.join(root, "overlap-target");
    const outcome = await Effect.runPromise(
      Effect.gen(function* () {
        const stores_ = { objects: yield* ObjectStore, refs: yield* RefStore };
        const fetched = yield* fetchRepository({
          url: `${server.url}/overlap-source`,
          stores: stores_,
          refspecs: [
            { force: false, source: "refs/heads/main", destination: "refs/heads/landed" },
            { force: false, source: "refs/heads/other", destination: "refs/heads/landed" },
          ],
        });
        return fetched.refs.filter((ref) => ref.name === "refs/heads/landed").length;
      }).pipe(Effect.provide(stores(target))),
    );

    assert.equal(outcome, 1, "one destination, one update");
  });

  it("clones an empty target whole, and sends no haves doing it", async () => {
    await commitFile(path.join(root, "clone-source"), "a.txt", "one\n", "one");
    await commitFile(path.join(root, "clone-source"), "b.txt", "two\n", "two");

    const { result, traffic } = await capturing(() =>
      fetchInto(path.join(root, "clone-target"), `${server.url}/clone-source`),
    );

    assert.equal(result.defaultBranch, "main");
    // One request, carrying `done` and nothing else: an empty repository has
    // nothing to offer, and a round that said so would be a round wasted.
    assert.deepEqual(traffic.rounds, [[]]);
    // Two commits, two trees, two blobs.
    assert.deepEqual(traffic.packs, [6]);
    assert.deepEqual(await refsOf(path.join(root, "clone-target")), [
      ["refs/heads/main", result.refs[0]?.value],
    ]);
  });

  it("transfers strictly fewer objects than a full clone on the second fetch", async () => {
    const source = path.join(root, "incremental-source");
    await commitFile(source, "a.txt", "one\n", "one");
    await commitFile(source, "b.txt", "two\n", "two");
    await commitFile(source, "c.txt", "three\n", "three");

    const target = path.join(root, "incremental-target");
    const first = await capturing(() => fetchInto(target, `${server.url}/incremental-source`));
    assert.deepEqual(first.traffic.rounds, [[]]);
    assert.deepEqual(first.traffic.packs, [9]);

    const head = await commitFile(source, "d.txt", "four\n", "four");

    const second = await capturing(() => fetchInto(target, `${server.url}/incremental-source`));
    assert.equal(second.result.refs[0]?.value, head);

    // The load-bearing numbers. A clone of the same four commits into an empty
    // repository is the control, and it is measured against the same server in
    // the same state — so the comparison is of two packs, not of a pack and a
    // memory of one.
    const control = await capturing(() =>
      fetchInto(path.join(root, "incremental-control"), `${server.url}/incremental-source`),
    );
    assert.deepEqual(control.traffic.packs, [12]);
    assert.deepEqual(second.traffic.packs, [3]);
    assert.ok(
      (second.traffic.packs[0] ?? Infinity) < (control.traffic.packs[0] ?? 0),
      "the incremental fetch must move fewer objects than the clone it replaces",
    );

    // Three haves offered — the three commits already held — in one round,
    // and then the round that carries `done` repeats them.
    assert.deepEqual(
      second.traffic.rounds.map((round) => round.length),
      [3, 3],
    );

    assert.deepEqual(await refsOf(target), await refsOf(source));
  });

  it("offers haves in rounds of 32, newest first", async () => {
    const source = path.join(root, "rounds-source");
    for (let index = 0; index < 40; index++) {
      await commitFile(source, "f.txt", `line ${index}\n`, `commit ${index}`);
    }

    const target = path.join(root, "rounds-target");
    const tip = (await fetchInto(target, `${server.url}/rounds-source`)).refs[0]?.value;
    const head = await commitFile(source, "f.txt", "last\n", "last");

    const { result, traffic } = await capturing(() =>
      fetchInto(target, `${server.url}/rounds-source`),
    );
    assert.equal(result.refs[0]?.value, head);

    // 32, then `done` repeating those 32. The server holds every commit
    // offered, so it acknowledges on the first round and the client stops
    // offering — the remaining 8 haves are never sent, which is the point of
    // negotiating in rounds rather than shipping the whole history at once.
    assert.deepEqual(
      traffic.rounds.map((round) => round.length),
      [32, 32],
    );
    // The tip goes first. Ordering is the whole reason a round is 32 lines and
    // not the entire history: the base is expected in the first handful.
    assert.equal(traffic.rounds[0]?.[0], tip);
    assert.deepEqual(traffic.packs, [3]);
  });

  it("fetches nothing when the target is already up to date", async () => {
    const source = path.join(root, "uptodate-source");
    await commitFile(source, "a.txt", "one\n", "one");

    const target = path.join(root, "uptodate-target");
    await fetchInto(target, `${server.url}/uptodate-source`);

    const { traffic } = await capturing(() => fetchInto(target, `${server.url}/uptodate-source`));
    // The want is the have: everything the client asked for is excluded by
    // what it offered, so the pack is empty rather than absent.
    assert.deepEqual(traffic.packs, [0]);
  });
});

describe.skipIf(!hasGit)("Fetch, checked by the git binary", () => {
  it("leaves a repository git fsck accepts, matching the source object for object", async () => {
    const source = path.join(root, "fsck-source");
    await commitFile(source, "a.txt", "one\n", "one");
    await commitFile(source, "nested/b.txt", "two\n", "two");
    await setHead(source, "refs/heads/main");

    const target = path.join(root, "fsck-target");
    await fetchInto(target, `${server.url}/fsck-source`);
    await commitFile(source, "nested/c.txt", "three\n", "three");
    await fetchInto(target, `${server.url}/fsck-source`);
    await setHead(target, "refs/heads/main");

    // The incrementally-fetched repository, read by a program that had no part
    // in writing it: every object present, every object well-formed.
    await git(root, "--git-dir", target, "fsck", "--strict", "--full");

    const listing = async (dir: string) =>
      (await git(root, "--git-dir", dir, "rev-list", "--objects", "--all")).stdout
        .split("\n")
        .filter((line) => line !== "")
        .sort();
    assert.deepEqual(await listing(target), await listing(source));
    assert.deepEqual(await refsOf(target), await refsOf(source));
  });

  it("serves an incremental fetch to the git binary itself", async () => {
    const source = path.join(root, "git-fetch-source");
    await commitFile(source, "a.txt", "one\n", "one");
    await commitFile(source, "b.txt", "two\n", "two");

    const work = path.join(root, "git-fetch-work");
    await git(root, "clone", "--quiet", `${server.url}/git-fetch-source`, work);

    const head = await commitFile(source, "c.txt", "three\n", "three");
    const packs = path.join(work, ".git", "objects", "pack");
    const before = new Set(await fs.readdir(packs));
    // `unpackLimit=1` keeps the pack instead of exploding it into loose
    // objects, which is the only way to read back what actually crossed.
    await git(work, "-c", "fetch.unpackLimit=1", "fetch", "origin");
    const arrived = (await fs.readdir(packs)).filter(
      (name) => name.endsWith(".pack") && !before.has(name),
    );

    // git's own negotiation against our upload-pack: the commit, its tree and
    // one blob, not the whole history it already has.
    assert.equal(arrived.length, 1);
    assert.equal(packObjectCount(await fs.readFile(path.join(packs, arrived[0] ?? ""))), 3);
    assert.equal((await git(work, "rev-parse", "origin/main")).stdout.trim(), head);
    await git(work, "fsck", "--strict");
  });
});

/**
 * A CGI host for `git-http-backend`, which is how stock git serves smart HTTP.
 *
 * Worth the fifty lines: it is the only reader in this file that can say our
 * `have`/`ACK` conversation is the protocol rather than a private dialect our
 * own server happens to accept.
 */
const httpBackend = async (projectRoot: string): Promise<Server> => {
  const server = http.createServer((incoming, outgoing) => {
    const url = new URL(incoming.url ?? "/", "http://backend");
    const child = spawn(httpBackendPath, [], {
      // upload-pack narrates on stderr whether or not anyone asked; leaving
      // that pipe unread is a full buffer away from a hang.
      stdio: ["pipe", "pipe", "ignore"],
      env: {
        ...process.env,
        GIT_PROJECT_ROOT: projectRoot,
        GIT_HTTP_EXPORT_ALL: "1",
        REQUEST_METHOD: incoming.method ?? "GET",
        PATH_INFO: url.pathname,
        QUERY_STRING: url.search.slice(1),
        CONTENT_TYPE: incoming.headers["content-type"] ?? "",
        CONTENT_LENGTH: incoming.headers["content-length"] ?? "",
        REMOTE_ADDR: "127.0.0.1",
      },
    });
    incoming.pipe(child.stdin);

    // CGI puts the headers, a blank line and the body on one stream, so the
    // response cannot start until the blank line has been seen.
    let pending = Buffer.alloc(0);
    let open = false;
    child.stdout.on("data", (chunk: Buffer) => {
      if (open) {
        outgoing.write(chunk);
        return;
      }
      pending = Buffer.concat([pending, chunk]);
      const blank = pending.indexOf("\r\n\r\n");
      if (blank === -1) return;

      const fields: Record<string, string> = {};
      let status = 200;
      for (const line of pending.subarray(0, blank).toString("utf8").split("\r\n")) {
        const colon = line.indexOf(":");
        if (colon <= 0) continue;
        const name = line.slice(0, colon).toLowerCase();
        const value = line.slice(colon + 1).trim();
        if (name === "status") status = Number.parseInt(value, 10);
        else fields[name] = value;
      }
      outgoing.writeHead(status, fields);
      open = true;
      outgoing.write(pending.subarray(blank + 4));
    });
    child.stdout.on("end", () => outgoing.end());
    child.on("error", () => outgoing.destroy());
  });

  await new Promise<void>((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });
  // A TCP listener's address is always the object form; the string form is
  // for pipes and unix sockets, and `null` for a server not yet listening.
  const address = server.address();
  if (address === null || Predicate.isString(address)) {
    throw new Error("the backend host did not bind a TCP port");
  }
  return {
    url: `http://127.0.0.1:${address.port}`,
    close: () =>
      new Promise((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      }),
  };
};

describe.skipIf(!hasHttpBackend)("Fetch, negotiating with git-http-backend", () => {
  let backendRoot: string;
  let backend: Server;

  beforeAll(async () => {
    backendRoot = await fs.mkdtemp(path.join(os.tmpdir(), "client-fetch-backend-"));
    backend = await httpBackend(backendRoot);
  });

  afterAll(async () => {
    await backend.close();
    await fs.rm(backendRoot, { recursive: true, force: true });
  });

  it("fetches incrementally from stock upload-pack", async () => {
    const bare = path.join(backendRoot, "origin.git");
    await git(backendRoot, "init", "--quiet", "--bare", bare);

    const work = path.join(backendRoot, "work");
    await git(backendRoot, "clone", "--quiet", bare, work);
    await fs.writeFile(path.join(work, "a.txt"), "one\n");
    await git(work, "add", "a.txt");
    await git(work, "commit", "--quiet", "-m", "one");
    await fs.writeFile(path.join(work, "b.txt"), "two\n");
    await git(work, "add", "b.txt");
    await git(work, "commit", "--quiet", "-m", "two");
    await git(work, "push", "--quiet", "origin", "main");

    const target = path.join(root, "backend-target");
    const first = await capturing(() => fetchInto(target, `${backend.url}/origin.git`));
    assert.equal(first.result.defaultBranch, "main");
    assert.deepEqual(first.traffic.rounds, [[]]);
    assert.deepEqual(first.traffic.packs, [6]);

    await fs.writeFile(path.join(work, "c.txt"), "three\n");
    await git(work, "add", "c.txt");
    await git(work, "commit", "--quiet", "-m", "three");
    await git(work, "push", "--quiet", "origin", "main");
    const head = (await git(work, "rev-parse", "HEAD")).stdout.trim();

    const second = await capturing(() => fetchInto(target, `${backend.url}/origin.git`));
    assert.equal(second.result.refs[0]?.value, head);
    // Stock upload-pack acknowledges as soon as it recognises a have, so the
    // negotiation ends on the first round and `done` follows immediately —
    // the shape our own server cannot produce, and the reason this test is
    // here.
    assert.ok(
      second.traffic.rounds.length <= 2,
      `expected the negotiation to settle in one round, saw ${second.traffic.rounds.length}`,
    );
    assert.deepEqual(second.traffic.packs, [3]);

    await setHead(target, "refs/heads/main");
    await git(root, "--git-dir", target, "fsck", "--strict", "--full");
  });
});
