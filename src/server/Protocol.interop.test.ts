/**
 * Interop: the real `git` binary clones from and pushes to this server.
 *
 * The server under test is the node host itself — `host/Node.ts`, the same
 * `Protocol.handle` and `Api.layer` the Durable Object serves, behind
 * `node:http`. Every test here is an end-to-end conversation with stock git
 * over smart HTTP.
 *
 * Skipped when `git` is not on PATH.
 */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import * as fs from "node:fs/promises";
import * as http from "node:http";
import * as os from "node:os";
import * as path from "node:path";
import { afterAll, beforeAll, describe, it } from "@effect/vitest";
import { promisify } from "node:util";

import { Effect, Layer } from "effect";

import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { serve, type Server } from "../host/Node.ts";
import { hasGit } from "../testing/Git.ts";

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

describe.skipIf(!hasGit)("Protocol interop with git", () => {
  let root: string;
  let base: string;
  let server: Server;

  const layerFor = (repo: string) =>
    GitRepository.layer.pipe(
      Layer.provide(GitRepository.hooksNoop),
      Layer.provide(stores(path.join(root, repo))),
    );

  const inRepo = <A, E>(repo: string, effect: Effect.Effect<A, E, Repository>) =>
    Effect.runPromise(effect.pipe(Effect.provide(layerFor(repo))));

  beforeAll(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "git-protocol-interop-"));
    server = await serve({ root, allowAnonymousWrites: true });
    base = server.url;
  });

  afterAll(async () => {
    await server.close();
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

  it.effect("clones a seeded repository", () =>
    Effect.promise(async () => {
      await seed("cloneme");
      const work = path.join(root, "work-clone");
      await git(root, "clone", "--quiet", `${base}/cloneme`, work);

      assert.equal(
        await fs.readFile(path.join(work, "hello.txt"), "utf8"),
        "hello from the server\n",
      );
      assert.match(await git(work, "log", "--format=%s"), /^seed$/m);
      await git(work, "fsck", "--strict");
    }),
  );

  /** A repository with `count` commits on main, oldest first. */
  const seedHistory = async (repo: string, count: number): Promise<ReadonlyArray<string>> => {
    const oids: string[] = [];
    for (let index = 0; index < count; index++) {
      oids.push(
        await inRepo(
          repo,
          Effect.gen(function* () {
            const repository = yield* Repository;
            const blob = yield* repository.writeBlob(new TextEncoder().encode(`v${index}\n`));
            const tree = yield* repository.writeTree([
              { mode: "100644", name: "file.txt", oid: blob },
            ]);
            return yield* repository.commit({
              branch: "main",
              tree,
              message: `commit ${index}`,
              // Spaced a day apart so `--shallow-since` has something to cut.
              author: { ...author, at: new Date(1_700_000_000_000 + index * 86_400_000) },
            });
          }),
        ),
      );
    }
    return oids;
  };

  it.effect("serves protocol v2: ls-refs and fetch", () =>
    Effect.promise(async () => {
      await seedHistory("v2", 3);
      await inRepo(
        "v2",
        Effect.gen(function* () {
          return yield* (yield* Repository).tag({
            name: "v1",
            target: "refs/heads/main",
            message: "tagged\n",
            tagger: author,
          });
        }),
      );

      const work = path.join(root, "work-v2");
      // v2 is what modern git prefers; forcing it here means the assertion is
      // about our v2 path rather than about git's default of the day.
      await git(root, "-c", "protocol.version=2", "clone", "--quiet", `${base}/v2`, work);

      assert.equal((await git(work, "rev-list", "--count", "HEAD")).trim(), "3");
      await git(work, "fsck", "--strict");

      // ls-refs with a prefix, which is the saving v2 exists for.
      const remote = await git(
        root,
        "-c",
        "protocol.version=2",
        "ls-remote",
        "--heads",
        `${base}/v2`,
      );
      assert.match(remote, /refs\/heads\/main$/m);
      assert.doesNotMatch(remote, /refs\/tags/);

      // Tag peeling: `ls-remote --tags` asks for it, and an annotated tag has
      // a target to report.
      const tags = await git(root, "-c", "protocol.version=2", "ls-remote", "--tags", `${base}/v2`);
      assert.match(tags, /refs\/tags\/v1$/m);
      assert.match(tags, /refs\/tags\/v1\^\{\}$/m);

      // An incremental fetch over v2 exercises the negotiation, not just the
      // first-clone path.
      await seedHistory("v2", 1);
      await git(work, "-c", "protocol.version=2", "fetch", "--quiet", "origin", "main");
      assert.equal((await git(work, "rev-list", "--count", "origin/main")).trim(), "4");
    }),
  );

  it.effect("serves a shallow clone over protocol v2", () =>
    Effect.promise(async () => {
      await seedHistory("v2shallow", 4);

      const work = path.join(root, "work-v2-shallow");
      await git(
        root,
        "-c",
        "protocol.version=2",
        "clone",
        "--quiet",
        "--depth=1",
        `${base}/v2shallow`,
        work,
      );

      assert.equal((await git(work, "rev-list", "--count", "HEAD")).trim(), "1");
      assert.equal((await git(work, "rev-parse", "--is-shallow-repository")).trim(), "true");
      await git(work, "fsck", "--strict");
    }),
  );

  it.effect("writes annotated tags the git binary reads back", () =>
    Effect.promise(async () => {
      const commit = await seed("tagged");

      const tag = await inRepo(
        "tagged",
        Effect.gen(function* () {
          return yield* (yield* Repository).tag({
            name: "v1.0.0",
            target: "refs/heads/main",
            message: "the first release\n",
            tagger: author,
          });
        }),
      );

      const work = path.join(root, "work-tag");
      await git(root, "clone", "--quiet", `${base}/tagged`, work);
      await git(work, "fetch", "--quiet", "--tags", "origin");

      // git's own reader on our bytes: the type, the target and the message.
      assert.equal((await git(work, "cat-file", "-t", tag.oid)).trim(), "tag");
      assert.equal((await git(work, "rev-parse", "v1.0.0^{commit}")).trim(), commit);
      assert.match(await git(work, "cat-file", "tag", tag.oid), /^the first release$/m);
      assert.match(await git(work, "tag", "-l", "-n1"), /v1\.0\.0\s+the first release/);
      await git(work, "fsck", "--strict");
    }),
  );

  it.effect("serves a shallow clone, and deepens it on request", () =>
    Effect.promise(async () => {
      await seedHistory("shallowme", 5);

      const work = path.join(root, "work-shallow");
      await git(root, "clone", "--quiet", "--depth=1", `${base}/shallowme`, work);

      // One commit, and git agrees the clone is shallow.
      assert.equal((await git(work, "rev-list", "--count", "HEAD")).trim(), "1");
      assert.match(await git(work, "log", "--format=%s"), /^commit 4$/m);
      assert.equal((await git(work, "rev-parse", "--is-shallow-repository")).trim(), "true");

      // The boundary the server reported is the one git recorded.
      const shallowFile = (await fs.readFile(path.join(work, ".git", "shallow"), "utf8")).trim();
      assert.equal(shallowFile, (await git(work, "rev-parse", "HEAD")).trim());

      // Deepening asks for more, and the boundary moves rather than resetting.
      await git(work, "fetch", "--quiet", "--depth=3", "origin", "main");
      assert.equal((await git(work, "rev-list", "--count", "origin/main")).trim(), "3");

      // And `--unshallow` completes it: no boundary left, whole history present.
      await git(work, "fetch", "--quiet", "--unshallow", "origin", "main");
      assert.equal((await git(work, "rev-list", "--count", "origin/main")).trim(), "5");
      assert.equal((await git(work, "rev-parse", "--is-shallow-repository")).trim(), "false");

      // The objects that arrived across three rounds are consistent.
      await git(work, "fsck", "--strict");
    }),
  );

  it.effect("honours --shallow-since", () =>
    Effect.promise(async () => {
      await seedHistory("sincey", 5);

      const work = path.join(root, "work-since");
      // Commits are a day apart starting at 1_700_000_000; cut before the last two.
      const cutoff = new Date(1_700_000_000_000 + 3 * 86_400_000).toISOString();
      await git(root, "clone", "--quiet", `--shallow-since=${cutoff}`, `${base}/sincey`, work);

      assert.equal((await git(work, "rev-list", "--count", "HEAD")).trim(), "2");
      assert.equal((await git(work, "rev-parse", "--is-shallow-repository")).trim(), "true");
      await git(work, "fsck", "--strict");
    }),
  );

  it.effect("serves the same repository with or without the .git suffix", () =>
    Effect.promise(async () => {
      await seed("suffixed");

      // git appends `.git` to a URL that has none, so both spellings reach
      // users. They must be one repository, not two empty ones.
      const bare = path.join(root, "work-bare-url");
      const suffixed = path.join(root, "work-suffixed-url");
      await git(root, "clone", "--quiet", `${base}/suffixed`, bare);
      await git(root, "clone", "--quiet", `${base}/suffixed.git`, suffixed);

      assert.equal(
        (await git(bare, "rev-parse", "HEAD")).trim(),
        (await git(suffixed, "rev-parse", "HEAD")).trim(),
      );

      // And a push through the suffixed spelling is visible through the other.
      await fs.writeFile(path.join(suffixed, "through-suffix.txt"), "pushed\n");
      await git(suffixed, "add", ".");
      await git(suffixed, "commit", "--quiet", "-m", "through the suffixed URL");
      await git(suffixed, "push", "--quiet", "origin", "main");

      await git(bare, "fetch", "--quiet", "origin", "main");
      assert.equal(
        (await git(bare, "rev-parse", "origin/main")).trim(),
        (await git(suffixed, "rev-parse", "HEAD")).trim(),
      );
    }),
  );

  it.effect("accepts a push and serves it back to a second clone", () =>
    Effect.promise(async () => {
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
    }),
  );

  it.effect("clones an empty repository, then receives its first push", () =>
    Effect.promise(async () => {
      const work = path.join(root, "work-empty");
      await git(root, "clone", "--quiet", `${base}/empty`, work);

      await fs.writeFile(path.join(work, "first.txt"), "first\n");
      await git(work, "add", ".");
      await git(work, "commit", "--quiet", "-m", "first");
      await git(work, "push", "--quiet", "-u", "origin", "main");

      const verify = path.join(root, "work-empty-verify");
      await git(root, "clone", "--quiet", `${base}/empty`, verify);
      assert.equal(await fs.readFile(path.join(verify, "first.txt"), "utf8"), "first\n");
    }),
  );

  it.effect("creates and deletes a branch over push", () =>
    Effect.promise(async () => {
      await seed("branchy");
      const work = path.join(root, "work-branch");
      await git(root, "clone", "--quiet", `${base}/branchy`, work);

      await git(work, "push", "--quiet", "origin", "main:feature");
      assert.match(await git(work, "ls-remote", "origin"), /refs\/heads\/feature/);

      await git(work, "push", "--quiet", "origin", ":feature");
      assert.doesNotMatch(await git(work, "ls-remote", "origin"), /refs\/heads\/feature/);
    }),
  );

  it.effect("rejects a push whose old-oid no longer matches the ref", () =>
    Effect.promise(async () => {
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
    }),
  );

  it.effect("serializes concurrent commits to one repository", () =>
    Effect.promise(async () => {
      // Five racing JSON commits: the host's per-repo gate is the DO input
      // gate's stand-in, so every one of them lands, in some order.
      const responses = await Promise.all(
        Array.from({ length: 5 }, (_, index) =>
          fetch(`${base}/gated/commit`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ message: `racer ${index}`, author }),
          }),
        ),
      );
      assert.deepEqual(
        responses.map((response) => response.status),
        [200, 200, 200, 200, 200],
      );

      // SAFETY: `/refs` is the JSON API under test, and its success schema is
      // `{ refs: [{ name, oid }, …] }` — a drift would fail the count below.
      const refs = (await (await fetch(`${base}/gated/refs`)).json()) as {
        refs: Array<{ oid: string }>;
      };
      const head = refs.refs[0]!.oid;
      // SAFETY: `/log/:oid` answers `{ commits: [{ message, oid }, …] }` per the
      // API's schema; only the count is read here.
      const log = (await (await fetch(`${base}/gated/log/${head}`)).json()) as {
        commits: Array<{ message: string; oid: string }>;
      };
      assert.equal(log.commits.length, 5);
    }),
  );

  it.effect("rejects repository names that could escape the root", () =>
    Effect.promise(async () => {
      // `fetch` (and the host's own `new URL`) normalize plain dot segments
      // away, so the raw request is the only way to present an evasive name.
      const status = await new Promise<number>((resolve, reject) => {
        const request = http.request(`${base}/..%2fescape/refs`, (response) => {
          response.resume();
          resolve(response.statusCode ?? 0);
        });
        request.on("error", reject);
        request.end();
      });
      assert.equal(status, 400);
    }),
  );

  it.effect("fetches incrementally after the server moves ahead", () =>
    Effect.promise(async () => {
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
    }),
  );

  it.effect("negotiates multi_ack_detailed with stock git over protocol v0", () =>
    Effect.promise(async () => {
      // More history than one 32-have round: with fewer haves fetch-pack sends
      // them all plus `done` in its first request, and `ready` — the round
      // terminator this test exists to observe — never has a round to end.
      await seedHistory("multiack", 40);
      const work = path.join(root, "work-multiack");
      await git(root, "clone", "--quiet", `${base}/multiack`, work);

      const publisher = path.join(root, "work-multiack-publisher");
      await git(root, "clone", "--quiet", `${base}/multiack`, publisher);
      await fs.writeFile(path.join(publisher, "ahead.txt"), "ahead\n");
      await git(publisher, "add", ".");
      await git(publisher, "commit", "--quiet", "-m", "ahead");
      await git(publisher, "push", "--quiet", "origin", "main");

      // The packet trace is the observable: the client must request the
      // capability, hear its haves tagged common, and be told ready — the
      // whole point of advertising multi_ack_detailed.
      const { stderr } = await execFileAsync(
        "git",
        ["-c", "protocol.version=0", "fetch", "--quiet", "origin", "main"],
        {
          cwd: work,
          encoding: "utf8",
          env: { ...process.env, GIT_TERMINAL_PROMPT: "0", GIT_TRACE_PACKET: "2" },
        },
      );
      assert.match(stderr, /want [0-9a-f]{40}.* multi_ack_detailed/);
      assert.match(stderr, /ACK [0-9a-f]{40} common/);
      assert.match(stderr, /ACK [0-9a-f]{40} ready/);

      assert.equal((await git(work, "rev-list", "--count", "origin/main")).trim(), "41");
      await git(work, "fsck", "--strict");
    }),
  );
});
