import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { NodeServices } from "@effect/platform-node";
import { Command } from "effect/unstable/cli";
import { fetchCommand } from "../cli/transport.ts";
import { stores } from "../git/Node.ts";
import * as Repository from "../git/Repository.ts";
import { isOid, ObjectStore, RefStore } from "../git/Store.ts";
import { hasGit, gitIn } from "../testing/Git.ts";
import { fetchRepository } from "./Fetch.ts";
import { push } from "./Push.ts";
import { resolveRev } from "../cli/shared.ts";
import { collected } from "../server/AfterPush.node.ts";
import type { PushResult } from "./Push.ts";

/** Real Git wire responses, without requiring a listening HTTP socket. */
describe.skipIf(!hasGit)("ref update regressions", () => {
  let root: string;
  let remote: string;
  let posts: number;
  const original = globalThis.fetch;
  const git = (...args: string[]) => gitIn(root)(...args).trim();
  const run = <A, E>(effect: Effect.Effect<A, E, Repository.Repository | ObjectStore | RefStore>) =>
    Effect.runPromise(
      effect.pipe(
        Effect.provide(
          Repository.layer.pipe(
            Layer.provide(Repository.hooksNoop),
            Layer.provideMerge(stores(path.join(root, ".git"))),
          ),
        ),
      ),
    );
  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "ref-update-review-"));
    git("init", "-q", "-b", "main");
    await fs.writeFile(path.join(root, "a"), "base\n");
    git("add", "a");
    git("commit", "-qm", "base");
    remote = path.join(root, "remote.git");
    git("clone", "-q", "--bare", ".", remote);
    posts = 0;
    globalThis.fetch = async (input, options) => {
      const target = new URL(input instanceof Request ? input.url : String(input));
      const advertised = (options?.method ?? "GET") === "GET";
      const service = advertised ? target.searchParams.get("service") : target.pathname.slice(1);
      assert.ok(service === "git-upload-pack" || service === "git-receive-pack");
      const body = options?.body;
      const inputBytes =
        body == null ? undefined : new Uint8Array(await new Response(body).arrayBuffer());
      if (!advertised) {
        posts++;
        assert.ok(inputBytes !== undefined);
      }
      const bytes = execFileSync(
        "git",
        [service.slice(4), "--stateless-rpc", ...(advertised ? ["--advertise-refs"] : []), remote],
        { input: inputBytes, stdio: ["pipe", "pipe", "pipe"] },
      );
      const line = `# service=${service}\n`;
      const prefix = Buffer.from(
        (Buffer.byteLength(line) + 4).toString(16).padStart(4, "0") + line + "0000",
      );
      return new Response(advertised ? Buffer.concat([prefix, bytes]) : bytes, {
        headers: {
          "content-type": `application/x-${service}-${advertised ? "advertisement" : "result"}`,
        },
      });
    };
  });
  afterEach(async () => {
    globalThis.fetch = original;
    await fs.rm(root, { recursive: true, force: true });
  });

  it("declares shallow boundaries to a stock receiver that accepts new roots", async () => {
    await fs.writeFile(path.join(root, "a"), "shallow tip\n");
    git("commit", "-qam", "shallow tip");
    const tip = git("rev-parse", "HEAD");
    assert.ok(isOid(tip));
    remote = path.join(root, "shallow-accept.git");
    git("init", "--bare", "-q", "-b", "main", remote);
    git("--git-dir", remote, "config", "receive.shallowUpdate", "true");
    const results = await run(
      Effect.gen(function* () {
        yield* (yield* RefStore).updateShallow({ add: [tip], remove: [] });
        return yield* push({
          url: "http://git.local",
          refs: [{ local: "refs/heads/main", remote: "refs/heads/main" }],
        });
      }),
    );
    assert.equal(results[0]?.ok, true);
    assert.equal(git("--git-dir", remote, "rev-parse", "HEAD"), tip);
    assert.equal(git("--git-dir", remote, "rev-list", "--count", "HEAD"), "1");
    assert.equal(git("--git-dir", remote, "rev-parse", "--is-shallow-repository"), "true");
  });

  it("forwards successive changes to one ref as one collected update", async () => {
    const base = git("rev-parse", "main");
    await fs.writeFile(path.join(root, "a"), "first\n");
    git("commit", "-qam", "first");
    const first = git("rev-parse", "main");
    await fs.writeFile(path.join(root, "a"), "second\n");
    git("commit", "-qam", "second");
    const second = git("rev-parse", "main");
    assert.ok(isOid(base) && isOid(first) && isOid(second));
    let verdicts: ReadonlyArray<PushResult> = [];
    await run(
      Effect.gen(function* () {
        const repository = yield* Repository.Repository;
        const receiver = Repository.Hooks.of({
          preReceive: () => Effect.void,
          update: () => Effect.void,
          postReceive: (results) =>
            push({
              url: "http://fixture",
              refs: results.flatMap((result) =>
                result.to === null ? [] : [{ local: result.to, remote: result.ref }],
              ),
            }).pipe(
              Effect.provideService(Repository.Repository, repository),
              Effect.tap((results) =>
                Effect.sync(() => {
                  verdicts = results;
                }),
              ),
              Effect.asVoid,
              Effect.orDie,
            ),
        });
        const batch = collected(Layer.succeed(Repository.Hooks, receiver));
        yield* Effect.gen(function* () {
          const hook = yield* Repository.Hooks;
          yield* hook.postReceive([{ ref: "refs/heads/main", from: base, to: first, ok: true }]);
          yield* hook.postReceive([{ ref: "refs/heads/main", from: first, to: second, ok: true }]);
        }).pipe(Effect.provide(batch.layer));
        yield* batch.flush;
      }),
    );
    assert.deepEqual(verdicts, [{ ref: "refs/heads/main", ok: true }]);
    assert.equal(posts, 1);
    assert.equal(git("--git-dir", remote, "rev-parse", "main"), second);
  });

  it("honors a deletion lease against stock receive-pack", async () => {
    const before = git("rev-parse", "main");
    await fs.writeFile(path.join(root, "a"), "newer\n");
    git("commit", "-qam", "newer");
    const newer = git("rev-parse", "main");
    assert.ok(isOid(before) && isOid(newer));
    git("push", remote, "main:refs/heads/topic");
    assert.throws(
      () =>
        git("push", `--force-with-lease=refs/heads/topic:${before}`, remote, ":refs/heads/topic"),
      /stale info/,
    );
    const remove = (expected: typeof before) =>
      run(
        push({
          url: "http://fixture",
          refs: [{ local: "refs/heads/topic", remote: "refs/heads/topic", delete: true, expected }],
        }),
      );
    const refused = await remove(before);
    assert.equal(refused[0]?.ok, false);
    assert.equal(posts, 0);
    assert.equal(git("--git-dir", remote, "rev-parse", "refs/heads/topic"), newer);
    const accepted = await remove(newer);
    assert.equal(accepted[0]?.ok, true);
    assert.equal(posts, 1);
    assert.throws(() => git("--git-dir", remote, "show-ref", "--verify", "refs/heads/topic"));
  });

  it("updates every matching refspec destination", async () => {
    const result = await run(
      Effect.gen(function* () {
        return yield* fetchRepository({
          url: "http://fixture",
          stores: { objects: yield* ObjectStore, refs: yield* RefStore },
          refspecs: [
            { force: false, source: "refs/heads/*", destination: "refs/heads/*" },
            { force: true, source: "refs/heads/*", destination: "refs/remotes/origin/*" },
          ],
        });
      }),
    );
    assert.deepEqual(
      result.refs.map((ref) => ref.name),
      ["refs/heads/main", "refs/remotes/origin/main"],
    );
    assert.equal(git("rev-parse", "main"), git("rev-parse", "refs/remotes/origin/main"));
  });

  it("fetches a branch when a partial refspec wildcard matches no characters", async () => {
    git("fetch", "-q", remote, "refs/heads/main*:refs/remotes/stock/main*");
    const expected = git("rev-parse", "refs/remotes/stock/main");
    const result = await run(
      Effect.gen(function* () {
        return yield* fetchRepository({
          url: "http://fixture",
          stores: { objects: yield* ObjectStore, refs: yield* RefStore },
          refspecs: [
            { force: false, source: "refs/heads/main*", destination: "refs/remotes/native/main*" },
          ],
        });
      }),
    );
    assert.deepEqual(
      result.refs.map((ref) => ref.name),
      ["refs/remotes/native/main"],
    );
    assert.deepEqual(result.rejected, []);
    assert.equal(git("rev-parse", "refs/remotes/native/main"), expected);
  });

  it("preserves a concurrent ref change and reports its rejection", async () => {
    const base = git("rev-parse", "main");
    await fs.writeFile(path.join(root, "a"), "concurrent\n");
    git("commit", "-qam", "concurrent");
    const concurrent = git("rev-parse", "main");
    git("update-ref", "refs/heads/main", base);
    const result = await run(
      Effect.gen(function* () {
        const refs = yield* RefStore;
        const racing = RefStore.of({
          ...refs,
          apply: (updates, options) =>
            Effect.gen(function* () {
              git("update-ref", "refs/heads/main", concurrent);
              return yield* refs.apply(updates, options);
            }),
        });
        return yield* fetchRepository({
          url: "http://fixture",
          branch: "main",
          stores: { objects: yield* ObjectStore, refs: racing },
        });
      }),
    );
    assert.equal(git("rev-parse", "main"), concurrent);
    assert.deepEqual(result.refs, []);
    assert.deepEqual(
      result.rejected.map((ref) => ref.name),
      ["refs/heads/main"],
    );
  });

  it("fails the native fetch command when its ref update is rejected", async () => {
    await fs.writeFile(path.join(root, "a"), "local change\n");
    git("commit", "-qam", "local change");
    const before = git("rev-parse", "main");
    const command = Command.runWith(fetchCommand, { version: "test" });
    const result = await Effect.runPromise(
      Effect.result(command(["--root", root, ".git", "http://fixture"])).pipe(
        Effect.provide(NodeServices.layer),
      ),
    );
    assert.equal(result._tag, "Failure");
    assert.equal(git("rev-parse", "main"), before);
  });

  it("rejects unsupported atomic pushes before uploading", async () => {
    gitIn(remote)("config", "receive.advertiseAtomic", "false");
    const result = await run(
      Effect.result(
        push({
          url: "http://fixture",
          atomic: true,
          refs: [{ local: "refs/heads/main", remote: "refs/heads/main" }],
        }),
      ),
    );
    assert.equal(result._tag, "Failure");
    assert.equal(posts, 0);
  });

  it("does not send the rest of an atomic batch after a local rejection", async () => {
    await fs.writeFile(path.join(root, "a"), "new\n");
    git("commit", "-qam", "new");
    const results = await run(
      push({
        url: "http://fixture",
        atomic: true,
        refs: [
          { local: "main", remote: "refs/heads/missing", delete: true },
          { local: "refs/heads/main", remote: "refs/heads/main" },
        ],
      }),
    );
    assert.ok(results.every((result) => !result.ok));
    assert.equal(posts, 0);
  });

  it("resolves an ambiguous short name to its tag like Git", async () => {
    git("tag", "release");
    const tag = git("rev-parse", "refs/tags/release");
    assert.ok(isOid(tag));
    await fs.writeFile(path.join(root, "a"), "new\n");
    git("commit", "-qam", "new");
    git("branch", "release");
    const resolved = await run(
      Effect.gen(function* () {
        return yield* resolveRev(yield* Repository.Repository, "release");
      }),
    );
    assert.equal(resolved, tag);
  });
});
