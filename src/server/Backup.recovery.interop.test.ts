import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import type { Namespace } from "@alchemy.run/cloudflare/Artifacts/Namespace";
import { ReadWriteNamespace } from "@alchemy.run/cloudflare/Artifacts/ReadWriteNamespace";
import { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Layer, Stream } from "effect";
import { localNode, RepoStores, Tokens } from "../artifacts/Namespace.ts";
import { serve } from "../host/Node.ts";
import { fastImport, gitEnv, gitIn, hasGit, importCommit } from "../testing/Git.ts";
import { enableHubUnder, grantMemberUnder, opensshPrivateKey } from "../testing/Hub.ts";
import { LfsStore, formatPointer } from "./Lfs.ts";
import { file as lfsFile } from "./Lfs.node.ts";
import { Remotes } from "./Remotes.ts";
import { file as remotesFile } from "./Remotes.node.ts";
import { Subscribers } from "./Subscribers.ts";
import { file as subscribersFile } from "./Subscribers.node.ts";

const namespace = {
  kind: "Cloudflare.Artifacts.Namespace",
  name: "REPOS",
  namespace: "backup-rehearsal",
} satisfies Namespace;
const bound = Effect.gen(function* () {
  return yield* (yield* ReadWriteNamespace)(namespace);
});
const local = <A, E>(
  root: string,
  effect: Effect.Effect<A, E, ReadWriteNamespace | RepoStores | Tokens | RuntimeContext>,
) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(
        Layer.merge(
          localNode({ root, remoteBase: "https://backup.invalid" }),
          RuntimeContext.phantom,
        ),
      ),
    ),
  );
const execute = promisify(execFile);
const cli = async (root: string, ...args: string[]) =>
  (
    await execute(process.execPath, [path.resolve("src/cli/bin.ts"), ...args, "--root", root], {
      timeout: 30_000,
    })
  ).stdout.trim();
const gitAs = async (cwd: string, token: string, ...args: string[]) =>
  (
    await execute(
      "git",
      [
        "-c",
        "credential.helper=",
        "-c",
        'credential.helper=!f() { echo username=git; echo "password=$GIT_AUTH_TOKEN"; }; f',
        ...args,
      ],
      { cwd, env: { ...gitEnv, GIT_AUTH_TOKEN: token, GIT_TERMINAL_PROMPT: "0" }, timeout: 30_000 },
    )
  ).stdout.trim();

describe.skipIf(!hasGit)("offline Node backup recovery", () => {
  it("repairs relocated linked worktrees while preserving their private index and dirty files", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-worktree-recovery-"));
    const original = path.join(root, "original");
    const restored = path.join(root, "restored");
    const main = path.join(original, "main");
    const linked = path.join(original, "linked");
    const detached = path.join(original, "detached");
    try {
      await fs.mkdir(main, { recursive: true });
      const git = gitIn(main);
      git("init", "-q", "-b", "main");
      await fs.writeFile(path.join(main, "file"), "committed\n");
      git("add", "file");
      git("commit", "-qm", "backup base");
      const head = git("rev-parse", "HEAD").trim();
      git("worktree", "add", "-q", "-b", "topic", linked);
      git("worktree", "add", "-q", "--detach", detached);
      git("worktree", "lock", "--reason", "offline operator checkout", detached);
      await fs.writeFile(path.join(linked, "file"), "staged\n");
      gitIn(linked)("add", "file");
      await fs.writeFile(path.join(linked, "file"), "unstaged\n");
      await fs.writeFile(path.join(linked, "untracked"), "operator notes\n");
      const status = gitIn(linked)("status", "--porcelain");
      const index = await fs.readFile(path.join(main, ".git", "worktrees", "linked", "index"));
      await fs.cp(original, restored, {
        recursive: true,
        preserveTimestamps: true,
        dereference: false,
        verbatimSymlinks: true,
      });
      await fs.rm(original, { recursive: true });
      const recoveredMain = path.join(restored, "main");
      const recoveredLinked = path.join(restored, "linked");
      const recoveredDetached = path.join(restored, "detached");
      // Git owns both directions of this mapping. Repair only inventoried paths.
      gitIn(recoveredMain)("worktree", "repair", recoveredLinked, recoveredDetached);
      for (const work of [recoveredMain, recoveredLinked, recoveredDetached]) {
        assert.equal(gitIn(work)("rev-parse", "HEAD").trim(), head);
        gitIn(work)("fsck", "--strict");
      }
      assert.deepEqual(
        await fs.readFile(path.join(recoveredMain, ".git", "worktrees", "linked", "index")),
        index,
      );
      assert.equal(gitIn(recoveredLinked)("status", "--porcelain"), status);
      assert.equal(gitIn(recoveredLinked)("show", ":file"), "staged\n");
      assert.equal(await fs.readFile(path.join(recoveredLinked, "file"), "utf8"), "unstaged\n");
      assert.equal(
        await fs.readFile(path.join(recoveredLinked, "untracked"), "utf8"),
        "operator notes\n",
      );
      const listing = gitIn(recoveredMain)("worktree", "list", "--porcelain");
      assert.ok(!listing.includes(original));
      assert.match(listing, /detached\nlocked offline operator checkout/);
      assert.equal(gitIn(recoveredLinked)("symbolic-ref", "HEAD").trim(), "refs/heads/topic");
      const inspected = await execute(
        process.execPath,
        [path.resolve("src/cli/bin.ts"), "status", "--porcelain"],
        {
          cwd: recoveredLinked,
          env: gitEnv,
          timeout: 30_000,
        },
      );
      assert.equal(inspected.stdout, status);
      gitIn(recoveredLinked)("commit", "-qm", "restored staged change");
      assert.equal(gitIn(recoveredMain)("show", "topic:file"), "staged\n");
      assert.equal(gitIn(recoveredMain)("rev-parse", "HEAD").trim(), head);
      assert.equal(gitIn(recoveredDetached)("rev-parse", "HEAD").trim(), head);
      gitIn(recoveredMain)("fsck", "--strict");
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("restores private Git, LFS, hub records, registries, tokens, and fork dependencies into a fresh root", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-backup-recovery-"));
    const original = path.join(root, "original");
    const data = path.join(original, "data");
    const backup = path.join(root, "backup");
    const restored = path.join(root, "restored");
    const recoveredData = path.join(restored, "data");
    try {
      const credentials = await local(
        data,
        Effect.gen(function* () {
          const client = yield* bound;
          yield* client.create("parent", { description: "backup fixture" });
          const parent = yield* client.get("parent");
          const valid = yield* parent.createToken("read", 3600);
          const revoked = yield* parent.createToken("write", 3600);
          yield* parent.revokeToken(revoked.id);
          return { valid: valid.plaintext, revoked: revoked.plaintext };
        }),
      );
      const content = new TextEncoder().encode("LFS payload preserved by backup\n");
      const oid = createHash("sha256").update(content).digest("hex");
      const parentPath = path.join(data, "parent");
      fastImport(
        parentPath,
        [
          importCommit({
            branch: "refs/heads/main",
            mark: 1,
            message: "backup base",
            files: [
              { path: "file", content: "original history\n" },
              { path: "asset.bin", content: formatPointer({ oid, size: content.length }) },
            ],
          }),
          importCommit({
            branch: "refs/heads/topic",
            mark: 2,
            from: 1,
            message: "proposal",
            files: [{ path: "proposal", content: "hub proposal\n" }],
          }),
        ].join(""),
      );
      const reader = await enableHubUnder(data, "parent", ["repo.read", "hub.create-pr"]);
      const writer = await grantMemberUnder(data, "parent", reader.root, reader.repoId, [
        "repo.read",
        "source.push",
      ]);
      const key = path.join(original, "keys", "operator");
      await fs.mkdir(path.dirname(key), { recursive: true, mode: 0o700 });
      await fs.writeFile(key, opensshPrivateKey(reader.member, "backup@example.com"), {
        mode: 0o600,
      });
      await fs.writeFile(
        path.join(path.dirname(key), "authority"),
        opensshPrivateKey(reader.root, "authority@example.com"),
        { mode: 0o600 },
      );
      await fs.symlink("operator", path.join(path.dirname(key), "current"));
      const pr = await cli(
        data,
        "pr",
        "open",
        "--title",
        "Backup recovery",
        "--head",
        "topic",
        "--key",
        key,
        "parent",
      );
      const hubBefore = await cli(data, "pr", "show", "parent", pr);
      assert.match(hubBefore, /"title": "Backup recovery"/);
      await Effect.runPromise(
        LfsStore.use((store) => store.write(oid, Stream.make(content))).pipe(
          Effect.provide(lfsFile(path.join(parentPath, "lfs"))),
        ),
      );
      await Effect.runPromise(
        Remotes.use((store) =>
          store.add({
            name: "archive",
            url: "https://archive.example.com/repo",
            credential: "fixture-private-credential",
          }),
        ).pipe(Effect.provide(remotesFile(path.join(parentPath, "remotes.json")))),
      );
      await Effect.runPromise(
        Subscribers.use((store) =>
          store.add({ url: "https://hooks.example.com/backup", secret: "fixture-webhook-secret" }),
        ).pipe(Effect.provide(subscribersFile(path.join(parentPath, "webhooks.json")))),
      );
      await local(
        data,
        Effect.gen(function* () {
          const parent = yield* (yield* bound).get("parent");
          yield* parent.fork("child");
        }),
      );
      const head = gitIn(parentPath)("rev-parse", "main").trim();
      const source = await serve({ root: data });
      try {
        assert.equal(
          (await fetch(`${source.url}/parent/info/refs?service=git-upload-pack`)).status,
          401,
        );
        assert.equal(
          (
            await fetch(`${source.url}/parent/info/lfs/objects/${oid}`, {
              headers: { authorization: `Bearer ${reader.credential}` },
            })
          ).status,
          200,
        );
      } finally {
        await source.close();
      }

      // Quiesced capture includes external operator material alongside data.
      await fs.cp(original, backup, {
        recursive: true,
        preserveTimestamps: true,
        dereference: false,
        verbatimSymlinks: true,
      });
      await fs.cp(backup, restored, {
        recursive: true,
        preserveTimestamps: true,
        dereference: false,
        verbatimSymlinks: true,
      });
      await fs.rm(original, { recursive: true, force: true });
      const recoveredParent = path.join(recoveredData, "parent");
      const recoveredChild = path.join(recoveredData, "child");
      // Relocate the inventoried internal alternate; never rewrite arbitrary
      // external paths by substring or rely on the old installation surviving.
      const alternate = path.join(recoveredChild, "objects", "info", "alternates");
      assert.equal((await fs.readFile(alternate, "utf8")).trim(), path.join(parentPath, "objects"));
      await fs.writeFile(
        alternate,
        `${path.relative(path.join(recoveredChild, "objects"), path.join(recoveredParent, "objects"))}\n`,
      );
      for (const directory of [recoveredParent, recoveredChild]) {
        const git = gitIn(directory);
        git("fsck", "--strict");
        assert.equal(git("rev-parse", "main").trim(), head);
        assert.equal(git("show", "main:file"), "original history\n");
      }
      assert.equal(await cli(recoveredData, "pr", "show", "parent", pr), hubBefore);
      for (const name of ["operator", "authority"]) {
        assert.equal((await fs.stat(path.join(restored, "keys", name))).mode & 0o777, 0o600);
        assert.equal(
          await fs.readFile(path.join(restored, "keys", name), "utf8"),
          await fs.readFile(path.join(backup, "keys", name), "utf8"),
        );
      }
      assert.equal(await fs.readlink(path.join(restored, "keys", "current")), "operator");
      assert.equal((await fs.stat(path.join(recoveredData, ".tokens.json"))).mode & 0o777, 0o600);
      for (const name of [".registry.json", ".forks.json", ".tokens.json"]) {
        assert.equal(
          await fs.readFile(path.join(recoveredData, name), "utf8"),
          await fs.readFile(path.join(backup, "data", name), "utf8"),
        );
      }
      for (const name of ["remotes.json", "webhooks.json"]) {
        assert.equal((await fs.stat(path.join(recoveredParent, name))).mode & 0o777, 0o600);
        assert.equal(
          await fs.readFile(path.join(recoveredParent, name), "utf8"),
          await fs.readFile(path.join(backup, "data", "parent", name), "utf8"),
        );
      }
      await local(
        recoveredData,
        Effect.gen(function* () {
          const client = yield* bound;
          assert.deepEqual((yield* client.list()).repos.map((repo) => repo.name).sort(), [
            "child",
            "parent",
          ]);
          const tokens = yield* Tokens;
          assert.equal(yield* tokens.verify("parent", credentials.valid), "read");
          assert.equal(yield* tokens.verify("parent", credentials.revoked), null);
          const refused = yield* client.delete("parent").pipe(Effect.result);
          assert.equal(refused._tag, "Failure", "fork ownership must survive restore");
        }),
      );
      // Disable the restored fixture subscriber before creating new events;
      // registry verification does not require contacting an external receiver.
      await Effect.runPromise(
        Subscribers.use((store) =>
          Effect.gen(function* () {
            const rows = yield* store.list;
            assert.equal(rows.length, 1);
            for (const row of rows) assert.equal(yield* store.remove(row.id), true);
          }),
        ).pipe(Effect.provide(subscribersFile(path.join(recoveredParent, "webhooks.json")))),
      );
      const signedAfterRestore = await cli(
        recoveredData,
        "pr",
        "open",
        "--title",
        "Restored signer",
        "--head",
        "topic",
        "--key",
        path.join(restored, "keys", "current"),
        "parent",
      );
      assert.match(
        await cli(recoveredData, "pr", "show", "parent", signedAfterRestore),
        /Restored signer/,
      );
      const host = await serve({ root: recoveredData });
      try {
        const remote = `${host.url}/parent`;
        assert.equal((await fetch(`${remote}/info/refs?service=git-upload-pack`)).status, 401);
        const work = path.join(root, "clone");
        await gitAs(root, reader.credential, "clone", "--quiet", remote, work);
        assert.equal(await fs.readFile(path.join(work, "file"), "utf8"), "original history\n");
        const lfs = await fetch(`${remote}/info/lfs/objects/${oid}`, {
          headers: { authorization: `Bearer ${reader.credential}` },
        });
        assert.equal(lfs.status, 200);
        assert.deepEqual(new Uint8Array(await lfs.arrayBuffer()), content);
        assert.equal((await fetch(`${remote}/info/lfs/objects/${oid}`)).status, 401);
        await fs.writeFile(path.join(work, "file"), "after restore\n");
        gitIn(work)("commit", "-qam", "post-restore write");
        await assert.rejects(gitAs(work, reader.credential, "push", remote, "main"));
        assert.equal(gitIn(recoveredParent)("rev-parse", "main").trim(), head);
        await gitAs(work, writer.credential, "push", remote, "main");
        assert.equal(
          gitIn(recoveredParent)("rev-parse", "main").trim(),
          gitIn(work)("rev-parse", "HEAD").trim(),
        );
        gitIn(recoveredParent)("fsck", "--strict");
        gitIn(recoveredChild)("fsck", "--strict");
        assert.equal(gitIn(recoveredChild)("rev-parse", "main").trim(), head);
      } finally {
        await host.close();
      }
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
});
