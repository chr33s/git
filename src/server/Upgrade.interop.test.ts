import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { hasGit } from "../testing/Git.ts";

const execute = promisify(execFile);
const baseline = "54c6199b79d3b121ad7964ac8d0edaeba85b4666";

describe.skipIf(!hasGit)("Node source upgrade rehearsal", () => {
  it("reopens old data, upgrades it, and verifies a quiesced source rollback", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-upgrade-"));
    const old = path.join(root, "baseline");
    try {
      await fs.mkdir(old);
      await execute("git", [
        "archive",
        "--format=tar",
        "--output",
        path.join(root, "old.tar"),
        baseline,
        "src",
        "package.json",
      ]);
      await execute("tar", ["-xf", path.join(root, "old.tar"), "-C", old]);
      // This is source/data compatibility with the installed dependencies,
      // not a rehearsal of a previously distributed binary or dependency set.
      await fs.symlink(path.resolve("node_modules"), path.join(old, "node_modules"));
      const script = path.join(root, "rehearse.mjs");
      await fs.writeFile(
        script,
        `
        import assert from "node:assert/strict";
        import * as fs from "node:fs/promises";
        import { join } from "node:path";
        import { pathToFileURL } from "node:url";
        import { createHash } from "node:crypto";
        import { execFile } from "node:child_process";
        import { promisify } from "node:util";
        import { Effect, Layer, Stream } from ${JSON.stringify(import.meta.resolve("effect"))};
        import { ReadWriteNamespace } from ${JSON.stringify(import.meta.resolve("@alchemy.run/cloudflare/Artifacts/ReadWriteNamespace"))};
        import { RuntimeContext } from ${JSON.stringify(import.meta.resolve("alchemy/RuntimeContext"))};
        const [source, root, phase] = process.argv.slice(2);
        const load = name => import(pathToFileURL(join(source, "src", name)).href);
        const { localNode, Tokens } = await load("artifacts/Namespace.ts");
        const { serve } = await load("host/Node.ts");
        const { enableHubUnder, opensshPrivateKey } = await load("testing/Hub.ts");
        const { fastImport, importCommit, gitIn, gitEnv } = await load("testing/Git.ts");
        const { LfsStore } = await load("server/Lfs.ts");
        const { file } = await load("server/Lfs.node.ts");
        const run = promisify(execFile);
        const data = join(root, "data");
        const parent = join(data, "parent");
        const namespace = { kind: "Cloudflare.Artifacts.Namespace", name: "REPOS", namespace: "upgrade" };
        const bound = ReadWriteNamespace.use(service => service(namespace));
        const local = effect => Effect.runPromise(effect.pipe(Effect.provide(Layer.merge(localNode({ root: data, remoteBase: "https://unused.invalid" }), RuntimeContext.phantom))));
        const content = new TextEncoder().encode("retained LFS bytes");
        const oid = createHash("sha256").update(content).digest("hex");
        const stateFile = join(root, "state.json");
        let state;
        if (phase === "old") {
          state = await local(Effect.gen(function* () {
            const client = yield* bound;
            yield* client.create("parent");
            const repo = yield* client.get("parent");
            const valid = yield* repo.createToken("read", 3600);
            const revoked = yield* repo.createToken("write", 3600);
            yield* repo.revokeToken(revoked.id);
            return { valid: valid.plaintext, revoked: revoked.plaintext, validId: valid.id };
          }));
          fastImport(parent, importCommit({ branch: "refs/heads/main", mark: 1, message: "old data", files: [{ path: "file", content: "base" }] }) + importCommit({ branch: "refs/heads/topic", mark: 2, from: 1, message: "proposal", files: [{ path: "proposal", content: "hub history" }] }));
          state.childTip = gitIn(parent)("rev-parse", "main").trim();
          const identity = await enableHubUnder(data, "parent", ["repo.read", "source.push", "hub.create-pr"]);
          state.credential = identity.credential;
          state.records = [];
          await fs.writeFile(join(root, "key"), opensshPrivateKey(identity.member, "upgrade@example.com"), { mode: 0o600 });
          await Effect.runPromise(LfsStore.use(store => store.write(oid, Stream.make(content))).pipe(Effect.provide(file(join(parent, "lfs")))));
          await local(Effect.gen(function* () { yield* (yield* (yield* bound).get("parent")).fork("child"); }));
        } else {
          state = JSON.parse(await fs.readFile(stateFile, "utf8"));
          assert.equal(gitIn(parent)("rev-parse", "main").trim(), state.tip);
        }
        await local(Effect.gen(function* () {
          const tokens = yield* Tokens;
          assert.equal(yield* tokens.verify("parent", state.valid), phase === "rollback" ? null : "read");
          assert.equal(yield* tokens.verify("parent", state.revoked), null);
          const client = yield* bound;
          assert.deepEqual((yield* client.list()).repos.map(repo => repo.name).sort(), ["child", "parent"]);
          assert.equal((yield* client.delete("parent").pipe(Effect.result))._tag, "Failure");
          if (phase === "upgrade") {
            const repo = yield* client.get("parent");
            yield* repo.revokeToken(state.validId);
            state.newToken = (yield* repo.createToken("write", 3600)).plaintext;
          }
          if (phase === "rollback") assert.equal(yield* tokens.verify("parent", state.newToken), "write");
        }));
        await assert.rejects(fs.access(join(parent, ".git-plus-ref-transaction.json")), { code: "ENOENT" });
        const server = await serve({ root: data });
        try {
          assert.equal((await fetch(server.url + "/parent/refs")).status, 401);
          const lfs = await fetch(server.url + "/parent/info/lfs/objects/" + oid, { headers: { authorization: "Bearer " + state.credential } });
          assert.equal(lfs.status, 200);
          assert.deepEqual(new Uint8Array(await lfs.arrayBuffer()), content);
          const work = join(root, "work-" + phase);
          const git = (...args) => run("git", ["-c", "credential.helper=", "-c", 'credential.helper=!f() { echo username=git; echo "password=$GIT_AUTH_TOKEN"; }; f', ...args], { cwd: root, env: { ...gitEnv, GIT_AUTH_TOKEN: state.credential, GIT_TERMINAL_PROMPT: "0" }, timeout: 30_000 });
          await git("clone", "-q", server.url + "/parent", work);
          await fs.writeFile(join(work, "file"), phase);
          gitIn(work)("add", "file");
          gitIn(work)("commit", "-qm", phase);
          await git("-C", work, "push", "origin", "main");
          state.tip = gitIn(work)("rev-parse", "HEAD").trim();
          assert.equal(gitIn(parent)("rev-parse", "main").trim(), state.tip);
          assert.equal(gitIn(join(data, "child"))("rev-parse", "main").trim(), state.childTip);
        } finally { await server.close(); }
        const cli = async (...args) => (await run(process.execPath, [join(source, "src/cli/bin.ts"), ...args, "--root", data], { timeout: 30_000 })).stdout.trim();
        for (const record of state.records) {
          assert.equal(JSON.parse(await cli("pr", "show", "parent", record.id)).title, record.title);
        }
        const title = "upgrade rehearsal " + phase;
        const id = await cli("pr", "open", "--title", title, "--head", "topic", "--key", join(root, "key"), "parent");
        assert.equal(JSON.parse(await cli("pr", "show", "parent", id)).title, title);
        state.records.push({ id, title });
        gitIn(parent)("fsck", "--strict");
        gitIn(join(data, "child"))("fsck", "--strict");
        await fs.writeFile(stateFile, JSON.stringify(state), { mode: 0o600 });
        console.log("verified " + phase);
      `,
      );
      for (const [source, phase] of [
        [old, "old"],
        [process.cwd(), "upgrade"],
        [old, "rollback"],
      ]) {
        assert.ok(source && phase);
        const result = await execute(process.execPath, [script, source, root, phase], {
          timeout: 45_000,
        });
        assert.match(result.stdout, new RegExp(`verified ${phase}`));
      }
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
});
