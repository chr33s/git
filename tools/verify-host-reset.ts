/** Two-phase rehearsal on an exclusively owned persistent Linux VM volume. */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { Effect, Layer, Schema } from "effect";
import { ReadWriteNamespace } from "@alchemy.run/cloudflare/Artifacts/ReadWriteNamespace";
import { RuntimeContext } from "alchemy/RuntimeContext";
import { localNode, Tokens } from "../src/artifacts/Namespace.ts";
import { serve } from "../src/host/Node.ts";
import { mintDelegation } from "../src/server/Auth.ts";
import { enableHubUnder } from "../src/testing/Hub.ts";
import { gitEnv, gitIn } from "../src/testing/Git.ts";

const mount = "/git-reset-rehearsal";
const data = path.join(mount, "data");
const control = path.join(mount, "control.json");
const [phase, report] = process.argv.slice(2);
assert.ok(phase === "prepare" || phase === "verify", "expected prepare or verify <report>");
assert.equal(process.platform, "linux");
const filesystem = await fs.statfs(mount);
assert.equal(filesystem.type, 0xef53, "requires the dedicated ext4 VM volume");
assert.notEqual((await fs.stat(mount)).dev, (await fs.stat("/")).dev);
const bootId = (await fs.readFile("/proc/sys/kernel/random/boot_id", "utf8")).trim();
const bound = ReadWriteNamespace.use((service) =>
  service({ kind: "Cloudflare.Artifacts.Namespace", name: "REPOS", namespace: "reset" }),
);
const layer = Layer.merge(
  localNode({ root: data, remoteBase: "https://unused.invalid" }),
  RuntimeContext.phantom,
);
const run = promisify(execFile);
const State = Schema.Struct({
  bootId: Schema.String,
  credential: Schema.String,
  valid: Schema.String,
  revoked: Schema.String,
  tip: Schema.String,
  blob: Schema.String,
  lfs: Schema.String,
});
const digest = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");
const scratch = await fs.mkdtemp(path.join(os.tmpdir(), "git-reset-client-"));
let server: Awaited<ReturnType<typeof serve>> | undefined;
const git = (credential: string, ...args: string[]) =>
  run(
    "git",
    [
      "-c",
      "credential.helper=",
      "-c",
      'credential.helper=!f() { echo username=git; echo "password=$GIT_AUTH_TOKEN"; }; f',
      "-c",
      "http.postBuffer=65536",
      ...args,
    ],
    {
      cwd: scratch,
      env: { ...gitEnv, GIT_AUTH_TOKEN: credential, GIT_TERMINAL_PROMPT: "0" },
      timeout: 30_000,
    },
  );
try {
  if (phase === "prepare") {
    assert.deepEqual(await fs.readdir(mount), [], "prepare requires an empty owned volume");
    const tokens = await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* bound;
        yield* client.create("parent");
        const repo = yield* client.get("parent");
        const valid = yield* repo.createToken("read", 3600);
        const revoked = yield* repo.createToken("write", 3600);
        yield* repo.revokeToken(revoked.id);
        return { valid: valid.plaintext, revoked: revoked.plaintext };
      }).pipe(Effect.provide(layer)),
    );
    const member = await enableHubUnder(data, "parent", ["repo.admin"]);
    const credential = await Effect.runPromise(
      mintDelegation({
        key: member.member,
        repo: member.repoId,
        capabilities: ["repo.admin"],
        ttlSeconds: 3600,
      }),
    );
    server = await serve({ root: data });
    await git(credential, "init", "-q", "-b", "main", "client");
    const client = path.join(scratch, "client");
    const bytes = randomBytes(256 * 1024);
    await fs.writeFile(path.join(client, "file"), bytes);
    gitIn(client)("add", "file");
    gitIn(client)("commit", "-qm", "acknowledged before guest reset");
    const tip = gitIn(client)("rev-parse", "HEAD").trim();
    const lfsBytes = randomBytes(256 * 1024);
    const lfs = digest(lfsBytes);
    // Persist expectations before the final application writes. This is not
    // an application-wide sync and must not flush the later writes for it.
    const file = await fs.open(control, "wx", 0o600);
    try {
      await file.writeFile(
        JSON.stringify({ bootId, credential, ...tokens, tip, blob: digest(bytes), lfs }),
      );
      await file.sync();
    } finally {
      await file.close();
    }
    const directory = await fs.open(mount, "r");
    try {
      await directory.sync();
    } finally {
      await directory.close();
    }
    await git(
      credential,
      "-C",
      client,
      "push",
      "--atomic",
      `${server.url}/parent`,
      "HEAD:refs/heads/main",
      "HEAD:refs/heads/side",
    );
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* (yield* (yield* bound).get("parent")).fork("child");
      }).pipe(Effect.provide(layer)),
    );
    const uploaded = await fetch(`${server.url}/parent/info/lfs/objects/${lfs}`, {
      method: "PUT",
      headers: { authorization: `Bearer ${credential}` },
      body: lfsBytes,
      signal: AbortSignal.timeout(15_000),
    });
    assert.equal(uploaded.status, 200);
    await uploaded.arrayBuffer();
    console.log("HOST_RESET_READY");
    // The operator force-stops this exclusively owned VM while this host lives.
    await new Promise<void>(() => {});
  } else {
    assert.ok(report, "verify requires a new report path");
    const state = Schema.decodeSync(Schema.fromJsonString(State))(
      await fs.readFile(control, "utf8"),
    );
    assert.notEqual(bootId, state.bootId, "verification requires a different guest kernel boot");
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* bound;
        assert.deepEqual(
          (yield* client.list()).repos.map((row: { readonly name: string }) => row.name).sort(),
          ["child", "parent"],
        );
        const tokens = yield* Tokens;
        assert.equal(yield* tokens.verify("parent", state.valid), "read");
        assert.equal(yield* tokens.verify("parent", state.revoked), null);
        assert.equal((yield* client.delete("parent").pipe(Effect.result))._tag, "Failure");
        yield* (yield* client.get("parent")).createToken("read", 3600);
        assert.equal(yield* tokens.verify("parent", state.revoked), null);
      }).pipe(Effect.provide(layer)),
    );
    for (const name of ["parent", "child"]) {
      const repo = gitIn(path.join(data, name));
      for (const ref of name === "parent" ? ["main", "side"] : ["main"])
        assert.equal(repo("rev-parse", ref).trim(), state.tip);
      repo("fsck", "--strict");
    }
    const leftovers = await fs.readdir(data, { recursive: true });
    assert.equal(
      leftovers.some(
        (name) =>
          name.endsWith(".lock") ||
          name.endsWith(".tmp") ||
          name.endsWith(".git-plus-ref-transaction.json"),
      ),
      false,
    );
    assert.equal((await fs.stat(path.join(data, ".tokens.json"))).mode & 0o777, 0o600);
    server = await serve({ root: data });
    const anonymous = await fetch(`${server.url}/parent/refs`, {
      signal: AbortSignal.timeout(10_000),
    });
    assert.equal(anonymous.status, 401);
    await anonymous.arrayBuffer();
    await git(state.credential, "clone", "-q", `${server.url}/parent`, "recovered");
    const recovered = path.join(scratch, "recovered");
    assert.equal(digest(await fs.readFile(path.join(recovered, "file"))), state.blob);
    gitIn(recovered)("fsck", "--strict");
    const downloaded = await fetch(`${server.url}/parent/info/lfs/objects/${state.lfs}`, {
      headers: { authorization: `Bearer ${state.credential}` },
      signal: AbortSignal.timeout(10_000),
    });
    assert.equal(downloaded.status, 200);
    assert.equal(digest(new Uint8Array(await downloaded.arrayBuffer())), state.lfs);
    await fs.writeFile(path.join(recovered, "file"), "after reset");
    gitIn(recovered)("commit", "-qam", "recovered writer");
    await git(
      state.credential,
      "-C",
      recovered,
      "push",
      "--atomic",
      `${server.url}/parent`,
      "HEAD:refs/heads/main",
      "HEAD:refs/heads/side",
    );
    const next = gitIn(recovered)("rev-parse", "HEAD").trim();
    for (const ref of ["main", "side"])
      assert.equal(gitIn(path.join(data, "parent"))("rev-parse", ref).trim(), next);
    await fs.writeFile(
      report,
      JSON.stringify(
        {
          schemaVersion: 1,
          measuredAt: new Date().toISOString(),
          node: process.version,
          platform: process.platform,
          arch: process.arch,
          kernel: os.release(),
          git: (await run("git", ["--version"])).stdout.trim(),
          filesystem: "ext4 on persistent Docker volume in isolated Colima VZ guest",
          bootBefore: state.bootId,
          bootAfter: bootId,
          acknowledgedAtomicRefsAndCloneVerified: true,
          acknowledgedLfsDigestVerified: true,
          tokenRevocationAndPrivateModeVerified: true,
          forkOwnershipAndBorrowedHistoryVerified: true,
          anonymousReadsDenied: true,
          noPendingTransactionOrTemporaryFiles: true,
          subsequentAtomicPushVerified: true,
          limitsOfEvidence:
            "One forced guest VM stop and kernel restart; not physical power loss, hardware cache qualification, or every interruption window. Host storage remains powered.",
        },
        null,
        2,
      ) + "\n",
      { flag: "wx", mode: 0o600 },
    );
    console.log(`Verified guest-reset recovery: ${report}`);
  }
} finally {
  await server?.close({ force: true });
  await fs.rm(scratch, { recursive: true, force: true });
}
