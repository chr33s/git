import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { hasGit } from "../testing/Git.ts";

const script = `
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { Effect, Layer } from ${JSON.stringify(import.meta.resolve("effect"))};
import { ReadWriteNamespace } from ${JSON.stringify(import.meta.resolve("@alchemy.run/cloudflare/Artifacts/ReadWriteNamespace"))};
import { RuntimeContext } from ${JSON.stringify(import.meta.resolve("alchemy/RuntimeContext"))};
import { localNode, Tokens } from ${JSON.stringify(new URL("./Namespace.ts", import.meta.url).href)};
import { fastImport, importCommit, gitIn } from ${JSON.stringify(new URL("../testing/Git.ts", import.meta.url).href)};
import { Remotes } from ${JSON.stringify(new URL("../server/Remotes.ts", import.meta.url).href)};
import { file as remotesFile } from ${JSON.stringify(new URL("../server/Remotes.node.ts", import.meta.url).href)};
import { Subscribers } from ${JSON.stringify(new URL("../server/Subscribers.ts", import.meta.url).href)};
import { file as subscribersFile } from ${JSON.stringify(new URL("../server/Subscribers.node.ts", import.meta.url).href)};
const [root, phase] = process.argv.slice(1);
const data = path.join(root, "data");
const parent = path.join(data, "parent");
const namespace = { kind: "Cloudflare.Artifacts.Namespace", name: "REPOS", namespace: "crash" };
const bound = ReadWriteNamespace.use(service => service(namespace));
const local = effect => Effect.runPromise(effect.pipe(Effect.provide(Layer.merge(localNode({ root: data, remoteBase: "https://unused.invalid" }), RuntimeContext.phantom))));
const remote = effect => Effect.runPromise(effect.pipe(Effect.provide(remotesFile(path.join(parent, "remotes.json")))));
const subscriber = effect => Effect.runPromise(effect.pipe(Effect.provide(subscribersFile(path.join(parent, "webhooks.json")))));
const stateFile = path.join(root, "fixture.json");
if (phase === "write") {
  const credentials = await local(Effect.gen(function* () {
    const client = yield* bound;
    yield* client.create("parent", { description: "acknowledged installation" });
    const repo = yield* client.get("parent");
    const valid = yield* repo.createToken("read", 3600);
    const revoked = yield* repo.createToken("write", 3600);
    yield* repo.revokeToken(revoked.id);
    return { valid: valid.plaintext, revoked: revoked.plaintext };
  }));
  fastImport(parent, importCommit({ branch: "refs/heads/main", mark: 1, message: "retained parent", files: [{ path: "file", content: "borrowed history" }] }));
  const tip = gitIn(parent)("rev-parse", "main").trim();
  await local(Effect.gen(function* () { yield* (yield* (yield* bound).get("parent")).fork("child"); }));
  await remote(Remotes.use(store => store.add({ name: "archive", url: "https://archive.example.com/repo", credential: "fixture-private-credential" })));
  const hook = await subscriber(Subscribers.use(store => store.add({ url: "https://hooks.example.com/crash", secret: "fixture-webhook-secret" })));
  // Test-only expectations, outside the installation. Every application
  // mutation above has returned success before the process dies.
  await fs.writeFile(stateFile, JSON.stringify({ ...credentials, tip, hook: hook.id }), { mode: 0o600 });
  process.kill(process.pid, "SIGKILL");
  throw new Error("SIGKILL did not terminate writer");
}
const state = JSON.parse(await fs.readFile(stateFile, "utf8"));
if (phase === "deleted") {
  await local(Effect.gen(function* () {
    const client = yield* bound;
    const tokens = yield* Tokens;
    assert.deepEqual((yield* client.list()).repos, []);
    assert.equal(yield* tokens.verify("parent", state.valid), null);
    assert.equal(yield* tokens.verify("parent", state.revoked), null);
    yield* client.create("parent");
    assert.equal(yield* tokens.verify("parent", state.valid), null, "reusing the name must not restore old access");
    assert.equal(yield* tokens.verify("parent", state.revoked), null);
    const fresh = yield* (yield* client.get("parent")).createToken("write", 3600);
    assert.equal(yield* tokens.verify("parent", fresh.plaintext), "write");
    yield* client.delete("parent");
  }));
  console.log("installation metadata survived process death");
  process.exit(0);
}
await local(Effect.gen(function* () {
  const client = yield* bound;
  assert.deepEqual((yield* client.list()).repos.map(row => row.name).sort(), ["child", "parent"]);
  const tokens = yield* Tokens;
  assert.equal(yield* tokens.verify("parent", state.valid), "read");
  assert.equal(yield* tokens.verify("parent", state.revoked), null);
  assert.equal((yield* client.delete("parent").pipe(Effect.result))._tag, "Failure", "borrower must still protect its parent");
  const fresh = yield* (yield* client.get("parent")).createToken("read", 3600);
  assert.equal(yield* tokens.verify("parent", fresh.plaintext), "read");
  assert.equal(yield* tokens.verify("parent", state.revoked), null, "later issuance cannot resurrect the revoked token");
}));
const remotes = await remote(Remotes.use(store => store.list));
assert.equal(remotes.length, 1);
assert.equal(remotes[0].name, "archive");
assert.equal(remotes[0].credential, "fixture-private-credential");
const hooks = await subscriber(Subscribers.use(store => store.list));
assert.equal(hooks.length, 1);
assert.equal(hooks[0].id, state.hook);
assert.equal(hooks[0].secret, "fixture-webhook-secret");
for (const name of ["parent", "child"]) {
  const git = gitIn(path.join(data, name));
  assert.equal(git("rev-parse", "main").trim(), state.tip);
  assert.equal(git("show", "main:file"), "borrowed history");
  git("fsck", "--strict");
}
for (const file of [path.join(data, ".tokens.json"), path.join(parent, "remotes.json"), path.join(parent, "webhooks.json")])
  assert.equal((await fs.stat(file)).mode & 0o777, 0o600);
const leftovers = await fs.readdir(data, { recursive: true });
assert.equal(leftovers.some(name => name.endsWith(".lock") || name.endsWith(".tmp")), false);
// Further lifecycle mutations must work without manual stale-lock removal.
await local(Effect.gen(function* () {
  const client = yield* bound;
  yield* client.delete("child");
  yield* client.delete("parent");
  assert.deepEqual((yield* client.list()).repos, []);
  assert.equal(yield* (yield* Tokens).verify("parent", state.valid), null);
}));
process.kill(process.pid, "SIGKILL");
throw new Error("SIGKILL did not terminate deletion writer");
`;

describe.skipIf(!hasGit)("acknowledged installation metadata after process death", () => {
  it("reopens tokens, registries, and fork ownership after SIGKILL and allows later lifecycle operations", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-installation-crash-"));
    try {
      await assert.rejects(
        promisify(execFile)(
          process.execPath,
          ["--input-type=module", "-e", script, root, "write"],
          { timeout: 30_000 },
        ),
        { signal: "SIGKILL" },
      );
      await assert.rejects(
        promisify(execFile)(
          process.execPath,
          ["--input-type=module", "-e", script, root, "verify"],
          { timeout: 30_000 },
        ),
        { signal: "SIGKILL" },
      );
      const verified = await promisify(execFile)(
        process.execPath,
        ["--input-type=module", "-e", script, root, "deleted"],
        { timeout: 30_000 },
      );
      assert.match(verified.stdout, /installation metadata survived process death/);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
});
