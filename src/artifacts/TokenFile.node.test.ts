import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { execFile, spawn } from "node:child_process";
import { promisify } from "node:util";
import { it } from "@effect/vitest";
import { Effect, Result } from "effect";

import { Tokens, tokensNode } from "./Namespace.ts";

const open = (root: string) => Effect.runPromise(Tokens.pipe(Effect.provide(tokensNode(root))));
const run = Effect.runPromise;
const moduleUrl = new URL("./Namespace.ts", import.meta.url).href;

const syncFailure = async (root: string, ancestor: string, mode: string, id = "") => {
  const program = `
    import fs from "node:fs/promises";
    import { syncBuiltinESMExports } from "node:module";
    import { Effect } from "effect";
    import { Tokens, tokensNode } from ${JSON.stringify(moduleUrl)};
    const [root, ancestor, mode, id] = process.argv.slice(1);
    const open = fs.open;
    fs.open = async function (...args) {
      const handle = await Reflect.apply(open, fs, args);
      const stat = await handle.stat();
      const expected = await fs.stat(ancestor);
      if (stat.isDirectory() === expected.isDirectory() && stat.dev === expected.dev && stat.ino === expected.ino) {
        handle.sync = async () => { throw Object.assign(new Error("fixture sync failure"), {code: "EIO"}); };
      }
      return handle;
    };
    syncBuiltinESMExports();
    const tokens = await Effect.runPromise(Tokens.pipe(Effect.provide(tokensNode(root))));
    const operation = mode === "issue"
      ? tokens.issue("repo", "read", 300).pipe(Effect.as("issued"))
      : tokens.revoke("repo", id).pipe(Effect.map(String));
    console.log(await Effect.runPromise(operation.pipe(Effect.match({onSuccess: value => value, onFailure: error => error._tag}))));
  `;
  return (
    await promisify(execFile)(
      process.execPath,
      ["--input-type=module", "-e", program, root, ancestor, mode, id],
      { timeout: 20_000 },
    )
  ).stdout.trim();
};

it("token issuance refuses failed ancestor syncs, including fresh-process retries", async () => {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "token-ancestor-"));
  const ancestor = path.join(temporary, "new");
  const root = path.join(ancestor, "namespace");
  try {
    for (let attempt = 0; attempt < 2; attempt++) {
      assert.equal(await syncFailure(root, ancestor, "issue"), "ArtifactsError");
      assert.deepEqual(await fs.readdir(root), [".tokens.json"]);
    }
    const tokens = await open(root);
    const issued = await run(tokens.issue("repo", "read", 300));
    assert.equal(await run((await open(root)).verify("repo", issued.plaintext)), "read");
    assert.equal((await fs.stat(path.join(root, ".tokens.json"))).mode & 0o777, 0o600);
  } finally {
    await fs.rm(temporary, { recursive: true, force: true });
  }
});

for (const fault of ["registry", "ancestor", "file"]) {
  it(`an already-visible token revocation still retries a failed ${fault} sync`, async () => {
    const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "token-revoke-sync-"));
    const root = path.join(temporary, "new", "namespace");
    try {
      const tokens = await open(root);
      const issued = await run(tokens.issue("repo", "write", 300));
      const directory =
        fault === "file"
          ? path.join(root, ".tokens.json")
          : fault === "registry"
            ? root
            : path.dirname(root);
      for (let attempt = 0; attempt < 2; attempt++) {
        assert.equal(await syncFailure(root, directory, "revoke", issued.id), "ArtifactsError");
        assert.equal(await run((await open(root)).verify("repo", issued.plaintext)), null);
        assert.deepEqual(await fs.readdir(root), [".tokens.json"]);
      }
      assert.equal(await run((await open(root)).revoke("repo", issued.id)), false);
      assert.equal(await run((await open(root)).verify("repo", issued.plaintext)), null);
    } finally {
      await fs.rm(temporary, { recursive: true, force: true });
    }
  });
}

it("revocations reach existing providers and cannot be resurrected by an unrelated write", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "token-revocation-"));
  try {
    const a = await open(root);
    const issued = await run(a.issue("repo", "write", 300));
    const b = await open(root);
    assert.equal(await run(b.revoke("repo", issued.id)), true);
    assert.equal(await run(a.verify("repo", issued.plaintext)), null);
    await run(a.issue("repo", "read", 300));
    assert.equal(await run((await open(root)).verify("repo", issued.plaintext)), null);
    assert.equal((await fs.stat(path.join(root, ".tokens.json"))).mode & 0o777, 0o600);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

it("a live second process observes revocation before it issues another token", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "token-process-"));
  const a = await open(root);
  const issued = await run(a.issue("repo", "write", 300));
  const child = spawn(
    process.execPath,
    [
      "--input-type=module",
      "-e",
      `
    import { Effect } from "effect";
    import { createInterface } from "node:readline";
    import { Tokens, tokensNode } from ${JSON.stringify(moduleUrl)};
    const root = process.argv[1];
    const tokens = await Effect.runPromise(Tokens.pipe(Effect.provide(tokensNode(root))));
    const lines = createInterface({ input: process.stdin });
    console.log("ready");
    for await (const token of lines) {
      console.log(JSON.stringify(await Effect.runPromise(tokens.verify("repo", token))));
      await Effect.runPromise(tokens.issue("repo", "read", 300));
      break;
    }
    lines.close();
  `,
      root,
    ],
    { stdio: ["pipe", "pipe", "pipe"] },
  );
  const ready = Promise.withResolvers<void>();
  let output = "";
  let error = "";
  child.stdout.on("data", (chunk: Buffer) => {
    output += chunk.toString();
    if (output.includes("ready\n")) ready.resolve();
  });
  child.stderr.on("data", (chunk: Buffer) => {
    error += chunk.toString();
  });
  const exited = new Promise<void>((resolve, reject) => {
    child.once("error", reject);
    child.once("close", (code) => (code === 0 ? resolve() : reject(new Error(error))));
  });
  void exited.then(() => ready.reject(new Error("child exited before ready")), ready.reject);
  try {
    await ready.promise;
    await run(a.revoke("repo", issued.id));
    child.stdin.end(`${issued.plaintext}\n`);
    await exited;
    assert.equal(output, "ready\nnull\n");
    assert.equal(await run((await open(root)).verify("repo", issued.plaintext)), null);
  } finally {
    child.stdin.end();
    await exited;
    await fs.rm(root, { recursive: true, force: true });
  }
});

it("concurrent processes preserve every acknowledged issuance", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "token-issuance-"));
  try {
    const program = `
      import { Effect, Schedule } from "effect";
      import { Tokens, tokensNode } from ${JSON.stringify(moduleUrl)};
      const tokens = await Effect.runPromise(Tokens.pipe(Effect.provide(tokensNode(process.argv[1]))));
      for (let i = 0; i < 10; i++) {
        await Effect.runPromise(tokens.issue("repo", "read", 300).pipe(
          Effect.retry({
            times: 1000,
            schedule: Schedule.spaced("10 millis"),
            while: error => error.message.startsWith("PRECONDITION_FAILED:"),
          })));
      }
    `;
    // Every writer must exit before cleanup, even when one fails. Otherwise
    // cleanup races live writers and hides their real error behind ENOTEMPTY.
    const writers = await Promise.allSettled(
      Array.from({ length: 4 }, () =>
        promisify(execFile)(process.execPath, ["--input-type=module", "-e", program, root], {
          timeout: 20_000,
        }),
      ),
    );
    for (const writer of writers) {
      if (writer.status === "rejected") throw writer.reason;
    }
    assert.equal((await run((await open(root)).list("repo"))).total, 40);
    assert.deepEqual(await fs.readdir(root), [".tokens.json"]);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

it("a failed replacement preserves existing revocations and releases the lock", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "token-write-failure-"));
  try {
    const tokens = await open(root);
    const issued = await run(tokens.issue("repo", "write", 300));
    await run(tokens.revoke("repo", issued.id));
    const target = path.join(root, ".tokens.json");
    const before = await fs.readFile(target, "utf8");
    const program = `
      import fs from "node:fs/promises";
      import { syncBuiltinESMExports } from "node:module";
      import { Effect } from "effect";
      import { Tokens, tokensNode } from ${JSON.stringify(moduleUrl)};
      const tokens = await Effect.runPromise(Tokens.pipe(Effect.provide(tokensNode(process.argv[1]))));
      fs.rename = async () => { throw Object.assign(new Error("fixture failure"), { code: "EIO" }); };
      syncBuiltinESMExports();
      console.log(await Effect.runPromise(tokens.issue("repo", "read", 300).pipe(
        Effect.match({ onSuccess: () => "unexpected success", onFailure: error => error._tag }))));
    `;
    const result = await promisify(execFile)(process.execPath, [
      "--input-type=module",
      "-e",
      program,
      root,
    ]);
    assert.equal(result.stdout.trim(), "ArtifactsError");
    assert.equal(await fs.readFile(target, "utf8"), before);
    assert.equal(await run((await open(root)).verify("repo", issued.plaintext)), null);
    assert.deepEqual(await fs.readdir(root), [".tokens.json"]);
    await run(tokens.issue("repo", "read", 300));
    assert.equal(await run(tokens.verify("repo", issued.plaintext)), null);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

it("corrupt or inaccessible token metadata fails closed without rewriting it", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "token-corrupt-"));
  const target = path.join(root, ".tokens.json");
  try {
    const tokens = await open(root);
    const issued = await run(tokens.issue("repo", "write", 300));
    const valid = await fs.readFile(target, "utf8");
    for (const content of [
      "{",
      "{}",
      "[{}]",
      valid.replace(/"expiresAt":"[^"]*"/, '"expiresAt":"invalid"'),
    ]) {
      await fs.writeFile(target, content);
      for (const operation of [
        tokens.verify("repo", issued.plaintext).pipe(Effect.asVoid),
        tokens.issue("repo", "read", 300).pipe(Effect.asVoid),
        tokens.revoke("repo", issued.id).pipe(Effect.asVoid),
        tokens.list("repo").pipe(Effect.asVoid),
      ]) {
        const result = await run(Effect.result(operation.pipe(Effect.asVoid)));
        assert.ok(Result.isFailure(result));
        assert.equal(result.failure._tag, "ArtifactsError");
      }
      assert.equal(await fs.readFile(target, "utf8"), content);
    }
    await fs.rm(target);
    await fs.mkdir(target);
    assert.ok(Result.isFailure(await run(Effect.result(tokens.list("repo")))));
    assert.ok(Result.isFailure(await run(Effect.result(tokens.issue("repo", "read", 300)))));
    await fs.rm(target, { recursive: true });
    await fs.writeFile(target, valid);
    await fs.writeFile(`${target}.lock`, "held by another writer");
    assert.ok(Result.isFailure(await run(Effect.result(tokens.revoke("repo", issued.id)))));
    assert.equal(await fs.readFile(target, "utf8"), valid);
    assert.equal(await run(tokens.verify("repo", issued.plaintext)), "write");
    await fs.rm(`${target}.lock`);
    await run(tokens.revoke("repo", issued.id));
    assert.equal(await run(tokens.verify("repo", issued.plaintext)), null);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
