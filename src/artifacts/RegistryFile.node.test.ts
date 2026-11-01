import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { it } from "@effect/vitest";
import { Effect } from "effect";
import { Registry, registryNode, RepoStores, repoStoresNode } from "./Namespace.ts";

const open = (root: string) => Effect.runPromise(Registry.pipe(Effect.provide(registryNode(root))));
const meta = { description: null, defaultBranch: "main", readOnly: false, source: null };
const child = `
import fs from "node:fs/promises";
import path from "node:path";
import { syncBuiltinESMExports } from "node:module";
import { Effect } from "effect";
import { Registry, registryNode } from ${JSON.stringify(new URL("./Namespace.ts", import.meta.url).href)};
const [root, fault, operation] = process.argv.slice(1);
const target = path.join(root, ".registry.json");
const open = fs.open;
fs.open = async function (...args) {
  const handle = await Reflect.apply(open, fs, args);
  const name = String(args[0]);
  const stat = await handle.stat();
  const ancestor = await fs.stat(path.dirname(root));
  if ((fault === "temporary" && name.startsWith(target + ".") && name.endsWith(".tmp")) ||
      (fault === "ancestor" && stat.isDirectory() && stat.dev === ancestor.dev && stat.ino === ancestor.ino)) {
    handle.sync = async () => { throw Object.assign(new Error("fixture sync failure"), {code: "EIO"}); };
  }
  return handle;
};
syncBuiltinESMExports();
const registry = await Effect.runPromise(Registry.pipe(Effect.provide(registryNode(root))));
const action = operation === "create"
  ? registry.create("created", ${JSON.stringify(meta)})
  : operation === "delete" ? registry.delete("existing") : registry.setDefaultBranch("existing", "topic");
console.log(await Effect.runPromise(action.pipe(Effect.match({onSuccess: () => "acknowledged", onFailure: error => error._tag}))));
`;
const failedEdit = async (root: string, fault: string, operation: string) =>
  (
    await promisify(execFile)(
      process.execPath,
      ["--input-type=module", "-e", child, root, fault, operation],
      { timeout: 20_000 },
    )
  ).stdout.trim();

it("repository metadata preserves old bytes on temporary-file sync failure", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "registry-file-sync-"));
  try {
    const registry = await open(root);
    await Effect.runPromise(registry.create("existing", meta));
    const target = path.join(root, ".registry.json");
    const before = await fs.readFile(target, "utf8");
    assert.equal(await failedEdit(root, "temporary", "create"), "ArtifactsError");
    assert.equal(await fs.readFile(target, "utf8"), before);
    assert.deepEqual(await fs.readdir(root), [".registry.json"]);
    await Effect.runPromise(registry.create("created", meta));
    assert.equal((await fs.stat(target)).mode & 0o777, 0o600);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

for (const operation of ["delete", "branch"]) {
  it(`repository ${operation} retries sync even when the earlier mutation is already visible`, async () => {
    const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "registry-ancestor-"));
    const root = path.join(temporary, "new", "namespace");
    try {
      await Effect.runPromise((await open(root)).create("existing", meta));
      for (let attempt = 0; attempt < 2; attempt++) {
        assert.equal(await failedEdit(root, "ancestor", operation), "ArtifactsError");
        const record = await Effect.runPromise((await open(root)).get("existing"));
        if (operation === "delete") assert.equal(record, null);
        else assert.equal(record?.defaultBranch, "topic");
        assert.deepEqual(await fs.readdir(root), [".registry.json"]);
      }
      const registry = await open(root);
      if (operation === "delete")
        assert.equal(await Effect.runPromise(registry.delete("existing")), false);
      else await Effect.runPromise(registry.setDefaultBranch("existing", "topic"));
      assert.equal((await fs.stat(root)).mode & 0o777, 0o700);
    } finally {
      await fs.rm(temporary, { recursive: true, force: true });
    }
  });
}

for (const fault of ["temporary", "directory"]) {
  it(`fork registry ${fault} sync failure is retried after reopening`, async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "fork-registry-sync-"));
    try {
      await fs.mkdir(path.join(root, "parent", "objects", "info"), { recursive: true });
      const program = `
        import fs from "node:fs/promises";
        import path from "node:path";
        import { syncBuiltinESMExports } from "node:module";
        import { Effect } from "effect";
        import { RepoStores, repoStoresNode } from ${JSON.stringify(new URL("./Namespace.ts", import.meta.url).href)};
        const [root, fault] = process.argv.slice(1);
        const target = path.join(root, ".forks.json");
        let publishing = false;
        const rename = fs.rename;
        fs.rename = async (from, to) => {
          await rename(from, to);
          if (to === target) publishing = true;
        };
        const open = fs.open;
        fs.open = async function (...args) {
          const handle = await Reflect.apply(open, fs, args);
          const stat = await handle.stat();
          const expected = await fs.stat(root);
          const name = String(args[0]);
          if (name === target) publishing = true;
          if ((fault === "temporary" && name.startsWith(target + ".") && name.endsWith(".tmp")) ||
              (fault === "directory" && publishing && stat.isDirectory() && stat.dev === expected.dev && stat.ino === expected.ino)) {
            handle.sync = async () => { throw Object.assign(new Error("fixture sync failure"), {code: "EIO"}); };
          }
          return handle;
        };
        syncBuiltinESMExports();
        console.log(await Effect.runPromise(RepoStores.use(stores => stores.fork("child", "parent")).pipe(
          Effect.provide(repoStoresNode(root)),
          Effect.match({onSuccess: () => "acknowledged", onFailure: error => error._tag}),
        )));
      `;
      for (let attempt = 0; attempt < 2; attempt++) {
        const result = await promisify(execFile)(
          process.execPath,
          ["--input-type=module", "-e", program, root, fault],
          { timeout: 20_000 },
        );
        assert.equal(result.stdout.trim(), "ArtifactsError");
        assert.ok(
          !(await fs.readdir(root)).some((name) => name.endsWith(".tmp") || name.endsWith(".lock")),
        );
        if (fault === "temporary")
          await assert.rejects(fs.stat(path.join(root, ".forks.json")), { code: "ENOENT" });
        else
          assert.equal(
            JSON.parse(await fs.readFile(path.join(root, ".forks.json"), "utf8")).child,
            "parent",
          );
      }
      await Effect.runPromise(
        RepoStores.use((stores) => stores.fork("child", "parent")).pipe(
          Effect.provide(repoStoresNode(root)),
        ),
      );
      assert.equal((await fs.stat(path.join(root, ".forks.json"))).mode & 0o777, 0o600);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
}

for (const damage of ["json", "field", "name", "id", "empty-id", "empty-branch", "date"] as const) {
  it(`repository registry corruption preserves evidence and redacts diagnostics: ${damage}`, async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "private-registry-corruption-"));
    const secret = "private-registry-fixture-secret";
    try {
      const registry = await open(root);
      const original = await Effect.runPromise(
        registry.create("existing", { ...meta, readOnly: true }),
      );
      const target = path.join(root, ".registry.json");
      const good = await fs.readFile(target, "utf8");
      const rows =
        damage === "name"
          ? [original, { ...original, id: "other", readOnly: false }]
          : damage === "id"
            ? [original, { ...original, name: "other" }]
            : damage === "empty-id"
              ? [{ ...original, id: "" }]
              : damage === "empty-branch"
                ? [{ ...original, defaultBranch: "" }]
                : damage === "date"
                  ? [{ ...original, createdAt: secret }]
                  : [{ ...original, readOnly: secret }];
      const damaged = damage === "json" ? `[${secret}` : JSON.stringify(rows);
      await fs.writeFile(target, damaged);
      for (const operation of [
        registry.get("existing").pipe(Effect.asVoid),
        registry.list().pipe(Effect.asVoid),
        registry.create("new", meta).pipe(Effect.asVoid),
        registry.delete("existing").pipe(Effect.asVoid),
        registry.setDefaultBranch("existing", "topic"),
        registry.touch("existing", new Date()),
        registry.finish("existing", original.id),
      ]) {
        const result = await Effect.runPromise(operation.pipe(Effect.result));
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") {
          assert.equal(result.failure._tag, "ArtifactsError");
          const diagnostic = `${result.failure.message} ${String(result.failure.cause)} ${JSON.stringify(result.failure)}`;
          assert.ok(
            !diagnostic.includes(secret),
            "diagnostics must not quote malformed private fields",
          );
          assert.ok(
            !diagnostic.includes(root),
            "diagnostics must not expose the private namespace path",
          );
        }
        assert.equal(await fs.readFile(target, "utf8"), damaged);
        assert.deepEqual(await fs.readdir(root), [".registry.json"]);
      }
      await fs.writeFile(target, good);
      assert.deepEqual(await Effect.runPromise((await open(root)).get("existing")), original);
      await Effect.runPromise(registry.setDefaultBranch("existing", "topic"));
      assert.equal((await Effect.runPromise(registry.get("existing")))?.readOnly, true);
      assert.equal((await Effect.runPromise(registry.get("existing")))?.defaultBranch, "topic");
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
}

it("repository registry contention retains a safe retry code and the owner's lock", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "private-registry-lock-"));
  try {
    const registry = await open(root);
    const lock = path.join(root, ".registry.json.lock");
    await fs.writeFile(lock, "private-lock-owner");
    const result = await Effect.runPromise(registry.create("repo", meta).pipe(Effect.result));
    assert.equal(result._tag, "Failure");
    if (result._tag === "Failure") {
      assert.match(result.failure.message, /^PRECONDITION_FAILED:/);
      assert.ok(result.failure.cause instanceof Error && "code" in result.failure.cause);
      assert.equal(result.failure.cause.code, "EEXIST");
      assert.ok(!JSON.stringify(result.failure).includes(root));
      assert.ok(!String(result.failure.cause).includes(root));
    }
    assert.equal(await fs.readFile(lock, "utf8"), "private-lock-owner");
    await fs.rm(lock);
    await Effect.runPromise(registry.create("repo", meta));
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
