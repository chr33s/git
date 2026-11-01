import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { it } from "@effect/vitest";
import { Effect } from "effect";
import { hashObject } from "./Format.ts";

const previous = "1111111111111111111111111111111111111111";
const next = "2222222222222222222222222222222222222222";
const data = new TextEncoder().encode("sync fixture");

// Inject only the filesystem sync boundary in an isolated child. Real writes,
// renames, handles, locks, and store error/rollback paths still execute.
const child = `
import fs from "node:fs/promises";
import path from "node:path";
import { syncBuiltinESMExports } from "node:module";
import { Effect } from "effect";
import { stores, filePacks, initializeBare } from ${JSON.stringify(new URL("./Node.ts", import.meta.url).href)};
import { ObjectStore, RefStore } from ${JSON.stringify(new URL("./Store.ts", import.meta.url).href)};
const [root, target, mode, fault] = process.argv.slice(1);
let armed = mode !== "cache-object";
const open = fs.open;
fs.open = async function (...args) {
  const handle = await Reflect.apply(open, fs, args);
  const name = String(args[0]);
  const stat = await handle.stat();
  const ancestorPath = mode === "cache-object" ? path.dirname(path.dirname(root)) : path.dirname(root);
  const ancestor = fault === "ancestor" && armed ? await fs.stat(ancestorPath) : undefined;
  if ((fault === "file" && name.startsWith(target + ".") && name.endsWith(".tmp")) ||
      (fault === "directory" && name === path.dirname(target)) ||
      (ancestor && stat.isDirectory() && stat.dev === ancestor.dev && stat.ino === ancestor.ino)) {
    handle.sync = async function () {
      throw Object.assign(new Error("fixture fsync failure"), { code: "EIO" });
    };
  }
  return handle;
};
syncBuiltinESMExports();
const operation = mode === "initialize"
  ? initializeBare(root).pipe(Effect.as("initialized"))
  : mode === "remove-object"
  ? ObjectStore.use(store => store.delete(path.basename(path.dirname(target)) + path.basename(target))).pipe(Effect.as("deleted"))
  : mode === "remove-pack"
  ? filePacks(root).delete("fixture").pipe(Effect.as("deleted"))
  : mode === "object" || mode === "cache-object"
  ? ObjectStore.use(store => store.write({ type: "blob", data: new TextEncoder().encode("sync fixture") })).pipe(Effect.as("written"))
  : RefStore.use(store => store.apply([{ name: "refs/heads/main", value: ${JSON.stringify(next)}, expected: ${JSON.stringify(previous)} }], { atomic: true })).pipe(Effect.map(rows => rows[0].applied ? "written" : "rejected"));
if (mode === "cache-object") {
  await Effect.runPromise(operation.pipe(Effect.provide(stores(root))));
  const parent = path.dirname(root);
  await fs.rename(parent, parent + ".moved");
  await fs.rename(parent + ".moved", parent);
  armed = true;
}
console.log(await Effect.runPromise(operation.pipe(
  Effect.provide(stores(root)),
  Effect.match({ onSuccess: value => value, onFailure: error => error._tag + ":" + error.operation }),
)));
`;

const run = async (root: string, target: string, mode: string, fault: string) => {
  const result = await promisify(execFile)(
    process.execPath,
    ["--input-type=module", "-e", child, root, target, mode, fault],
    { timeout: 20_000 },
  );
  // The final line is the child result; storage diagnostics precede it.
  return result.stdout.trim().split("\n").at(-1);
};

it("loose-object publication and existing-object retries repeat failed ancestor syncs", async () => {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "git-object-ancestor-"));
  const root = path.join(temporary, "new", "outer", "repo");
  const oid = await Effect.runPromise(hashObject({ type: "blob", data }));
  const target = path.join(root, "objects", oid.slice(0, 2), oid.slice(2));
  try {
    for (let attempt = 0; attempt < 3; attempt++) {
      assert.equal(await run(root, target, "object", "ancestor"), "StorageFailure:write");
    }
    assert.equal(await run(root, target, "object", "none"), "written");
    assert.deepEqual(await fs.readdir(path.dirname(target)), [oid.slice(2)]);
  } finally {
    await fs.rm(temporary, { recursive: true, force: true });
  }
});

it("cached durability rechecks changed ancestors above an unchanged object directory", async () => {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "git-object-cache-"));
  const root = path.join(temporary, "new", "outer", "repo");
  const oid = await Effect.runPromise(hashObject({ type: "blob", data }));
  const target = path.join(root, "objects", oid.slice(0, 2), oid.slice(2));
  try {
    assert.equal(await run(root, target, "cache-object", "ancestor"), "StorageFailure:write");
    assert.equal(await run(root, target, "object", "none"), "written");
  } finally {
    await fs.rm(temporary, { recursive: true, force: true });
  }
});

it("bare initialization retries do not infer durability from existing directories or HEAD", async () => {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "git-init-ancestor-"));
  const root = path.join(temporary, "new", "outer", "repo");
  try {
    assert.equal(
      await run(root, path.join(root, "HEAD"), "initialize", "ancestor"),
      "StorageFailure:initialize",
    );
    assert.equal(
      await run(root, path.join(root, "HEAD"), "initialize", "ancestor"),
      "StorageFailure:initialize",
    );
    assert.equal(await run(root, path.join(root, "HEAD"), "initialize", "none"), "initialized");
    assert.equal(
      await run(root, path.join(root, "HEAD"), "initialize", "ancestor"),
      "StorageFailure:initialize",
    );
    assert.equal(await fs.readFile(path.join(root, "HEAD"), "utf8"), "ref: refs/heads/main\n");
  } finally {
    await fs.rm(temporary, { recursive: true, force: true });
  }
});

for (const fault of ["file", "directory"] as const) {
  it(`loose-object ${fault} sync failure cannot acknowledge publication or a retry`, async () => {
    const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "git-object-sync-")));
    const oid = await Effect.runPromise(hashObject({ type: "blob", data }));
    const target = path.join(root, "objects", oid.slice(0, 2), oid.slice(2));
    try {
      await fs.mkdir(path.dirname(target), { recursive: true });
      assert.equal(await run(root, target, "object", fault), "StorageFailure:write");
      const names = await fs.readdir(path.dirname(target));
      assert.deepEqual(names, fault === "file" ? [] : [oid.slice(2)]);
      if (fault === "directory") {
        assert.equal(
          await run(root, target, "object", fault),
          "StorageFailure:write",
          "an existing object must still retry the failed sync",
        );
      }
      assert.equal(await run(root, target, "object", "none"), "written");
      assert.deepEqual(await fs.readdir(path.dirname(target)), [oid.slice(2)]);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it(`atomic ref ${fault} sync failure does not acknowledge an update`, async () => {
    const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "git-ref-sync-")));
    const target = path.join(root, "refs", "heads", "main");
    try {
      await fs.mkdir(path.dirname(target), { recursive: true });
      await fs.writeFile(target, `${previous}\n`);
      assert.equal(
        await run(root, target, "ref", fault),
        fault === "file" ? "rejected" : "StorageFailure:rollback",
      );
      assert.equal(await fs.readFile(target, "utf8"), `${previous}\n`);
      assert.deepEqual(await fs.readdir(path.dirname(target)), ["main"]);
      assert.equal(await run(root, target, "ref", "none"), "written");
      assert.equal(await fs.readFile(target, "utf8"), `${next}\n`);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
}

for (const mode of ["remove-object", "remove-pack"] as const) {
  it(`${mode} retries directory sync even after an earlier unlink succeeded`, async () => {
    const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "git-remove-sync-")));
    const oid = await Effect.runPromise(hashObject({ type: "blob", data }));
    const target =
      mode === "remove-object"
        ? path.join(root, "objects", oid.slice(0, 2), oid.slice(2))
        : path.join(root, "objects", "pack", "fixture.pack");
    try {
      await fs.mkdir(path.dirname(target), { recursive: true });
      await fs.writeFile(target, "unreferenced fixture");
      if (mode === "remove-pack")
        await fs.writeFile(path.join(path.dirname(target), "fixture.idx"), "index fixture");
      const failure =
        mode === "remove-object" ? "StorageFailure:delete" : "StorageFailure:packs.delete";
      assert.equal(await run(root, target, mode, "directory"), failure);
      await assert.rejects(fs.stat(target), { code: "ENOENT" });
      assert.equal(await run(root, target, mode, "directory"), failure);
      assert.equal(await run(root, target, mode, "none"), "deleted");
      assert.deepEqual(await fs.readdir(path.dirname(target)), []);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
}
