import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { it } from "@effect/vitest";
import { Effect, Result } from "effect";

import { file as remotesFile } from "./Remotes.node.ts";
import { Remotes } from "./Remotes.ts";
import { file as subscribersFile } from "./Subscribers.node.ts";
import { Subscribers } from "./Subscribers.ts";

type Kind = "remotes" | "subscribers";
const url = (name: string) => `https://example.com/${name}`;
const open = Effect.fn("test.JsonRows.open")(function* (kind: Kind, file: string) {
  if (kind === "remotes") {
    const registry = yield* Remotes.pipe(Effect.provide(remotesFile(file)));
    return {
      add: (name: string) =>
        registry.add({ name, url: url(name) }).pipe(Effect.map((row) => row.name)),
      remove: registry.remove,
      list: registry.list.pipe(Effect.map((rows) => rows.map((row) => row.url))),
    };
  }
  const registry = yield* Subscribers.pipe(Effect.provide(subscribersFile(file)));
  return {
    add: (name: string) =>
      registry
        .add({ url: url(name), secret: "fixture-only-secret" })
        .pipe(Effect.map((row) => row.id)),
    remove: registry.remove,
    list: registry.list.pipe(Effect.map((rows) => rows.map((row) => row.url))),
  };
});

// Pause a real writer after it reads the file. The parent changes the same
// registry while the child holds that snapshot, then releases its stdin gate.
const writer = `
  import fs from "node:fs";
  import path from "node:path";
  import { syncBuiltinESMExports } from "node:module";
  import { Effect } from "effect";
  import { file as remotesFile } from ${JSON.stringify(new URL("./Remotes.node.ts", import.meta.url).href)};
  import { Remotes } from ${JSON.stringify(new URL("./Remotes.ts", import.meta.url).href)};
  import { file as subscribersFile } from ${JSON.stringify(new URL("./Subscribers.node.ts", import.meta.url).href)};
  import { Subscribers } from ${JSON.stringify(new URL("./Subscribers.ts", import.meta.url).href)};
  const [kind, file, fault] = process.argv.slice(1);
  const registry = await Effect.runPromise(kind === "remotes"
    ? Remotes.pipe(Effect.provide(remotesFile(file)))
    : Subscribers.pipe(Effect.provide(subscribersFile(file))));
  if (fault === "fsyncFile" || fault === "fsyncDirectory" || fault === "fsyncAncestor") {
    let published = false;
    const rename = fs.renameSync;
    fs.renameSync = function (...args) {
      const result = Reflect.apply(rename, fs, args);
      if (args[1] === file) published = true;
      return result;
    };
    const sync = fs.fsyncSync;
    fs.fsyncSync = function (descriptor) {
      const stat = fs.fstatSync(descriptor);
      const ancestor = fs.statSync(path.dirname(path.dirname(file)));
      if ((fault === "fsyncFile" && !stat.isDirectory()) ||
          (fault === "fsyncDirectory" && stat.isDirectory() && published) ||
          (fault === "fsyncAncestor" && stat.isDirectory() && stat.dev === ancestor.dev && stat.ino === ancestor.ino)) {
        throw Object.assign(new Error("fixture sync failure"), { code: "EIO" });
      }
      return sync(descriptor);
    };
  } else if (fault) {
    const original = fs[fault];
    fs[fault] = function (...args) {
      if (args[0] === file || String(args[0]).startsWith(file + ".")) {
        throw Object.assign(new Error("fixture I/O failure"), { code: "EIO" });
      }
      return Reflect.apply(original, fs, args);
    };
  } else {
    const read = fs.readFileSync;
    let paused = false;
    fs.readFileSync = function (...args) {
      const contents = Reflect.apply(read, fs, args);
      if (args[0] === file && !paused) {
        paused = true;
        fs.writeSync(1, "ready\\n");
        fs.readSync(0, Buffer.alloc(1), 0, 1, null);
      }
      return contents;
    };
  }
  syncBuiltinESMExports();
  const edit = registry.add(kind === "remotes"
    ? { name: "child", url: "https://example.com/child" }
    : { url: "https://example.com/child", secret: "fixture-only-secret" });
  if (fault) {
    console.log(await Effect.runPromise(edit.pipe(Effect.match({
      onSuccess: () => "unexpected success",
      onFailure: error => error._tag + ":" + error.cause?.code,
    }))));
  } else await Effect.runPromise(edit);
`;

for (const kind of ["remotes", "subscribers"] as const) {
  it(`${kind} retry ancestor directory sync after reopening a failed creation`, async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "json-rows-ancestor-"));
    const location = path.join(root, "new", "nested", `${kind}.json`);
    try {
      for (let attempt = 0; attempt < 2; attempt++) {
        const result = await promisify(execFile)(
          process.execPath,
          ["--input-type=module", "-e", writer, kind, location, "fsyncAncestor"],
          { timeout: 20_000 },
        );
        assert.equal(result.stdout.trim(), "StorageFailure:EIO");
        assert.deepEqual(
          await fs.readdir(path.dirname(location)),
          [],
          "failed directory preparation must not publish or leave a reservation",
        );
      }
      const registry = await Effect.runPromise(open(kind, location));
      await Effect.runPromise(registry.add("healthy"));
      assert.deepEqual(await Effect.runPromise(registry.list), [url("healthy")]);
      assert.equal((await fs.stat(location)).mode & 0o777, 0o600);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it(`${kind} refuse malformed metadata and preserve its original bytes`, async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "json-rows-corrupt-"));
    const location = path.join(root, `${kind}.json`);
    try {
      const registry = await Effect.runPromise(open(kind, location));
      await Effect.runPromise(registry.add("existing"));
      const valid = await fs.readFile(location, "utf8");
      for (const contents of [
        "[",
        "{}",
        "[null]",
        "[{}]",
        valid.replace(/"createdAt": "[^"]*"/, '"createdAt": "not-a-date"'),
      ]) {
        await fs.writeFile(location, contents);
        for (const operation of [
          registry.list.pipe(Effect.asVoid),
          registry.add("new").pipe(Effect.asVoid),
          registry.remove("existing").pipe(Effect.asVoid),
        ]) {
          const result = await Effect.runPromise(Effect.result(operation));
          assert.ok(Result.isFailure(result));
          assert.equal(result.failure._tag, "StorageFailure");
          assert.doesNotMatch(String(result.failure), /fixture-only-secret/);
        }
        assert.equal(await fs.readFile(location, "utf8"), contents);
        assert.deepEqual(await fs.readdir(root), [`${kind}.json`]);
      }
      await fs.rm(location);
      await fs.mkdir(location);
      assert.ok(Result.isFailure(await Effect.runPromise(Effect.result(registry.list))));
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it(`${kind} keep credential files private with a permissive umask and after replacement`, async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "json-rows-mode-"));
    const location = path.join(root, "private", `${kind}.json`);
    try {
      const program = `
        import fs from "node:fs";
        import { Effect } from "effect";
        import { file as remotesFile } from ${JSON.stringify(new URL("./Remotes.node.ts", import.meta.url).href)};
        import { Remotes } from ${JSON.stringify(new URL("./Remotes.ts", import.meta.url).href)};
        import { file as subscribersFile } from ${JSON.stringify(new URL("./Subscribers.node.ts", import.meta.url).href)};
        import { Subscribers } from ${JSON.stringify(new URL("./Subscribers.ts", import.meta.url).href)};
        process.umask(0);
        const [kind, location] = process.argv.slice(1);
        const service = await Effect.runPromise(kind === "remotes"
          ? Remotes.pipe(Effect.provide(remotesFile(location)))
          : Subscribers.pipe(Effect.provide(subscribersFile(location))));
        const add = name => Effect.runPromise(service.add(kind === "remotes"
          ? {name, url:"https://example.com/", credential:"fixture-only-secret"}
          : {url:"https://example.com/", secret:"fixture-only-secret"}));
        await add("one");
        const modes = [fs.statSync(location).mode & 0o777];
        await add("two");
        modes.push(fs.statSync(location).mode & 0o777);
        fs.chmodSync(location, 0o644);
        await add("three");
        modes.push(fs.statSync(location).mode & 0o777);
        console.log(JSON.stringify(modes));
      `;
      const result = await promisify(execFile)(process.execPath, [
        "--input-type=module",
        "-e",
        program,
        kind,
        location,
      ]);
      assert.deepEqual(JSON.parse(result.stdout), [0o600, 0o600, 0o600]);
      assert.equal((await fs.stat(path.dirname(location))).mode & 0o777, 0o700);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  for (const operation of ["add", "remove"] as const) {
    it(`${kind} preserve acknowledged changes across a competing ${operation}`, async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "json-rows-process-"));
      const location = path.join(root, `${kind}.json`);
      const registry = await Effect.runPromise(open(kind, location));
      const existing = await Effect.runPromise(registry.add("existing"));
      const ready = Promise.withResolvers<void>();
      const child = spawn(process.execPath, ["--input-type=module", "-e", writer, kind, location], {
        stdio: ["pipe", "pipe", "pipe"],
      });
      let stderr = "";
      child.stderr.on("data", (chunk: Buffer) => {
        stderr += chunk.toString();
      });
      child.stdout.on("data", (chunk: Buffer) => {
        if (chunk.toString().includes("ready")) ready.resolve();
      });
      const completed = new Promise<void>((resolve, reject) => {
        child.on("error", reject);
        child.on("close", (code) => (code === 0 ? resolve() : reject(new Error(stderr))));
      });
      void completed.then(
        () => ready.reject(new Error("writer completed without pausing")),
        (cause: unknown) => ready.reject(cause),
      );
      try {
        await ready.promise;
        const mutation =
          operation === "add"
            ? registry.add("parent").pipe(Effect.asVoid)
            : registry.remove(existing).pipe(Effect.asVoid);
        const result = await Effect.runPromise(Effect.result(mutation));
        if (Result.isFailure(result)) {
          assert.equal((await fs.stat(`${location}.lock`)).isFile(), true);
          assert.deepEqual(await Effect.runPromise(registry.list), [url("existing")]);
        }
        child.stdin.end("\n");
        await completed;
        const expected =
          operation === "add" ? [url("child"), url("existing"), url("parent")] : [url("child")];
        if (Result.isSuccess(result)) {
          assert.deepEqual(
            (await Effect.runPromise(registry.list)).sort(),
            expected,
            "every acknowledged edit must survive the competing writer",
          );
        }
        // Contention must be reported, rather than acknowledging a change
        // that the writer holding the old snapshot can silently overwrite.
        assert.equal(Result.isFailure(result), true, "the second writer must report contention");
        if (Result.isFailure(result)) {
          assert.equal(result.failure._tag, "StorageFailure");
          if (result.failure._tag === "StorageFailure")
            assert.match(String(result.failure.cause), /EEXIST/);
        }
        assert.deepEqual((await Effect.runPromise(registry.list)).sort(), [
          url("child"),
          url("existing"),
        ]);
        await Effect.runPromise(mutation);
        assert.deepEqual((await Effect.runPromise(registry.list)).sort(), expected);
        assert.equal(await Effect.runPromise(registry.remove("absent")), false);
        assert.deepEqual(await fs.readdir(root), [`${kind}.json`]);
      } finally {
        if (!child.stdin.writableEnded) child.stdin.end("\n");
        await completed;
        await fs.rm(root, { recursive: true, force: true });
      }
    });
  }

  for (const fault of ["readFileSync", "writeFileSync", "renameSync", "fsyncFile"] as const) {
    it(`${kind} release reservations and preserve rows after ${fault} fails`, async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "json-rows-failure-"));
      const location = path.join(root, `${kind}.json`);
      try {
        const registry = await Effect.runPromise(open(kind, location));
        await Effect.runPromise(registry.add("existing"));
        const original = await fs.readFile(location, "utf8");
        const result = await promisify(execFile)(
          process.execPath,
          ["--input-type=module", "-e", writer, kind, location, fault],
          { timeout: 20_000 },
        );
        assert.equal(result.stdout.trim(), "StorageFailure:EIO");
        assert.equal(await fs.readFile(location, "utf8"), original);
        assert.deepEqual(await fs.readdir(root), [`${kind}.json`]);
        await Effect.runPromise(registry.add("child"));
        assert.deepEqual((await Effect.runPromise(registry.list)).sort(), [
          url("child"),
          url("existing"),
        ]);
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    });
  }

  it(`${kind} report directory-sync failure after publication without erasing the new rows`, async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "json-rows-sync-"));
    const location = path.join(root, `${kind}.json`);
    try {
      const registry = await Effect.runPromise(open(kind, location));
      await Effect.runPromise(registry.add("existing"));
      const result = await promisify(execFile)(
        process.execPath,
        ["--input-type=module", "-e", writer, kind, location, "fsyncDirectory"],
        { timeout: 20_000 },
      );
      assert.equal(result.stdout.trim(), "StorageFailure:EIO");
      const reopened = await Effect.runPromise(open(kind, location));
      assert.deepEqual((await Effect.runPromise(reopened.list)).sort(), [
        url("child"),
        url("existing"),
      ]);
      assert.deepEqual(await fs.readdir(root), [`${kind}.json`]);
      await Effect.runPromise(reopened.add("later"));
      assert.equal((await Effect.runPromise(reopened.list)).length, 3);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
}
