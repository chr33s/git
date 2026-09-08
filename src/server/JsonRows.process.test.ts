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
  if (fault) {
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

  for (const fault of ["readFileSync", "writeFileSync", "renameSync"] as const) {
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
}
