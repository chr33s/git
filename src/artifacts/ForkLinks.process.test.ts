import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { it } from "@effect/vitest";
import { Effect, Schema } from "effect";

it.effect("separate processes preserve every acknowledged fork link", () =>
  Effect.promise(async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "fork-link-processes-"));
    const script = `
      import { Effect } from "effect";
      import { RepoStores, repoStoresNode } from ${JSON.stringify(new URL("./Namespace.ts", import.meta.url).href)};
      const stores = await Effect.runPromise(RepoStores.pipe(Effect.provide(repoStoresNode(process.argv[1]))));
      for (let index = 0; index < 12; index++) {
        for (let attempt = 0; ; attempt++) {
          try {
            await Effect.runPromise(stores.fork(process.argv[2] + "-" + index, "parent"));
            break;
          } catch (error) {
            if (!error.message.includes("EEXIST") || attempt >= 500) throw error;
            await new Promise(resolve => setTimeout(resolve, 5));
          }
        }
      }
    `;
    try {
      await fs.mkdir(path.join(root, "parent", "objects", "info"), { recursive: true });
      const results = await Promise.allSettled(
        ["first", "second"].map((name) =>
          promisify(execFile)(process.execPath, ["--input-type=module", "-e", script, root, name], {
            timeout: 20_000,
          }),
        ),
      );
      for (const result of results) {
        if (result.status === "rejected") throw result.reason;
      }
      const links = await Effect.runPromise(
        Schema.decodeEffect(Schema.fromJsonString(Schema.Record(Schema.String, Schema.String)))(
          await fs.readFile(path.join(root, ".forks.json"), "utf8"),
        ),
      );
      const expected = ["first", "second"]
        .flatMap((name) => Array.from({ length: 12 }, (_, i) => `${name}-${i}`))
        .sort();
      assert.deepEqual(Object.keys(links).sort(), expected);
      assert.ok(Object.values(links).every((parent) => parent === "parent"));
      assert.deepEqual(
        (await fs.readFile(path.join(root, "parent", "objects", "info", "borrowers"), "utf8"))
          .trim()
          .split("\n")
          .sort(),
        expected,
      );
      assert.equal(
        (await fs.readdir(root)).some((name) => name.endsWith(".lock")),
        false,
      );
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  }),
);
