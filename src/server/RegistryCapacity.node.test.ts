import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { it } from "@effect/vitest";
import { ConfigProvider, Effect, Layer, Logger } from "effect";
import { Remotes } from "./Remotes.ts";
import { Subscribers } from "./Subscribers.ts";
import { file as remotesFile } from "./Remotes.node.ts";
import { file as subscribersFile } from "./Subscribers.node.ts";

type Kind = "remotes" | "webhooks";
const secret = "private-registry-恢复";
const open = Effect.fn("test.RegistryCapacity.open")(function* (
  kind: Kind,
  file: string,
  rows: number,
  bytes: number,
) {
  const config = ConfigProvider.layer(
    ConfigProvider.fromUnknown({
      GIT_REGISTRY_ROWS: String(rows),
      GIT_REGISTRY_BYTES: String(bytes),
    }),
  );
  if (kind === "remotes") {
    const store = yield* Remotes.pipe(
      Effect.provide(remotesFile(file).pipe(Layer.provide(config))),
    );
    return {
      add: (name: string) =>
        store
          .add({ name, url: "https://example.com/repo", credential: secret })
          .pipe(Effect.map((row) => row.name)),
      remove: store.remove,
      list: store.list.pipe(Effect.map((rows) => rows.map((row) => row.name))),
    };
  }
  const store = yield* Subscribers.pipe(
    Effect.provide(subscribersFile(file).pipe(Layer.provide(config))),
  );
  return {
    add: (_name: string) =>
      store.add({ url: "https://example.com/hook", secret }).pipe(Effect.map((row) => row.id)),
    remove: store.remove,
    list: store.list.pipe(Effect.map((rows) => rows.map((row) => row.id))),
  };
});

for (const kind of ["remotes", "webhooks"] as const) {
  it.effect(`${kind}: file quotas preserve bytes, allow removals and recover after reopening`, () =>
    Effect.gen(function* () {
      const root = yield* Effect.promise(() =>
        fs.mkdtemp(path.join(os.tmpdir(), "registry-capacity-")),
      );
      const file = path.join(root, "registry.json");
      const messages: string[] = [];
      const logger = Logger.make<unknown, void>(({ message }) => {
        messages.push(JSON.stringify(message));
      });
      try {
        yield* Effect.gen(function* () {
          const initial = yield* open(kind, file, 1, 1024 ** 2);
          const first = yield* initial.add("one");
          const before = yield* Effect.promise(() => fs.readFile(file));
          const atLimit = yield* initial.add("two").pipe(Effect.result);
          assert.equal(atLimit._tag, "Failure");
          if (atLimit._tag === "Failure" && atLimit.failure._tag === "StorageFailure")
            assert.equal(atLimit.failure.operation, "quota.registry");
          assert.deepEqual(yield* Effect.promise(() => fs.readFile(file)), before);
          assert.deepEqual(yield* Effect.promise(() => fs.readdir(root)), ["registry.json"]);
          const lowered = yield* open(kind, file, 10, before.byteLength - 1);
          assert.equal((yield* lowered.add("two").pipe(Effect.result))._tag, "Failure");
          assert.deepEqual(yield* lowered.list, [first]);
          assert.equal(yield* lowered.remove(first), true);
          const raised = yield* open(kind, file, 10, 1024 ** 2);
          yield* raised.add("one");
          yield* raised.add("two");
          const pair = yield* Effect.promise(() => fs.readFile(file));
          for (const id of yield* raised.list) yield* raised.remove(id);
          yield* raised.add("one");
          const below = yield* open(kind, file, 10, pair.byteLength - 1);
          assert.equal((yield* below.add("two").pipe(Effect.result))._tag, "Failure");
          const exact = yield* open(kind, file, 10, pair.byteLength);
          yield* exact.add("two");
          assert.equal((yield* Effect.promise(() => fs.stat(file))).size, pair.byteLength);
          assert.deepEqual(yield* Effect.promise(() => fs.readdir(root)), ["registry.json"]);
          assert.match(messages.join(""), /quota_exceeded/);
          assert.ok(!messages.join("").includes(secret));
          assert.ok(!messages.join("").includes(root));
        }).pipe(Effect.provide(Logger.layer([logger])));
      } finally {
        yield* Effect.promise(() => fs.rm(root, { recursive: true }));
      }
    }),
  );

  it.effect(`${kind}: separate processes cannot exceed one retained registration`, () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "registry-capacity-process-"));
      const file = path.join(root, "registry.json");
      const source = `
      import { Effect } from "effect";
      import { Remotes } from ${JSON.stringify(new URL("./Remotes.ts", import.meta.url).href)};
      import { Subscribers } from ${JSON.stringify(new URL("./Subscribers.ts", import.meta.url).href)};
      import { file as remotesFile } from ${JSON.stringify(new URL("./Remotes.node.ts", import.meta.url).href)};
      import { file as subscribersFile } from ${JSON.stringify(new URL("./Subscribers.node.ts", import.meta.url).href)};
      const [kind, file, name] = process.argv.slice(1);
      const result = await Effect.runPromise(Effect.gen(function* () {
        if (kind === "remotes") {
          const store = yield* Remotes.pipe(Effect.provide(remotesFile(file)));
          return yield* store.add({ name, url: "https://example.com/repo" }).pipe(Effect.result);
        }
        const store = yield* Subscribers.pipe(Effect.provide(subscribersFile(file)));
        return yield* store.add({ url: "https://example.com/hook", secret: "fixture-secret-123456" }).pipe(Effect.result);
      }));
      process.stdout.write("RESULT:" + result._tag + "\\n");
    `;
      try {
        const results = await Promise.all(
          ["one", "two", "three", "four"].map((name) =>
            promisify(execFile)(
              process.execPath,
              ["--input-type=module", "-e", source, kind, file, name],
              {
                timeout: 20_000,
                env: { ...process.env, GIT_REGISTRY_ROWS: "1", GIT_REGISTRY_BYTES: "1048576" },
              },
            ),
          ),
        );
        assert.equal(
          results.filter((result) => result.stdout.includes("RESULT:Success")).length,
          1,
        );
        assert.equal(
          results.filter((result) => result.stdout.includes("RESULT:Failure")).length,
          3,
        );
        const store = await Effect.runPromise(open(kind, file, 1, 1024 ** 2));
        assert.equal((await Effect.runPromise(store.list)).length, 1);
        assert.deepEqual(await fs.readdir(root), ["registry.json"]);
      } finally {
        await fs.rm(root, { recursive: true });
      }
    }),
  );
}

it.effect("Node HTTP enforces configured registry quotas and permits removal/retry", () =>
  Effect.promise(async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "registry-capacity-http-"));
    const source = `
    import assert from "node:assert/strict";
    import { serve } from ${JSON.stringify(new URL("../host/Node.ts", import.meta.url).href)};
    import { enableHubUnder } from ${JSON.stringify(new URL("../testing/Hub.ts", import.meta.url).href)};
    const root = process.argv[1];
    if (process.argv[2] === "invalid") {
      await assert.rejects(() => serve({ root }));
      process.stdout.write("INVALID_CONFIG_REFUSED\\n");
    } else {
      const admin = await enableHubUnder(root, "quota", ["repo.read", "repo.admin"]);
      const server = await serve({ root });
      const secret = "http-private-fixture-secret";
      const headers = { authorization: "Bearer " + admin.credential, "content-type": "application/json" };
      try {
        for (const route of ["remotes", "webhooks"]) {
          const base = server.url + "/quota/" + route;
          const add = name => fetch(base, { method: "POST", headers,
            body: JSON.stringify(route === "remotes" ? { name, url: "https://example.invalid/repo", credential: secret } : { url: "https://example.invalid/hook", secret }),
            signal: AbortSignal.timeout(10000) });
          const first = await add("one");
          assert.equal(first.status, 200);
          const registered = await first.json();
          const refused = await add("two");
          assert.equal(refused.status, 500);
          assert.ok(!(await refused.text()).includes(secret));
          const listed = await fetch(base, { headers, signal: AbortSignal.timeout(10000) });
          assert.equal(listed.status, 200);
          assert.equal((await listed.json())[route].length, 1);
          const deleted = await fetch(base + "/" + (route === "remotes" ? "one" : registered.id), { method: "DELETE", headers, signal: AbortSignal.timeout(10000) });
          assert.equal(deleted.status, 200);
          await deleted.arrayBuffer();
          const retried = await add("two");
          assert.equal(retried.status, 200);
          await retried.arrayBuffer();
        }
      } finally { await server.close(); }
      process.stdout.write("HTTP_QUOTAS_PASSED\\n");
    }
  `;
    try {
      for (const mode of ["invalid", "valid"]) {
        const result = await promisify(execFile)(
          process.execPath,
          ["--input-type=module", "-e", source, root, mode],
          {
            timeout: 30_000,
            env: {
              ...process.env,
              GIT_REGISTRY_ROWS: mode === "invalid" ? "0" : "1",
              GIT_REGISTRY_BYTES: "1048576",
            },
          },
        );
        assert.match(
          result.stdout,
          mode === "invalid" ? /INVALID_CONFIG_REFUSED/ : /HTTP_QUOTAS_PASSED/,
        );
        assert.doesNotMatch(result.stdout + result.stderr, /http-private-fixture-secret/);
      }
    } finally {
      await fs.rm(root, { recursive: true });
    }
  }),
);
