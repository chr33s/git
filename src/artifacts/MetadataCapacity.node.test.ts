import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import {
  type ArtifactsError,
  ReadWriteNamespace,
} from "@alchemy.run/cloudflare/Artifacts/ReadWriteNamespace";
import type { Namespace as ArtifactsNamespace } from "@alchemy.run/cloudflare/Artifacts/Namespace";
import { RuntimeContext } from "alchemy/RuntimeContext";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { it } from "@effect/vitest";
import { ConfigProvider, Effect, Layer, Logger } from "effect";
import { Registry, Tokens, registryNode, tokensNode, localNode } from "./Namespace.ts";
import * as Capacity from "./MetadataCapacity.node.ts";

const meta = {
  description: "private-capacity-恢复",
  defaultBranch: "main",
  readOnly: false,
  source: null,
};
const configuration = (rows: number, bytes: number) =>
  ConfigProvider.layer(
    ConfigProvider.fromUnknown({
      GIT_ARTIFACTS_REGISTRY_ROWS: String(rows),
      GIT_ARTIFACTS_REGISTRY_BYTES: String(bytes),
      GIT_ARTIFACTS_TOKEN_ROWS: String(rows),
      GIT_ARTIFACTS_TOKEN_BYTES: String(bytes),
    }),
  );
const openRegistry = (root: string, rows: number, bytes: number) =>
  Effect.runPromise(
    Registry.pipe(
      Effect.provide(registryNode(root).pipe(Layer.provide(configuration(rows, bytes)))),
    ),
  );
const openTokens = (root: string, rows: number, bytes: number) =>
  Effect.runPromise(
    Tokens.pipe(Effect.provide(tokensNode(root).pipe(Layer.provide(configuration(rows, bytes))))),
  );
const refused = async (operation: Effect.Effect<void, ArtifactsError>) => {
  const messages: string[] = [];
  const logger = Logger.make<unknown, void>(({ message }) => {
    messages.push(JSON.stringify(message));
  });
  const result = await Effect.runPromise(
    operation.pipe(Effect.result, Effect.provide(Logger.layer([logger]))),
  );
  assert.match(messages.join(""), /quota_exceeded/);
  assert.doesNotMatch(messages.join(""), /private-capacity|artifacts-capacity/);
  assert.equal(result._tag, "Failure", "a full registry must refuse growth");
  if (result._tag === "Failure") {
    assert.match(result.failure.message, /RESOURCE_EXHAUSTED/);
    assert.doesNotMatch(JSON.stringify(result.failure), /private-capacity/);
  }
};

it("Node repository metadata bounds growth and permits removal after limits are lowered", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "artifacts-capacity-"));
  try {
    const registry = await openRegistry(root, 1, 1024 * 1024);
    const first = await Effect.runPromise(
      registry.create("first", { ...meta, initializing: "create" }),
    );
    const target = path.join(root, ".registry.json");
    const before = await fs.readFile(target);
    await refused(registry.create("second", meta).pipe(Effect.asVoid));
    assert.deepEqual(await fs.readFile(target), before);
    const exact = await openRegistry(root, 1, before.byteLength);
    await refused(exact.setDefaultBranch("first", "x".repeat(before.byteLength)));
    await refused(exact.touch("first", new Date()));
    assert.deepEqual(await fs.readFile(target), before);
    const lowered = await openRegistry(root, 1, 1);
    assert.deepEqual(await Effect.runPromise(lowered.get("first")), first);
    await Effect.runPromise(lowered.finish("first", first.id));
    assert.equal((await Effect.runPromise(lowered.get("first")))?.initializing, null);
    assert.equal(await Effect.runPromise(lowered.delete("first")), true);
    const raised = await openRegistry(root, 2, 1024 * 1024);
    await Effect.runPromise(raised.create("second", meta));
    assert.equal((await Effect.runPromise(raised.list())).repos.length, 1);
    assert.deepEqual(await fs.readdir(root), [".registry.json"]);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

it("Node token capacity spans repositories and never blocks revocation", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "artifacts-token-capacity-"));
  try {
    const tokens = await openTokens(root, 1, 1024 * 1024);
    const first = await Effect.runPromise(tokens.issue("first", "write", 3600));
    const target = path.join(root, ".tokens.json");
    const before = await fs.readFile(target);
    await refused(tokens.issue("second", "read", 3600).pipe(Effect.asVoid));
    assert.deepEqual(await fs.readFile(target), before);
    const lowered = await openTokens(root, 1, 1);
    assert.equal(await Effect.runPromise(lowered.verify("first", first.plaintext)), "write");
    assert.equal(await Effect.runPromise(lowered.revoke("first", first.id)), true);
    assert.equal(await Effect.runPromise(tokens.verify("first", first.plaintext)), null);
    await refused(tokens.issue("second", "read", 3600).pipe(Effect.asVoid));
    const raised = await openTokens(root, 2, 1024 * 1024);
    const next = await Effect.runPromise(raised.issue("second", "read", 3600));
    assert.equal(await Effect.runPromise(raised.verify("second", next.plaintext)), "read");
    assert.equal(await Effect.runPromise(raised.verify("first", first.plaintext)), null);
    assert.deepEqual(await fs.readdir(root), [".tokens.json"]);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

it("Node metadata additions reject oversized files before parsing their contents", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "artifacts-capacity-inspect-"));
  try {
    const bytes = "private-capacity-invalid-json";
    await fs.writeFile(path.join(root, ".registry.json"), bytes);
    await fs.writeFile(path.join(root, ".tokens.json"), bytes);
    await refused((await openRegistry(root, 1, 1)).create("first", meta).pipe(Effect.asVoid));
    await refused((await openTokens(root, 1, 1)).issue("first", "read", 3600).pipe(Effect.asVoid));
    assert.equal(await fs.readFile(path.join(root, ".registry.json"), "utf8"), bytes);
    assert.equal(await fs.readFile(path.join(root, ".tokens.json"), "utf8"), bytes);
    assert.deepEqual((await fs.readdir(root)).sort(), [".registry.json", ".tokens.json"]);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

for (const kind of ["repositories", "tokens"] as const) {
  it(`Node ${kind} enforce exact serialized UTF-8 byte boundaries`, async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "artifacts-exact-capacity-"));
    const addition = async (directory: string, bytes: number) =>
      kind === "repositories"
        ? (await openRegistry(directory, 1, bytes)).create("first", meta).pipe(Effect.asVoid)
        : (await openTokens(directory, 1, bytes))
            .issue("private-capacity-恢复", "read", 3600)
            .pipe(Effect.asVoid);
    const file = kind === "repositories" ? ".registry.json" : ".tokens.json";
    try {
      const sample = path.join(root, "sample");
      const candidate = path.join(root, "candidate");
      await Effect.runPromise(await addition(sample, 1024 * 1024));
      const size = (await fs.stat(path.join(sample, file))).size;
      await refused(await addition(candidate, size - 1));
      await assert.rejects(fs.stat(path.join(candidate, file)), { code: "ENOENT" });
      assert.deepEqual(await fs.readdir(candidate), []);
      await Effect.runPromise(await addition(candidate, size));
      assert.equal((await fs.stat(path.join(candidate, file))).size, size);
      assert.deepEqual(await fs.readdir(candidate), [file]);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it(`Node ${kind} capacity is shared by competing processes`, async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "artifacts-process-capacity-"));
    const program = `
      import { Effect } from "effect";
      import { Registry, Tokens, registryNode, tokensNode, localNode } from ${JSON.stringify(new URL("./Namespace.ts", import.meta.url).href)};
      const [root, kind, index] = process.argv.slice(1);
      const operation = kind === "repositories"
        ? Registry.use(registry => registry.create("repo-" + index, ${JSON.stringify(meta)})).pipe(Effect.provide(registryNode(root)), Effect.asVoid)
        : Tokens.use(tokens => tokens.issue("repo-" + index, "read", 3600)).pipe(Effect.provide(tokensNode(root)), Effect.asVoid);
      const result = await Effect.runPromise(operation.pipe(Effect.result));
      console.log("RESULT:" + result._tag);
    `;
    try {
      const children = await Promise.allSettled(
        [0, 1, 2, 3].map((index) =>
          promisify(execFile)(
            process.execPath,
            ["--input-type=module", "-e", program, root, kind, String(index)],
            {
              timeout: 20_000,
              env: {
                ...process.env,
                GIT_ARTIFACTS_REGISTRY_ROWS: "1",
                GIT_ARTIFACTS_TOKEN_ROWS: "1",
                GIT_ARTIFACTS_REGISTRY_BYTES: "1048576",
                GIT_ARTIFACTS_TOKEN_BYTES: "1048576",
              },
            },
          ),
        ),
      );
      const output: string[] = [];
      for (const child of children) {
        if (child.status === "rejected") throw child.reason;
        output.push(child.value.stdout);
        assert.ok(!(child.value.stdout + child.value.stderr).includes(root));
        assert.ok(!(child.value.stdout + child.value.stderr).includes(meta.description));
      }
      assert.equal(output.filter((value) => value.includes("RESULT:Success")).length, 1);
      assert.equal(output.filter((value) => value.includes("RESULT:Failure")).length, 3);
      if (kind === "repositories")
        assert.equal(
          (await Effect.runPromise((await openRegistry(root, 1, 1048576)).list())).repos.length,
          1,
        );
      else {
        const tokens = await openTokens(root, 1, 1048576);
        let count = 0;
        for (const index of [0, 1, 2, 3])
          count += (await Effect.runPromise(tokens.list(`repo-${index}`))).total;
        assert.equal(count, 1);
      }
      assert.deepEqual(await fs.readdir(root), [
        kind === "repositories" ? ".registry.json" : ".tokens.json",
      ]);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
}

it.effect("Node administrative capacity configuration rejects invalid budgets", () =>
  Effect.gen(function* () {
    for (const kind of ["repositories", "tokens"] as const) {
      const read = (values: Record<string, string>) =>
        Capacity.configuration(kind).pipe(
          Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(values))),
        );
      assert.deepEqual(
        yield* read({}),
        kind === "repositories"
          ? { rows: 1024, bytes: 1024 ** 2 }
          : { rows: 4096, bytes: 2 * 1024 ** 2 },
      );
      const prefix = kind === "repositories" ? "GIT_ARTIFACTS_REGISTRY" : "GIT_ARTIFACTS_TOKEN";
      for (const suffix of ["ROWS", "BYTES"])
        for (const value of ["0", "-1", "1.5", "Infinity", "9007199254740992"])
          assert.equal(
            (yield* read({ [`${prefix}_${suffix}`]: value }).pipe(Effect.result))._tag,
            "Failure",
          );
      assert.equal(
        (yield* read({ [`${prefix}_ROWS`]: "1000001" }).pipe(Effect.result))._tag,
        "Failure",
      );
    }
  }),
);

it("Node provider cleans up a repository when its initial token exceeds capacity", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "artifacts-provider-capacity-"));
  const namespace = {
    kind: "Cloudflare.Artifacts.Namespace",
    name: "REPOS",
    namespace: "capacity-test",
  } satisfies ArtifactsNamespace;
  const limited = ConfigProvider.layer(
    ConfigProvider.fromUnknown({
      GIT_ARTIFACTS_REGISTRY_ROWS: "2",
      GIT_ARTIFACTS_TOKEN_ROWS: "1",
    }),
  );
  try {
    const first = await Effect.runPromise(
      Effect.gen(function* () {
        const bind = yield* ReadWriteNamespace;
        const client = yield* bind(namespace);
        const first = yield* client.create("first");
        const failed = yield* client.create("second").pipe(Effect.result);
        assert.equal(failed._tag, "Failure");
        if (failed._tag === "Failure") assert.match(failed.failure.message, /RESOURCE_EXHAUSTED/);
        const registry = yield* Registry;
        assert.equal(yield* registry.get("second"), null);
        assert.equal((yield* registry.list()).repos.length, 1);
        const tokens = yield* Tokens;
        assert.equal(yield* tokens.verify("first", first.token), "write");
        return first;
      }).pipe(
        Effect.provide(
          Layer.merge(localNode({ root }).pipe(Layer.provide(limited)), RuntimeContext.phantom),
        ),
      ),
    );
    await assert.rejects(fs.stat(path.join(root, "second")), { code: "ENOENT" });
    await Effect.runPromise(
      Effect.gen(function* () {
        const bind = yield* ReadWriteNamespace;
        const client = yield* bind(namespace);
        const second = yield* client.create("second");
        const tokens = yield* Tokens;
        assert.equal(yield* tokens.verify("second", second.token), "write");
        assert.equal(yield* tokens.verify("first", first.token), "write");
        assert.equal((yield* (yield* Registry).list()).repos.length, 2);
      }).pipe(
        Effect.provide(
          Layer.merge(
            localNode({ root }).pipe(Layer.provide(configuration(2, 1024 * 1024))),
            RuntimeContext.phantom,
          ),
        ),
      ),
    );
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
