import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";
import { ReadWriteNamespace } from "alchemy/Cloudflare/Artifacts/ReadWriteNamespace";
import { RuntimeContext } from "alchemy/RuntimeContext";
import { Deferred, Effect, Exit, Fiber, Layer } from "effect";

import { EMPTY_TREE_OID } from "../git/Format.ts";
import { stores } from "../git/Node.ts";
import * as Repository from "../git/Repository.ts";
import { serve } from "../host/Node.ts";
import {
  localNamespace,
  localNode,
  Registry,
  registryMemory,
  registryNode,
  RepoStores,
  repoStoresNode,
  repoStoresMemory,
  tokensMemory,
  tokensNode,
} from "./Namespace.ts";

const namespace = {
  kind: "Cloudflare.Artifacts.Namespace",
  name: "REPOS",
  namespace: "test",
} as const;

describe("Artifacts initialization", () => {
  it.effect("releases only its own reservations after partial acquisition and interruption", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "artifacts-reservations-"));
      try {
        const first = await Effect.runPromise(
          RepoStores.pipe(Effect.provide(repoStoresNode(root))),
        );
        const second = await Effect.runPromise(
          RepoStores.pipe(Effect.provide(repoStoresNode(root))),
        );
        await Effect.runPromise(
          Effect.gen(function* () {
            const entered = yield* Deferred.make<void>();
            const holding = yield* first
              .reserve(
                ["z-held"],
                "FORK_IN_PROGRESS",
                Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
              )
              .pipe(Effect.forkScoped);
            yield* Deferred.await(entered);
            const failed = yield* second
              .reserve(["a-free", "z-held"], "PRECONDITION_FAILED", Effect.void)
              .pipe(Effect.flip);
            assert.match(failed.message, /FORK_IN_PROGRESS/);
            // The earlier acquisition is released, but the other owner survives.
            yield* second.reserve(["a-free"], "PRECONDITION_FAILED", Effect.void);
            const stillHeld = yield* second
              .reserve(["z-held"], "PRECONDITION_FAILED", Effect.void)
              .pipe(Effect.flip);
            assert.match(stillHeld.message, /FORK_IN_PROGRESS/);
            yield* Fiber.interrupt(holding);
            yield* second.reserve(["z-held"], "PRECONDITION_FAILED", Effect.void);
          }).pipe(Effect.scoped),
        );
        assert.deepEqual(await fs.readdir(path.join(root, ".operations")), []);
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );

  it.effect("preserves published readiness when the creation response is interrupted", () =>
    Effect.gen(function* () {
      const registry = yield* Registry.pipe(Effect.provide(registryMemory));
      const published = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const client = yield* (yield* ReadWriteNamespace)(namespace);
        const creating = yield* client.create("ready").pipe(Effect.forkScoped);
        yield* Deferred.await(published);
        yield* client.get("ready");
        yield* Effect.sync(() => creating.interruptUnsafe());
        yield* Deferred.succeed(release, undefined);
        assert.ok(Exit.isFailure(yield* Fiber.await(creating)));
        yield* client.get("ready");
        assert.equal(yield* client.delete("ready"), true);
      }).pipe(
        Effect.ensuring(Deferred.succeed(release, undefined)),
        Effect.scoped,
        Effect.provide(
          Layer.merge(
            localNamespace().pipe(
              Layer.provide(
                Layer.mergeAll(
                  Layer.succeed(Registry)(
                    Registry.of({
                      ...registry,
                      finish: (name, id) =>
                        registry
                          .finish(name, id)
                          .pipe(
                            Effect.tap(() =>
                              Deferred.succeed(published, undefined).pipe(
                                Effect.andThen(Deferred.await(release)),
                              ),
                            ),
                          ),
                    }),
                  ),
                  tokensMemory,
                  repoStoresMemory,
                ),
              ),
            ),
            RuntimeContext.phantom,
          ),
        ),
      );
    }),
  );

  it.effect("reserves both ends of a fork until its refs and metadata are ready", () =>
    Effect.gen(function* () {
      const backing = yield* RepoStores.pipe(Effect.provide(repoStoresMemory));
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const controlled = RepoStores.of({
        ...backing,
        fork: (child, parent) =>
          backing
            .fork(child, parent)
            .pipe(
              Effect.tap(() =>
                Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
              ),
            ),
      });
      yield* Effect.gen(function* () {
        const client = yield* (yield* ReadWriteNamespace)(namespace);
        yield* client.create("parent");
        const parent = yield* client.get("parent");
        const forking = yield* parent.fork("child").pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        const pending = yield* client.get("child").pipe(Effect.flip);
        assert.match(pending.message, /FORK_IN_PROGRESS/);
        for (const name of ["parent", "child"]) {
          const refused = yield* client.delete(name).pipe(Effect.flip);
          assert.match(refused.message, /FORK_IN_PROGRESS/);
        }
        yield* client.create("unrelated");
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(forking);
        yield* client.get("child");
        yield* client.delete("child");
        yield* client.delete("parent");
        yield* client.create("parent");
        const replaced = yield* parent.fork("stale").pipe(Effect.flip);
        assert.match(replaced.message, /NOT_FOUND/);
        assert.deepEqual(
          (yield* client.list()).repos.map((repo) => repo.name),
          ["parent", "unrelated"],
        );
      }).pipe(
        Effect.ensuring(Deferred.succeed(release, undefined)),
        Effect.scoped,
        Effect.provide(
          Layer.merge(
            localNamespace().pipe(
              Layer.provide(
                Layer.mergeAll(registryMemory, tokensMemory, Layer.succeed(RepoStores)(controlled)),
              ),
            ),
            RuntimeContext.phantom,
          ),
        ),
      );
    }),
  );

  it.effect("keeps unfinished initialization unavailable after reopening and permits cleanup", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "artifacts-reopen-pending-"));
      try {
        const registry = await Effect.runPromise(Registry.pipe(Effect.provide(registryNode(root))));
        const meta = { description: null, defaultBranch: "main", readOnly: false, source: null };
        // A legacy row has no initialization field. A terminated process can
        // leave a newer row with this durable marker and partial storage.
        await Effect.runPromise(registry.create("legacy", meta));
        await Effect.runPromise(registry.create("pending", { ...meta, initializing: "import" }));
        await fs.mkdir(path.join(root, "pending"), { recursive: true });
        await Effect.runPromise(
          Effect.gen(function* () {
            const client = yield* (yield* ReadWriteNamespace)(namespace);
            yield* client.get("legacy");
            const refused = yield* client.get("pending").pipe(Effect.flip);
            assert.match(refused.message, /IMPORT_IN_PROGRESS/);
            assert.equal(yield* client.delete("pending"), true);
            yield* client.create("pending");
            yield* client.get("pending");
          }).pipe(Effect.provide(Layer.merge(localNode({ root }), RuntimeContext.phantom))),
        );
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );

  for (const outcome of [
    "complete",
    "independent provider",
    "interrupt",
    "metadata failure",
    "ref refusal",
  ] as const) {
    it.live(`protects a running import through ${outcome}`, () =>
      Effect.promise(async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "artifacts-lifecycle-"));
        const server = await serve({ root: path.join(root, "remote"), allowAnonymousWrites: true });
        try {
          const tip = await Effect.runPromise(
            Effect.gen(function* () {
              return yield* (yield* Repository.Repository).commit({
                branch: "main",
                tree: EMPTY_TREE_OID,
                message: "imported",
                author: { name: "T", email: "t@e.com", at: new Date(1700000000000), offset: 0 },
              });
            }).pipe(
              Effect.provide(
                Repository.layer.pipe(
                  Layer.provide(Repository.hooksNoop),
                  Layer.provide(stores(path.join(root, "remote", "origin"))),
                ),
              ),
            ),
          );
          const target = path.join(root, "local");
          const registry = await Effect.runPromise(
            Registry.pipe(Effect.provide(registryNode(target))),
          );
          const backing = await Effect.runPromise(
            RepoStores.pipe(Effect.provide(repoStoresNode(target))),
          );
          const entered = Deferred.makeUnsafe<void>();
          const release = Deferred.makeUnsafe<void>();
          let held = false;
          const controlled = RepoStores.of({
            ...backing,
            open: (name) =>
              backing.open(name).pipe(
                Effect.map((opened) => ({
                  ...opened,
                  objects: {
                    ...opened.objects,
                    write: (object) =>
                      Effect.gen(function* () {
                        if (name === "imported" && !held) {
                          held = true;
                          yield* Deferred.succeed(entered, undefined);
                          yield* Deferred.await(release);
                        }
                        return yield* opened.objects.write(object);
                      }),
                  },
                })),
              ),
          });
          await Effect.runPromise(
            Effect.gen(function* () {
              const bind = yield* ReadWriteNamespace;
              const client = yield* bind(namespace);
              const importing = yield* client
                .import({
                  source: { url: `${server.url}/origin` },
                  target: { name: "imported" },
                })
                .pipe(Effect.forkScoped);
              yield* Deferred.await(entered);
              if (outcome === "independent provider") {
                yield* Effect.promise(() =>
                  promisify(execFile)(
                    process.execPath,
                    [
                      "--input-type=module",
                      "-e",
                      `
                    import assert from "node:assert/strict";
                    import { Effect, Layer } from "effect";
                    import { ReadWriteNamespace } from "alchemy/Cloudflare/Artifacts/ReadWriteNamespace";
                    import { RuntimeContext } from "alchemy/RuntimeContext";
                    import { localNode } from ${JSON.stringify(new URL("./Namespace.ts", import.meta.url).href)};
                    await Effect.runPromise(Effect.gen(function* () {
                      const client = yield* (yield* ReadWriteNamespace)(${JSON.stringify(namespace)});
                      for (const operation of [client.delete("imported"), client.create("imported")]) {
                        const result = yield* operation.pipe(Effect.result);
                        assert.equal(result._tag, "Failure");
                        assert.match(result.failure.message, /IMPORT_IN_PROGRESS/);
                      }
                    }).pipe(Effect.provide(Layer.merge(localNode({ root: process.argv[1] }), RuntimeContext.phantom))));
                  `,
                      target,
                    ],
                    { timeout: 20_000 },
                  ),
                );
              }
              if (outcome === "ref refusal") {
                yield* Effect.promise(async () => {
                  const directory = path.join(target, "imported", "refs", "heads");
                  await fs.mkdir(directory, { recursive: true });
                  await fs.writeFile(path.join(directory, "main.lock"), "", { flag: "wx" });
                });
              }
              const lookup = yield* client.get("imported").pipe(Effect.result);
              let forked = false;
              if (lookup._tag === "Success") {
                const fork = yield* lookup.success.fork("child").pipe(Effect.result);
                forked = fork._tag === "Success";
                if (forked) yield* client.delete("child");
              }
              const secondBinding =
                outcome === "independent provider"
                  ? yield* Effect.gen(function* () {
                      return yield* (yield* ReadWriteNamespace)(namespace);
                    }).pipe(Effect.provide(localNode({ root: target })))
                  : yield* bind(namespace);
              const deleted = yield* secondBinding.delete("imported").pipe(Effect.result);
              if (deleted._tag === "Success" && deleted.success)
                yield* secondBinding.create("imported");
              yield* client.create("unrelated");
              if (outcome === "interrupt") {
                // Request interruption without awaiting a write that is held
                // behind this test's gate. The directory stays reserved until
                // that pending write finishes and rollback has removed it.
                yield* Effect.sync(() => importing.interruptUnsafe());
                const stillHeld = yield* client.delete("imported").pipe(Effect.flip);
                assert.match(stillHeld.message, /IMPORT_IN_PROGRESS/);
              }
              yield* Deferred.succeed(release, undefined);
              const settled = yield* Fiber.await(importing);
              if (deleted._tag === "Success" && deleted.success) {
                assert.notEqual(
                  yield* registry.get("imported"),
                  null,
                  "import cleanup removed the repository created by another provider",
                );
              }
              const completes = outcome === "complete" || outcome === "independent provider";
              assert.equal(Exit.isSuccess(settled), completes);
              if (completes) {
                const imported = yield* backing.open("imported");
                assert.equal(yield* imported.refs.read("refs/heads/main"), tip);
                yield* client.get("imported");
                assert.equal(yield* client.delete("imported"), true);
              } else {
                assert.equal(yield* registry.get("imported"), null);
                assert.equal(
                  yield* Effect.promise(() =>
                    fs.stat(path.join(target, "imported")).then(
                      () => true,
                      () => false,
                    ),
                  ),
                  false,
                );
              }
              assert.deepEqual(
                { lookup: lookup._tag, forked, deleted: deleted._tag },
                {
                  lookup: "Failure",
                  forked: false,
                  deleted: "Failure",
                },
              );
              if (lookup._tag === "Failure")
                assert.match(lookup.failure.message, /IMPORT_IN_PROGRESS/);
              if (deleted._tag === "Failure")
                assert.match(deleted.failure.message, /IMPORT_IN_PROGRESS/);
              yield* client.create("imported");
              assert.equal(
                yield* (yield* backing.open("imported")).refs.read("refs/heads/main"),
                null,
              );
            }).pipe(
              Effect.ensuring(Deferred.succeed(release, undefined)),
              Effect.scoped,
              Effect.provide(
                Layer.merge(
                  localNamespace().pipe(
                    Layer.provide(
                      Layer.mergeAll(
                        Layer.succeed(Registry)(
                          Registry.of({
                            ...registry,
                            touch:
                              outcome === "metadata failure"
                                ? () => Effect.die(new Error("metadata write refused"))
                                : registry.touch,
                          }),
                        ),
                        tokensNode(target),
                        Layer.succeed(RepoStores)(controlled),
                      ),
                    ),
                  ),
                  RuntimeContext.phantom,
                ),
              ),
            ),
          );
        } finally {
          await server.close();
          await fs.rm(root, { recursive: true, force: true });
        }
      }),
    );
  }
});
