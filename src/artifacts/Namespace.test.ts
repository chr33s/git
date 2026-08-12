/**
 * The local Artifacts provider, driven through alchemy's own binding tag.
 *
 * Every test resolves `ReadWriteNamespace` from the layer and speaks
 * the same `ReadWriteNamespaceClient` a Worker would — create, list with a
 * cursor, tokens, fork over alternates, import over real smart HTTP from the
 * node host. `RepoClient.raw` failing as a *typed* error is the payoff of
 * `patches/alchemy+2.0.0-beta.70.patch`.
 */
import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "node:test";

import type { Namespace as ArtifactsNamespace } from "alchemy/Cloudflare/Artifacts/Namespace";
import { ReadWriteNamespace } from "alchemy/Cloudflare/Artifacts/ReadWriteNamespace";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Layer } from "effect";

import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { stores as nodeStores } from "../git/Node.ts";
import { ObjectStore, type Oid, RefStore } from "../git/Store.ts";
import { serve } from "../host/Node.ts";
import { localMemory, RepoStores, type StoreInstances } from "./Namespace.ts";

const namespace = {
  kind: "Cloudflare.Artifacts.Namespace",
  name: "REPOS",
  namespace: "test-ns",
} as ArtifactsNamespace;

const author = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

/**
 * A fresh provider per call: `Layer.sync` state does not leak across tests.
 * `RuntimeContext` appears in the client's signatures but the local provider
 * never touches it, so the cast discharges what nothing reads.
 */
const run = <A, E>(
  effect: Effect.Effect<A, E, ReadWriteNamespace | RepoStores | RuntimeContext>,
): Promise<A> =>
  Effect.runPromise(
    effect.pipe(Effect.provide(localMemory({ remoteBase: "http://git.local" }))) as Effect.Effect<
      A,
      E
    >,
  );

const bound = Effect.gen(function* () {
  const bind = yield* ReadWriteNamespace;
  return yield* bind(namespace);
});

/** The domain service over a repo's raw store instances. */
const repositoryFor = (instances: StoreInstances) =>
  GitRepository.layer.pipe(
    Layer.provide(GitRepository.hooksNoop),
    Layer.provide(Layer.succeed(ObjectStore)(instances.objects)),
    Layer.provide(Layer.succeed(RefStore)(instances.refs)),
  );

describe("Artifacts local provider", () => {
  it("creates, lists with a cursor, and deletes repositories", async () => {
    await run(
      Effect.gen(function* () {
        const client = yield* bound;

        const alpha = yield* client.create("alpha", {
          description: "first",
          setDefaultBranch: "trunk",
        });
        assert.equal(alpha.name, "alpha");
        assert.equal(alpha.defaultBranch, "trunk");
        assert.equal(alpha.remote, "http://git.local/alpha");
        assert.match(alpha.token, /^art_/);

        const duplicate = yield* client.create("alpha").pipe(Effect.flip);
        assert.match(duplicate.message, /ALREADY_EXISTS/);

        const invalid = yield* client.create("No/Slashes").pipe(Effect.flip);
        assert.match(invalid.message, /INVALID_REPO_NAME/);

        yield* client.create("beta");
        yield* client.create("gamma");

        const first = yield* client.list({ limit: 2 });
        assert.equal(first.total, 3);
        assert.deepEqual(
          first.repos.map((repo) => repo.name),
          ["alpha", "beta"],
        );
        assert.ok(first.cursor);
        const rest = yield* client.list({ limit: 2, cursor: first.cursor! });
        assert.deepEqual(
          rest.repos.map((repo) => repo.name),
          ["gamma"],
        );

        assert.equal(yield* client.delete("beta"), true);
        assert.equal(yield* client.delete("beta"), false);
        const missing = yield* client.get("beta").pipe(Effect.flip);
        assert.match(missing.message, /NOT_FOUND/);
      }),
    );
  });

  it("issues, lists, revokes tokens — and raw fails as a typed error", async () => {
    await run(
      Effect.gen(function* () {
        const client = yield* bound;
        yield* client.create("tokens");
        const repo = yield* client.get("tokens");

        const token = yield* repo.createToken("read", 60);
        assert.match(token.plaintext, /^art_/);
        assert.equal(token.scope, "read");

        const bad = yield* repo.createToken("read", 0).pipe(Effect.flip);
        assert.match(bad.message, /INVALID_TTL/);

        // create() issued a write token; ours makes two.
        const listed = yield* repo.listTokens();
        assert.equal(listed.total, 2);
        assert.deepEqual(
          listed.tokens.map((entry) => entry.state),
          ["active", "active"],
        );

        assert.equal(yield* repo.revokeToken(token.plaintext), true);
        assert.equal(yield* repo.revokeToken(token.id), false); // already revoked
        const after = yield* repo.listTokens();
        assert.ok(after.tokens.some((entry) => entry.state === "revoked"));

        // The patched `raw`: a typed failure a caller can handle, not a
        // throwing Proxy and not a crash.
        const raw = yield* repo.raw.pipe(Effect.flip);
        assert.equal(raw._tag, "ArtifactsError");
        assert.match(raw.message, /off-platform/);
      }),
    );
  });

  it("forks over alternates: shared history, isolated writes", async () => {
    await run(
      Effect.gen(function* () {
        const client = yield* bound;
        const repoStores = yield* RepoStores;

        yield* client.create("parent");
        const parentStores = yield* repoStores.open("parent");
        const seeded = yield* Effect.gen(function* () {
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(new TextEncoder().encode("shared\n"));
          const tree = yield* repository.writeTree([
            { mode: "100644", name: "shared.txt", oid: blob },
          ]);
          const head = yield* repository.commit({
            branch: "main",
            tree,
            message: "seed",
            author,
          });
          yield* repository.branch({ name: "side", base: "refs/heads/main" });
          return { blob, head };
        }).pipe(Effect.provide(repositoryFor(parentStores)));

        const parent = yield* client.get("parent");
        const fork = yield* parent.fork("child", { defaultBranchOnly: true });
        assert.equal(fork.name, "child");

        const childRecord = yield* client.get("child");
        assert.ok(childRecord);

        const childStores = yield* repoStores.open("child");
        // Only the default branch crossed; `side` did not.
        const refs = yield* childStores.refs.list("refs/");
        assert.deepEqual(
          refs.map(([name]) => name),
          ["refs/heads/main"],
        );

        // Reads fall through to the parent: no object was copied.
        const shared = yield* childStores.objects.read(seeded.blob);
        assert.equal(new TextDecoder().decode(shared.data), "shared\n");

        // Writes stay in the child.
        const childOid = yield* Effect.gen(function* () {
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(new TextEncoder().encode("fork only\n"));
          const tree = yield* repository.writeTree([{ mode: "100644", name: "f.txt", oid: blob }]);
          return yield* repository.commit({ branch: "main", tree, message: "fork", author });
        }).pipe(Effect.provide(repositoryFor(childStores)));

        assert.equal(yield* parentStores.objects.has(childOid), false);
        assert.equal(yield* childStores.objects.has(childOid), true);
      }),
    );
  });

  it("imports a repository over smart HTTP from the node host", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-artifacts-import-"));
    const server = await serve({ root });

    // Seed the remote the same way the interop suite does.
    const seededHead = await Effect.runPromise(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const blob = yield* repository.writeBlob(new TextEncoder().encode("imported content\n"));
        const tree = yield* repository.writeTree([{ mode: "100644", name: "i.txt", oid: blob }]);
        return yield* repository.commit({ branch: "main", tree, message: "remote", author });
      }).pipe(
        Effect.provide(
          GitRepository.layer.pipe(
            Layer.provide(GitRepository.hooksNoop),
            Layer.provide(nodeStores(path.join(root, "origin"))),
          ),
        ),
      ) as Effect.Effect<Oid>,
    );

    try {
      await run(
        Effect.gen(function* () {
          const client = yield* bound;
          const repoStores = yield* RepoStores;

          const imported = yield* client.import({
            source: { url: `${server.url}/origin` },
            target: { name: "imported" },
          });
          assert.equal(imported.name, "imported");

          const stores = yield* repoStores.open("imported");
          assert.equal(yield* stores.refs.read("refs/heads/main"), seededHead);
          assert.equal(yield* stores.refs.head, "refs/heads/main");
          const record = yield* client.get("imported");
          assert.ok(record);

          const notAUrl = yield* client
            .import({ source: { url: "not a url" }, target: { name: "nope" } })
            .pipe(Effect.flip);
          assert.match(notAUrl.message, /INVALID_URL/);

          const noBranch = yield* client
            .import({
              source: { url: `${server.url}/origin`, branch: "does-not-exist" },
              target: { name: "nobranch" },
            })
            .pipe(Effect.flip);
          assert.match(noBranch.message, /NOT_FOUND/);
        }),
      );
    } finally {
      await server.close();
      await fs.rm(root, { recursive: true, force: true });
    }
  });
});
