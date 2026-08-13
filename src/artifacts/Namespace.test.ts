/**
 * The local Artifacts provider, driven through alchemy's own binding tag.
 *
 * Every test resolves `ReadWriteNamespace` from the layer and speaks
 * the same `ReadWriteNamespaceClient` a Worker would — create, list with a
 * cursor, tokens, fork over alternates, import over real smart HTTP from the
 * node host. `RepoClient.raw` failing as a *typed* error is the payoff of
 * `patches/alchemy+2.0.0-beta.72.patch`.
 */
import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";

import type { Namespace as ArtifactsNamespace } from "alchemy/Cloudflare/Artifacts/Namespace";
import { ReadWriteNamespace } from "alchemy/Cloudflare/Artifacts/ReadWriteNamespace";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Layer } from "effect";

import { noPacks } from "../git/Packed.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { stores as nodeStores } from "../git/Node.ts";
import { ObjectStore, RefStore } from "../git/Store.ts";
import { serve } from "../host/Node.ts";
import { localMemory, localNode, RepoStores, type StoreInstances, Tokens } from "./Namespace.ts";

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
  effect: Effect.Effect<A, E, ReadWriteNamespace | RepoStores | Tokens | RuntimeContext>,
): Promise<A> =>
  // The local provider never reads `RuntimeContext`, but alchemy's client
  // signatures carry it and no off-platform value can be constructed.
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
    // The provider hands out raw store instances, which have no packs of
    // their own; a fork reads through alternates rather than through a pack.
    Layer.provide(noPacks),
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

  it("revokes a deleted repository's tokens, so the name can be reused safely", async () => {
    await run(
      Effect.gen(function* () {
        const client = yield* bound;
        const tokens = yield* Tokens;

        const created = yield* client.create("recycled");
        assert.equal(yield* tokens.verify("recycled", created.token), "write");

        yield* client.delete("recycled");
        // Tokens are keyed by repository name, and nothing cascades: an old
        // write token that still verified would authorise pushes into whatever
        // the next owner creates under the same name.
        assert.equal(yield* tokens.verify("recycled", created.token), null);

        yield* client.create("recycled");
        assert.equal(yield* tokens.verify("recycled", created.token), null);
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

  it("refuses a delete whose name is not a repository name", async () => {
    await run(
      Effect.gen(function* () {
        const client = yield* bound;
        // This name reaches `fs.rm(join(root, name), { recursive: true })`.
        const refused = yield* client.delete("../../../tmp/somewhere").pipe(Effect.flip);
        assert.match(refused.message, /INVALID_REPO_NAME/);
      }),
    );
  });

  it("refuses to delete a repository its forks read through", async () => {
    await run(
      Effect.gen(function* () {
        const client = yield* bound;

        yield* client.create("origin");
        const origin = yield* client.get("origin");
        yield* origin.fork("derived");

        // The fork holds no copy of what it inherited, so deleting the store
        // it reads through would erase its history while leaving its refs,
        // its row and its remote URL advertising objects nothing holds.
        const refused = yield* client.delete("origin").pipe(Effect.flip);
        assert.match(refused.message, /derived/);
        assert.notEqual(yield* client.get("origin"), null);

        // In the other order it is an ordinary delete.
        assert.equal(yield* client.delete("derived"), true);
        assert.equal(yield* client.delete("origin"), true);
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

  it("survives a provider restart when backed by the node layers", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "artifacts-durable-"));
    const provider = () => localNode({ root, remoteBase: "http://git.local" });
    const session = <A, E>(
      effect: Effect.Effect<A, E, ReadWriteNamespace | RepoStores | Tokens | RuntimeContext>,
    ) => Effect.runPromise(effect.pipe(Effect.provide(provider())) as Effect.Effect<A, E>);

    try {
      // First life: create, seed, fork, mint.
      const { blob, token } = await session(
        Effect.gen(function* () {
          const client = yield* bound;
          const repoStores = yield* RepoStores;
          yield* client.create("parent");
          const stores = yield* repoStores.open("parent");
          const seeded = yield* Effect.gen(function* () {
            const repository = yield* Repository;
            const blob = yield* repository.writeBlob(new TextEncoder().encode("durable\n"));
            const tree = yield* repository.writeTree([
              { mode: "100644", name: "d.txt", oid: blob },
            ]);
            yield* repository.commit({ branch: "main", tree, message: "durable", author });
            return blob;
          }).pipe(Effect.provide(repositoryFor(stores)));

          const parent = yield* client.get("parent");
          yield* parent.fork("child", { defaultBranchOnly: true });
          const token = yield* parent.createToken("write", 300);
          return { blob: seeded, token: token.plaintext };
        }),
      );

      // Second life: a fresh provider over the same directory remembers all of it.
      await session(
        Effect.gen(function* () {
          const client = yield* bound;
          const repoStores = yield* RepoStores;
          const tokens = yield* Tokens;

          const listed = yield* client.list();
          assert.deepEqual(
            listed.repos.map((repo) => repo.name),
            ["child", "parent"],
          );

          // The fork link came back from `.forks.json`: reads still fall through.
          const child = yield* repoStores.open("child");
          const shared = yield* child.objects.read(blob);
          assert.equal(new TextDecoder().decode(shared.data), "durable\n");

          // The token digest came back from `.tokens.json`.
          assert.equal(yield* tokens.verify("parent", token), "write");
        }),
      );
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
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
      ),
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
