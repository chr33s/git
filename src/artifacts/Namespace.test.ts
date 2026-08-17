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
import {
  localMemory,
  localNode,
  RepoStores,
  repoStoresNode,
  type StoreInstances,
  Tokens,
} from "./Namespace.ts";

const namespace = {
  kind: "Cloudflare.Artifacts.Namespace",
  name: "REPOS",
  namespace: "test-ns",
} satisfies ArtifactsNamespace;

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
  // SAFETY: the local provider never reads `RuntimeContext`; alchemy's client
  // signatures carry it only because no off-platform value can be constructed,
  // so providing the memory layers discharges every requirement actually read.
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

  it("refuses in memory too to drop a repository something reads through", async () => {
    await run(
      Effect.gen(function* () {
        const repoStores = yield* RepoStores;
        yield* repoStores.open("parent");
        yield* repoStores.fork("child", "parent");

        // The port's own answer, not the namespace's check above it: `delete`
        // asks before it revokes tokens, and a fork landing in between would
        // otherwise read through a parent that went anyway — and find an
        // empty store built in its place, rather than an error.
        const refused = yield* Effect.flip(repoStores.drop("parent"));
        assert.match(refused.message, /child/);

        const child = yield* repoStores.open("child");
        const parent = yield* repoStores.open("parent");
        const kept = yield* Effect.gen(function* () {
          const repository = yield* Repository;
          return yield* repository.writeBlob(new TextEncoder().encode("still lent\n"));
        }).pipe(Effect.provide(repositoryFor(parent)));
        assert.equal(yield* child.objects.has(kept), true, "the fork lost what it reads through");
      }),
    );
  });

  it("re-forks in memory without emptying the child or stranding its own forks", async () => {
    await run(
      Effect.gen(function* () {
        const repoStores = yield* RepoStores;

        const write = (stores: StoreInstances, text: string) =>
          Effect.gen(function* () {
            const repository = yield* Repository;
            return yield* repository.writeBlob(new TextEncoder().encode(text));
          }).pipe(Effect.provide(repositoryFor(stores)));

        const first = yield* repoStores.open("first");
        const second = yield* repoStores.open("second");
        const inSecond = yield* write(second, "only in the second parent\n");

        const child = yield* repoStores.fork("child", "first");
        const mine = yield* write(child, "the child's own\n");
        yield* repoStores.fork("grandchild", "child");

        // Re-pointed at a parent it did not have: the child keeps what it
        // wrote, reaches the new parent's objects, and its own fork — built
        // over the store just replaced — must see the same.
        yield* repoStores.fork("child", "second");
        const after = yield* repoStores.open("child");
        assert.equal(yield* after.objects.has(mine), true, "the re-fork emptied the child");
        assert.equal(
          yield* after.objects.has(inSecond),
          true,
          "the new parent is not read through",
        );

        const grandchild = yield* repoStores.open("grandchild");
        assert.equal(
          yield* grandchild.objects.has(inSecond),
          true,
          "a fork of the re-forked fork still reads through the parent it had",
        );
        assert.equal(yield* first.objects.has(inSecond), false);
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
    // SAFETY: as with `run` above, the local provider never reads
    // `RuntimeContext`, so the node layers discharge every requirement
    // actually read.
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
    const server = await serve({ root, allowAnonymousWrites: true });

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

/**
 * The fork bookkeeping the node layer keeps outside the registry: the fork
 * links in `.forks.json`, the child's `alternates`, and the parent's
 * `borrowers` — the file `Maintenance.gc` reads before it collects anything.
 * Every one of these is a way for a fork to lose the history it borrows, or
 * for a repository to become undeletable, and none of them is visible from
 * the client surface the tests above drive.
 */
describe("the node provider's fork links", () => {
  const withRoot = async (body: (root: string, stores: RepoStores["Service"]) => Promise<void>) => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "artifacts-links-"));
    try {
      // SAFETY: `repoStoresNode` discharges the only requirement, and the
      // layer's own construction cannot fail — the cast names what the
      // provided effect already is.
      const stores = await Effect.runPromise(
        Effect.provide(RepoStores, repoStoresNode(root)) as Effect.Effect<RepoStores["Service"]>,
      );
      await body(root, stores);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  };

  /** A repository with a directory, which is what "exists" means on disk. */
  const seed = async (root: string, name: string) => {
    await fs.mkdir(path.join(root, name, "objects", "info"), { recursive: true });
  };

  const borrowers = (root: string, name: string) =>
    fs
      .readFile(path.join(root, name, "objects", "info", "borrowers"), "utf8")
      .then((text) => text.trim().split("\n"))
      .catch(() => []);

  const links = async (root: string): Promise<Record<string, string>> => {
    const text = await fs.readFile(path.join(root, ".forks.json"), "utf8").catch(() => "{}");
    // SAFETY: this file is written only by the provider's own `saveJson`, from
    // a `Map<string, string>`; an unreadable one reads as no links at all.
    return JSON.parse(text) as Record<string, string>;
  };

  it("refuses a fork of a parent whose storage is gone", async () => {
    await withRoot(async (root, stores) => {
      await seed(root, "parent");
      await fs.rm(path.join(root, "parent"), { recursive: true });

      const outcome = await Effect.runPromise(Effect.flip(stores.fork("child", "parent")));
      assert.match(outcome.message, /NOT_FOUND/);
      assert.deepEqual(await links(root), {}, "a refused fork still recorded a link");
    });
  });

  it("writes what `gc` reads before the link only this process reads", async () => {
    await withRoot(async (root, stores) => {
      await seed(root, "parent");
      await Effect.runPromise(stores.fork("child", "parent"));

      // The link is the last thing written, so a fork the links describe is
      // one both halves of the on-disk record already describe too —
      // `borrowersOf` is what `Maintenance.gc` gates on, and it has never
      // heard of `.forks.json`.
      assert.deepEqual(await links(root), { child: "parent" });
      assert.deepEqual(await borrowers(root, "parent"), ["child"]);
      assert.equal(
        await fs.readFile(path.join(root, "child", "objects", "info", "alternates"), "utf8"),
        `${path.resolve(root, "parent", "objects")}\n`,
      );
    });
  });

  it("hands a re-forked child back from its old parent", async () => {
    await withRoot(async (root, stores) => {
      for (const name of ["a", "b"]) await seed(root, name);
      await Effect.runPromise(stores.fork("child", "a"));
      assert.deepEqual(await borrowers(root, "a"), ["child"]);

      await Effect.runPromise(stores.fork("child", "b"));
      assert.deepEqual(await borrowers(root, "a"), [], "the old parent still lends to the child");
      assert.deepEqual(await borrowers(root, "b"), ["child"]);
      assert.deepEqual(await links(root), { child: "b" });
    });
  });

  it("keeps every link when forks are made at once", async () => {
    await withRoot(async (root, stores) => {
      await seed(root, "parent");
      const children = Array.from({ length: 12 }, (_, index) => `child-${index}`);
      await Effect.runPromise(
        Effect.all(
          children.map((child) => stores.fork(child, "parent")),
          { concurrency: "unbounded" },
        ),
      );

      const byName = (a: string, b: string) => a.localeCompare(b);
      const expected = [...children].sort(byName);
      assert.deepEqual(Object.keys(await links(root)).sort(byName), expected);
      assert.deepEqual((await borrowers(root, "parent")).sort(byName), expected);
    });
  });

  it("takes a deleted name out of the lists that lend to it", async () => {
    await withRoot(async (root, stores) => {
      await seed(root, "parent");
      await Effect.runPromise(stores.fork("child", "parent"));
      // A line this provider did not write — `git clone --shared` leaves one
      // like it, and nothing in the fork links accounts for it.
      const file = path.join(root, "parent", "objects", "info", "borrowers");
      await fs.appendFile(file, "shared-by-git\n");

      await Effect.runPromise(stores.drop("child"));
      assert.deepEqual(
        await borrowers(root, "parent"),
        ["shared-by-git"],
        "a delete either left its own line behind or took somebody else's",
      );
      assert.deepEqual(await links(root), {});
    });
  });

  it("rebuilds a fork of a fork when the middle one is re-forked", async () => {
    await withRoot(async (root, stores) => {
      for (const name of ["a1", "a2"]) await seed(root, name);
      await Effect.runPromise(stores.fork("b", "a1"));
      const grandchild = await Effect.runPromise(stores.fork("c", "b"));

      // `c` closed over the `b` that read through `a1`; re-pointing `b` at
      // `a2` leaves `c` reading through `a1` for the life of the process,
      // against what its own alternates chain says on disk.
      await Effect.runPromise(stores.fork("b", "a2"));
      assert.notEqual(
        await Effect.runPromise(stores.open("c")),
        grandchild,
        "a fork of the re-forked fork still reads through the parent it had",
      );
    });
  });

  it("goes looking only for a name whose storage is already gone", async () => {
    await withRoot(async (root, stores) => {
      for (const name of ["lender", "plain", "forked"]) await seed(root, name);
      await Effect.runPromise(stores.fork("forked", "lender"));
      // And a line about a repository this namespace never made, under a name
      // it has never used: somebody else's record, which no delete here has
      // any business removing.
      const file = path.join(root, "lender", "objects", "info", "borrowers");
      await fs.appendFile(file, "plain\nstranger\n");

      // A repository that is there and reads through nobody is lent nothing,
      // whatever a stale line says, so its delete does not go looking.
      await Effect.runPromise(stores.drop("plain"));
      assert.deepEqual(await borrowers(root, "lender"), ["forked", "plain", "stranger"]);

      // A fork whose storage went behind the provider's back cannot say who
      // lent to it, and its line is the one nothing else goes back for. The
      // stranger's line stays: this delete is not about that name.
      await fs.rm(path.join(root, "forked"), { recursive: true });
      await Effect.runPromise(stores.drop("forked"));
      assert.deepEqual(await borrowers(root, "lender"), ["plain", "stranger"]);

      // A name with no storage and no fork link was never a repository here.
      // Sweeping for it would delete a line about somebody else's, and the
      // objects would go on the lender's next `gc`.
      await Effect.runPromise(stores.drop("stranger"));
      assert.deepEqual(await borrowers(root, "lender"), ["plain", "stranger"]);
    });
  });

  it("goes looking when the lender its borrower names cannot be named back", async () => {
    await withRoot(async (root, stores) => {
      // The parent's directory is itself a link out of the tree — a
      // repository kept on another disk. Its objects resolve to a path that
      // names no repository here, so a delete cannot say who to unlend and
      // has to look instead.
      const other = await fs.mkdtemp(path.join(os.tmpdir(), "artifacts-elsewhere-"));
      const elsewhere = path.join(other, "on-another-disk");
      await fs.mkdir(path.join(elsewhere, "objects", "info"), { recursive: true });
      await fs.symlink(elsewhere, path.join(root, "parent"));
      await seed(root, "child");
      await fs.writeFile(
        path.join(root, "child", "objects", "info", "alternates"),
        `${path.join(elsewhere, "objects")}\n`,
      );
      await fs.writeFile(path.join(elsewhere, "objects", "info", "borrowers"), "child\n");

      await Effect.runPromise(stores.drop("child"));
      assert.deepEqual(
        await borrowers(root, "parent"),
        [],
        "the lender was left holding a line for a repository that is gone",
      );
      await fs.rm(other, { recursive: true, force: true });
    });
  });

  it("unlends a parent its borrower named through a symlink", async () => {
    await withRoot(async (root, stores) => {
      await seed(root, "parent");
      await seed(root, "child");
      // The same object directory, spelled the way `git clone --shared` would
      // have written it — through whatever path its caller had.
      const link = path.join(root, "by-another-name");
      await fs.symlink(path.join(root, "parent"), link);
      await fs.writeFile(
        path.join(root, "child", "objects", "info", "alternates"),
        `${path.join(link, "objects")}\n`,
      );
      await fs.writeFile(path.join(root, "parent", "objects", "info", "borrowers"), "child\n");

      await Effect.runPromise(stores.drop("child"));
      assert.deepEqual(
        await borrowers(root, "parent"),
        [],
        "the parent was left lending to a repository that is gone",
      );
    });
  });

  it("refuses to drop a repository something still borrows", async () => {
    await withRoot(async (root, stores) => {
      await seed(root, "parent");
      await Effect.runPromise(stores.fork("child", "parent"));

      const outcome = await Effect.runPromise(Effect.flip(stores.drop("parent")));
      assert.match(outcome.message, /child/);
      assert.ok(
        await fs.stat(path.join(root, "parent")).catch(() => null),
        "the parent was removed anyway",
      );
    });
  });

  it("counts a borrower that has no fork link, as `gc` does", async () => {
    await withRoot(async (root, stores) => {
      await seed(root, "parent");
      // A fork interrupted after its `alternates` and before its link, or one
      // made by `git clone --shared`, which writes no link at all.
      await seed(root, "child");
      await fs.writeFile(
        path.join(root, "child", "objects", "info", "alternates"),
        `${path.join(root, "parent", "objects")}\n`,
      );

      assert.deepEqual(await Effect.runPromise(stores.dependents("parent")), ["child"]);
    });
  });

  it("does not write down which way round it broke a cycle", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "artifacts-links-"));
    try {
      for (const name of ["a", "b"]) await seed(root, name);
      // Only a corrupted file says this, and the recursion that reads it has
      // to stop somewhere — but where it stops depends on which name was
      // asked for first, so the answer must not outlive the call. The layer
      // is built after the file, because that is when it reads it.
      await fs.writeFile(path.join(root, ".forks.json"), JSON.stringify({ a: "b", b: "a" }));
      // SAFETY: `repoStoresNode` discharges the only requirement; the cast
      // names what the provided effect already is.
      const stores = await Effect.runPromise(
        Effect.provide(RepoStores, repoStoresNode(root)) as Effect.Effect<RepoStores["Service"]>,
      );

      // Both open at all, rather than recursing until the stack ends.
      const first = await Effect.runPromise(stores.open("a"));
      await Effect.runPromise(stores.open("b"));
      assert.notEqual(
        await Effect.runPromise(stores.open("a")),
        first,
        "a store built by not following a link was cached",
      );
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("forgets a name's alternates as well as its link", async () => {
    await withRoot(async (root, stores) => {
      await seed(root, "parent");
      await Effect.runPromise(stores.fork("child", "parent"));

      await Effect.runPromise(stores.forget("child"));
      assert.deepEqual(await links(root), {});
      assert.deepEqual(await borrowers(root, "parent"), []);
      assert.equal(
        await fs
          .readFile(path.join(root, "child", "objects", "info", "alternates"), "utf8")
          .catch(() => null),
        null,
        "the repository created under this name next would read the old parent's objects",
      );
    });
  });

  it("picks up a grandparent restored after the fork was opened without it", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "artifacts-links-"));
    try {
      // SAFETY: `repoStoresNode` discharges the only requirement; the cast
      // names what the provided effect already is, failures included.
      const layered = <A, E>(effect: Effect.Effect<A, E, RepoStores>) =>
        Effect.runPromise(Effect.provide(effect, repoStoresNode(root)) as Effect.Effect<A, E>);

      await layered(
        Effect.gen(function* () {
          const stores = yield* RepoStores;
          yield* Effect.promise(() => seed(root, "a"));
          yield* stores.fork("b", "a");
          yield* stores.fork("c", "b");
        }),
      );
      await fs.rm(path.join(root, "a"), { recursive: true });

      // A second life over the same root: the links are on disk, the
      // grandparent is not, and only the grandchild is ever asked for.
      await layered(
        Effect.gen(function* () {
          const stores = yield* RepoStores;
          // Not cached while anything under it is missing, however deep: a
          // cached one is a store that answers "no such object" for history
          // the repository has, for as long as the process lives.
          const first = yield* stores.open("c");
          assert.notEqual(yield* stores.open("c"), first, "an incomplete store was cached");

          yield* Effect.promise(() => seed(root, "a"));
          const restored = yield* stores.open("c");
          assert.equal(yield* stores.open("c"), restored, "a whole store was not cached");
        }),
      );
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
});
