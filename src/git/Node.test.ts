/**
 * The filesystem backend against the same contract as the in-memory one.
 *
 * Two backends, one suite — and with `Cloudflare.integration.test.ts`, three.
 */
import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";

import { Effect, Stream } from "effect";

import * as Maintenance from "./Maintenance.ts";
import { stores } from "./Node.ts";
import { PackStore } from "./Packed.ts";
import { storeContract } from "./Store.contract.ts";
import { ObjectStore, type Oid, RefStore } from "./Store.ts";

storeContract(
  "Node",
  {
    run: async (effect) => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-store-"));
      try {
        return await Effect.runPromise(effect.pipe(Effect.provide(stores(root))));
      } finally {
        await fs.rm(root, { force: true, recursive: true });
      }
    },
  },
  { describe, it },
);

/**
 * `git gc` and `git pack-refs` move loose refs into a `packed-refs` file and
 * delete them from `refs/`. A backend that only walked the directory would
 * report a repository with a full history as having no refs at all — and `gc`,
 * which asks exactly that question before deleting, would believe it.
 */
describe("packed-refs", () => {
  const withRoot = async <A>(body: (root: string) => Promise<A>): Promise<A> => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-packed-refs-"));
    try {
      return await body(root);
    } finally {
      await fs.rm(root, { force: true, recursive: true });
    }
  };

  const oid = "1".repeat(40) as Oid;
  const tagged = "2".repeat(40) as Oid;

  const packedRefsFile = [
    "# pack-refs with: peeled fully-peeled sorted ",
    `${oid} refs/heads/main`,
    `${tagged} refs/tags/v1`,
    // A peeled tag target: the previous line's commit, not a ref of its own.
    `^${oid}`,
    "",
  ].join("\n");

  it("lists and reads refs that live only in packed-refs", async () => {
    const listed = await withRoot(async (root) => {
      await fs.writeFile(path.join(root, "packed-refs"), packedRefsFile);
      return Effect.runPromise(
        Effect.gen(function* () {
          const refs = yield* RefStore;
          return {
            all: yield* refs.list("refs/"),
            read: yield* refs.read("refs/heads/main"),
            resolved: yield* refs.resolve("refs/tags/v1"),
          };
        }).pipe(Effect.provide(stores(root))) as Effect.Effect<{
          all: ReadonlyArray<readonly [string, Oid]>;
          read: Oid | null;
          resolved: Oid | null;
        }>,
      );
    });

    const byName = (rows: ReadonlyArray<readonly [string, Oid]>) =>
      [...rows].sort((left, right) => left[0].localeCompare(right[0]));
    assert.deepEqual(byName(listed.all), [
      ["refs/heads/main", oid],
      ["refs/tags/v1", tagged],
    ]);
    assert.equal(listed.read, oid);
    assert.equal(listed.resolved, tagged);
  });

  it("prefers a loose ref over the packed entry, as git does", async () => {
    const moved = "3".repeat(40) as Oid;
    const read = await withRoot(async (root) => {
      await fs.writeFile(path.join(root, "packed-refs"), packedRefsFile);
      return Effect.runPromise(
        Effect.gen(function* () {
          const refs = yield* RefStore;
          yield* refs.apply([{ name: "refs/heads/main", value: moved }]);
          return yield* refs.read("refs/heads/main");
        }).pipe(Effect.provide(stores(root))) as Effect.Effect<Oid | null>,
      );
    });

    assert.equal(read, moved);
  });

  it("deletes a ref that lives only in packed-refs", async () => {
    const after = await withRoot(async (root) => {
      await fs.writeFile(path.join(root, "packed-refs"), packedRefsFile);
      return Effect.runPromise(
        Effect.gen(function* () {
          const refs = yield* RefStore;
          // The loose file does not exist, so a delete that only unlinks it
          // reports success and leaves the ref exactly where it was — still
          // advertised, and still a gc root.
          const [result] = yield* refs.apply([{ name: "refs/heads/main", value: null }]);
          return {
            applied: result?.applied === true,
            read: yield* refs.read("refs/heads/main"),
            listed: (yield* refs.list("refs/")).map(([name]) => name),
          };
        }).pipe(Effect.provide(stores(root))) as unknown as Effect.Effect<{
          applied: boolean;
          read: Oid | null;
          listed: ReadonlyArray<string>;
        }>,
      );
    });

    assert.equal(after.applied, true);
    assert.equal(after.read, null);
    assert.deepEqual(after.listed, ["refs/tags/v1"]);
  });

  it("refuses to read a reflog outside this repository", async () => {
    const read = await withRoot(async (root) => {
      // A second repository beside this one, as a host serves them.
      const other = path.join(path.dirname(root), `${path.basename(root)}-other`);
      await fs.mkdir(path.join(other, "logs", "refs", "heads"), { recursive: true });
      await fs.writeFile(
        path.join(other, "logs", "refs", "heads", "main"),
        `${"0".repeat(40)} ${oid} someone <someone@example.com> 1700000000 +0000\tcommit: SECRET\n`,
      );

      try {
        return await Effect.runPromise(
          Effect.gen(function* () {
            const refs = yield* RefStore;
            // What `GET /repo/reflog?ref=…` passes straight through.
            return yield* refs.reflog(`../${path.basename(other)}/logs/refs/heads/main`);
          }).pipe(Effect.provide(stores(root))) as Effect.Effect<ReadonlyArray<unknown>>,
        );
      } finally {
        await fs.rm(other, { force: true, recursive: true });
      }
    });

    assert.deepEqual(read, [], "a ref name reached another repository's logs");
  });

  it("resolves a detached HEAD to the commit it holds", async () => {
    const resolved = await withRoot(async (root) => {
      await fs.writeFile(path.join(root, "HEAD"), `${oid}\n`);
      return Effect.runPromise(
        Effect.gen(function* () {
          const refs = yield* RefStore;
          return yield* refs.resolve("HEAD");
        }).pipe(Effect.provide(stores(root))) as Effect.Effect<Oid | null>,
      );
    });

    // `git checkout <sha>`, a rebase or a bisect all leave HEAD like this, and
    // gc takes its roots from what HEAD resolves to.
    assert.equal(resolved, oid);
  });

  it("keeps what a packed ref reaches when gc runs", async () => {
    const held = await withRoot(async (root) => {
      const layer = stores(root);
      return Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          const refs = yield* RefStore;
          const packs = yield* PackStore;

          const blob = yield* objects.write({
            type: "blob",
            data: new TextEncoder().encode("history\n"),
          });
          yield* refs.apply([{ name: "refs/heads/main", value: blob }]);

          // `git pack-refs --all` leaves exactly this: the loose ref gone, the
          // same ref in one file. The object is reachable either way, and gc
          // has to be able to see that.
          yield* Effect.promise(async () => {
            await fs.rm(path.join(root, "refs"), { recursive: true, force: true });
            await fs.writeFile(path.join(root, "packed-refs"), `${blob} refs/heads/main\n`);
          });
          yield* Maintenance.gc({ objects, packs, refs }, { reflogGrace: 0 });

          return yield* objects.has(blob);
        }).pipe(Effect.provide(layer)) as Effect.Effect<boolean>,
      );
    });

    assert.equal(held, true, "gc collected an object a packed ref reaches");
  });
});

describe("the ref directory", () => {
  const withRoot = async <A>(body: (root: string) => Promise<A>): Promise<A> => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-refs-"));
    try {
      return await body(root);
    } finally {
      await fs.rm(root, { force: true, recursive: true });
    }
  };

  const oid = "5".repeat(40) as Oid;
  const other = "6".repeat(40) as Oid;

  it("lists a ref whose name ends in .tmp", async () => {
    const listed = await withRoot((root) =>
      Effect.runPromise(
        Effect.gen(function* () {
          const refs = yield* RefStore;
          // A legal name — only `.lock` is reserved — and a ref that is
          // written but never listed is a branch whose history gc collects
          // while the push that created it reports success.
          yield* refs.apply([{ name: "refs/heads/foo.tmp", value: oid }]);
          return (yield* refs.list("refs/")).map(([name]) => name);
        }).pipe(Effect.provide(stores(root))) as unknown as Effect.Effect<ReadonlyArray<string>>,
      ),
    );

    assert.deepEqual(listed, ["refs/heads/foo.tmp"]);
  });

  it("resolves a symbolic ref rather than listing its text as an oid", async () => {
    const listed = await withRoot(async (root) => {
      await fs.mkdir(path.join(root, "refs", "remotes", "origin"), { recursive: true });
      await fs.writeFile(
        path.join(root, "refs", "remotes", "origin", "HEAD"),
        "ref: refs/remotes/origin/main\n",
      );
      await fs.writeFile(path.join(root, "refs", "remotes", "origin", "main"), `${oid}\n`);

      return Effect.runPromise(
        Effect.gen(function* () {
          const refs = yield* RefStore;
          return yield* refs.list("refs/");
        }).pipe(Effect.provide(stores(root))) as Effect.Effect<
          ReadonlyArray<readonly [string, Oid]>
        >,
      );
    });

    // Every mirror git clones has one of these; advertising its `ref: …` text
    // as an oid would put that text on the wire.
    assert.deepEqual(
      [...listed].sort((left, right) => left[0].localeCompare(right[0])),
      [
        ["refs/remotes/origin/HEAD", oid],
        ["refs/remotes/origin/main", oid],
      ],
    );
  });

  it("puts an atomic batch back when one of its writes fails", async () => {
    const state = await withRoot((root) =>
      Effect.runPromise(
        Effect.gen(function* () {
          const refs = yield* RefStore;
          yield* refs.apply([{ name: "refs/heads/keep", value: oid }]);

          // The second update needs `refs/heads/feature` to be a directory,
          // and the first has just made it a file. All-or-nothing means the
          // first has to come back out.
          const results = yield* refs.apply(
            [
              { name: "refs/heads/feature", value: other },
              { name: "refs/heads/feature/sub", value: other },
            ],
            { atomic: true },
          );

          return {
            applied: results.filter((result) => result.applied).length,
            feature: yield* refs.read("refs/heads/feature"),
            keep: yield* refs.read("refs/heads/keep"),
          };
        }).pipe(Effect.provide(stores(root))) as unknown as Effect.Effect<{
          applied: number;
          feature: Oid | null;
          keep: Oid | null;
        }>,
      ),
    );

    assert.equal(state.applied, 0);
    assert.equal(state.feature, null, "an atomic batch left half of itself applied");
    assert.equal(state.keep, oid);
  });

  it("lists loose objects without mistaking pack files for them", async () => {
    const listed = await withRoot(async (root) => {
      await fs.mkdir(path.join(root, "objects", "pack"), { recursive: true });
      await fs.writeFile(path.join(root, "objects", "pack", "pack-abc.idx"), "");
      await fs.mkdir(path.join(root, "objects", "info"), { recursive: true });
      await fs.writeFile(path.join(root, "objects", "info", "packs"), "");

      return Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          const written = yield* objects.write({
            type: "blob",
            data: new TextEncoder().encode("real\n"),
          });
          return { written, all: yield* Stream.runCollect(objects.list) };
        }).pipe(Effect.provide(stores(root))) as unknown as Effect.Effect<{
          written: Oid;
          all: ReadonlyArray<Oid>;
        }>,
      );
    });

    assert.deepEqual([...listed.all], [listed.written]);
  });
});

/**
 * git's `alternates`: a repository that reads objects out of another's store
 * and keeps no copy. A fork is served by opening its directory directly, so
 * everything here has to work through the plain `stores()` layer.
 */
describe("alternates", () => {
  const lend = async (child: string, parent: string) => {
    const info = path.join(child, "objects", "info");
    await fs.mkdir(info, { recursive: true });
    await fs.writeFile(path.join(info, "alternates"), `${path.join(parent, "objects")}\n`);
  };

  it("reads through a chain of them, as git does", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-alternates-"));
    try {
      const [a, b, c] = [path.join(root, "a"), path.join(root, "b"), path.join(root, "c")];

      const oid = await Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          return yield* objects.write({
            type: "blob",
            data: new TextEncoder().encode("shared\n"),
          });
        }).pipe(Effect.provide(stores(a))) as Effect.Effect<Oid>,
      );

      // A fork of a fork: `c` reaches `a`'s object only if the chain is
      // followed, which is what git itself does.
      await lend(b, a);
      await lend(c, b);

      const held = await Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          return { has: yield* objects.has(oid), read: (yield* objects.read(oid)).type };
        }).pipe(Effect.provide(stores(c))) as unknown as Effect.Effect<{
          has: boolean;
          read: string;
        }>,
      );

      assert.equal(held.has, true, "a fork of a fork lost its grandparent's objects");
      assert.equal(held.read, "blob");
    } finally {
      await fs.rm(root, { force: true, recursive: true });
    }
  });

  it("does not list what it only borrows", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-alternates-"));
    try {
      const [parent, child] = [path.join(root, "parent"), path.join(root, "child")];

      const lent = await Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          const refs = yield* RefStore;
          const packs = yield* PackStore;
          const blob = yield* objects.write({
            type: "blob",
            data: new TextEncoder().encode("lent\n"),
          });
          yield* refs.apply([{ name: "refs/heads/main", value: blob }]);
          // Packed, which is the case that used to leak: the borrowed pack's
          // index was enumerated as if the fork owned every object in it.
          yield* Maintenance.gc({ objects, packs, refs }, { repack: true, reflogGrace: 0 });
          return blob;
        }).pipe(Effect.provide(stores(parent))) as Effect.Effect<Oid>,
      );

      await lend(child, parent);

      const listed = await Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          return { has: yield* objects.has(lent), all: yield* Stream.runCollect(objects.list) };
        }).pipe(Effect.provide(stores(child))) as unknown as Effect.Effect<{
          has: boolean;
          all: ReadonlyArray<Oid>;
        }>,
      );

      assert.equal(listed.has, true, "the fork cannot read what it borrows");
      assert.deepEqual([...listed.all], [], "the fork listed its parent's objects as its own");
    } finally {
      await fs.rm(root, { force: true, recursive: true });
    }
  });

  it("refuses to collect a repository git itself shared", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-alternates-"));
    try {
      const [parent, child] = [path.join(root, "parent"), path.join(root, "child")];

      const lent = await Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          return yield* objects.write({
            type: "blob",
            data: new TextEncoder().encode("borrowed\n"),
          });
        }).pipe(Effect.provide(stores(parent))) as Effect.Effect<Oid>,
      );

      // What `git clone --shared` leaves behind: the child's alternates point
      // at the parent, and nothing is written on the parent's side at all.
      await lend(child, parent);

      const outcome = await Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          const refs = yield* RefStore;
          const packs = yield* PackStore;
          return yield* Effect.flip(Maintenance.gc({ objects, packs, refs }, { reflogGrace: 0 }));
        }).pipe(Effect.provide(stores(parent))) as unknown as Effect.Effect<{
          _tag: string;
          reason?: string;
        }>,
      );

      assert.equal(outcome._tag, "Invalid");
      assert.match(String(outcome.reason), /child/);

      // And the object the borrower reaches is still there.
      const held = await Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          return yield* objects.has(lent);
        }).pipe(Effect.provide(stores(parent))) as Effect.Effect<boolean>,
      );
      assert.equal(held, true);
    } finally {
      await fs.rm(root, { force: true, recursive: true });
    }
  });

  it("sees a fork made after the store was built", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-alternates-"));
    try {
      const [parent, child] = [path.join(root, "parent"), path.join(root, "child")];
      const layer = stores(child);

      const oid = await Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          return yield* objects.write({
            type: "blob",
            data: new TextEncoder().encode("later\n"),
          });
        }).pipe(Effect.provide(stores(parent))) as Effect.Effect<Oid>,
      );

      const seen = await Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          // Read once before the fork exists, which is what materialised an
          // empty answer for the life of the store.
          const before = yield* objects.has(oid);
          yield* Effect.promise(() => lend(child, parent));
          return { after: yield* objects.has(oid), before };
        }).pipe(Effect.provide(layer)) as unknown as Effect.Effect<{
          after: boolean;
          before: boolean;
        }>,
      );

      assert.equal(seen.before, false);
      assert.equal(seen.after, true, "the fork's alternates were never re-read");
    } finally {
      await fs.rm(root, { force: true, recursive: true });
    }
  });
});

/** The bound is on the file, so it has to be measured in the same unit. */
describe("reflog bound", () => {
  it("keeps a log with long messages under the cap", async () => {
    const size = await Effect.runPromise(
      Effect.promise(async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-reflog-"));
        try {
          const oid = "4".repeat(40) as Oid;
          await Effect.runPromise(
            Effect.gen(function* () {
              const refs = yield* RefStore;
              // 400-byte messages: 999 of them would still be far past the cap,
              // which is what a line-count trim would leave behind.
              for (let index = 0; index < 900; index++) {
                yield* refs.apply([
                  { name: "refs/heads/main", value: oid, reason: `commit: ${"m".repeat(400)}` },
                ]);
              }
            }).pipe(Effect.provide(stores(root))) as Effect.Effect<void>,
          );
          return (await fs.stat(path.join(root, "logs", "refs", "heads", "main"))).size;
        } finally {
          await fs.rm(root, { force: true, recursive: true });
        }
      }),
    );

    assert.ok(size < 512 * 1024, `reflog grew to ${size} bytes`);
  });
});
