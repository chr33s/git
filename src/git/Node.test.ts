/**
 * The filesystem backend against the same contract as the in-memory one.
 *
 * Two backends, one suite — and with `Cloudflare.integration.test.ts`, three.
 */
import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { beforeEach, describe, it } from "@effect/vitest";

import { Effect, Stream } from "effect";

import * as Maintenance from "./Maintenance.ts";
import { retirePacksUnder, stores } from "./Node.ts";
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

  // SAFETY: forty lowercase hex characters by construction, which is exactly
  // what the Oid brand stands for.
  const oid = "1".repeat(40) as Oid;
  // SAFETY: as above — forty lowercase hex characters by construction.
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
        }).pipe(Effect.provide(stores(root))),
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
    // SAFETY: forty lowercase hex characters by construction, which is exactly
    // what the Oid brand stands for.
    const moved = "3".repeat(40) as Oid;
    const read = await withRoot(async (root) => {
      await fs.writeFile(path.join(root, "packed-refs"), packedRefsFile);
      return Effect.runPromise(
        Effect.gen(function* () {
          const refs = yield* RefStore;
          yield* refs.apply([{ name: "refs/heads/main", value: moved }]);
          return yield* refs.read("refs/heads/main");
        }).pipe(Effect.provide(stores(root))),
      );
    });

    assert.equal(read, moved);
  });

  it("re-reads packed-refs after another process rewrites it", async () => {
    // SAFETY: forty lowercase hex characters by construction, which is exactly
    // what the Oid brand stands for.
    const later = "4".repeat(40) as Oid;
    const seen = await withRoot(async (root) => {
      const file = path.join(root, "packed-refs");
      await fs.writeFile(file, packedRefsFile);
      return Effect.runPromise(
        Effect.gen(function* () {
          const refs = yield* RefStore;

          // Read once, so the parse is memoized…
          const before = yield* refs.read("refs/heads/main");

          // …then `git pack-refs` (or `git gc`) rewrites the file underneath.
          // The memo is keyed on the file, not on how long ago it was read, or
          // this store would serve a branch's old tip until the process died.
          yield* Effect.promise(() =>
            fs.writeFile(
              file,
              `# pack-refs with: peeled fully-peeled sorted \n${later} refs/heads/main\n`,
            ),
          );

          return { before, after: yield* refs.read("refs/heads/main") };
        }).pipe(Effect.provide(stores(root))),
      );
    });

    assert.equal(seen.before, oid);
    assert.equal(seen.after, later);
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
        }).pipe(Effect.provide(stores(root))),
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
          }).pipe(Effect.provide(stores(root))),
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
        }).pipe(Effect.provide(stores(root))),
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
        }).pipe(Effect.provide(layer)),
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

  // SAFETY: forty lowercase hex characters by construction, which is exactly
  // what the Oid brand stands for.
  const oid = "5".repeat(40) as Oid;
  // SAFETY: as above — forty lowercase hex characters by construction.
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
        }).pipe(Effect.provide(stores(root))),
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
        }).pipe(Effect.provide(stores(root))),
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
        }).pipe(Effect.provide(stores(root))),
      ),
    );

    assert.equal(state.applied, 0);
    assert.equal(state.feature, null, "an atomic batch left half of itself applied");
    assert.equal(state.keep, oid);
  });

  it("writes no reflog for an atomic batch it rolled back", async () => {
    const state = await withRoot((root) =>
      Effect.runPromise(
        Effect.gen(function* () {
          const refs = yield* RefStore;

          // Same shape as above: the first write lands, the second cannot, and
          // the batch is undone. The ref goes back — but a log line already
          // appended cannot be taken out again, so `logs/refs/heads/feature`
          // recorded a move that did not happen. `Maintenance.gc` reads reflog
          // entries as roots, so that phantom entry also pinned the commit it
          // named for the whole grace window.
          yield* refs.apply(
            [
              { name: "refs/heads/feature", value: other },
              { name: "refs/heads/feature/sub", value: other },
            ],
            { atomic: true },
          );

          return {
            log: (yield* refs.reflog("refs/heads/feature")).length,
            logged: yield* refs.logged,
          };
        }).pipe(Effect.provide(stores(root))),
      ),
    );

    assert.equal(state.log, 0, "a rolled-back update left its move in the reflog");
    assert.deepEqual(state.logged, []);
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
        }).pipe(Effect.provide(stores(root))),
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
        }).pipe(Effect.provide(stores(a))),
      );

      // A fork of a fork: `c` reaches `a`'s object only if the chain is
      // followed, which is what git itself does.
      await lend(b, a);
      await lend(c, b);

      const held = await Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          return { has: yield* objects.has(oid), read: (yield* objects.read(oid)).type };
        }).pipe(Effect.provide(stores(c))),
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
        }).pipe(Effect.provide(stores(parent))),
      );

      await lend(child, parent);

      const listed = await Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          return { has: yield* objects.has(lent), all: yield* Stream.runCollect(objects.list) };
        }).pipe(Effect.provide(stores(child))),
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
        }).pipe(Effect.provide(stores(parent))),
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
        }).pipe(Effect.provide(stores(parent))),
      );

      assert.equal(outcome._tag, "Invalid");
      assert.match(String(outcome.reason), /child/);

      // And the object the borrower reaches is still there.
      const held = await Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          return yield* objects.has(lent);
        }).pipe(Effect.provide(stores(parent))),
      );
      assert.equal(held, true);
    } finally {
      await fs.rm(root, { force: true, recursive: true });
    }
  });

  it("refuses to collect a lender whose borrower is linked into place", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-alternates-"));
    try {
      const parent = path.join(root, "parent");
      // The borrower kept somewhere else and linked in beside the parent,
      // which is how a repository ends up on another disk.
      const elsewhere = path.join(root, "elsewhere", "child");
      const child = path.join(root, "child");

      const lent = await Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          return yield* objects.write({
            type: "blob",
            data: new TextEncoder().encode("borrowed\n"),
          });
        }).pipe(Effect.provide(stores(parent))),
      );

      // Nothing on the parent's side: this is the `git clone --shared` shape
      // the sibling scan exists for.
      await lend(elsewhere, parent);
      await fs.symlink(elsewhere, child);

      const outcome = await Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          const refs = yield* RefStore;
          const packs = yield* PackStore;
          return yield* Effect.flip(Maintenance.gc({ objects, packs, refs }, { reflogGrace: 0 }));
        }).pipe(Effect.provide(stores(parent))),
      );

      assert.equal(outcome._tag, "Invalid");
      assert.match(String(outcome.reason), /child/);

      const held = await Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          return yield* objects.has(lent);
        }).pipe(Effect.provide(stores(parent))),
      );
      assert.equal(held, true, "gc collected an object its borrower reads");
    } finally {
      await fs.rm(root, { force: true, recursive: true });
    }
  });

  it("refuses to collect a lender its borrower names through a symlink", async () => {
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
        }).pipe(Effect.provide(stores(parent))),
      );

      // The same object directory, spelled the way whoever made the fork had
      // it: `git clone --shared` writes the path it was given.
      const link = path.join(root, "by-another-name");
      await fs.symlink(parent, link);
      const info = path.join(child, "objects", "info");
      await fs.mkdir(info, { recursive: true });
      await fs.writeFile(path.join(info, "alternates"), `${path.join(link, "objects")}\n`);
      const record = path.join(parent, "objects", "info");
      await fs.mkdir(record, { recursive: true });
      await fs.writeFile(path.join(record, "borrowers"), "child\n");

      const outcome = await Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          const refs = yield* RefStore;
          const packs = yield* PackStore;
          return yield* Effect.flip(Maintenance.gc({ objects, packs, refs }, { reflogGrace: 0 }));
        }).pipe(Effect.provide(stores(parent))),
      );

      assert.equal(outcome._tag, "Invalid");
      assert.match(String(outcome.reason), /child/);

      const held = await Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          return yield* objects.has(lent);
        }).pipe(Effect.provide(stores(parent))),
      );
      assert.equal(held, true, "gc collected an object its borrower reads");
    } finally {
      await fs.rm(root, { force: true, recursive: true });
    }
  });

  it("refuses to collect a recorded lender opened by a relative path", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-alternates-"));
    const cwd = process.cwd();
    try {
      const [parent, child] = [path.join(root, "parent"), path.join(root, "child")];

      const lent = await Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          return yield* objects.write({
            type: "blob",
            data: new TextEncoder().encode("borrowed\n"),
          });
        }).pipe(Effect.provide(stores(parent))),
      );
      await lend(child, parent);
      // The other half, which this server writes and `git` does not: the
      // parent's own record of who reads through it.
      const record = path.join(parent, "objects", "info");
      await fs.mkdir(record, { recursive: true });
      await fs.writeFile(path.join(record, "borrowers"), "child\n");

      // `gc .` from inside the repository, which is what the CLI's default
      // `--root` gives it. A borrower is looked up beside the repository, and
      // `path.dirname(".")` is `"."` — inside it.
      process.chdir(parent);
      const outcome = await Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          const refs = yield* RefStore;
          const packs = yield* PackStore;
          return yield* Effect.flip(Maintenance.gc({ objects, packs, refs }, { reflogGrace: 0 }));
        }).pipe(Effect.provide(stores("."))),
      );

      assert.equal(outcome._tag, "Invalid");
      assert.match(String(outcome.reason), /child/);

      const held = await Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          return yield* objects.has(lent);
        }).pipe(Effect.provide(stores(parent))),
      );
      assert.equal(held, true, "gc collected an object its borrower reads");
    } finally {
      process.chdir(cwd);
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
        }).pipe(Effect.provide(stores(parent))),
      );

      const seen = await Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          // Read once before the fork exists, which is what materialised an
          // empty answer for the life of the store.
          const before = yield* objects.has(oid);
          yield* Effect.promise(() => lend(child, parent));
          return { after: yield* objects.has(oid), before };
        }).pipe(Effect.provide(layer)),
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
          // SAFETY: forty lowercase hex characters by construction, which is exactly
          // what the Oid brand stands for.
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
            }).pipe(Effect.provide(stores(root))),
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

/**
 * Pack reads share one descriptor per file, and give it back.
 *
 * The cost of getting this wrong is not slowness. A descriptor left to the
 * garbage collector is a fatal `ERR_INVALID_STATE` on node 26 — the host dies
 * mid-clone — and one held against a deleted pack keeps its blocks allocated,
 * which is the space `gc` was asked to reclaim.
 */
describe("pack descriptors", () => {
  // Every repository these tests build lives under the temp directory, so
  // this empties the process-wide pool between them. They count descriptors
  // exactly, and a pool left at its cap by a neighbour turns the next read
  // into an evict-and-reopen that adds nothing to the count.
  beforeEach(() => retirePacksUnder(os.tmpdir()));

  /**
   * Descriptors this process holds against `.pack` files, by `/proc`.
   *
   * Substring, not suffix: an unlinked file's link reads `…​.pack (deleted)`,
   * and a descriptor on a deleted pack — the one still pinning its blocks — is
   * the entire thing these tests are looking for.
   */
  const packDescriptors = async (): Promise<number> => {
    const entries = await fs.readdir("/proc/self/fd");
    const targets = await Promise.all(
      entries.map((entry) => fs.readlink(path.join("/proc/self/fd", entry)).catch(() => "")),
    );
    return targets.filter((target) => target.includes(".pack")).length;
  };

  /**
   * The same count, once the closes the pool started have landed.
   *
   * Eviction closes without waiting — the caller has nothing to wait for —
   * so a count taken immediately after can still see a descriptor on its way
   * out. Anything that settles is not a leak; anything that does not, is.
   */
  const settledDescriptors = async (within: number): Promise<number> => {
    let held = await packDescriptors();
    for (let attempt = 0; attempt < 50 && held > within; attempt++) {
      await new Promise((resolve) => setTimeout(resolve, 10));
      held = await packDescriptors();
    }
    return held;
  };

  const packed = async (root: string, blobs: number): Promise<ReadonlyArray<Oid>> =>
    await Effect.runPromise(
      Effect.gen(function* () {
        const objects = yield* ObjectStore;
        const refs = yield* RefStore;
        const packs = yield* PackStore;
        const written: Oid[] = [];
        for (let index = 0; index < blobs; index++) {
          written.push(
            yield* objects.write({
              type: "blob",
              data: new TextEncoder().encode(`packed ${index}\n`),
            }),
          );
        }
        yield* refs.apply(
          written.map((oid, index) => ({ name: `refs/heads/b${index}`, value: oid })),
        );
        yield* Maintenance.gc({ objects, packs, refs }, { repack: true, reflogGrace: 0 });
        return written;
      }).pipe(Effect.provide(stores(root))),
    );

  it.skipIf(process.platform !== "linux")(
    "reads a pack many times through one descriptor",
    async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-packfd-"));
      try {
        const oids = await packed(root, 25);
        const before = await packDescriptors();

        const read = await Effect.runPromise(
          Effect.gen(function* () {
            const objects = yield* ObjectStore;
            let count = 0;
            for (const oid of oids) if ((yield* objects.read(oid)).data.length > 0) count += 1;
            return count;
          }).pipe(Effect.provide(stores(root))),
        );

        assert.equal(read, oids.length, "the pack did not read back");
        // 25 objects, one descriptor: the open/read/close per object is the bug
        // this pool exists to remove, and more than one would mean it leaks.
        assert.equal(
          await packDescriptors(),
          before + 1,
          "a pack read leaked or reopened descriptors",
        );
      } finally {
        await retirePacksUnder(root);
        await fs.rm(root, { force: true, recursive: true });
        await retirePacksUnder(root);
      }
    },
  );

  it.skipIf(process.platform !== "linux")(
    "survives more packs read at once than the pool may hold",
    async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-packfd-"));
      try {
        // More repositories than `OPEN_PACKS`, all read at once: the pool has
        // to shed entries while every one of them is busy. Evicting a claimed
        // entry closes a descriptor out from under a read — `EBADF: file
        // closed` — and evicting only idle ones leaves the map above its cap
        // until something brings it back down.
        const repositories: Array<{ root: string; oids: ReadonlyArray<Oid> }> = [];
        for (let index = 0; index < 70; index++) {
          const each = path.join(root, `r${index}`);
          repositories.push({ root: each, oids: await packed(each, 1) });
        }

        const read = ({ root: from, oids }: { root: string; oids: ReadonlyArray<Oid> }) =>
          Effect.runPromise(
            Effect.gen(function* () {
              const objects = yield* ObjectStore;
              for (const oid of oids) yield* objects.read(oid);
            }).pipe(Effect.provide(stores(from))),
          );

        await Promise.all(repositories.map(read));
        await Promise.all(repositories.flatMap((each) => [read(each), read(each), read(each)]));

        // What the cap holds *during* a burst is not observable from here —
        // sampling `/proc` gets a handful of samples against hundreds of
        // reads and misses the peak either way — so this asserts the part
        // that is: nothing crashed, and the pool comes back to its cap.
        for (const repository of repositories) await read(repository);
        const held = await settledDescriptors(64);
        assert.ok(held <= 64, `the pool settled at ${held} descriptors, over its cap`);
      } finally {
        // These 70 repositories would otherwise leave the pool full for
        // whatever runs next, which is what dropping one is supposed to
        // avoid — and what the test above this one is about.
        await retirePacksUnder(root);
        await fs.rm(root, { force: true, recursive: true });
        await retirePacksUnder(root);
      }
    },
  );

  it.skipIf(process.platform !== "linux")(
    "retires a repository whose objects live behind a symlink",
    async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-packfd-"));
      const elsewhere = await fs.mkdtemp(path.join(os.tmpdir(), "git-objects-"));
      try {
        const oids = await packed(root, 4);

        // `objects` moved to another disk and symlinked back, which git has
        // always allowed: the packs are not under the repository at all.
        await fs.rename(path.join(root, "objects"), path.join(elsewhere, "objects"));
        await fs.symlink(path.join(elsewhere, "objects"), path.join(root, "objects"));

        const before = await packDescriptors();
        await Effect.runPromise(
          Effect.gen(function* () {
            const objects = yield* ObjectStore;
            for (const oid of oids) yield* objects.read(oid);
          }).pipe(Effect.provide(stores(root))),
        );
        assert.equal(await packDescriptors(), before + 1, "the pack was not pooled");

        await retirePacksUnder(root);
        assert.equal(await packDescriptors(), before, "the retire missed the symlinked objects");
      } finally {
        await retirePacksUnder(root);
        await fs.rm(root, { force: true, recursive: true });
        await fs.rm(elsewhere, { force: true, recursive: true });
      }
    },
  );

  it.skipIf(process.platform !== "linux")(
    "releases a removed pack whose spelling can no longer be resolved",
    async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-packfd-"));
      const link = `${root}-link`;
      try {
        const oids = await packed(root, 4);
        await fs.symlink(root, link);
        const before = await packDescriptors();

        await Effect.runPromise(
          Effect.gen(function* () {
            const objects = yield* ObjectStore;
            for (const oid of oids) yield* objects.read(oid);
          }).pipe(Effect.provide(stores(link))),
        );
        assert.equal(await packDescriptors(), before + 1, "the pack was not pooled");

        // The memo that remembers what a path resolved to is bounded, and a
        // busy host fills it — only paths that resolve are remembered, so
        // these are made first. Once this entry is out, the link below is the
        // only way back to the canonical name, and it is about to go.
        const filler = await fs.mkdtemp(path.join(os.tmpdir(), "git-packfd-memo-"));
        for (let index = 0; index < 1100; index += 1) {
          const each = path.join(filler, String(index));
          await fs.mkdir(each);
          await retirePacksUnder(each);
        }
        await fs.rm(filler, { force: true, recursive: true });

        await fs.rm(link);
        await fs.rm(root, { force: true, recursive: true });
        await retirePacksUnder(link);

        assert.equal(
          await settledDescriptors(before),
          before,
          "a descriptor on a deleted pack was held for the life of the process",
        );
      } finally {
        await retirePacksUnder(root);
        await fs.rm(link, { force: true });
        await fs.rm(root, { force: true, recursive: true });
      }
    },
  );

  it.skipIf(process.platform !== "linux")(
    "retires and re-checks through a symlinked root too",
    async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-packfd-"));
      const other = await fs.mkdtemp(path.join(os.tmpdir(), "git-packfd-"));
      const link = `${root}-link`;
      try {
        const mine = await packed(root, 4);
        const theirs = await packed(other, 6);
        await fs.symlink(root, link);
        const before = await packDescriptors();

        const read = (from: string, oids: ReadonlyArray<Oid>) =>
          Effect.runPromise(
            Effect.gen(function* () {
              const objects = yield* ObjectStore;
              for (const oid of oids) yield* objects.read(oid);
            }).pipe(Effect.provide(stores(from))),
          );

        // Everything below goes through the link, which is what a host given
        // a symlinked root does, and what `os.tmpdir()` is on macOS.
        await read(link, mine);

        const packs = path.join(root, "objects", "pack");
        const theirPacks = path.join(other, "objects", "pack");
        const [name] = (await fs.readdir(packs)).filter((each) => each.endsWith(".pack"));
        const [replacement] = (await fs.readdir(theirPacks)).filter((each) =>
          each.endsWith(".pack"),
        );
        for (const extension of [".pack", ".idx"]) {
          const staged = path.join(packs, `incoming${extension}`);
          await fs.copyFile(
            path.join(theirPacks, `${`${replacement}`.slice(0, -5)}${extension}`),
            staged,
          );
          await fs.rename(staged, path.join(packs, `${`${name}`.slice(0, -5)}${extension}`));
        }

        await read(link, theirs);

        await retirePacksUnder(link);
        assert.equal(await packDescriptors(), before, "a retire through the link released nothing");
      } finally {
        await retirePacksUnder(root);
        await retirePacksUnder(other);
        await fs.rm(link, { force: true });
        await fs.rm(root, { force: true, recursive: true });
        await fs.rm(other, { force: true, recursive: true });
      }
    },
  );

  it.skipIf(process.platform !== "linux")(
    "keys one pack by one descriptor through a symlinked root",
    async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-packfd-"));
      const link = `${root}-link`;
      try {
        const oids = await packed(root, 4);
        await fs.symlink(root, link);
        const before = await packDescriptors();

        const read = (from: string) =>
          Effect.runPromise(
            Effect.gen(function* () {
              const objects = yield* ObjectStore;
              for (const oid of oids) yield* objects.read(oid);
            }).pipe(Effect.provide(stores(from))),
          );

        // The same repository, reached two ways — which is what an alternate
        // recorded through a symlink gives, and what a case-insensitive
        // filesystem gives for free.
        await read(root);
        await read(link);

        assert.equal(await packDescriptors(), before + 1, "one pack took two descriptors");
      } finally {
        await retirePacksUnder(root);
        await fs.rm(link, { force: true });
        await fs.rm(root, { force: true, recursive: true });
        await retirePacksUnder(root);
      }
    },
  );

  it.skipIf(process.platform !== "linux")(
    "reads the pack that is there now, not the one it opened",
    async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-packfd-"));
      const other = await fs.mkdtemp(path.join(os.tmpdir(), "git-packfd-"));
      try {
        const mine = await packed(root, 4);
        const theirs = await packed(other, 6);

        const read = (from: string, oids: ReadonlyArray<Oid>) =>
          Effect.runPromise(
            Effect.gen(function* () {
              const objects = yield* ObjectStore;
              const found: string[] = [];
              for (const oid of oids)
                found.push(new TextDecoder().decode((yield* objects.read(oid)).data));
              return found;
            }).pipe(Effect.provide(stores(from))),
          );

        assert.equal((await read(root, mine)).length, 4, "the pack did not read back");

        // Somebody else puts a different pack in place under the same name —
        // a restore from backup, an `mv`, anything that is not this process.
        // The descriptor still points at the old inode; the `.idx` read on the
        // next listing describes the new one.
        const packs = path.join(root, "objects", "pack");
        const theirPacks = path.join(other, "objects", "pack");
        const [name] = (await fs.readdir(packs)).filter((each) => each.endsWith(".pack"));
        const [replacement] = (await fs.readdir(theirPacks)).filter((each) =>
          each.endsWith(".pack"),
        );
        const stem = `${name}`.slice(0, -5);
        const theirStem = `${replacement}`.slice(0, -5);
        // Staged and renamed into place, which is how git writes a pack and
        // how rsync replaces one: a new inode under the old name. A plain
        // copy truncates the file that is already there, and the pooled
        // descriptor would still be looking at the right bytes.
        for (const extension of [".pack", ".idx"]) {
          const staged = path.join(packs, `incoming${extension}`);
          await fs.copyFile(path.join(theirPacks, `${theirStem}${extension}`), staged);
          await fs.rename(staged, path.join(packs, `${stem}${extension}`));
        }

        const after = await read(root, theirs);
        assert.equal(after.length, 6, "the replaced pack did not read back");
      } finally {
        await retirePacksUnder(root);
        await retirePacksUnder(other);
        await fs.rm(root, { force: true, recursive: true });
        await fs.rm(other, { force: true, recursive: true });
      }
    },
  );

  it.skipIf(process.platform !== "linux")(
    "lets go of a pack another process repacked away",
    async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-packfd-"));
      try {
        const oids = await packed(root, 4);
        const before = await packDescriptors();

        const read = () =>
          Effect.runPromise(
            Effect.gen(function* () {
              const objects = yield* ObjectStore;
              for (const oid of oids) yield* objects.read(oid);
            }).pipe(Effect.provide(stores(root))),
          );
        await read();
        assert.equal(await packDescriptors(), before + 1, "the pack was not pooled");

        // What another process's repack leaves behind: the pack this one is
        // holding open is gone from the directory under the name it knows,
        // and nothing told it. `packs.delete` never ran — only the listing
        // changed, which is the one place it can still find out.
        const directory = path.join(root, "objects", "pack");
        for (const name of await fs.readdir(directory)) {
          const extension = path.extname(name);
          await fs.rename(
            path.join(directory, name),
            path.join(directory, `pack-${"e".repeat(40)}${extension}`),
          );
        }

        await read();

        assert.equal(await packDescriptors(), before + 1, "the repacked-away pack is still held");
      } finally {
        await retirePacksUnder(root);
        await fs.rm(root, { force: true, recursive: true });
        await retirePacksUnder(root);
      }
    },
  );

  it.skipIf(process.platform !== "linux")(
    "releases a whole repository's descriptors when the directory goes",
    async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-packfd-"));
      try {
        const oids = await packed(root, 4);
        const before = await packDescriptors();

        await Effect.runPromise(
          Effect.gen(function* () {
            const objects = yield* ObjectStore;
            for (const oid of oids) yield* objects.read(oid);
          }).pipe(Effect.provide(stores(root))),
        );
        assert.equal(await packDescriptors(), before + 1, "the pack was not pooled");

        // What dropping a repository does, in the order `drop` does it:
        // release the descriptors, unlink the directory naming no pack in
        // it, then collect anything a read re-pooled in between.
        await retirePacksUnder(root);
        await fs.rm(root, { recursive: true, force: true });
        await retirePacksUnder(root);

        assert.equal(await packDescriptors(), before, "a dropped repository kept its descriptors");
      } finally {
        await retirePacksUnder(root);
        await fs.rm(root, { force: true, recursive: true });
        await retirePacksUnder(root);
      }
    },
  );

  it.skipIf(process.platform !== "linux")(
    "keys one pack by one descriptor however the root is spelled",
    async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-packfd-"));
      try {
        const oids = await packed(root, 4);
        const before = await packDescriptors();

        // `--root .` hands the CLI a relative root, while an alternate is
        // always resolved absolute before it is followed: the same pack,
        // spelled two ways. Keyed as they come, the second spelling opens a
        // second descriptor and the owner's delete retires only its own.
        const relative = path.relative(process.cwd(), root);
        await Effect.runPromise(
          Effect.gen(function* () {
            const objects = yield* ObjectStore;
            for (const oid of oids) yield* objects.read(oid);
          }).pipe(Effect.provide(stores(root))),
        );
        await Effect.runPromise(
          Effect.gen(function* () {
            const objects = yield* ObjectStore;
            for (const oid of oids) yield* objects.read(oid);
          }).pipe(Effect.provide(stores(relative))),
        );

        assert.equal(await packDescriptors(), before + 1, "one pack took two descriptors");

        await Effect.runPromise(
          Effect.gen(function* () {
            const packs = yield* PackStore;
            for (const handle of yield* packs.list) yield* packs.delete(handle.name);
          }).pipe(Effect.provide(stores(relative))),
        );

        assert.equal(await packDescriptors(), before, "a delete missed the other spelling");
      } finally {
        await retirePacksUnder(root);
        await fs.rm(root, { force: true, recursive: true });
        await retirePacksUnder(root);
      }
    },
  );

  it.skipIf(process.platform !== "linux")(
    "gives the descriptor back when the pack is deleted",
    async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-packfd-"));
      try {
        const oids = await packed(root, 4);
        const before = await packDescriptors();

        await Effect.runPromise(
          Effect.gen(function* () {
            const objects = yield* ObjectStore;
            const packs = yield* PackStore;
            for (const oid of oids) yield* objects.read(oid);
            for (const handle of yield* packs.list) yield* packs.delete(handle.name);
          }).pipe(Effect.provide(stores(root))),
        );

        assert.equal(await packDescriptors(), before, "a deleted pack kept its descriptor open");
      } finally {
        await retirePacksUnder(root);
        await fs.rm(root, { force: true, recursive: true });
        await retirePacksUnder(root);
      }
    },
  );
});
