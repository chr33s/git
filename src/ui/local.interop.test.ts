import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, describe, it } from "@effect/vitest";
import { Effect, Stream } from "effect";
import { vi } from "vitest";

import { fakeRoot } from "../adapters/Opfs.fake.ts";
import * as Opfs from "../adapters/Opfs.ts";
import { stores } from "../git/Node.ts";
import { ObjectStore, RefStore, isOid } from "../git/Store.ts";
import { fastImport, gitIn, hasGit, importCommit } from "../testing/Git.ts";

describe.skipIf(!hasGit)("local browser sync against git", () => {
  let directory: string | undefined;
  afterEach(async () => {
    vi.unstubAllGlobals();
    if (directory !== undefined) await fs.rm(directory, { recursive: true, force: true });
  });

  it.live("does not let a second initial clone hide an unpushed commit", () =>
    Effect.promise(async () => {
      directory = await fs.mkdtemp(path.join(os.tmpdir(), "local-open-"));
      const remoteDirectory = directory;
      gitIn(directory)("init", "-q", "--bare", "-b", "main");
      fastImport(
        directory,
        importCommit({ branch: "refs/heads/main", mark: 1, message: "base", files: [] }),
      );
      const origin: FileSystemDirectoryHandle = fakeRoot();
      const firstStarted = Promise.withResolvers<void>();
      const secondStarted = Promise.withResolvers<void>();
      const firstAllowed = Promise.withResolvers<void>();
      const secondAllowed = Promise.withResolvers<void>();
      let requests = 0;
      let locks = 0;
      let tail = Promise.resolve();
      vi.stubGlobal("document", { querySelector: () => null });
      vi.stubGlobal("location", { hostname: "fixture" });
      vi.stubGlobal("navigator", {
        storage: { getDirectory: async () => origin },
        locks: {
          request: <A>(_name: string, work: () => Promise<A>) => {
            if (++locks === 2) secondStarted.resolve();
            const result = tail.then(work);
            tail = result.then(
              () => {},
              () => {},
            );
            return result;
          },
        },
      });
      vi.stubGlobal("fetch", async (_url: string, options?: RequestInit) => {
        const advertised = options?.method !== "POST";
        if (advertised) {
          if (++requests === 1) {
            firstStarted.resolve();
            await firstAllowed.promise;
          } else {
            secondStarted.resolve();
            await secondAllowed.promise;
          }
        }
        const body = options?.body;
        assert.ok(advertised || body instanceof Uint8Array);
        const bytes = execFileSync(
          "git",
          [
            "upload-pack",
            "--stateless-rpc",
            ...(advertised ? ["--advertise-refs"] : []),
            remoteDirectory,
          ],
          { input: body instanceof Uint8Array ? body : undefined },
        );
        return new Response(bytes);
      });
      const { LocalGitApi } = await import("./local.ts");
      const options = { repo: "fixture", cloneUrl: "https://fixture" };
      const firstOpening = LocalGitApi.open(options);
      await firstStarted.promise;
      const secondOpening = LocalGitApi.open(options);
      try {
        // With locking, the second tab waits for the lock; without it, it
        // reaches a second advertisement. Both cases are explicitly observed.
        await secondStarted.promise;
        firstAllowed.resolve();
        const first = await firstOpening;
        assert.ok(first !== null);
        const committed = await first.commitFiles({
          branch: "main",
          message: "local work",
          files: [{ path: "a", content: "unpushed" }],
        });
        secondAllowed.resolve();
        const second = await secondOpening;
        assert.ok(second !== null);
        const sync = await second.sync("main");
        assert.equal(sync.ahead, 1);
        assert.equal(sync.behind, 0);
        assert.equal(
          (await second.refs()).find((ref) => ref.name === "refs/heads/main")?.oid,
          committed.oid,
        );
        assert.equal(requests, 1, "only the first opener clones");
      } finally {
        firstAllowed.resolve();
        secondAllowed.resolve();
        await Promise.allSettled([firstOpening, secondOpening]);
      }
    }),
  );

  it.live("reports mode-only and binary changes in local diffs", () =>
    Effect.promise(async () => {
      directory = await fs.mkdtemp(path.join(os.tmpdir(), "local-diff-"));
      const git = gitIn(directory);
      git("init", "-q", "-b", "main");
      await fs.writeFile(path.join(directory, "run.sh"), "echo hello\n");
      await fs.writeFile(path.join(directory, "image.bin"), new Uint8Array([0, 1]));
      await fs.writeFile(path.join(directory, "text.txt"), "old\n");
      git("add", ".");
      git("commit", "-qm", "before");
      const before = git("rev-parse", "HEAD").trim();
      git("update-index", "--chmod=+x", "run.sh");
      await fs.writeFile(path.join(directory, "image.bin"), new Uint8Array([0, 2]));
      await fs.writeFile(path.join(directory, "text.txt"), "new\n");
      git("add", "image.bin", "text.txt");
      git("commit", "-qm", "after");
      const after = git("rev-parse", "HEAD").trim();
      assert.ok(isOid(after));
      const origin: FileSystemDirectoryHandle = fakeRoot();
      const scope = await origin.getDirectoryHandle("git-plus", { create: true });
      const root = await scope.getDirectoryHandle("fixture", { create: true });
      const objects = await Effect.runPromise(
        Effect.gen(function* () {
          const store = yield* ObjectStore;
          return yield* Stream.runCollect(store.list.pipe(Stream.mapEffect(store.read)));
        }).pipe(Effect.provide(stores(path.join(directory, ".git")))),
      );
      await Effect.runPromise(
        Effect.gen(function* () {
          const store = yield* ObjectStore;
          for (const object of objects) yield* store.write(object);
          yield* (yield* RefStore).apply([{ name: "refs/heads/main", value: after }]);
        }).pipe(Effect.provide(Opfs.stores(root))),
      );
      vi.stubGlobal("document", { querySelector: () => null });
      vi.stubGlobal("location", { hostname: "fixture" });
      vi.stubGlobal("navigator", { storage: { getDirectory: async () => origin } });
      const { LocalGitApi } = await import("./local.ts");
      const api = await LocalGitApi.open({ repo: "fixture", cloneUrl: "https://fixture" });
      assert.ok(api !== null);
      const diff = await api.diff(before, after);
      assert.deepEqual(
        diff.map((file) => file.path),
        git("diff", "--name-only", before, after).trim().split("\n"),
      );
      assert.match(git("diff", "--numstat", before, after), /-\s+-\s+image.bin/);
      assert.deepEqual(
        diff.find((file) => file.path === "image.bin"),
        { path: "image.bin", status: "modified", binary: true, patch: "" },
      );
      assert.deepEqual(
        diff.find((file) => file.path === "run.sh"),
        { path: "run.sh", status: "modified", binary: false, patch: "" },
      );
      assert.match(
        diff.find((file) => file.path === "text.txt")?.patch ?? "",
        /--- a\/text.txt\n\+\+\+ b\/text.txt/,
      );
    }),
  );

  it.live("counts merged side commits even after encountering a common ancestor", () =>
    Effect.promise(async () => {
      directory = await fs.mkdtemp(path.join(os.tmpdir(), "local-sync-"));
      const git = gitIn(directory);
      git("init", "-q", "--bare");
      fastImport(
        directory,
        [
          importCommit({ branch: "refs/heads/main", mark: 1, message: "base", files: [] }),
          importCommit({ branch: "refs/heads/side", mark: 2, from: 1, message: "side", files: [] }),
          importCommit({
            branch: "refs/heads/remote",
            mark: 3,
            from: 1,
            message: "remote",
            files: [],
          }),
          importCommit({
            branch: "refs/heads/main",
            mark: 4,
            from: 3,
            merge: 2,
            message: "merge",
            files: [],
          }),
          importCommit({
            branch: "refs/heads/diverged",
            mark: 5,
            from: 3,
            message: "diverged",
            files: [],
          }),
          ...Array.from({ length: 256 }, (_, index) =>
            importCommit({
              branch: "refs/heads/long",
              mark: index + 6,
              from: index === 0 ? 3 : index + 5,
              message: `long ${index}`,
              files: [],
            }),
          ),
        ].join(""),
      );

      const origin: FileSystemDirectoryHandle = fakeRoot();
      const scope = await origin.getDirectoryHandle("git-plus", { create: true });
      const root = await scope.getDirectoryHandle("fixture", { create: true });
      const objects = await Effect.runPromise(
        Effect.gen(function* () {
          const store = yield* ObjectStore;
          return yield* Stream.runCollect(store.list.pipe(Stream.mapEffect(store.read)));
        }).pipe(Effect.provide(stores(directory))),
      );
      await Effect.runPromise(
        Effect.gen(function* () {
          const store = yield* ObjectStore;
          for (const object of objects) yield* store.write(object);
        }).pipe(Effect.provide(Opfs.stores(root))),
      );

      vi.stubGlobal("document", { querySelector: () => null });
      vi.stubGlobal("location", { hostname: "fixture" });
      vi.stubGlobal("navigator", { storage: { getDirectory: async () => origin } });
      const { LocalGitApi } = await import("./local.ts");
      for (const [left, right] of [
        ["main", "remote"],
        ["main", "diverged"],
        ["diverged", "main"],
        ["long", "remote"],
        ["remote", "long"],
        ["main", "main"],
      ]) {
        assert.ok(left !== undefined && right !== undefined);
        const local = git("rev-parse", left).trim();
        const remote = git("rev-parse", right).trim();
        assert.ok(isOid(local) && isOid(remote));
        await Effect.runPromise(
          Effect.gen(function* () {
            const refs = yield* RefStore;
            yield* refs.apply([
              { name: "refs/heads/main", value: local },
              { name: "refs/remotes/origin/main", value: remote },
            ]);
          }).pipe(Effect.provide(Opfs.stores(root))),
        );
        const api = await LocalGitApi.open({ repo: "fixture", cloneUrl: "https://fixture" });
        assert.ok(api !== null);
        const actual = await api.sync("main");
        const [ahead, behind] = git("rev-list", "--left-right", "--count", `${left}...${right}`)
          .trim()
          .split(/\s+/)
          .map(Number);
        assert.ok(ahead !== undefined && behind !== undefined);
        assert.deepEqual(
          { ahead: actual.ahead, behind: actual.behind },
          {
            ahead: Math.min(ahead, 250),
            behind: Math.min(behind, 250),
          },
        );
      }
    }),
  );
});
