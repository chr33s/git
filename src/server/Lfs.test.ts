/**
 * Git LFS over the wire, against the node host.
 *
 * The `git-lfs` binary is not a dependency of this repository, so the test
 * speaks the batch protocol itself — which is all `git-lfs` does: ask the
 * batch endpoint where to put or get an object, then PUT or GET it there.
 * Driving it through `host/Node.ts` covers the routing too, and the routing
 * is the interesting part: LFS shares the `info/` prefix with the smart-HTTP
 * advertisement.
 */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterAll, beforeAll, describe, it } from "@effect/vitest";

import { Deferred, Effect, Fiber, Stream } from "effect";

import { StorageFailure } from "../git/Error.ts";

import { serve, type Server } from "../host/Node.ts";
import { formatPointer, LfsStore, MEDIA_TYPE, parsePointer } from "./Lfs.ts";
import { file } from "./Lfs.node.ts";

const sha256 = (content: string): string => createHash("sha256").update(content).digest("hex");

interface BatchObject {
  readonly oid: string;
  readonly size: number;
  readonly actions?: {
    readonly upload?: { readonly href: string };
    readonly download?: { readonly href: string };
  };
  readonly error?: { readonly code: number; readonly message: string };
}

describe("Git LFS", () => {
  let root: string;
  let server: Server;

  const batch = async (
    repo: string,
    operation: "upload" | "download",
    objects: ReadonlyArray<{ oid: string; size: number }>,
  ) => {
    const response = await fetch(`${server.url}/${repo}/info/lfs/objects/batch`, {
      method: "POST",
      headers: { "content-type": MEDIA_TYPE, accept: MEDIA_TYPE },
      body: JSON.stringify({ operation, transfers: ["basic"], objects }),
    });
    assert.equal(response.headers.get("content-type"), MEDIA_TYPE);
    // SAFETY: the server under test speaks the batch protocol; a reply that
    // does not, fails the assertions each test makes on these fields.
    return { status: response.status, body: (await response.json()) as { objects: BatchObject[] } };
  };

  beforeAll(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "git-lfs-"));
    server = await serve({ root, allowAnonymousWrites: true });
  });

  afterAll(async () => {
    await server.close();
    await fs.rm(root, { recursive: true, force: true });
  });

  it.effect("round-trips an object through batch, upload and download", () =>
    Effect.promise(async () => {
      const content = "a large file, notionally\n";
      const oid = sha256(content);

      // Upload: not held yet, so the server hands back somewhere to put it.
      const asked = await batch("r", "upload", [{ oid, size: content.length }]);
      const action = asked.body.objects[0]!;
      assert.equal(action.oid, oid);
      const target = action.actions?.upload?.href;
      assert.ok(target !== undefined, "the batch reply offers an upload href");

      const put = await fetch(target, { method: "PUT", body: content });
      assert.equal(put.status, 200);

      // Asking again offers no action: the object is already here, which is
      // what makes re-pushing an unchanged file free.
      const again = await batch("r", "upload", [{ oid, size: content.length }]);
      assert.equal(again.body.objects[0]!.actions, undefined);
      assert.equal(again.body.objects[0]!.size, content.length);

      const download = await batch("r", "download", [{ oid, size: content.length }]);
      const href = download.body.objects[0]!.actions?.download?.href;
      assert.ok(href !== undefined, "the batch reply offers a download href");

      const got = await fetch(href);
      assert.equal(got.status, 200);
      assert.equal(await got.text(), content);
      assert.equal(got.headers.get("content-length"), String(content.length));
    }),
  );

  for (const fault of ["invalid-shard", "directory-object"] as const) {
    it.effect(`reports ${fault} as a storage outage and recovers after repair`, () =>
      Effect.promise(async () => {
        const repo = `outage-${fault}`;
        const content = "LFS storage outage recovery";
        const oid = sha256(content);
        const directory = path.join(root, repo, "lfs");
        const target = path.join(directory, oid.slice(0, 2), oid.slice(2, 4), oid);
        const broken = fault === "invalid-shard" ? path.dirname(target) : target;
        await fs.mkdir(path.dirname(broken), { recursive: true });
        if (fault === "invalid-shard") await fs.writeFile(broken, "not a directory");
        else await fs.mkdir(broken);
        const href = `${server.url}/${repo}/info/lfs/objects/${oid}`;
        try {
          for (const operation of ["download", "upload"] as const) {
            const asked = await batch(repo, operation, [{ oid, size: content.length }]);
            assert.equal(asked.status, 200);
            assert.equal(asked.body.objects[0]?.error?.code, 500);
            assert.equal(asked.body.objects[0]?.actions, undefined);
          }
          const response = await fetch(href);
          assert.equal(response.status, 500);
          assert.match(await response.text(), /object storage unavailable/);
          const direct = await Effect.runPromise(
            LfsStore.use((store) => store.read(oid)).pipe(
              Effect.provide(file(directory)),
              Effect.flip,
            ),
          );
          assert.equal(direct._tag, "StorageFailure");
        } finally {
          await fs.rm(broken, { recursive: true, force: true });
        }
        const absent = await fetch(href);
        assert.equal(absent.status, 404);
        await absent.arrayBuffer();
        const upload = await fetch(href, { method: "PUT", body: content });
        assert.equal(upload.status, 200);
        await upload.arrayBuffer();
        const recovered = await fetch(href);
        assert.equal(recovered.status, 200);
        assert.equal(await recovered.text(), content);
      }),
    );
  }

  it.effect("refuses content that does not hash to the name it was given", () =>
    Effect.promise(async () => {
      const oid = sha256("what was promised");

      const asked = await batch("r", "upload", [{ oid, size: 9 }]);
      const href = asked.body.objects[0]!.actions!.upload!.href;

      const put = await fetch(href, { method: "PUT", body: "something else entirely" });
      assert.equal(put.status, 422);

      // And nothing was stored: a rejected upload leaves no trace to serve.
      const download = await batch("r", "download", [{ oid, size: 9 }]);
      assert.equal(download.body.objects[0]!.error?.code, 404);
    }),
  );

  it.effect("reports a missing object rather than inventing an action", () =>
    Effect.promise(async () => {
      const oid = sha256("never uploaded");
      const download = await batch("r", "download", [{ oid, size: 4 }]);
      assert.equal(download.body.objects[0]!.actions, undefined);
      assert.equal(download.body.objects[0]!.error?.code, 404);
    }),
  );

  it.effect("rejects malformed requests with the LFS media type", () =>
    Effect.promise(async () => {
      const bad = await batch("r", "download", [{ oid: "not-an-oid", size: 1 }]);
      assert.equal(bad.body.objects[0]!.error?.code, 422);

      const unsupported = await fetch(`${server.url}/r/info/lfs/objects/batch`, {
        method: "POST",
        headers: { "content-type": MEDIA_TYPE },
        body: JSON.stringify({ operation: "teleport", objects: [] }),
      });
      assert.equal(unsupported.status, 422);

      const wrongMethod = await fetch(`${server.url}/r/info/lfs/objects/batch`);
      assert.equal(wrongMethod.status, 405);
    }),
  );

  it.effect("keeps repositories apart", () =>
    Effect.promise(async () => {
      const content = "belongs to one repo\n";
      const oid = sha256(content);

      const asked = await batch("first", "upload", [{ oid, size: content.length }]);
      await fetch(asked.body.objects[0]!.actions!.upload!.href, { method: "PUT", body: content });

      const elsewhere = await batch("second", "download", [{ oid, size: content.length }]);
      assert.equal(elsewhere.body.objects[0]!.error?.code, 404);
    }),
  );

  it.effect("does not shadow the smart-HTTP advertisement it shares a prefix with", () =>
    Effect.promise(async () => {
      const advertisement = await fetch(`${server.url}/r/info/refs?service=git-upload-pack`);
      assert.equal(advertisement.status, 200);
      assert.equal(
        advertisement.headers.get("content-type"),
        "application/x-git-upload-pack-advertisement",
      );
    }),
  );

  it.effect("reads and writes pointer files", () =>
    Effect.sync(() => {
      const pointer = formatPointer({ oid: sha256("x"), size: 1234 });
      assert.deepEqual(parsePointer(pointer), { oid: sha256("x"), size: 1234 });

      assert.equal(parsePointer("not a pointer"), null);
      assert.equal(
        parsePointer("version https://git-lfs.github.com/spec/v1\noid sha256:short\nsize 1\n"),
        null,
      );
    }),
  );

  it.effect("leaves no partial file behind when an upload fails part-way", () =>
    Effect.gen(function* () {
      const root = yield* Effect.promise(() => fs.mkdtemp(path.join(os.tmpdir(), "lfs-abort-")));
      const oid = sha256("never arrives");
      // A client that sends some bytes and then drops the connection.
      const body = Stream.make(new TextEncoder().encode("never")).pipe(
        Stream.concat(
          Stream.fail(new StorageFailure({ operation: "lfs.upload", path: oid, cause: "reset" })),
        ),
      );

      const result = yield* LfsStore.use((store) => store.write(oid, body)).pipe(
        Effect.provide(file(root)),
        Effect.flip,
      );
      assert.equal(result._tag, "StorageFailure");

      const left = yield* Effect.promise(() => fs.readdir(root, { recursive: true }));
      assert.deepEqual(
        left.filter((entry) => entry.endsWith(".tmp")),
        [],
      );
      yield* Effect.promise(() => fs.rm(root, { recursive: true, force: true }));
    }),
  );

  it.effect("publishes private files through new directory chains and reopens them", () =>
    Effect.promise(async () => {
      const base = await fs.mkdtemp(path.join(os.tmpdir(), "lfs-publish-"));
      const directory = path.join(base, "new", "repository", "lfs");
      const content = "durable publication fixture";
      const oid = sha256(content);
      const target = path.join(directory, oid.slice(0, 2), oid.slice(2, 4), oid);
      try {
        const result = await Effect.runPromise(
          LfsStore.use((store) =>
            store.write(oid, Stream.make(new TextEncoder().encode(content))),
          ).pipe(Effect.provide(file(directory))),
        );
        assert.deepEqual(result, { oid, size: content.length });
        assert.equal((await fs.stat(target)).mode & 0o777, 0o600);
        assert.equal((await fs.stat(path.dirname(target))).mode & 0o777, 0o700);
        assert.equal((await fs.stat(path.join(base, "new"))).mode & 0o777, 0o700);
        const reopened = await Effect.runPromise(
          LfsStore.use((store) => store.read(oid).pipe(Effect.flatMap(Stream.runCollect))).pipe(
            Effect.provide(file(directory)),
          ),
        );
        assert.equal(Buffer.concat([...reopened]).toString(), content);
        assert.deepEqual(
          (await fs.readdir(directory, { recursive: true })).filter((entry) =>
            entry.endsWith(".tmp"),
          ),
          [],
        );
      } finally {
        await fs.rm(base, { recursive: true, force: true });
      }
    }),
  );

  it.effect("reports publication failure and removes the fully written temporary file", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "lfs-rename-"));
      const content = "verified bytes cannot be published";
      const oid = sha256(content);
      const target = path.join(root, oid.slice(0, 2), oid.slice(2, 4), oid);
      try {
        await fs.mkdir(target, { recursive: true });
        const outcome = await Effect.runPromise(
          LfsStore.use((store) =>
            store.write(oid, Stream.make(new TextEncoder().encode(content))),
          ).pipe(Effect.provide(file(root)), Effect.result),
        );
        assert.equal(outcome._tag, "Failure");
        if (outcome._tag !== "Failure") return;
        assert.equal(outcome.failure._tag, "StorageFailure");
        assert.equal((await fs.stat(target)).isDirectory(), true);
        assert.deepEqual(
          (await fs.readdir(root, { recursive: true })).filter((entry) => entry.endsWith(".tmp")),
          [],
        );
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );

  it.effect("interrupts an unfinished upload before removing its temporary file", () =>
    Effect.gen(function* () {
      const root = yield* Effect.promise(() =>
        fs.mkdtemp(path.join(os.tmpdir(), "lfs-interrupt-")),
      );
      yield* Effect.addFinalizer(() =>
        Effect.promise(() => fs.rm(root, { recursive: true, force: true })),
      );
      const started = yield* Deferred.make<void>();
      let finalized = false;
      const body = Stream.make(new TextEncoder().encode("partial")).pipe(
        Stream.concat(Stream.fromEffect(Deferred.succeed(started, undefined)).pipe(Stream.drain)),
        Stream.concat(Stream.never),
        Stream.ensuring(
          Effect.sync(() => {
            finalized = true;
          }),
        ),
      );
      const upload = yield* LfsStore.use((store) =>
        store.write(sha256("partial and more"), body),
      ).pipe(Effect.provide(file(root)), Effect.forkScoped);
      yield* Deferred.await(started);
      yield* Fiber.interrupt(upload);
      assert.equal(finalized, true, "interruption must close the source before returning");
      const remaining = yield* Effect.promise(() => fs.readdir(root, { recursive: true }));
      assert.deepEqual(
        remaining.filter((entry) => entry.endsWith(".tmp")),
        [],
      );
      const stored = yield* LfsStore.use((store) => store.head(sha256("partial and more"))).pipe(
        Effect.provide(file(root)),
      );
      assert.equal(stored, null);
    }),
  );

  it.effect.skipIf(process.getuid?.() === 0)(
    "closes an unfinished source when the disk refuses its upload",
    () =>
      Effect.gen(function* () {
        const root = yield* Effect.promise(() => fs.mkdtemp(path.join(os.tmpdir(), "lfs-disk-")));
        const oid = sha256("cannot write");
        const shard = path.join(root, oid.slice(0, 2), oid.slice(2, 4));
        yield* Effect.promise(() => fs.mkdir(shard, { recursive: true }));
        yield* Effect.addFinalizer(() =>
          Effect.promise(async () => {
            await fs.chmod(shard, 0o700);
            await fs.rm(root, { recursive: true, force: true });
          }),
        );
        yield* Effect.promise(() => fs.chmod(shard, 0o500));
        let finalized = false;
        const body = Stream.make(new TextEncoder().encode("partial")).pipe(
          Stream.concat(Stream.never),
          Stream.ensuring(
            Effect.sync(() => {
              finalized = true;
            }),
          ),
        );
        const result = yield* LfsStore.use((store) => store.write(oid, body)).pipe(
          Effect.provide(file(root)),
          Effect.result,
        );
        assert.equal(result._tag, "Failure");
        assert.equal(finalized, true, "disk failure must stop the source's pending read");
        assert.deepEqual(yield* Effect.promise(() => fs.readdir(shard)), []);
      }),
  );
});
