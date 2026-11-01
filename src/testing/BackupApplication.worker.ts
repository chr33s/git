/** Production storage services exercised only by the native backup fixture. */
import { Effect, Layer, Result, Schema, Stream } from "effect";
import * as Cloudflare from "../git/Cloudflare.ts";
import { encodeTree } from "../git/Format.ts";
import { ObjectStore, RefStore } from "../git/Store.ts";
import * as Remotes from "../server/Remotes.ts";
import * as Subscribers from "../server/Subscribers.ts";
import * as Provisioning from "../server/ProvisioningState.ts";
import * as Lfs from "../server/Lfs.cloudflare.ts";
import { LfsStore } from "../server/Lfs.ts";
import { LfsQuotaBusy, LfsQuotaExceeded } from "../git/Error.ts";
import { lockKey } from "../server/LfsCapacity.cloudflare.ts";
import * as Repository from "../git/Repository.ts";
import { PackStore } from "../git/Packed.ts";
import * as Snapshot from "../server/Snapshot.ts";
import * as Publisher from "../server/SnapshotPublisher.ts";
import * as Hub from "./BackupHub.worker.ts";

const repo = "backup-application";
// SHA-256 of nine MiB of 0x07, independently computed with Node's crypto module.
const lfsOid = "108c2c561ebe9d5557ea1468799eabb7832494df0b4f3f1b51e56a3f213ee210";
const services = (storage: DurableObjectStorage, bucket: R2Bucket) => {
  const stores = Layer.mergeAll(
    Cloudflare.stores({ storage, bucket, repo }),
    Remotes.sql(storage.sql, repo),
    Subscribers.sql(storage.sql, repo),
    Lfs.r2({ bucket, repo, capacity: { bytes: 16 * 1024 ** 2, objects: 1 } }),
  );
  return Layer.mergeAll(
    Repository.layer.pipe(Layer.provide(Repository.hooksNoop)),
    Publisher.cloudflare(bucket, storage),
  ).pipe(Layer.provideMerge(stores));
};

export const seedApplication = Effect.fn("BackupFixture.seedApplication")(function* (
  storage: DurableObjectStorage,
  bucket: R2Bucket,
  includeHub = false,
) {
  yield* Effect.gen(function* () {
    const objects = yield* ObjectStore;
    const refs = yield* RefStore;
    const remotes = yield* Remotes.Remotes;
    const subscribers = yield* Subscribers.Subscribers;
    const lfs = yield* LfsStore;
    const chunk = new Uint8Array(1024 ** 2).fill(7);
    yield* lfs.write(lfsOid, Stream.fromIterable(Array.from({ length: 9 }, () => chunk)));
    // Model the durable fence retained after an uncertain multipart outcome.
    yield* Effect.promise(() => bucket.put(lockKey("uncertain-upload"), ""));
    let tree = yield* objects.write({ type: "tree", data: new Uint8Array() });
    if (includeHub) {
      const readme = yield* objects.write({
        type: "blob",
        data: new TextEncoder().encode("Recovered repository\nUnicode: café 日本語\n"),
      });
      const script = yield* objects.write({
        type: "blob",
        data: new TextEncoder().encode("#!/bin/sh\nprintf 'recovered\\n'\n"),
      });
      const binary = yield* objects.write({
        type: "blob",
        data: new Uint8Array([0, 255, 128, 13, 10, 0, 42]),
      });
      const link = yield* objects.write({
        type: "blob",
        data: new TextEncoder().encode("README.md"),
      });
      const nested = yield* objects.write({
        type: "tree",
        data: encodeTree([{ mode: "100644", name: "payload.bin", oid: binary }]),
      });
      tree = yield* objects.write({
        type: "tree",
        data: encodeTree([
          { mode: "100644", name: "README.md", oid: readme },
          { mode: "120000", name: "current", oid: link },
          { mode: "40000", name: "data", oid: nested },
          { mode: "100755", name: "run.sh", oid: script },
        ]),
      });
    }
    const commit = yield* objects.write({
      type: "commit",
      data: new TextEncoder().encode(
        `tree ${tree}\nauthor Test <test@example.com> 1700000000 +0000\ncommitter Test <test@example.com> 1700000000 +0000\n\nBackup fixture\n`,
      ),
    });
    yield* refs.apply(
      [{ name: "refs/heads/main", value: commit, expected: null, reason: "fixture" }],
      { atomic: true },
    );
    yield* refs.setHead("refs/heads/main");
    yield* refs.updateShallow({ add: [commit], remove: [] });
    yield* remotes.add({
      name: "origin",
      url: "https://example.com/private",
      credential: "fixture-private-credential",
    });
    yield* subscribers.add({ url: "https://example.com/hook", secret: "fixture-private-secret" });
    const provisioning = yield* Provisioning.make(storage.sql, (work) =>
      storage.transactionSync(work),
    );
    yield* provisioning.reserve("pending-repository", {
      digest: "a".repeat(64),
      repoId: `SHA256:${"A".repeat(43)}`,
      genesis: commit,
      trust: commit,
    });
    yield* Effect.promise(() => storage.put(`snapshot-seq:${repo}`, 23));
    const repository = yield* Repository.Repository;
    if (includeHub) yield* Hub.seed(commit);
    const collected = yield* repository.gc({ repack: true });
    if (collected.packed === undefined) return yield* Effect.die("fixture was not packed");
    const publish = yield* Publisher.make((_repo, previous) =>
      Snapshot.capture(previous).pipe(Effect.provideService(Repository.Repository, repository)),
    );
    yield* publish(repo, Effect.void);
  }).pipe(Effect.provide(services(storage, bucket)));
});

export const inspectApplication = Effect.fn("BackupFixture.inspectApplication")(function* (
  storage: DurableObjectStorage,
  bucket: R2Bucket,
  mutate: boolean,
) {
  return yield* Effect.gen(function* () {
    const objects = yield* ObjectStore;
    const refs = yield* RefStore;
    const remotes = yield* Remotes.Remotes;
    const subscribers = yield* Subscribers.Subscribers;
    const lfs = yield* LfsStore;
    const fenced = yield* Effect.gen(function* () {
      const uncertain = yield* LfsStore;
      return yield* uncertain.write(lfsOid, Stream.empty).pipe(Effect.result);
    }).pipe(Effect.provide(Lfs.r2({ bucket, repo: "uncertain-upload" })));
    if (!Result.isFailure(fenced) || !Schema.is(LfsQuotaBusy)(fenced.failure))
      return yield* Effect.die("uncertain LFS upload fence was not preserved");
    if (mutate) {
      const repository = yield* Repository.Repository;
      const oid = yield* refs.read("refs/heads/main");
      if (oid === null) return yield* Effect.die("missing restored ref");
      const publish = yield* Publisher.make((_repo, previous) =>
        Snapshot.capture(previous).pipe(Effect.provideService(Repository.Repository, repository)),
      );
      yield* publish(
        repo,
        refs.apply([{ name: "refs/heads/restored", value: oid, expected: null }], {
          atomic: true,
        }),
      );
      const rejected = yield* lfs.write(lfsOid, Stream.empty).pipe(Effect.result);
      if (
        !Result.isFailure(rejected) ||
        !Schema.is(LfsQuotaExceeded)(rejected.failure) ||
        rejected.failure.resource !== "objects"
      )
        return yield* Effect.die("restored LFS inventory did not enforce its quota");
    }
    let lfsBytes = 0;
    yield* (yield* lfs.read(lfsOid)).pipe(
      Stream.runForEach((chunk) =>
        Effect.sync(() => {
          if (chunk.some((byte) => byte !== 7)) throw new Error("restored LFS bytes differ");
          lfsBytes += chunk.byteLength;
        }),
      ),
    );
    const oids = yield* Stream.runCollect(objects.list);
    const contents = yield* Effect.forEach([...oids].sort(), (oid) =>
      objects.read(oid).pipe(Effect.map((value) => ({ oid, ...value }))),
    );
    const provisioning = yield* Provisioning.make(storage.sql, (work) =>
      storage.transactionSync(work),
    );
    const packs = yield* PackStore;
    const publisher = yield* Publisher.Store;
    const sequence = yield* publisher.sequence(repo);
    const published = yield* publisher.read(Snapshot.keyOf(repo));
    const journal = yield* publisher.read(Snapshot.journalKeyOf(repo, sequence));
    const view = published === null ? null : Snapshot.decode(published);
    const entry = journal === null ? null : Snapshot.decodeJournal(journal);
    if (view === null || entry === null)
      return yield* Effect.die("restored snapshot or journal is missing");
    const main = yield* refs.read("refs/heads/main");
    if (main === null) return yield* Effect.die("restored main is missing");
    const loose = yield* Effect.promise(() => bucket.head(`${repo}/objects/${main}`));
    if (loose !== null) return yield* Effect.die("fixture must read the commit from its pack");
    return {
      head: yield* refs.head,
      refs: yield* refs.list(),
      shallow: [...(yield* refs.shallow)],
      reflog: yield* refs.reflog("refs/heads/main"),
      contents,
      lfs: {
        uncertainUploadBlocked: true,
        head: yield* lfs.head(lfsOid),
        bytes: lfsBytes,
        lock: yield* Effect.promise(() => bucket.head(lockKey(repo))),
      },
      remotes: yield* remotes.list,
      subscribers: yield* subscribers.list,
      provisioning: yield* provisioning.read("pending-repository"),
      snapshot: sequence,
      published: view,
      journal: entry,
      packs: (yield* packs.list).map((pack) => ({ name: pack.name, index: pack.index })),
      metadata: storage.sql.exec("SELECT * FROM git_ref_capacity ORDER BY repo").toArray(),
      retained: storage.sql.exec("SELECT * FROM git_capacity ORDER BY repo").toArray(),
    };
  }).pipe(Effect.provide(services(storage, bucket)));
});

export const inspectApplicationHub = Effect.fn("BackupFixture.inspectApplicationHub")(
  (storage: DurableObjectStorage, bucket: R2Bucket) =>
    Hub.inspect().pipe(Effect.provide(services(storage, bucket))),
);
