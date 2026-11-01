/** Durable source release or explicit capture abandonment. Global isolation is operator-owned. */
import * as path from "node:path";
import { Effect, Schema, Stream } from "effect";
import { Invalid } from "../src/git/Error.ts";
import {
  verifyNamespaceArchive,
  verifyCaptureIntent,
  type Verify,
} from "./cloudflare-namespace-archive.ts";
import type { Client } from "./cloudflare-object-archive.ts";
import { encodeBackupFrame } from "./cloudflare-backup-codec.ts";
import { readCloudflareObjectInventory } from "./inventory-cloudflare-objects.ts";
import { openRestoreJournal } from "./cloudflare-restore-journal.ts";
import { inventoryR2Backup, type Source } from "./copy-r2-backup.ts";

interface SourceSet extends Verify {
  readonly journalDirectory: string;
  readonly client: Client;
}
export interface AbandonSources extends SourceSet {
  readonly decisionSha256: string;
}
export interface SourceRelease extends SourceSet {
  readonly archiveBucket: Source;
}
const Meta = Schema.Struct({
  identity: Schema.String,
  phase: Schema.Literals(["source-releasing", "source-released"]),
});
const Row = Schema.Struct({
  source: Schema.String,
  target: Schema.String,
  receipt: Schema.Null,
  phase: Schema.Literals(["pending", "released"]),
});
const invalid = () => new Invalid({ field: "backup", reason: "source release rejected" });
const io = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: invalid });
const sync = <A>(run: () => A) => Effect.try({ try: run, catch: invalid });

/** Shared journal protocol; callers authenticate the source set and distinct operation identity first. */
const resumeSources = Effect.fn("Backup.resumeSources")(function* (
  options: SourceSet,
  identity: string,
  objects: number,
  command: "Release" | "AbandonCapture",
  beforeRelease: Effect.Effect<void, Invalid>,
) {
  const records = () =>
    Stream.fromAsyncIterable(
      readCloudflareObjectInventory(path.join(options.directory, "namespace")),
      invalid,
    );
  return yield* Effect.acquireUseRelease(
    io(() => openRestoreJournal(options.journalDirectory)),
    (database) =>
      Effect.gen(function* () {
        const stored = yield* sync(() =>
          database.prepare("SELECT identity, phase FROM meta WHERE id = 1").get(),
        );
        const meta =
          stored === undefined
            ? undefined
            : yield* Schema.decodeUnknownEffect(Meta)(stored).pipe(Effect.mapError(invalid));
        if (meta !== undefined && meta.identity !== identity) return yield* invalid();
        const row = (sequence: number) =>
          sync(() =>
            Schema.decodeUnknownSync(Row)(
              database
                .prepare(
                  "SELECT source, target, receipt, phase FROM objects JOIN release_objects USING(sequence) WHERE sequence = ?",
                )
                .get(sequence),
            ),
          );
        const size = (table: "objects" | "release_objects" | "release_meta") =>
          sync(() => database.prepare(`SELECT COUNT(*) AS count FROM ${table}`).get()?.count);
        if ((yield* size("release_meta")) !== 0) return yield* invalid();
        const expectedRows = meta === undefined ? 0 : objects;
        if (
          (yield* size("objects")) !== expectedRows ||
          (yield* size("release_objects")) !== expectedRows
        )
          return yield* invalid();
        if (meta !== undefined) {
          let sequence = 0;
          let pending = false;
          yield* Stream.runForEach(records(), (record) =>
            Effect.gen(function* () {
              const saved = yield* row(sequence++);
              if (saved.source !== record.id || saved.target !== record.id) return yield* invalid();
              if (saved.phase === "pending") pending = true;
              else if (pending) return yield* invalid();
              if (meta.phase === "source-released" && pending) return yield* invalid();
            }),
          );
          if (sequence !== objects) return yield* invalid();
        }
        const result = { phase: "source-released" as const, objects: objects };
        // Completed retries must not contact sources that may be serving traffic or held again.
        if (meta?.phase === "source-released") return result;
        yield* beforeRelease;
        if (meta === undefined) {
          yield* sync(() => database.exec("BEGIN IMMEDIATE"));
          yield* Effect.gen(function* () {
            let sequence = 0;
            yield* Stream.runForEach(records(), (record) =>
              sync(() => {
                database
                  .prepare("INSERT INTO objects VALUES (?, ?, ?, NULL)")
                  .run(sequence, record.id, record.id);
                database
                  .prepare("INSERT INTO release_objects VALUES (?, 'pending')")
                  .run(sequence++);
              }),
            );
            if (sequence !== objects) return yield* invalid();
            yield* sync(() => {
              database.prepare("INSERT INTO meta VALUES (1, ?, 'source-releasing')").run(identity);
              database.exec("COMMIT");
            });
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                if (database.isTransaction) database.exec("ROLLBACK");
              }),
            ),
          );
        }
        for (let sequence = 0; sequence < objects; sequence++) {
          const saved = yield* row(sequence);
          if (saved.phase === "released") continue;
          const response = yield* io(() =>
            options.client.backup({
              objectId: saved.source,
              snapshotId: options.scope.snapshotId,
              command: { _tag: command },
            }),
          );
          if (response !== (command === "Release" ? "released" : "abandoned"))
            return yield* invalid();
          yield* sync(() =>
            database
              .prepare("UPDATE release_objects SET phase = 'released' WHERE sequence = ?")
              .run(sequence),
          );
        }
        yield* sync(() => database.exec("UPDATE meta SET phase = 'source-released' WHERE id = 1"));
        return result;
      }),
    (database) => Effect.sync(() => database.close()),
  );
});

/** Completed-capture release requires both the authenticated archive and a fresh R2 readback. */
export const releaseCapturedSources = Effect.fn("Backup.releaseCapturedSources")(function* (
  options: SourceRelease,
) {
  const completion = yield* verifyNamespaceArchive(options);
  const identity = encodeBackupFrame({
    kind: "source-release",
    completion,
    signer: options.signer,
  });
  const readback = Effect.gen(function* () {
    const actual = yield* io(() => inventoryR2Backup(options.archiveBucket));
    const expected = completion.resources.r2;
    if (
      actual.objects !== expected.objects ||
      actual.bytes !== expected.bytes ||
      actual.contentInventorySha256 !== expected.contentInventorySha256 ||
      actual.metadataInventorySha256 !== expected.metadataInventorySha256
    )
      return yield* invalid();
  });
  return yield* resumeSources(options, identity, completion.objects, "Release", readback);
});

/** Explicitly abandon capture without certifying or deleting any copied data. */
export const abandonCapturedSources = Effect.fn("Backup.abandonCapturedSources")(function* (
  options: AbandonSources,
) {
  const decision = yield* Schema.decodeEffect(
    Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  )(options.decisionSha256).pipe(Effect.mapError(invalid));
  const intent = yield* verifyCaptureIntent(options);
  const identity = encodeBackupFrame({
    kind: "source-abandon",
    intent,
    signer: options.signer,
    decision,
  });
  const result = yield* resumeSources(
    options,
    identity,
    intent.objects,
    "AbandonCapture",
    Effect.void,
  );
  return { phase: "source-abandoned" as const, objects: result.objects };
});
