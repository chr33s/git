/** Explicit post-verification retirement/release. Caller retains installation-wide isolation. */
import { Effect, Schema, Stream } from "effect";
import { Invalid } from "../src/git/Error.ts";
import { Target, type Restore } from "./cloudflare-namespace-restore.ts";
import { verifyNamespaceArchive } from "./cloudflare-namespace-archive.ts";
import { openRestoreJournal } from "./cloudflare-restore-journal.ts";
import { readBackupLines } from "./cloudflare-backup-read.ts";
import { decodeBackupFrame, encodeBackupFrame } from "./cloudflare-backup-codec.ts";
import { Checkpoint } from "./cloudflare-object-import.ts";
import { inventoryR2Backup } from "./copy-r2-backup.ts";

export interface Release extends Omit<Restore, "archiveBucket"> {
  /** SHA-256 of the operator's retained application/authority recovery verification report. */
  readonly verificationSha256: string;
}
const Digest = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const Mapping = Schema.Struct({ sourceId: Digest, targetId: Digest });
const Meta = Schema.Struct({
  identity: Schema.String,
  phase: Schema.Literals(["ready", "releasing", "released"]),
});
const Row = Schema.Struct({ source: Digest, target: Digest, receipt: Schema.String });
const Progress = Schema.Struct({ phase: Schema.Literals(["pending", "retired", "released"]) });
const invalid = () => new Invalid({ field: "backup", reason: "namespace release rejected" });
const io = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: invalid });
const sync = <A>(run: () => A) => Effect.try({ try: run, catch: invalid });

export const releaseNamespaceArchive = Effect.fn("Backup.releaseNamespaceArchive")(function* (
  options: Release,
) {
  const completion = yield* verifyNamespaceArchive(options);
  const target = yield* Schema.decodeEffect(Target)(options.target).pipe(Effect.mapError(invalid));
  const verification = yield* Schema.decodeEffect(Digest)(options.verificationSha256).pipe(
    Effect.mapError(invalid),
  );
  const identity = encodeBackupFrame({ completion, signer: options.signer, target });
  return yield* Effect.acquireUseRelease(
    io(() => openRestoreJournal(options.journalDirectory)),
    (database) =>
      Effect.gen(function* () {
        const meta = yield* sync(() =>
          Schema.decodeUnknownSync(Meta)(
            database.prepare("SELECT identity, phase FROM meta WHERE id = 1").get(),
          ),
        );
        if (meta.identity !== identity) return yield* invalid();
        const row = (sequence: number) =>
          sync(() =>
            Schema.decodeUnknownSync(Row)(
              database
                .prepare("SELECT source, target, receipt FROM objects WHERE sequence = ?")
                .get(sequence),
            ),
          );
        const progress = (sequence: number) =>
          sync(() =>
            Schema.decodeUnknownSync(Progress)(
              database
                .prepare("SELECT phase FROM release_objects WHERE sequence = ?")
                .get(sequence),
            ),
          );
        let count = 0;
        // Validate the complete mapping and all local receipts before remote operations.
        yield* Stream.runForEach(
          Stream.fromAsyncIterable(readBackupLines(options.mappingFile, 512), invalid),
          (line) =>
            Effect.gen(function* () {
              const mapping = yield* Schema.decodeEffect(Schema.fromJsonString(Mapping))(line).pipe(
                Effect.mapError(invalid),
              );
              const object = yield* row(count);
              if (object.source !== mapping.sourceId || object.target !== mapping.targetId)
                return yield* invalid();
              const receipt = yield* sync(() => decodeBackupFrame(object.receipt, Checkpoint));
              if (
                receipt.phase !== "complete" ||
                receipt.plan.objectId !== object.source ||
                receipt.plan.snapshotId !== options.scope.snapshotId ||
                receipt.plan.installationId !== options.scope.installationId ||
                receipt.plan.signer !== options.signer
              )
                return yield* invalid();
              count++;
            }),
        );
        const total = yield* sync(
          () => database.prepare("SELECT COUNT(*) AS count FROM objects").get()?.count,
        );
        if (count !== completion.objects || total !== count) return yield* invalid();
        if (meta.phase !== "ready") {
          const saved = yield* sync(
            () =>
              database.prepare("SELECT verification FROM release_meta WHERE id = 1").get()
                ?.verification,
          );
          const rows = yield* sync(
            () => database.prepare("SELECT COUNT(*) AS count FROM release_objects").get()?.count,
          );
          if (saved !== verification || rows !== count) return yield* invalid();
          let sawPending = false;
          let sawReleased = false;
          let sawUnreleased = false;
          for (let sequence = 0; sequence < count; sequence++) {
            const current = yield* progress(sequence);
            if (meta.phase === "released" && current.phase !== "released") return yield* invalid();
            // Retirement and release each advance a prefix, and release cannot
            // start while any retirement remains pending.
            if (sawPending && current.phase !== "pending") return yield* invalid();
            if (current.phase === "released") {
              if (sawUnreleased) return yield* invalid();
              sawReleased = true;
            } else {
              sawUnreleased = true;
              if (current.phase === "pending") {
                if (sawReleased) return yield* invalid();
                sawPending = true;
              }
            }
          }
        }
        const result = {
          phase: "released" as const,
          objects: count,
          verificationSha256: verification,
        };
        // A completed journal is historical evidence; do not contact objects that may now serve traffic.
        if (meta.phase === "released") return result;
        const actual = yield* io(() => inventoryR2Backup(options.targetBucket));
        const expected = completion.resources.r2;
        if (
          actual.objects !== expected.objects ||
          actual.bytes !== expected.bytes ||
          actual.contentInventorySha256 !== expected.contentInventorySha256 ||
          actual.metadataInventorySha256 !== expected.metadataInventorySha256
        )
          return yield* invalid();
        if (meta.phase === "ready") {
          for (let sequence = 0; sequence < count; sequence++) {
            const object = yield* row(sequence);
            const status = yield* io(() =>
              options.client.backup({
                objectId: object.target,
                snapshotId: options.scope.snapshotId,
                command: { _tag: "Status" },
              }),
            );
            const receipt = yield* sync(() => decodeBackupFrame(status, Checkpoint));
            if (encodeBackupFrame(receipt) !== object.receipt) return yield* invalid();
          }
          // Record the verified release intent before the first destructive retirement.
          yield* sync(() => {
            database.exec("BEGIN IMMEDIATE");
            try {
              database.prepare("INSERT INTO release_meta VALUES (1, ?)").run(verification);
              database.exec(
                "INSERT INTO release_objects SELECT sequence, 'pending' FROM objects; UPDATE meta SET phase = 'releasing' WHERE id = 1; COMMIT",
              );
            } catch (error) {
              database.exec("ROLLBACK");
              throw error;
            }
          });
        }
        for (let sequence = 0; sequence < count; sequence++) {
          if ((yield* progress(sequence)).phase !== "pending") continue;
          const object = yield* row(sequence);
          const response = yield* io(() =>
            options.client.backup({
              objectId: object.target,
              snapshotId: options.scope.snapshotId,
              command: { _tag: "Retire", receipt: object.receipt },
            }),
          );
          if (response !== "retired") return yield* invalid();
          yield* sync(() =>
            database
              .prepare("UPDATE release_objects SET phase = 'retired' WHERE sequence = ?")
              .run(sequence),
          );
        }
        // All receipts have been retired and recorded before any target can reopen.
        for (let sequence = 0; sequence < count; sequence++) {
          if ((yield* progress(sequence)).phase === "released") continue;
          const object = yield* row(sequence);
          const response = yield* io(() =>
            options.client.backup({
              objectId: object.target,
              snapshotId: options.scope.snapshotId,
              command: { _tag: "Release" },
            }),
          );
          if (response !== "released") return yield* invalid();
          yield* sync(() =>
            database
              .prepare("UPDATE release_objects SET phase = 'released' WHERE sequence = ?")
              .run(sequence),
          );
        }
        yield* sync(() => database.exec("UPDATE meta SET phase = 'released' WHERE id = 1"));
        return result;
      }),
    (database) => Effect.sync(() => database.close()),
  );
});
