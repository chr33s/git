/** Durable namespace restore orchestration. Global isolation remains the operator's responsibility. */
import * as path from "node:path";
import { Effect, Schema, Stream } from "effect";
import { Invalid } from "../src/git/Error.ts";
import type { Fingerprint } from "../src/crypto/SshSignature.ts";
import { Scope, verifyNamespaceArchive } from "./cloudflare-namespace-archive.ts";
import { restoreObjectArchive, type Client } from "./cloudflare-object-archive.ts";
import { Checkpoint } from "./cloudflare-object-import.ts";
import { decodeBackupFrame, encodeBackupFrame } from "./cloudflare-backup-codec.ts";
import { readBackupLines } from "./cloudflare-backup-read.ts";
import { readCloudflareObjectInventory } from "./inventory-cloudflare-objects.ts";
import { openRestoreJournal } from "./cloudflare-restore-journal.ts";
import { restoreR2Backup, type Source, type Destination } from "./copy-r2-backup.ts";

const ObjectId = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const Mapping = Schema.Struct({ sourceId: ObjectId, targetId: ObjectId });
export const Target = Schema.Struct({
  account: Scope.fields.account,
  namespace: Scope.fields.namespace,
  bucket: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
});
export interface Target extends Schema.Schema.Type<typeof Target> {}
export interface Restore {
  readonly directory: string;
  readonly scope: Scope;
  readonly signer: Fingerprint;
  readonly mappingFile: string;
  readonly journalDirectory: string;
  readonly target: Target;
  readonly client: Client;
  readonly archiveBucket: Source;
  readonly targetBucket: Destination;
}
const Meta = Schema.Struct({
  identity: Schema.String,
  phase: Schema.Literals(["restoring", "ready"]),
});
const Row = Schema.Struct({
  source: ObjectId,
  target: ObjectId,
  receipt: Schema.NullOr(Schema.String),
});
const invalid = () => new Invalid({ field: "backup", reason: "namespace restore rejected" });
const io = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: invalid });
const sync = <A>(run: () => A) => Effect.try({ try: run, catch: invalid });

/** Resumes R2 and object imports, persisting receipts. Never retires receipts or releases a fence. */
export const restoreNamespaceArchive = Effect.fn("Backup.restoreNamespaceArchive")(function* (
  options: Restore,
) {
  const completion = yield* verifyNamespaceArchive(options);
  const target = yield* Schema.decodeEffect(Target)(options.target).pipe(Effect.mapError(invalid));
  if (
    target.account === completion.scope.account &&
    target.namespace === completion.scope.namespace
  )
    return yield* invalid();
  const identity = encodeBackupFrame({ completion, signer: options.signer, target });
  return yield* Effect.acquireUseRelease(
    io(() => openRestoreJournal(options.journalDirectory)),
    (database) =>
      Effect.gen(function* () {
        const stored = yield* sync(() =>
          database.prepare("SELECT identity, phase FROM meta WHERE id = 1").get(),
        );
        const previous =
          stored === undefined
            ? undefined
            : yield* Schema.decodeUnknownEffect(Meta)(stored).pipe(Effect.mapError(invalid));
        if (previous !== undefined && previous.identity !== identity) return yield* invalid();
        const fresh = previous === undefined;
        const mappings = readBackupLines(options.mappingFile, 512);
        const sources = readCloudflareObjectInventory(path.join(options.directory, "namespace"));
        let count = 0;
        // Mapping initialization commits completely before the first remote operation.
        yield* sync(() => database.exec("BEGIN IMMEDIATE"));
        yield* Effect.acquireUseRelease(
          Effect.succeed(sources),
          (rows) =>
            Stream.runForEach(Stream.fromAsyncIterable(mappings, invalid), (line) =>
              Effect.gen(function* () {
                const mapping = yield* Schema.decodeEffect(Schema.fromJsonString(Mapping))(
                  line,
                ).pipe(Effect.mapError(invalid));
                const source = yield* io(() => rows.next());
                if (
                  source.done ||
                  source.value.id !== mapping.sourceId ||
                  mapping.targetId === mapping.sourceId
                )
                  return yield* invalid();
                yield* sync(() => {
                  if (fresh)
                    database
                      .prepare("INSERT INTO objects(sequence, source, target) VALUES (?, ?, ?)")
                      .run(count, mapping.sourceId, mapping.targetId);
                  else {
                    const held = Schema.decodeUnknownSync(Row)(
                      database
                        .prepare("SELECT source, target, receipt FROM objects WHERE sequence = ?")
                        .get(count),
                    );
                    if (held.source !== mapping.sourceId || held.target !== mapping.targetId)
                      throw invalid();
                  }
                });
                count += 1;
              }),
            ).pipe(
              Effect.andThen(
                Effect.gen(function* () {
                  if (!(yield* io(() => rows.next())).done || count !== completion.objects)
                    return yield* invalid();
                  yield* sync(() => {
                    const total = database.prepare("SELECT COUNT(*) AS count FROM objects").get();
                    if (total?.count !== completion.objects) throw invalid();
                    if (!fresh) database.exec("UPDATE meta SET phase = 'restoring' WHERE id = 1");
                    if (fresh)
                      database.prepare("INSERT INTO meta VALUES (1, ?, 'restoring')").run(identity);
                    database.exec("COMMIT");
                  });
                }),
              ),
            ),
          (rows) => io(() => rows.return()).pipe(Effect.orDie),
        ).pipe(Effect.onError(() => sync(() => database.exec("ROLLBACK")).pipe(Effect.orDie)));
        const row = (sequence: number) =>
          sync(() =>
            Schema.decodeUnknownSync(Row)(
              database
                .prepare("SELECT source, target, receipt FROM objects WHERE sequence = ?")
                .get(sequence),
            ),
          );
        for (let sequence = 0; sequence < completion.objects; sequence++) {
          const object = yield* row(sequence);
          const response = yield* io(() =>
            options.client.backup({
              objectId: object.target,
              snapshotId: completion.scope.snapshotId,
              command: { _tag: "Hold" },
            }),
          );
          if (response !== "held") return yield* invalid();
        }
        // Bind every held target to this restore plan before writing R2 data.
        for (let sequence = 0; sequence < completion.objects; sequence++) {
          const object = yield* row(sequence);
          const frame = yield* io(() =>
            options.client.backup({
              objectId: object.target,
              snapshotId: completion.scope.snapshotId,
              command: {
                _tag: "Start",
                plan: {
                  installationId: completion.scope.installationId,
                  snapshotId: completion.scope.snapshotId,
                  objectId: object.source,
                  signer: options.signer,
                },
              },
            }),
          );
          const checkpoint = yield* sync(() => decodeBackupFrame(frame, Checkpoint));
          if (
            checkpoint.plan.installationId !== completion.scope.installationId ||
            checkpoint.plan.snapshotId !== completion.scope.snapshotId ||
            checkpoint.plan.objectId !== object.source ||
            checkpoint.plan.signer !== options.signer
          )
            return yield* invalid();
          if (object.receipt !== null && encodeBackupFrame(checkpoint) !== object.receipt)
            return yield* invalid();
        }
        yield* io(() =>
          restoreR2Backup(options.archiveBucket, options.targetBucket, completion.resources.r2),
        );
        for (let sequence = 0; sequence < completion.objects; sequence++) {
          const object = yield* row(sequence);
          const receipt = yield* restoreObjectArchive({
            directory: path.join(options.directory, "objects", object.source),
            client: options.client,
            resources: completion.resources,
            plan: {
              installationId: completion.scope.installationId,
              snapshotId: completion.scope.snapshotId,
              objectId: object.source,
              signer: options.signer,
            },
            targetObjectId: object.target,
          });
          const checkpoint = yield* sync(() => decodeBackupFrame(receipt, Checkpoint));
          if (checkpoint.phase !== "complete") return yield* invalid();
          yield* sync(() =>
            database
              .prepare("UPDATE objects SET receipt = ? WHERE sequence = ?")
              .run(receipt, sequence),
          );
        }
        yield* sync(() => database.exec("UPDATE meta SET phase = 'ready' WHERE id = 1"));
        return {
          phase: "ready" as const,
          objects: completion.objects,
          resources: completion.resources,
        };
      }),
    (database) => Effect.sync(() => database.close()),
  );
});
