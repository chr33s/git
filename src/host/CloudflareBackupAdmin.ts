/** Private DO RPC, reachable only through a namespace binding; never routed from HTTP. */
import type { DurableObjectStorage } from "@cloudflare/workers-types";
import { Effect, Schema, Semaphore } from "effect";
import { Invalid } from "../git/Error.ts";
import * as Fence from "./CloudflareBackupFence.ts";
import * as Import from "../../tools/cloudflare-object-import.ts";
import { Resources } from "../../tools/cloudflare-backup-inventory.ts";
import { encodeBackupFrame } from "../../tools/cloudflare-backup-codec.ts";
import {
  captureSqlCatalog,
  captureSqlPage,
  sqlRestoreTable,
} from "../../tools/cloudflare-sql-backup.ts";
import { captureKvPage, restoreKey } from "../../tools/cloudflare-kv-backup.ts";
import { legacyCheckpointKey } from "../../tools/cloudflare-backup-control.ts";

const Command = Schema.TaggedUnion({
  Hold: {},
  Release: {},
  AbandonCapture: {},
  Catalog: {},
  SqlPage: { table: Schema.NonEmptyString, after: Schema.NullOr(Schema.String), limit: Schema.Int },
  KvPage: { after: Schema.NullOr(Schema.String), limit: Schema.Int },
  Start: { plan: Import.Plan },
  Apply: { packet: Schema.String },
  Status: {},
  Finish: { packet: Schema.String, resources: Resources },
  Retire: { receipt: Schema.String },
});
export const Request = Schema.Struct({
  objectId: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  snapshotId: Fence.Marker.fields.snapshotId,
  command: Command,
});
export interface Request extends Schema.Schema.Type<typeof Request> {}
export interface Interface {
  readonly backup: (request: Request) => Effect.Effect<string, Invalid>;
}
const invalid = () => new Invalid({ field: "backup", reason: "backup administration rejected" });

export const make = (
  storage: DurableObjectStorage,
  objectId: string,
  fence: ReturnType<typeof Fence.make>,
): Interface => {
  const serial = Semaphore.makeUnsafe(1);
  const backup = Effect.fn("Backup.administer")(function* (input: Request) {
    const request = yield* Schema.decodeEffect(Request)(input).pipe(
      Effect.mapError(() => new Invalid({ field: "backup", reason: "backup request malformed" })),
    );
    if (request.objectId !== objectId)
      return yield* new Invalid({ field: "backup", reason: "backup target identity mismatch" });
    const { command, snapshotId } = request;
    if (command._tag === "Hold") {
      yield* Effect.tryPromise({ try: () => fence.hold(snapshotId), catch: invalid });
      return "held";
    }
    if (command._tag === "Release" || command._tag === "AbandonCapture") {
      yield* Effect.try({
        try: () => {
          if (
            storage.kv.get(Import.checkpointKey) !== undefined ||
            storage.kv.get(legacyCheckpointKey) !== undefined ||
            storage.kv.get(restoreKey) !== undefined ||
            !storage.sql
              .exec("SELECT name FROM sqlite_schema WHERE name = ?", sqlRestoreTable)
              .next().done
          )
            throw invalid();
        },
        catch: invalid,
      });
      yield* Effect.tryPromise({
        try: () =>
          command._tag === "Release" ? fence.release(snapshotId) : fence.abandon(snapshotId),
        catch: invalid,
      });
      return command._tag === "Release" ? "released" : "abandoned";
    }
    const stored = yield* Effect.try({ try: () => storage.kv.get(Fence.key), catch: invalid });
    const marker = yield* Schema.decodeUnknownEffect(Fence.Marker)(stored).pipe(
      Effect.mapError(invalid),
    );
    if (marker.phase !== "held" || marker.snapshotId !== snapshotId) return yield* invalid();
    switch (command._tag) {
      case "Catalog":
        return yield* Effect.try({ try: () => captureSqlCatalog(storage.sql), catch: invalid });
      case "SqlPage":
        return yield* Effect.try({
          try: () => captureSqlPage(storage.sql, command.table, command.after, command.limit),
          catch: invalid,
        });
      case "KvPage":
        return yield* Effect.tryPromise({
          try: () => captureKvPage(storage, snapshotId, command.after, command.limit),
          catch: invalid,
        });
      case "Start":
        if (command.plan.snapshotId !== snapshotId) return yield* invalid();
        return encodeBackupFrame(yield* Import.startObjectImport(storage, command.plan));
      case "Apply":
        return encodeBackupFrame(yield* Import.applyObjectImport(storage, command.packet));
      case "Status":
        return encodeBackupFrame(yield* Import.readObjectImport(storage));
      case "Finish":
        return encodeBackupFrame(
          yield* Import.finishObjectImport(storage, command.packet, command.resources),
        );
      case "Retire":
        yield* Import.retireObjectImport(storage, snapshotId, command.receipt);
        return "retired";
    }
  }, serial.withPermits(1));
  return { backup };
};
