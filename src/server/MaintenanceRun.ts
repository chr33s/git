/**
 * Desired-state maintenance: read a snapshot, plan, execute one unit.
 *
 * Restart-safe by construction. A crash mid-build leaves no manifest entry,
 * so the next tick plans the same unit again. Operations record progress;
 * they are not the source of truth for whether the work still needs doing.
 */
import { Context, Effect, Layer, Option, Result, Schema, Stream } from "effect";

import { hiddenFromAdvertisement } from "../git/Refspec.ts";
import { Repository } from "../git/Repository.ts";
import { PackStore } from "../git/Packed.ts";
import { isOid, ObjectStore, type Oid, storageOf } from "../git/Store.ts";
import { type StorageFailure } from "../git/Error.ts";
import { BundleStore } from "./BundleStore.ts";
import { Bundles } from "./Bundles.ts";
import { defaults, type Features, ServerFeatures } from "./Features.ts";
import { Operations, run as runOperation } from "./Operations.ts";
import {
  type MaintenanceConfig,
  type MaintenanceSnapshot,
  type MaintenanceUnit,
  nextUnit,
  planMaintenance,
} from "./Planner.ts";

export interface MaintenanceRecord {
  readonly lastFsck?: string;
  readonly lastGc?: string;
  readonly lastRepack?: string;
  readonly lastBundleAttempt?: string;
}

const RecordWire = Schema.Struct({
  lastFsck: Schema.optionalKey(Schema.String),
  lastGc: Schema.optionalKey(Schema.String),
  lastRepack: Schema.optionalKey(Schema.String),
  lastBundleAttempt: Schema.optionalKey(Schema.String),
});
const decodeRecord = Schema.decodeUnknownResult(RecordWire);

export const parseRecord = (text: string): MaintenanceRecord => {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return {};
  }
  const decoded = decodeRecord(parsed);
  return Result.isFailure(decoded) ? {} : decoded.success;
};

export class MaintenanceMeta extends Context.Service<
  MaintenanceMeta,
  {
    readonly read: Effect.Effect<MaintenanceRecord, StorageFailure>;
    readonly write: (record: MaintenanceRecord) => Effect.Effect<void, StorageFailure>;
  }
>()("server/MaintenanceMeta") {}

export const memoryMeta = (): MaintenanceMeta["Service"] => {
  let record: MaintenanceRecord = {};
  return MaintenanceMeta.of({
    read: Effect.sync(() => record),
    write: (next) =>
      Effect.sync(() => {
        record = next;
      }),
  });
};

export const memoryMetaLayer = Layer.sync(MaintenanceMeta, memoryMeta);

const featuresOf = Effect.fnUntraced(function* () {
  return Option.getOrElse(yield* Effect.serviceOption(ServerFeatures), () => defaults);
});

const asConfig = (features: Features): MaintenanceConfig => ({
  bundles: features.bundles,
  maintenance: features.maintenance,
});

const parseDate = (value: string | undefined): Date | undefined => {
  if (value === undefined) return undefined;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? undefined : date;
};

export const snapshot = Effect.fn("Maintenance.snapshot")(function* () {
  const repository = yield* Repository;
  const objects = yield* ObjectStore;
  const listed = yield* repository.refs;
  const refs: Record<string, Oid> = {};
  for (const [name, oid] of listed) {
    if (hiddenFromAdvertisement(name) || !isOid(oid)) continue;
    refs[name] = oid;
  }

  const bundles = yield* Effect.serviceOption(Bundles);
  const store = yield* Effect.serviceOption(BundleStore);
  const repo = (yield* storageOf()) ?? "repository";
  const manifest = Option.isSome(bundles)
    ? yield* bundles.value.manifest
    : Option.isSome(store)
      ? yield* store.value.list(repo)
      : null;
  const storedIds = Option.isSome(store) ? yield* store.value.listIds(repo) : [];

  const meta = yield* Effect.serviceOption(MaintenanceMeta);
  const record = Option.isSome(meta) ? yield* meta.value.read : {};

  const packs = yield* Effect.serviceOption(PackStore);
  const packCount = Option.isSome(packs) ? (yield* packs.value.list).length : undefined;

  let looseObjectCount: number | undefined;
  if (Option.isSome(yield* Effect.serviceOption(ObjectStore))) {
    let count = 0;
    yield* Stream.runForEach(objects.list, () =>
      Effect.sync(() => {
        count += 1;
      }),
    );
    looseObjectCount = count;
  }

  return {
    now: new Date(),
    refs,
    bundles: manifest,
    storedIds,
    lastFsck: parseDate(record.lastFsck),
    lastGc: parseDate(record.lastGc),
    lastRepack: parseDate(record.lastRepack),
    lastBundleAttempt: parseDate(record.lastBundleAttempt),
    looseObjectCount,
    packCount,
  } satisfies MaintenanceSnapshot;
});

export const plan = Effect.fn("Maintenance.plan")(function* () {
  const captured = yield* snapshot();
  const features = yield* featuresOf();
  return planMaintenance(captured, asConfig(features));
});

const stamp = (record: MaintenanceRecord, field: keyof MaintenanceRecord): MaintenanceRecord => ({
  ...record,
  [field]: new Date().toISOString(),
});

const executeUnit = Effect.fn("Maintenance.execute")(function* (unit: MaintenanceUnit) {
  const store = yield* Effect.serviceOption(BundleStore);
  const bundles = yield* Effect.serviceOption(Bundles);
  const meta = yield* Effect.serviceOption(MaintenanceMeta);
  const repository = yield* Repository;
  const record = Option.isSome(meta) ? yield* meta.value.read : {};

  switch (unit.kind) {
    case "prune-invalid":
    case "prune-retired": {
      if (Option.isNone(store)) return { unit, result: { pruned: 0 } };
      yield* Effect.forEach(unit.ids, (id) => store.value.delete(id), { discard: true });
      return { unit, result: { pruned: unit.ids.length } };
    }
    case "build-full":
    case "build-incremental": {
      if (Option.isNone(bundles)) {
        return { unit, result: { skipped: "bundles are not configured" } };
      }
      const kind = unit.kind === "build-full" ? "full" : "incremental";
      const built = yield* bundles.value.build({ kind, filter: unit.filter });
      if (Option.isSome(meta)) {
        yield* meta.value.write(stamp(record, "lastBundleAttempt"));
      }
      return { unit, result: { id: built.id, bytes: built.bytes, objects: built.objectId } };
    }
    case "fsck": {
      const report = yield* repository.fsck;
      if (Option.isSome(meta)) yield* meta.value.write(stamp(record, "lastFsck"));
      return { unit, result: report };
    }
    case "gc": {
      const report = yield* repository.gc({});
      if (Option.isSome(meta)) yield* meta.value.write(stamp(record, "lastGc"));
      return { unit, result: report };
    }
    case "repack": {
      const report = yield* repository.gc({ repack: true });
      if (Option.isSome(meta)) {
        yield* meta.value.write({
          ...stamp(record, "lastGc"),
          lastRepack: new Date().toISOString(),
        });
      }
      return { unit, result: report };
    }
  }
});

export interface TickResult {
  readonly planned: ReadonlyArray<MaintenanceUnit>;
  readonly ran: MaintenanceUnit | null;
  readonly result?: unknown;
  readonly operationId?: string;
}

const kindName = (unit: MaintenanceUnit): string => {
  switch (unit.kind) {
    case "build-full":
      return "bundle.build";
    case "build-incremental":
      return "bundle.build";
    case "prune-invalid":
    case "prune-retired":
      return "bundle.prune";
    case "fsck":
      return "maintenance.fsck";
    case "gc":
      return "maintenance.gc";
    case "repack":
      return "maintenance.repack";
  }
};

/** Plan and execute the highest-priority unit. */
export const tick = Effect.fn("Maintenance.tick")(function* () {
  const features = yield* featuresOf();
  if (!features.maintenance.enabled && !features.bundles.enabled) {
    return { planned: [], ran: null } satisfies TickResult;
  }
  const captured = yield* snapshot();
  const planned = planMaintenance(captured, asConfig(features));
  const unit = nextUnit(captured, asConfig(features));
  if (unit === null) return { planned, ran: null } satisfies TickResult;

  const operations = yield* Effect.serviceOption(Operations);
  const repo = (yield* storageOf()) ?? "repository";
  if (Option.isNone(operations)) {
    const executed = yield* executeUnit(unit);
    return { planned, ran: unit, result: executed.result } satisfies TickResult;
  }

  const ran = yield* runOperation(
    {
      repo,
      kind: kindName(unit),
      cancellable: unit.kind === "build-full" || unit.kind === "build-incremental",
    },
    (handle) => executeUnit(unit).pipe(Effect.tap(() => handle.info("complete"))),
  );
  return {
    planned,
    ran: unit,
    operationId: ran.operation.id,
  } satisfies TickResult;
});

/** Replan after each unit until nothing remains. */
export const runAll = Effect.fn("Maintenance.runAll")(function* () {
  const ticks: TickResult[] = [];
  for (let step = 0; step < 32; step++) {
    const result = yield* tick();
    ticks.push(result);
    if (result.ran === null) break;
  }
  return ticks;
});

export { planMaintenance };
export type { MaintenanceUnit };
