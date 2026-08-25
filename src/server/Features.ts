/**
 * Server-side bundle and maintenance configuration.
 *
 * Read through Effect `Config`, not `process.env`, so a malformed duration
 * fails naming the variable. Invalid values fail closed at layer build.
 */
import { Config, Context, Duration, Effect, Layer } from "effect";

import { Invalid } from "../git/Error.ts";
import type { BundleFilter } from "./BundleFormat.ts";

export interface BundlePolicy {
  readonly enabled: boolean;
  readonly fullMaxAge: Duration.Duration;
  readonly incrementalMinAge: Duration.Duration;
  readonly maxIncrementals: number;
  readonly minNewObjects: number;
  readonly filters: ReadonlyArray<BundleFilter>;
}

export interface MaintenancePolicy {
  readonly enabled: boolean;
  readonly interval: Duration.Duration;
  readonly fsckInterval: Duration.Duration;
  readonly gcInterval: Duration.Duration;
  readonly repackThreshold: number;
}

export interface Features {
  readonly bundles: BundlePolicy;
  readonly maintenance: MaintenancePolicy;
}

export const defaults: Features = {
  bundles: {
    enabled: true,
    fullMaxAge: Duration.days(7),
    incrementalMinAge: Duration.hours(1),
    maxIncrementals: 24,
    minNewObjects: 1,
    filters: [null, "blob:none"],
  },
  maintenance: {
    enabled: true,
    interval: Duration.hours(1),
    fsckInterval: Duration.days(7),
    gcInterval: Duration.days(1),
    repackThreshold: 50,
  },
};

const parseFilters = (value: string): Effect.Effect<ReadonlyArray<BundleFilter>, Invalid> =>
  Effect.suspend(() => {
    const filters: BundleFilter[] = [];
    for (const part of value.split(",")) {
      const item = part.trim();
      if (item === "" || item === "full" || item === "none") {
        if (!filters.includes(null)) filters.push(null);
        continue;
      }
      if (item === "blob:none" || item === "blobnone") {
        if (!filters.includes("blob:none")) filters.push("blob:none");
        continue;
      }
      return Effect.fail(
        new Invalid({
          field: "BUNDLES_FILTERS",
          reason: `unknown bundle filter '${item}'`,
        }),
      );
    }
    return Effect.succeed(filters.length === 0 ? defaults.bundles.filters : filters);
  });

const requireCount = (field: string, value: number): Effect.Effect<number, Invalid> =>
  Number.isInteger(value) && value >= 0
    ? Effect.succeed(value)
    : Effect.fail(new Invalid({ field, reason: "must be a non-negative integer" }));

export class ServerFeatures extends Context.Service<ServerFeatures, Features>()(
  "server/ServerFeatures",
) {}

export const configuration = Effect.fn("Features.configuration")(function* () {
  const enabled = yield* Config.boolean("BUNDLES_ENABLED").pipe(
    Config.withDefault(defaults.bundles.enabled),
  );
  const fullMaxAge = yield* Config.duration("BUNDLES_FULL_MAX_AGE").pipe(
    Config.withDefault(defaults.bundles.fullMaxAge),
  );
  const incrementalMinAge = yield* Config.duration("BUNDLES_INCREMENTAL_MIN_AGE").pipe(
    Config.withDefault(defaults.bundles.incrementalMinAge),
  );
  const maxIncrementals = yield* requireCount(
    "BUNDLES_MAX_INCREMENTALS",
    yield* Config.number("BUNDLES_MAX_INCREMENTALS").pipe(
      Config.withDefault(defaults.bundles.maxIncrementals),
    ),
  );
  const minNewObjects = yield* requireCount(
    "BUNDLES_MIN_NEW_OBJECTS",
    yield* Config.number("BUNDLES_MIN_NEW_OBJECTS").pipe(
      Config.withDefault(defaults.bundles.minNewObjects),
    ),
  );
  const filterText = yield* Config.string("BUNDLES_FILTERS").pipe(
    Config.withDefault("full,blob:none"),
  );
  const filters = yield* parseFilters(filterText);

  const maintenanceEnabled = yield* Config.boolean("MAINTENANCE_ENABLED").pipe(
    Config.withDefault(defaults.maintenance.enabled),
  );
  const interval = yield* Config.duration("MAINTENANCE_INTERVAL").pipe(
    Config.withDefault(defaults.maintenance.interval),
  );
  const fsckInterval = yield* Config.duration("MAINTENANCE_FSCK_INTERVAL").pipe(
    Config.withDefault(defaults.maintenance.fsckInterval),
  );
  const gcInterval = yield* Config.duration("MAINTENANCE_GC_INTERVAL").pipe(
    Config.withDefault(defaults.maintenance.gcInterval),
  );
  const repackThreshold = yield* requireCount(
    "MAINTENANCE_REPACK_THRESHOLD",
    yield* Config.number("MAINTENANCE_REPACK_THRESHOLD").pipe(
      Config.withDefault(defaults.maintenance.repackThreshold),
    ),
  );

  return {
    bundles: { enabled, fullMaxAge, incrementalMinAge, maxIncrementals, minNewObjects, filters },
    maintenance: {
      enabled: maintenanceEnabled,
      interval,
      fsckInterval,
      gcInterval,
      repackThreshold,
    },
  } satisfies Features;
});

export const layer = Layer.effect(
  ServerFeatures,
  configuration().pipe(Effect.map((features) => ServerFeatures.of(features))),
);

export const defaultsLayer = Layer.succeed(ServerFeatures, ServerFeatures.of(defaults));
