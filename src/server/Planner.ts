/**
 * Pure desired-state maintenance planner.
 *
 * Given repository state and configuration now, what is the single most
 * valuable bounded unit of missing work? The planner does no I/O. A missed
 * timer therefore causes delay, not permanently missing state.
 */
import { Duration } from "effect";

import type { Oid } from "../git/Oid.ts";
import {
  advertisedIds,
  type BundleFilter,
  type BundleManifest,
  familyState,
} from "./BundleFormat.ts";
import type { BundlePolicy, MaintenancePolicy } from "./Features.ts";

export type MaintenanceUnit =
  | { readonly kind: "prune-invalid"; readonly ids: ReadonlyArray<string> }
  | { readonly kind: "build-full"; readonly filter: BundleFilter }
  | { readonly kind: "build-incremental"; readonly filter: BundleFilter }
  | { readonly kind: "prune-retired"; readonly ids: ReadonlyArray<string> }
  | { readonly kind: "fsck" }
  | { readonly kind: "gc" }
  | { readonly kind: "repack" };

export interface MaintenanceSnapshot {
  readonly now: Date;
  readonly refs: Readonly<Record<string, Oid>>;
  readonly bundles: BundleManifest | null;
  readonly storedIds: ReadonlyArray<string>;
  readonly lastFsck?: Date;
  readonly lastGc?: Date;
  readonly lastRepack?: Date;
  readonly lastBundleAttempt?: Date;
  readonly looseObjectCount?: number;
  readonly packCount?: number;
}

export interface MaintenanceConfig {
  readonly bundles: BundlePolicy;
  readonly maintenance: MaintenancePolicy;
}

const olderThan = (last: Date | undefined, now: Date, max: Duration.Duration): boolean => {
  if (last === undefined) return true;
  return now.getTime() - last.getTime() >= Duration.toMillis(max);
};

const newerThan = (last: Date | undefined, now: Date, min: Duration.Duration): boolean => {
  if (last === undefined) return false;
  return now.getTime() - last.getTime() < Duration.toMillis(min);
};

const hasRefs = (refs: Readonly<Record<string, Oid>>): boolean => Object.keys(refs).length > 0;

const latestFamilyActivity = (
  snapshot: MaintenanceSnapshot,
  filter: BundleFilter,
): Date | undefined => {
  const family = familyState(snapshot.bundles, filter);
  const latest = family.incrementals.at(-1) ?? family.full;
  if (latest !== undefined && latest !== null) return new Date(latest.createdAt);
  return snapshot.lastBundleAttempt;
};

const invalidIds = (snapshot: MaintenanceSnapshot): ReadonlyArray<string> =>
  snapshot.storedIds.filter((id) => id.startsWith("tmp/"));

const retiredIds = (snapshot: MaintenanceSnapshot): ReadonlyArray<string> => {
  const advertised = advertisedIds(snapshot.bundles);
  return snapshot.storedIds.filter((id) => !id.startsWith("tmp/") && !advertised.has(id));
};

/**
 * Planned units in priority order. The scheduler executes the first expensive
 * unit; cheap cleanup may be grouped by the executor when its cost is nothing.
 */
export const planMaintenance = (
  snapshot: MaintenanceSnapshot,
  config: MaintenanceConfig,
): ReadonlyArray<MaintenanceUnit> => {
  const units: MaintenanceUnit[] = [];
  const advertised = hasRefs(snapshot.refs);

  const invalid = invalidIds(snapshot);
  if (invalid.length > 0) units.push({ kind: "prune-invalid", ids: invalid });

  if (config.bundles.enabled && advertised) {
    for (const filter of config.bundles.filters) {
      const family = familyState(snapshot.bundles, filter);
      const last = latestFamilyActivity(snapshot, filter);
      const incrementals = family.incrementals.length;
      const fullStale =
        family.full === null ||
        olderThan(new Date(family.full.createdAt), snapshot.now, config.bundles.fullMaxAge);
      const overLong = incrementals >= config.bundles.maxIncrementals;

      if (family.full === null || (fullStale && incrementals > 0) || overLong) {
        units.push({ kind: "build-full", filter });
        continue;
      }
      if (
        !newerThan(last, snapshot.now, config.bundles.incrementalMinAge) &&
        incrementals < config.bundles.maxIncrementals
      ) {
        units.push({ kind: "build-incremental", filter });
      }
    }
  }

  const retired = retiredIds(snapshot);
  if (retired.length > 0) units.push({ kind: "prune-retired", ids: retired });

  if (olderThan(snapshot.lastFsck, snapshot.now, config.maintenance.fsckInterval)) {
    units.push({ kind: "fsck" });
  }

  const loose = snapshot.looseObjectCount ?? 0;
  const packs = snapshot.packCount ?? 0;
  if (
    olderThan(snapshot.lastGc, snapshot.now, config.maintenance.gcInterval) ||
    loose >= config.maintenance.repackThreshold
  ) {
    units.push({ kind: "gc" });
  }
  if (packs >= 2 || loose >= config.maintenance.repackThreshold) {
    units.push({ kind: "repack" });
  }

  return units;
};

/** Highest-priority unit, or `null` when the repository needs nothing. */
export const nextUnit = (
  snapshot: MaintenanceSnapshot,
  config: MaintenanceConfig,
): MaintenanceUnit | null => planMaintenance(snapshot, config)[0] ?? null;
