import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Duration, Effect } from "effect";

import type { Oid } from "../git/Oid.ts";
import { defaults } from "./Features.ts";
import { nextUnit, planMaintenance, type MaintenanceSnapshot } from "./Planner.ts";

// SAFETY: forty hex characters by construction.
const oid = (char: string): Oid => char.repeat(40) as Oid;

const now = new Date("2024-06-01T00:00:00.000Z");

const empty: MaintenanceSnapshot = {
  now,
  refs: { "refs/heads/main": oid("a") },
  bundles: null,
  storedIds: [],
};

describe("Planner priority", () => {
  it.effect("cleans incomplete artifacts before building", () =>
    Effect.sync(() => {
      const planned = planMaintenance({ ...empty, storedIds: ["tmp/half"] }, defaults);
      assert.equal(planned[0]?.kind, "prune-invalid");
      assert.equal(
        nextUnit({ ...empty, storedIds: ["tmp/half"] }, defaults)?.kind,
        "prune-invalid",
      );
    }),
  );

  it.effect("builds a full bundle before incrementals and fsck", () =>
    Effect.sync(() => {
      const planned = planMaintenance(empty, defaults);
      assert.equal(planned[0]?.kind, "build-full");
      assert.ok(planned.some((unit) => unit.kind === "fsck"));
    }),
  );

  it.effect("is deterministic for identical state", () =>
    Effect.sync(() => {
      const left = planMaintenance(empty, defaults);
      const right = planMaintenance({ ...empty }, defaults);
      assert.deepEqual(left, right);
    }),
  );

  it.effect("asks for an incremental once a full base exists and is young", () =>
    Effect.sync(() => {
      const snapshot: MaintenanceSnapshot = {
        ...empty,
        now: new Date(now.getTime() + Duration.toMillis(Duration.hours(2))),
        bundles: {
          version: 1,
          families: [
            {
              filter: null,
              full: {
                id: "full-1",
                kind: "full",
                filter: null,
                creationToken: 1n,
                refs: empty.refs,
                prerequisites: [],
                objectId: "full/1-aa.bundle",
                bytes: 1,
                checksum: "aa",
                createdAt: now.toISOString(),
              },
              incrementals: [],
            },
          ],
        },
      };
      const planned = planMaintenance(snapshot, defaults);
      assert.ok(planned.some((unit) => unit.kind === "build-incremental"));
      assert.ok(!planned.some((unit) => unit.kind === "build-full" && unit.filter === null));
    }),
  );

  it.effect("does not plan work for a repository with no refs", () =>
    Effect.sync(() => {
      const planned = planMaintenance({ ...empty, refs: {} }, defaults);
      assert.ok(!planned.some((unit) => unit.kind.startsWith("build-")));
    }),
  );
});
