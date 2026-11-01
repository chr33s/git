import { Config, Schema } from "effect";

/** Zero disables periodic export; counters remain available to the host owner. */
export const interval = Config.schema(
  Schema.Int.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(2_147_483_647)),
  "GIT_HTTP_METRICS_INTERVAL_MS",
).pipe(Config.withDefault(60_000));

/** Fixed-size counters: no client-controlled strings become metric labels. */
export const make = () => {
  const started = performance.now();
  const counters = {
    admitted: 0,
    active: 0,
    completed: 0,
    interrupted: 0,
    refusedGlobal: 0,
    refusedRepository: 0,
    deadlines: 0,
    unauthorized: 0,
    forbidden: 0,
    oversized: 0,
    serverErrors: 0,
    durationMsSum: 0,
    durationMsMax: 0,
  };
  return {
    snapshot: () => ({ ...counters, uptimeMs: performance.now() - started }),
    refuse: (scope: "global" | "repository") => {
      if (scope === "global") counters.refusedGlobal++;
      else counters.refusedRepository++;
    },
    deadline: () => {
      counters.deadlines++;
    },
    begin: () => {
      counters.admitted++;
      counters.active++;
      const started = performance.now();
      let completed = false;
      return (status: number | undefined) => {
        if (completed) return;
        completed = true;
        counters.active--;
        counters.completed++;
        const duration = performance.now() - started;
        counters.durationMsSum += duration;
        counters.durationMsMax = Math.max(counters.durationMsMax, duration);
        if (status === undefined) counters.interrupted++;
        else if (status === 401) counters.unauthorized++;
        else if (status === 403) counters.forbidden++;
        else if (status === 413) counters.oversized++;
        else if (status >= 500) counters.serverErrors++;
      };
    },
  };
};

export type Snapshot = ReturnType<ReturnType<typeof make>["snapshot"]>;
