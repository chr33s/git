import { Clock, Effect, Result, Schema } from "effect";

const Timestamp = Schema.String.check(
  Schema.makeFilter((value) => {
    const date = new Date(value);
    return Number.isFinite(date.getTime()) && date.toISOString() === value;
  }),
);
interface Age {
  readonly ageState: "known" | "future" | "invalid";
  readonly ageMs: number | null;
}

/** Age describes capture time, not whether any repository change was missed. */
const age = (publishedAt: string, now: number): Age => {
  const decoded = Schema.decodeResult(Timestamp)(publishedAt);
  if (Result.isFailure(decoded)) return { ageState: "invalid", ageMs: null };
  const elapsed = now - Date.parse(decoded.success);
  return elapsed < 0 ? { ageState: "future", ageMs: null } : { ageState: "known", ageMs: elapsed };
};

export const observed = Effect.fn("SnapshotTelemetry.observed")(function* (publishedAt: string) {
  yield* Effect.logInfo("snapshot age observed", {
    component: "SnapshotReader",
    operation: "observed",
    ...age(publishedAt, yield* Clock.currentTimeMillis),
  });
});

export const published = Effect.fn("SnapshotTelemetry.published")(function* (
  publishedAt: string,
  started: number,
  changed: boolean,
) {
  const now = yield* Clock.currentTimeMillis;
  yield* Effect.logInfo("snapshot publication completed", {
    component: "SnapshotPublisher",
    operation: "published",
    durationMs: Math.max(0, now - started),
    changed,
    ...age(publishedAt, now),
  });
});
