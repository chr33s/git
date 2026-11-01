import { Config, Effect, Schema } from "effect";

const positive = Schema.Int.check(
  Schema.isGreaterThan(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
);

export const configuration = Effect.fn("LfsCapacity.configuration")(function* () {
  return {
    bytes: yield* Config.schema(positive, "GIT_LFS_RETAINED_BYTES").pipe(
      Config.withDefault(10 * 1024 ** 3),
    ),
    objects: yield* Config.schema(
      Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_000_000)),
      "GIT_LFS_RETAINED_OBJECTS",
    ).pipe(Config.withDefault(100_000)),
  };
});

export interface Limits {
  readonly bytes: number;
  readonly objects: number;
}
