import { ArtifactsError } from "@alchemy.run/cloudflare/Artifacts/ReadWriteNamespace";
import { Config, Effect, Schema } from "effect";

export type RegistryKind = "repositories" | "tokens" | "forks";
export interface Limits {
  readonly rows: number;
  readonly bytes: number;
}

export const configuration = Effect.fn("Artifacts.MetadataCapacity.configuration")(function* (
  kind: RegistryKind,
  host: "GIT" | "GIT_CF" = "GIT",
) {
  const suffix = { repositories: "REGISTRY", tokens: "TOKEN", forks: "FORK" }[kind];
  const prefix = `${host}_ARTIFACTS_${suffix}`;
  return {
    rows: yield* Config.schema(
      Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_000_000)),
      `${prefix}_ROWS`,
    ).pipe(Config.withDefault(kind === "tokens" ? 4096 : 1024)),
    bytes: yield* Config.schema(
      Schema.Int.check(
        Schema.isGreaterThan(0),
        Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
      ),
      `${prefix}_BYTES`,
    ).pipe(Config.withDefault(kind === "tokens" ? 2 * 1024 ** 2 : 1024 ** 2)),
  };
});

const exhausted = () =>
  new ArtifactsError({
    message: "RESOURCE_EXHAUSTED: administrative registry capacity exhausted",
    cause: new Error("ARTIFACTS_METADATA_CAPACITY"),
  });

export const check = (limits: Limits, rows: number, bytes: number): void => {
  if (rows > limits.rows || bytes > limits.bytes) throw exhausted();
};

export const report = (kind: RegistryKind) =>
  Effect.fn("Artifacts.MetadataCapacity.report")(function* (error: ArtifactsError) {
    if (error.message.startsWith("RESOURCE_EXHAUSTED:"))
      yield* Effect.logWarning("Administrative metadata capacity exhausted", {
        component: "ArtifactsMetadata",
        event: "quota_exceeded",
        registry: kind,
      });
  });
