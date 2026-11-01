import { Config, Effect, Schema } from "effect";

export interface Limits {
  readonly active: number;
  readonly perRepository: number;
  readonly connections: number;
  readonly deadlineMs: number;
  readonly idleMs: number;
  readonly headersMs: number;
}

// Node timers overflow above this value and become one-millisecond timers.
const positive = Schema.Int.check(
  Schema.isGreaterThan(0),
  Schema.isLessThanOrEqualTo(2_147_483_647),
);

export const configuration = Effect.fn("host.Admission.configuration")(function* () {
  return {
    active: yield* Config.schema(positive, "GIT_HTTP_MAX_ACTIVE").pipe(Config.withDefault(32)),
    perRepository: yield* Config.schema(positive, "GIT_HTTP_MAX_PER_REPO").pipe(
      Config.withDefault(8),
    ),
    connections: yield* Config.schema(positive, "GIT_HTTP_MAX_CONNECTIONS").pipe(
      Config.withDefault(128),
    ),
    deadlineMs: yield* Config.schema(positive, "GIT_HTTP_DEADLINE_MS").pipe(
      Config.withDefault(300_000),
    ),
    idleMs: yield* Config.schema(positive, "GIT_HTTP_IDLE_MS").pipe(Config.withDefault(30_000)),
    headersMs: yield* Config.schema(positive, "GIT_HTTP_HEADERS_MS").pipe(
      Config.withDefault(15_000),
    ),
  } satisfies Limits;
});

/** A lease includes queued handlers and response delivery, not only execution. */
export const make = (limits: Limits) => {
  let active = 0;
  const repositories = new Map<string, number>();
  return {
    acquire: () => {
      if (active >= limits.active) return null;
      active++;
      let repository: string | undefined;
      let released = false;
      return {
        enterRepository: (name: string): boolean => {
          if (released || repository !== undefined) return false;
          const count = repositories.get(name) ?? 0;
          if (count >= limits.perRepository) return false;
          repositories.set(name, count + 1);
          repository = name;
          return true;
        },
        release: () => {
          if (released) return;
          released = true;
          active--;
          if (repository !== undefined) {
            const count = repositories.get(repository) ?? 0;
            if (count <= 1) repositories.delete(repository);
            else repositories.set(repository, count - 1);
          }
        },
      };
    },
  };
};
