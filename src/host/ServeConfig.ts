/** Shared configuration for the node host and `git+ serve`. */
import { Config, Effect } from "effect";

export interface ServeConfig {
  readonly root: string;
  readonly port: number;
  readonly hostname: string;
}

/** Environment values and the one set of defaults for both node entry points. */
export const configuration = Effect.fn("host.ServeConfig.configuration")(function* () {
  return {
    root: yield* Config.string("GIT_ROOT").pipe(Config.withDefault("repos")),
    port: yield* Config.number("PORT").pipe(Config.withDefault(8080)),
    hostname: yield* Config.string("HOSTNAME").pipe(Config.withDefault("127.0.0.1")),
  } satisfies ServeConfig;
});

/** Explicit CLI flags take precedence over environment configuration. */
export const resolve = Effect.fn("host.ServeConfig.resolve")(function* (
  overrides: Partial<ServeConfig>,
) {
  const configured = yield* configuration();
  return {
    root: overrides.root ?? configured.root,
    port: overrides.port ?? configured.port,
    hostname: overrides.hostname ?? configured.hostname,
  } satisfies ServeConfig;
});
