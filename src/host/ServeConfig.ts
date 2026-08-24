/** Shared configuration for the node host and `git+ serve`. */
import { Config, Effect } from "effect";

export interface ServeConfig {
  readonly root: string;
  readonly port: number;
  readonly hostname: string;
  /**
   * The public authorities this server answers to — `host` or `host:port`, as
   * they appear in a request's `Host` header (e.g. `git.example.com`,
   * `git.example.com:8443`).
   *
   * A delegated credential carries the host it was minted for signed into its
   * bytes, and the guard must check that against the host the request actually
   * arrived at. A self-hosted server cannot read that host from the request —
   * the `Host` header is the client's to set — so it is matched against the
   * server's own bound authority and this list: a header naming either is
   * trusted as the audience, and anything else leaves it unknown, which refuses
   * the credential.
   *
   * The server's real `hostname:port` is always trusted, so a server reached at
   * the address it binds needs no entry here. Set `GIT_HOSTS` only where the
   * public name differs from the bind address — behind a reverse proxy or under
   * a virtual host — naming every authority clients (and the credentials minted
   * for this server) actually use.
   */
  readonly hosts: ReadonlyArray<string>;
}

/** A comma- or space-separated host list, emptied of blanks. */
const parseHosts = (raw: string): ReadonlyArray<string> =>
  raw
    .split(/[,\s]+/)
    .map((host) => host.trim())
    .filter((host) => host !== "");

/** Environment values and the one set of defaults for both node entry points. */
export const configuration = Effect.fn("host.ServeConfig.configuration")(function* () {
  return {
    root: yield* Config.string("GIT_ROOT").pipe(Config.withDefault("repos")),
    port: yield* Config.number("PORT").pipe(Config.withDefault(8080)),
    hostname: yield* Config.string("HOSTNAME").pipe(Config.withDefault("127.0.0.1")),
    hosts: yield* Config.string("GIT_HOSTS").pipe(Config.withDefault(""), Config.map(parseHosts)),
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
    hosts: overrides.hosts ?? configured.hosts,
  } satisfies ServeConfig;
});
