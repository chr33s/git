/** Shared configuration for the node host and `git+ serve`. */
import { Config, ConfigProvider, Effect, Result } from "effect";

import { commaList } from "../text.ts";

export interface ServeConfig {
  readonly root: string;
  readonly port: number;
  readonly hostname: string;
  /**
   * The public authorities this server answers to — `host` or `host:port`, as
   * they appear in a request's `Host` header (e.g. `git.example.com`,
   * `git.example.com:8443`), comma-separated in `GIT_HOSTS` or `--hosts`.
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
   * the address it binds needs no entry here. Set it only where the public name
   * differs from the bind address — behind a reverse proxy, under a virtual
   * host, or bound to a wildcard address, where the bound authority names no
   * host a client can have used — naming every authority clients (and the
   * credentials minted for this server) actually use.
   */
  readonly hosts: ReadonlyArray<string>;
}

/**
 * The authority shape a `Host` header can carry: a host name or a bracketed
 * IPv6 literal, optionally with a port. No scheme, no path, no user info.
 */
const AUTHORITY = /^(?:\[[0-9A-Fa-f:.]+\]|[0-9A-Za-z._-]+)(?::\d{1,5})?$/;

/** The authority a mistyped entry looks like it meant, where it holds one. */
const suggestion = (entry: string): string => {
  try {
    const { host } = new URL(entry);
    return host === "" ? "" : `; write '${host}'`;
  } catch {
    return "";
  }
};

/**
 * The trusted authorities an operator wrote, lowercased for the
 * case-insensitive match a `Host` header needs — or what is wrong with the
 * list, for a caller to refuse over.
 *
 * Validated rather than taken as written, because the failure is otherwise
 * silent and undiagnosable: a `Host` header never carries a scheme, so
 * `https://git.example.com` — the plausible paste, since a registered remote is
 * written as a URL — is an entry no request can ever match. It would sit there
 * looking configured while every host-bound credential was refused for want of
 * the very authority it names. Refused at startup instead, naming the entry.
 */
export const parseHosts = (raw: string): Result.Result<ReadonlyArray<string>, string> => {
  const hosts = commaList(raw);
  const invalid = hosts.find((host) => !AUTHORITY.test(host));
  return invalid === undefined
    ? Result.succeed(hosts.map((host) => host.toLowerCase()))
    : Result.fail(
        `'${invalid}' is not a host authority: name the host as a Host header carries it, ` +
          `'git.example.com' or 'git.example.com:8443'${suggestion(invalid)}`,
      );
};

/** Environment values and the one set of defaults for both node entry points. */
export const configuration = Effect.fn("host.ServeConfig.configuration")(function* () {
  return {
    root: yield* Config.string("GIT_ROOT").pipe(Config.withDefault("repos")),
    port: yield* Config.number("PORT").pipe(Config.withDefault(8080)),
    hostname: yield* Config.string("HOSTNAME").pipe(Config.withDefault("127.0.0.1")),
    hosts: yield* Config.string("GIT_HOSTS").pipe(
      Config.withDefault(""),
      Config.mapOrFail((raw) => {
        const parsed = parseHosts(raw);
        return Result.isFailure(parsed)
          ? Effect.fail(new ConfigProvider.SourceError({ message: `GIT_HOSTS: ${parsed.failure}` }))
          : Effect.succeed(parsed.success);
      }),
    ),
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
