/** Shared configuration for the node host and `git+ serve`. */
import { Config, Effect, Result } from "effect";

import { Invalid } from "../git/Error.ts";
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

/**
 * Environment values and the one set of defaults for both node entry points.
 *
 * `GIT_HOSTS` comes back as the raw string rather than a parsed list, and that
 * is what makes `--hosts` able to override it. Parsed here, a malformed
 * environment value failed the whole read — so an operator whose `GIT_HOSTS`
 * held a pasted URL could not start the server *even by naming the hosts
 * explicitly on the command line*, which is the precedence this module's own
 * docstring promises. Now the value is only parsed if it is going to be used.
 */
export const configuration = Effect.fn("host.ServeConfig.configuration")(function* () {
  return {
    root: yield* Config.string("GIT_ROOT").pipe(Config.withDefault("repos")),
    port: yield* Config.number("PORT").pipe(Config.withDefault(8080)),
    hostname: yield* Config.string("HOSTNAME").pipe(Config.withDefault("127.0.0.1")),
    hosts: yield* Config.string("GIT_HOSTS").pipe(Config.withDefault("")),
  } as const;
});

/**
 * The configuration a set of flags and a set of environment values amount to.
 *
 * Explicit CLI flags take precedence over environment configuration — and the
 * case that matters is an operator reaching for `--hosts` *because* their
 * `GIT_HOSTS` is wrong. So the environment value is parsed only when it is
 * going to be used: validated eagerly, a malformed one failed the whole read
 * and the flag that was there to fix it could never be reached.
 *
 * Pure, and separate from reading the environment, because the precedence is
 * the part worth testing and `Config`'s provider snapshots `process.env` once
 * per process — which leaves a test that sets it able to prove nothing.
 */
export const merge = (
  configured: {
    readonly root: string;
    readonly port: number;
    readonly hostname: string;
    readonly hosts: string;
  },
  overrides: Partial<ServeConfig>,
): Result.Result<ServeConfig, string> => {
  const parsed = overrides.hosts === undefined ? parseHosts(configured.hosts) : null;
  if (parsed !== null && Result.isFailure(parsed)) {
    return Result.fail(`GIT_HOSTS: ${parsed.failure}`);
  }
  return Result.succeed({
    root: overrides.root ?? configured.root,
    port: overrides.port ?? configured.port,
    hostname: overrides.hostname ?? configured.hostname,
    hosts: overrides.hosts ?? (parsed === null ? [] : parsed.success),
  });
};

/**
 * The same, over this process's environment.
 *
 * The one place `GIT_HOSTS` is validated, so both node entry points refuse the
 * same value with the same message and neither starts on a list no request can
 * match.
 */
export const resolve = Effect.fn("host.ServeConfig.resolve")(function* (
  overrides: Partial<ServeConfig>,
) {
  const merged = merge(yield* configuration(), overrides);
  if (Result.isFailure(merged)) {
    return yield* new Invalid({ field: "GIT_HOSTS", reason: merged.failure });
  }
  return merged.success;
});
