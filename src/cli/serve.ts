/** `git+ serve` — the node host, with the same configuration as its standalone entry. */
import * as path from "node:path";

import { Config, Console, Effect, Predicate, Result } from "effect";
import { Command, Flag } from "effect/cli";

import { Invalid } from "../git/Error.ts";
import { parseHosts, resolve } from "../host/ServeConfig.ts";
import { GitInvocation } from "./GitCompat.ts";
import { discoverRepository } from "./GitCompat.node.ts";
import * as Static from "../server/Static.node.ts";

/** The bundle is beside this source when run from a checkout or package. */
const defaultUiDir = path.join(import.meta.dirname, "..", "..", "dist", "ui");

/**
 * Explicit flags win over environment configuration. With no hosting root,
 * discover the current repository; an explicit `--root` or `GIT_ROOT` selects
 * the multi-repository host. Bind options share the standalone node host defaults.
 */
export const serveCommand = Command.make(
  "serve",
  {
    root: Flag.String("root").pipe(
      Flag.optional,
      Flag.withDescription("Directory holding one bare repository per subdirectory"),
    ),
    port: Flag.Int("port").pipe(Flag.optional, Flag.withAlias("p")),
    hostname: Flag.String("hostname").pipe(
      Flag.optional,
      Flag.withDescription("Host interface to bind (or HOSTNAME)"),
    ),
    hosts: Flag.String("hosts").pipe(
      Flag.optional,
      Flag.withDescription(
        "Public authorities this server answers to, comma-separated (or GIT_HOSTS)",
      ),
    ),
    open: Flag.Boolean("open").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Serve writes to repositories that have no genesis"),
    ),
    wake: Flag.Boolean("wake").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Run each repository's wake.json rules when a push moves its hub refs"),
    ),
    ui: Flag.Boolean("ui").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Serve the built browser UI from this origin as well"),
    ),
    uiDir: Flag.String("ui-dir").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Where the built UI is, if not the one built beside this install"),
    ),
  },
  ({ hostname, hosts, open, port, root, ui, uiDir, wake }) =>
    Effect.gen(function* () {
      // Validated here as well as in the environment, because an entry no
      // request can match is worth refusing at startup whichever way it
      // arrived. `resolve` checks `GIT_HOSTS`; this checks the flag that
      // overrides it. See `parseHosts`.
      const named = hosts._tag === "Some" ? parseHosts(hosts.value) : null;
      if (named !== null && Result.isFailure(named)) {
        return yield* new Invalid({ field: "hosts", reason: named.failure });
      }
      // An explicit hosting root keeps the multi-repository mode. Otherwise
      // bind exactly the repository selected by the normal CLI invocation.
      const configuredRoot = yield* Config.String("GIT_ROOT").pipe(Config.option);
      const hostingRoot = root._tag === "Some" ? root : configuredRoot;
      const repository =
        hostingRoot._tag === "Some" ? null : yield* discoverRepository(yield* GitInvocation);
      if (hostingRoot._tag !== "Some" && repository === null) {
        return yield* new Invalid({
          field: "repository",
          reason: "not inside a Git repository; run from a repository or pass --root",
        });
      }
      const options = yield* resolve({
        root:
          hostingRoot._tag === "Some"
            ? hostingRoot.value
            : repository === null
              ? undefined
              : path.dirname(repository.gitDir),
        port: port._tag === "Some" ? port.value : undefined,
        hostname: hostname._tag === "Some" ? hostname.value : undefined,
        hosts: named === null ? undefined : named.success,
      });
      let assets: Static.AssetSource | undefined;
      if (ui) {
        assets = uiDir === "" ? defaultUiDir : uiDir;
        if (uiDir === "" && (yield* Effect.promise(() => Static.built(Static.embeddedFile)))) {
          assets = Static.embeddedFile;
        }
      }
      if (assets !== undefined && !(yield* Effect.promise(() => Static.built(assets)))) {
        return yield* new Invalid({
          field: "ui",
          reason: `${assets} holds no built UI; run \`npm run build:ui\` first, or point --ui-dir at one`,
        });
      }
      const server = yield* Effect.acquireRelease(
        Effect.promise(() =>
          import("../host/Node.ts").then(({ serve }) =>
            serve({
              ...options,
              allowAnonymousWrites: open,
              wake,
              ui: assets,
              repository: repository?.gitDir,
            }),
          ),
        ),
        (server) => Effect.promise(() => server.close({ force: true })),
      );
      yield* Console.log(
        repository === null
          ? `git smart-HTTP server on ${server.url}, repositories under ${options.root}/`
          : `git smart-HTTP server on ${server.url}, repository ${repository.gitDir} at /repo`,
      );
      if (assets !== undefined) {
        yield* Console.log(
          `browser UI on ${server.url}/hub/code from ${Predicate.isString(assets) ? assets : "embedded assets"}`,
        );
      }
      if (wake) {
        yield* Console.error(
          "--wake: a push that moves a repository's hub refs runs the rules in its wake.json",
        );
      }
      yield* Console.error(
        (open
          ? "--open: repositories with no genesis accept writes from anyone who can reach the port. "
          : "repositories with no genesis are readable by anyone who can reach the port and " +
            "writable by nobody; pass --open to serve writes to them anyway. ") +
          "run `git+ hub init <repo> --key <key>` to give a repository a membership " +
          "of its own. A repository whose members hold no read capability is still public: " +
          "membership restricts, so restricting nothing restricts nobody",
      );
      return yield* Effect.never;
    }).pipe(Effect.scoped),
);
