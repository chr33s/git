/** `git+ serve` — the node host, with the same configuration as its standalone entry. */
import * as path from "node:path";

import { Console, Effect } from "effect";
import { Command, Flag } from "effect/unstable/cli";

import { Invalid } from "../git/Error.ts";
import { resolve } from "../host/ServeConfig.ts";
import * as Static from "../server/Static.ts";

/** The bundle is beside this source when run from a checkout or package. */
const defaultUiDir = path.join(import.meta.dirname, "..", "..", "dist", "ui");

/**
 * Explicit flags win, followed by `GIT_ROOT`, `PORT` and `HOSTNAME`, then the
 * node host defaults. Keeping the resolution in the host makes `git+ serve`
 * and `node src/host/Node.ts` one configuration surface.
 */
export const serveCommand = Command.make(
  "serve",
  {
    root: Flag.string("root").pipe(
      Flag.optional,
      Flag.withDescription("Directory holding one bare repository per subdirectory"),
    ),
    port: Flag.integer("port").pipe(Flag.optional, Flag.withAlias("p")),
    hostname: Flag.string("hostname").pipe(
      Flag.optional,
      Flag.withDescription("Host interface to bind (or HOSTNAME)"),
    ),
    open: Flag.boolean("open").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Serve writes to repositories that have no genesis"),
    ),
    wake: Flag.boolean("wake").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Run each repository's wake.json rules when a push moves its hub refs"),
    ),
    ui: Flag.boolean("ui").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Serve the built browser UI from this origin as well"),
    ),
    uiDir: Flag.string("ui-dir").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Where the built UI is, if not the one built beside this install"),
    ),
  },
  ({ hostname, open, port, root, ui, uiDir, wake }) =>
    Effect.gen(function* () {
      const options = yield* resolve({
        root: root._tag === "Some" ? root.value : undefined,
        port: port._tag === "Some" ? port.value : undefined,
        hostname: hostname._tag === "Some" ? hostname.value : undefined,
      });
      const assets = ui ? (uiDir === "" ? defaultUiDir : uiDir) : undefined;
      if (assets !== undefined && !(yield* Effect.promise(() => Static.built(assets)))) {
        return yield* new Invalid({
          field: "ui",
          reason: `${assets} holds no built UI; run \`npm run build:ui\` first, or point --ui-dir at one`,
        });
      }
      const server = yield* Effect.promise(() =>
        import("../host/Node.ts").then(({ serve }) =>
          serve({ ...options, allowAnonymousWrites: open, wake, ui: assets }),
        ),
      );
      yield* Console.log(
        `git smart-HTTP server on ${server.url}, repositories under ${options.root}/`,
      );
      if (assets !== undefined) {
        yield* Console.log(
          `browser UI on ${server.url} from ${assets}, showing the repository its index.html names`,
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
    }),
);
