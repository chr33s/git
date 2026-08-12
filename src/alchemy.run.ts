/**
 * Infrastructure: the bucket and Durable Object are values in the program
 * that uses them, so each binding has one source of truth and its type is a
 * return type rather than codegen output. Replaces `wrangler.json`'s
 * `r2_buckets`, `durable_objects` and `migrations`, plus the generated
 * `worker-configuration.d.ts`.
 *
 * `wrangler.json` still ships and is what the integration suite drives.
 * Deploying this path needs Cloudflare credentials, so CI only checks that
 * the stack builds — see `alchemy.run.test.ts`.
 */
import * as Alchemy from "alchemy/Cloudflare";
import { Effect } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http";

import { Repo } from "./host/Cloudflare.ts";

export { Objects } from "./objects.ts";

/**
 * The Worker is a router: resolve `/:repo` to a DO stub and forward. Same job
 * as `git/Durable.ts`'s default export, with one difference — `repos` is a
 * typed stub derived from the DO class, so a renamed method is a compile
 * error rather than a 500 at the edge.
 */
/** The Worker's public contract: it serves HTTP, nothing more. */
export type GitShape = {
  readonly fetch: Effect.Effect<
    HttpServerResponse.HttpServerResponse,
    never,
    HttpServerRequest.HttpServerRequest
  >;
};

export class Git extends Alchemy.Worker<Git, GitShape>()("git") {}

/**
 * The layer form, because the Worker binds the DO: `Repo.make` in
 * `host/Cloudflare.ts` provides `Repo`, this requires it, and the two compose
 * into the stack. The class-with-implementation form cannot express that.
 */
export default Git.make(
  {
    main: import.meta.url,
    compatibility: { date: "2025-12-10", flags: ["nodejs_compat"] },
  },
  Effect.gen(function* () {
    const repos = yield* Repo;

    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const name = new URL(request.url, "http://x").pathname.split("/")[1];
        if (name === undefined || name === "") {
          return HttpServerResponse.text("No repository in URL", { status: 400 });
        }

        const response = yield* repos
          .getByName(name)
          .fetch(new Request(request.url, { method: request.method }));
        return HttpServerResponse.raw(response);
      }),
    };
  }),
);
