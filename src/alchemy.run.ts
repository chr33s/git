/**
 * Infrastructure — phase 5.
 *
 * The bucket and the Durable Object are values in the same program that uses
 * them: `Objects` is bound by `host/Cloudflare.ts` by reference, and `Repo`
 * is bound here the same way. One source of truth per binding, and its type
 * is the return type of the call rather than the output of a codegen step.
 *
 * What this replaces, concretely:
 *
 *   wrangler.json `r2_buckets`       -> `Objects` in `src/objects.ts`
 *   wrangler.json `durable_objects`  -> the `Repo` class, bound by reference
 *   wrangler.json `migrations`       -> derived from the DO's storage kind
 *   worker-configuration.d.ts        -> the return type of the binding call
 *   npm postinstall `wrangler types` -> nothing
 *
 * What it adds: `--stage` previews (a full stack per PR, destroyed on close).
 *
 *   alchemy deploy · alchemy deploy --stage pr-123 · alchemy destroy
 *
 * `wrangler.json` still ships and is what the integration suite drives; this
 * is the second path, not yet the only one. Deploying it needs Cloudflare
 * credentials, so CI checks that the stack *builds* — see `alchemy.run.test.ts`.
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
