/**
 * Infrastructure.
 *
 * Today: `wrangler.json` declares the R2 bucket, the DO binding, the migration
 * tag and the compat date; `worker.ts` re-derives the repo name from the URL
 * and calls `env.GIT_SERVER.getByName(repo)`; `Env` comes from a generated
 * `worker-configuration.d.ts` that `postinstall` regenerates. Three sources of
 * truth for one binding, and the type only exists after a codegen step.
 *
 * Sketch: alchemy@next is "infrastructure as effects" — the bucket and the DO
 * are values in the same program that uses them, and the binding, the env var
 * and the typed client come from one call. `wrangler.json` and the generated
 * env types both go away.
 *
 * Deploy: `alchemy deploy` / `alchemy deploy --stage pr-123` / `alchemy destroy`.
 */
import * as Cloudflare from "alchemy/Cloudflare";
import { Effect } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { Repo } from "./server/Repo.ts";

/** Git objects and LFS payloads. One bucket, prefixed per repo. */
export const Objects = Cloudflare.R2.Bucket("git-objects");

/**
 * The Worker is a router: resolve `/:repo` to a DO stub and forward. That is
 * all `worker.ts` does today too — the difference is that `repos` is a typed
 * stub derived from the DO class, so a renamed method is a compile error rather
 * than a 500 at the edge. `Repo` in the third type argument is the Worker's
 * public contract: this script hosts the DO, and other scripts may bind it.
 */
export class Git extends Cloudflare.Worker<Git, {}, Repo>()("git") {}

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
        const name = new URL(request.url, "http://x").pathname.match(
          /^\/([a-z0-9-_.]+?)(?:\.git)?(?:\/|$)/,
        )?.[1];

        if (name === undefined) {
          return HttpServerResponse.text("No repository in URL", { status: 400 });
        }

        // Typed stub: `fetch` here is `RepoShape["fetch"]`, not `any`.
        return yield* repos.getByName(name).fetch(yield* toRequest(request));
      }),
    };
  }),
);

declare const toRequest: (
  request: HttpServerRequest.HttpServerRequest,
) => Effect.Effect<Request>;

/**
 * What this replaces, concretely:
 *
 *   wrangler.json `r2_buckets`       -> `Objects` above
 *   wrangler.json `durable_objects`  -> the `Repo` class, bound by reference
 *   wrangler.json `migrations`       -> derived from the DO class's storage kind
 *   worker-configuration.d.ts        -> the return type of the binding call
 *   npm postinstall `wrangler types` -> nothing
 *
 * What it adds that we do not have today: `--stage` previews (a full stack per
 * PR, torn down on close), and a local runtime that runs the same program
 * against local R2/DO emulation, so `npm run dev` and the e2e suite stop
 * needing a spawned `wrangler dev` on port 8080 (`test.helpers.ts`).
 */
