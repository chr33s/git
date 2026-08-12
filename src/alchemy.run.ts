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
 *
 *   alchemy dev · alchemy deploy --stage pr-123 · alchemy destroy
 */
import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import { Effect } from "effect";

import GitLive, { Git } from "./worker.ts";

export { Objects } from "./objects.ts";
export { Git, type GitShape } from "./worker.ts";

export default Alchemy.Stack(
  "git",
  // Local state (`.alchemy/`), so planning and `alchemy dev` need no
  // Cloudflare account; deploys still authenticate through the provider.
  { providers: Cloudflare.providers(), state: Alchemy.localState() },
  Effect.gen(function* () {
    // Declaring the Worker provisions everything it hosts and binds: the DO
    // by contract, and the bucket the DO resolves `Objects` through.
    const worker = yield* Git;
    return { url: worker.url };
  }).pipe(Effect.provide(GitLive)),
);
