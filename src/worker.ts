/**
 * The Worker: a router that resolves `/:repo` to a DO stub and forwards.
 * Same job as `git/Durable.ts`'s default export, with one difference —
 * `repos` is a typed stub derived from the DO class, so a renamed method is
 * a compile error rather than a 500 at the edge.
 *
 * Its own module because the bundler requires this file's *default export*
 * to be the worker layer, and `alchemy.run.ts`'s default export must be the
 * stack. The class stays importable on its own; consumers that bind it do
 * not pull in `.make()` — the bundler tree-shakes it.
 */
import * as Alchemy from "alchemy/Cloudflare";
import { Config, Effect, Redacted } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http";

import Repos from "./host/Cloudflare.ts";
import { Repo } from "./host/Cloudflare.ts";
import * as Auth from "./server/Auth.ts";
import { normalize, routeOf } from "./server/Route.ts";

/** The Worker's public contract: it serves HTTP, nothing more. */
export type GitShape = {
  readonly fetch: Effect.Effect<
    HttpServerResponse.HttpServerResponse,
    never,
    HttpServerRequest.HttpServerRequest
  >;
};

/** `Repo` in the third slot: this Worker hosts the DO, so it is contract. */
export class Git extends Alchemy.Worker<Git, GitShape, Repo>()("git") {}

/**
 * Pinned here and asserted against `wrangler.test.json` in
 * `alchemy.run.test.ts`: the runtime the integration suite proves must be
 * the runtime this Worker deploys.
 */
export const compatibility = { date: "2025-12-10", flags: ["nodejs_compat"] };

export default Git.make(
  {
    main: import.meta.url,
    compatibility,
  },
  Effect.gen(function* () {
    const repos = yield* Repo;

    // Read at init, so the deploy-time interceptor registers it as a
    // `secret_text` binding: a deploy without `GIT_AUTH_SECRET` in the
    // environment fails naming the variable, rather than shipping open.
    const secret = yield* Config.redacted("GIT_AUTH_SECRET");

    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const route = routeOf(new URL(request.url, "http://x").pathname);
        if (route === null) {
          return HttpServerResponse.text("No repository in URL", { status: 400 });
        }

        // The platform request, headers and body intact — the effect wrapper
        // was built from it (`HttpServerRequest.fromWeb`), so this is the
        // same object, not a reconstruction. Normalised so the DO sees one
        // spelling of the path whichever the client used.
        const raw = normalize(request.source as Request, route);

        // Auth lives at the edge: the DO trusts its callers, because the
        // only ways in are this guard and another Worker's binding.
        const denied = yield* Auth.guard(raw, (credential) =>
          Auth.hmacVerify(Redacted.value(secret), route.repo, credential),
        );
        if (denied !== null) return HttpServerResponse.raw(denied);

        const response = yield* repos.getByName(route.repo).fetch(raw);
        return HttpServerResponse.raw(response);
      }),
    };
    // The host Worker's layer also provides its DO's live implementation —
    // that is what makes `yield* Repo` above resolve.
  }).pipe(Effect.provide(Repos)),
);
