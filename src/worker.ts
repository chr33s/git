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
import { Effect } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http";

import Repos from "./host/Cloudflare.ts";
import { Repo } from "./host/Cloudflare.ts";
import { normalize, routeOf } from "./server/Route.ts";

/** The Worker's public contract: it serves HTTP, nothing more. */
export type GitBindings = {
  readonly fetch: Effect.Effect<
    HttpServerResponse.HttpServerResponse,
    never,
    HttpServerRequest.HttpServerRequest
  >;
};

/** `Repo` in the third slot: this Worker hosts the DO, so it is contract. */
export class Git extends Alchemy.Worker<Git, GitBindings, Repo>()("git") {}

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
    // The compiled UI (`node ui/build.ts` → `dist/ui`) rides along as the
    // Worker's static assets, so `/` serves the page and the page's requests
    // to `/:repo/...` stay same-origin — the arrangement the UI's readme
    // promises. Routing is assets-first: a request matching a file (the
    // entry page, `main.js`, a hashed chunk) is answered by the asset layer
    // and never invokes this script; everything else — every repository
    // route — falls through to the router below. Cache rules ship as
    // `dist/ui/_headers`, written by the build. The build fails if the
    // entry outputs are missing, so a deploy cannot publish the API with no
    // UI behind it; a repository whose *name* collides with an asset file
    // would be shadowed, which the path shapes make implausible (assets live
    // at the root and repository routes always carry a second segment).
    assets: "dist/ui",
  },
  Effect.gen(function* () {
    const repos = yield* Repo;

    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const route = routeOf(new URL(request.url, "http://x").pathname);
        if (route === null) {
          // The asset layer already served everything that matches a file,
          // `/` included — so a path that names neither an asset nor a
          // repository is simply not found, not a malformed API call.
          return HttpServerResponse.text("not found", { status: 404 });
        }

        // SAFETY: the effect wrapper was built from the platform request
        // (`HttpServerRequest.fromWeb`), so `source` is that same `Request`,
        // headers and body intact — not a reconstruction. Normalised so the
        // DO sees one spelling of the path whichever the client used.
        const raw = normalize(request.source as Request, route);

        // Auth no longer lives at the edge. It used to, because a shared
        // secret is something an edge can hold; repository authority is not —
        // it is the genesis and membership log inside the repository, which
        // only the Durable Object can read. So the guard moved in there, and
        // this Worker is a router again.

        // A client that hangs up mid-clone is not a server error. 499 is
        // nginx's code for it, and it keeps aborted fetches out of the 5xx
        // rate that pages someone.
        // The stub's `fetch` speaks the effect request and response, which is
        // what the Durable Object bridge on the other side hands its handler;
        // passing the platform `Request` here would leave the DO rebuilding a
        // request it cannot read a body from.
        return yield* repos
          .getByName(route.repo)
          .fetch(HttpServerRequest.fromWeb(raw))
          .pipe(
            Effect.catchCause((cause) =>
              raw.signal.aborted
                ? Effect.succeed(HttpServerResponse.empty({ status: 499 }))
                : // The contract says this Worker does not fail: a fault
                  // inside the DO is a 500 at the edge, not a rejected effect.
                  Effect.as(Effect.logError(cause), HttpServerResponse.empty({ status: 500 })),
            ),
          );
      }),
    };
    // The host Worker's layer also provides its DO's live implementation —
    // that is what makes `yield* Repo` above resolve.
  }).pipe(Effect.provide(Repos)),
);
