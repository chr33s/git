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
import type { RuntimeContext } from "alchemy";
import * as Alchemy from "alchemy/Cloudflare";
import { Effect, Layer } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http";

import { objectStore, packStore } from "./git/Cloudflare.ts";
import * as GitRepository from "./git/Repository.ts";
import { Storage } from "./git/Store.ts";
import Repos from "./host/Cloudflare.ts";
import { Repo } from "./host/Cloudflare.ts";
import { normalize, routeOf } from "./server/Route.ts";
import * as Snapshot from "./server/Snapshot.ts";
import { Objects } from "./objects.ts";

/**
 * The Worker's public contract: it serves HTTP, nothing more.
 *
 * `RuntimeContext` is in the requirements because the stateless read path
 * resolves the R2 binding per request (`bucket.raw` is handler-coloured by
 * design); the bridge provides it, exactly as it does inside the DO.
 */
export type GitBindings = {
  readonly fetch: Effect.Effect<
    HttpServerResponse.HttpServerResponse,
    never,
    HttpServerRequest.HttpServerRequest | RuntimeContext
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
    // The Vite+ UI build (`vp build` → `dist/ui`) rides along as the
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
    const bucket = yield* Alchemy.R2.ReadWriteBucket(Objects);

    /**
     * Serve an anonymous `git-upload-pack` read from R2 alone, or `null`.
     *
     * The refs come from the snapshot the Durable Object publishes before it
     * acknowledges any ref-moving request (`Snapshot.ts`); the objects and
     * packs are already in R2. Between them this Worker can answer the
     * advertisement and cut the pack without ever waking the repository's
     * single writer — which is where clone traffic at agent scale stops
     * being the writer's problem. Anything this path cannot answer — no
     * snapshot yet, restricted reads, a credential, a write, any failure at
     * all — falls through to the Durable Object, which is always right.
     */
    const fromSnapshot = (
      r2: R2Bucket,
      repo: string,
      request: Request,
    ): Effect.Effect<Response | null> =>
      Effect.gen(function* () {
        const held = yield* Effect.promise(() => r2.get(Snapshot.keyOf(repo)).catch(() => null));
        if (held === null) return null;
        const bytes = yield* Effect.promise(() =>
          held
            .arrayBuffer()
            .then((buffer) => new Uint8Array(buffer))
            .catch((): Uint8Array | null => null),
        );
        if (bytes === null) return null;
        const snapshot = Snapshot.decode(bytes);
        if (snapshot === null || !snapshot.anonymousRead) return null;

        const stores = Layer.mergeAll(
          objectStore(r2, repo),
          Snapshot.refStore(snapshot),
          Layer.succeed(Storage)(repo),
        ).pipe(Layer.provideMerge(packStore(r2, repo)));
        return yield* Snapshot.serve(request).pipe(
          Effect.provide(
            GitRepository.layer.pipe(
              Layer.provide(GitRepository.hooksNoop),
              Layer.provideMerge(stores),
            ),
          ),
          // A read this path cannot complete is not an error the client
          // sees; it is a read the writer serves instead.
          Effect.orElseSucceed((): Response | null => null),
        );
      }).pipe(Effect.catchCause(() => Effect.succeed(null)));

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

        // Anonymous clone and fetch traffic is served from R2 when the
        // repository's published snapshot allows it; see `fromSnapshot`.
        if (Snapshot.readable(raw)) {
          // SAFETY: Alchemy's R2 binding and the generated worker runtime
          // types disagree but describe the same runtime object — the same
          // seam `host/Cloudflare.ts` crosses, in the same single place.
          const r2: R2Bucket = (yield* bucket.raw) as never;
          const served = yield* fromSnapshot(r2, route.repo, raw);
          if (served !== null) return HttpServerResponse.raw(served);
        }

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
    // that is what makes `yield* Repo` above resolve — and the bucket binding
    // implementation serves both phases, exactly as it does inside the DO.
  }).pipe(Effect.provide([Repos, Alchemy.R2.ReadWriteBucketBinding])),
);
