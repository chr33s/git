/**
 * Cloudflare host: the repository as an alchemy-native Durable Object.
 *
 * `git/Durable.ts` is the same server under wrangler's test harness — one DO
 * per repository, `Protocol.handle` and `Api.layer` inside it. The difference
 * is where the bindings come from: there, `wrangler.test.json` plus a
 * generated `Env` interface; here, the R2 bucket is a value (`objects.ts`'s
 * `Objects`) and the binding, its type and the migration all follow from it.
 *
 * `DurableObjectState.storage` is the raw `DurableObjectStorage`, so the
 * backend in `git/Cloudflare.ts` plugs straight in, unchanged — the whole
 * point of having kept the ports platform-shaped.
 */
import * as Alchemy from "alchemy/Cloudflare";
import { Effect, Layer } from "effect";
import { HttpRouter } from "effect/unstable/http";

import { stores } from "../git/Cloudflare.ts";
import { type GitError, statusOf } from "../git/Error.ts";
import type { Sql } from "../git/Sql.ts";
import * as GitRepository from "../git/Repository.ts";
import type { Repository } from "../git/Repository.ts";
import * as Api from "../server/Api.ts";
import * as Archive from "../server/Archive.ts";
import { r2 as lfsR2 } from "../server/Lfs.cloudflare.ts";
import * as LfsCore from "../server/Lfs.ts";
import * as Protocol from "../server/Protocol.ts";
import { normalize, routeOf } from "../server/Route.ts";
import * as Subscribers from "../server/Subscribers.ts";
import * as Webhooks from "../server/Webhooks.ts";
import { Objects } from "../objects.ts";

/** What other scripts may call on a repository: it is an HTTP surface. */
export interface RepoShape {
  readonly fetch: (request: Request) => Effect.Effect<Response>;
}

export class Repo extends Alchemy.DurableObject<Repo, RepoShape>()("Repo") {}

export default Repo.make(
  Effect.gen(function* () {
    // init: bind the bucket and resolve this instance's storage, once.
    const bucket = yield* Alchemy.R2.ReadWriteBucket(Objects);
    const state = yield* Alchemy.DurableObjectState;

    // The nested `Effect<Effect<…>>` is alchemy's DO contract, not a mistake:
    // the outer generator binds resources once per instance, the inner runs
    // per request with `RuntimeContext` available.
    // @effect-diagnostics-next-line returnEffectInGen:off
    return Effect.gen(function* () {
      // `raw` is RuntimeContext-coloured: available here, not at init. The
      // cast crosses one seam only — alchemy types bindings from
      // `@cloudflare/workers-types`, the backend from the generated worker
      // types, and they are the same object at runtime.
      const r2 = (yield* bucket.raw) as unknown as R2Bucket;

      /**
       * Built per instance, not per request: `Repository` is constructed
       * from the stores, so storage has to resolve when the layer is built.
       * That correspondence is why a repository maps onto a DO at all.
       */
      const layers = new Map<string, Layer.Layer<Repository>>();
      const handlers = new Map<string, (request: Request) => Promise<Response>>();

      /**
       * The subscriber registry on this instance's own SQLite, beside the
       * refs it reports on — and serialized by the same input gate.
       */
      const subscribers = (repo: string) =>
        Subscribers.sql(state.raw.storage.sql as unknown as Sql, repo);

      const live = (repo: string): Layer.Layer<Repository> => {
        const existing = layers.get(repo);
        if (existing !== undefined) return existing;
        const built = GitRepository.layer.pipe(
          // Delivery runs in `waitUntil`, so a slow receiver never adds its
          // latency to the push that triggered it.
          Layer.provide(
            Webhooks.hooksFetch({
              background: (effect) =>
                // `runPromiseWith` rather than `runPromise`: the delivery is
                // detached from this fiber, not from its services, and a
                // fresh runtime would drop the ones it was handed.
                Effect.context<never>().pipe(
                  Effect.map((context) => {
                    state.raw.waitUntil(Effect.runPromiseWith(context)(Effect.ignore(effect)));
                  }),
                ),
            }).pipe(Layer.provide(subscribers(repo))),
          ),
          Layer.provide(stores({ bucket: r2, repo, storage: state.raw.storage })),
        );
        layers.set(repo, built);
        return built;
      };

      const api = (repo: string) => {
        const existing = handlers.get(repo);
        if (existing !== undefined) return existing;
        const built = HttpRouter.toWebHandler(
          Api.layer.pipe(Layer.provideMerge(live(repo)), Layer.provideMerge(subscribers(repo))),
          { disableLogger: true },
        ).handler;
        handlers.set(repo, built);
        return built;
      };

      return {
        fetch: (request: Request) =>
          Effect.suspend(() => {
            const matched = routeOf(new URL(request.url).pathname);
            if (matched === null) {
              return Effect.succeed(Response.json({ error: "Invalid" }, { status: 400 }));
            }
            const { repo, route } = matched;
            request = normalize(request, matched);

            // LFS shares the `info/` prefix with the advertisement, so it is
            // tried first; its bodies are the large ones.
            if (route === "info" && matched.rest.includes("/lfs/")) {
              return LfsCore.handle(request).pipe(
                Effect.map(
                  (response) => response ?? Response.json({ error: "NotFound" }, { status: 404 }),
                ),
                Effect.provide(lfsR2({ bucket: r2, repo })),
              );
            }

            if (route === "archive") {
              return Archive.handle(request).pipe(
                Effect.map(
                  (response) => response ?? Response.json({ error: "NotFound" }, { status: 404 }),
                ),
                Effect.catch((error: GitError) =>
                  Effect.succeed(Response.json({ _tag: error._tag }, { status: statusOf(error) })),
                ),
                Effect.provide(live(repo)),
              );
            }

            if (route === "info" || route === "git-upload-pack" || route === "git-receive-pack") {
              return Protocol.handle(request).pipe(
                Effect.map(
                  (response) => response ?? Response.json({ error: "NotFound" }, { status: 404 }),
                ),
                Effect.catch((error: GitError) =>
                  Effect.succeed(Response.json({ _tag: error._tag }, { status: statusOf(error) })),
                ),
                Effect.provide(live(repo)),
              );
            }

            return Effect.promise(() => api(repo)(request));
          }),
      };
    });
    // One binding implementation serves both phases: at deploy time it
    // registers `r2_bucket` on the host Worker, at runtime it builds the
    // client. Its own requirements are absorbed by `.make()`.
  }).pipe(Effect.provide(Alchemy.R2.ReadWriteBucketBinding)),
);
