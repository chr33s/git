/**
 * Cloudflare host: the repository as an alchemy-native Durable Object.
 *
 * `git/Durable.ts` is the same server on the wrangler path — one DO per
 * repository, `Protocol.handle` and `Api.layer` inside it. The difference is
 * where the bindings come from: there, `wrangler.json` plus a generated `Env`
 * interface; here, the R2 bucket is a value (`alchemy.run.ts`'s `Objects`)
 * and the binding, its type and the migration all follow from that.
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
import * as GitRepository from "../git/Repository.ts";
import type { Repository } from "../git/Repository.ts";
import * as Api from "../server/Api.ts";
import * as Protocol from "../server/Protocol.ts";
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

      const live = (repo: string): Layer.Layer<Repository> => {
        const existing = layers.get(repo);
        if (existing !== undefined) return existing;
        const built = GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provide(stores({ bucket: r2, repo, storage: state.raw.storage })),
        );
        layers.set(repo, built);
        return built;
      };

      const api = (repo: string) => {
        const existing = handlers.get(repo);
        if (existing !== undefined) return existing;
        const built = HttpRouter.toWebHandler(Api.layer.pipe(Layer.provideMerge(live(repo))), {
          disableLogger: true,
        }).handler;
        handlers.set(repo, built);
        return built;
      };

      return {
        fetch: (request: Request) =>
          Effect.suspend(() => {
            const [, repo = "default", route = ""] = new URL(request.url).pathname.split("/");

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
  }),
);
