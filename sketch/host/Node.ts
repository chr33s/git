/**
 * Node / Bun host.
 *
 * This is the file that proves the seam is real: the same `server/App.ts` that
 * a Durable Object serves, served instead from `node:http` over a directory on
 * disk. Nothing in `server/` or `git/` changes.
 *
 * It is also immediately useful, independent of any provider story:
 *
 *   - `npm run dev` stops needing `wrangler dev` on port 8080, and so does the
 *     e2e suite (`test.helpers.ts` spawns one today);
 *   - self-hosting `@chr33s/git` on a box or in a container becomes a supported
 *     shape rather than a fork;
 *   - the CLI can run the server in-process for offline work.
 */
import * as Http from "alchemy/Http";
import { Effect, Layer, PartitionedSemaphore, RcMap } from "effect";
import { FileSystem, Path } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { node as nodeStores } from "../adapters/Local.ts";
import * as App from "../server/App.ts";
import { RepoHost } from "./Host.ts";

/**
 * What the Durable Object got for free has to be built here.
 *
 * Isolation is a directory per repo. Serialization is a `PartitionedSemaphore`
 * keyed by repo — one permit per partition is exactly "the DO input gate, in a
 * process", and it is a core data structure rather than a `Map` of locks
 * maintained by hand. A read-heavy deployment would raise the permit count and
 * take two for writers; the port is narrow enough that this stays local.
 */
export const layer = (root: string): Layer.Layer<RepoHost, never, FileSystem.FileSystem | Path.Path> =>
  Layer.effect(
    RepoHost,
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const platform = yield* Effect.context<FileSystem.FileSystem | Path.Path>();
      const locks = yield* PartitionedSemaphore.make<string>({ permits: 1 });

      return RepoHost.of({
        stores: (name) =>
          nodeStores(path.join(root, name)).pipe(Layer.provide(Layer.succeedContext(platform))),
        serialize: (name, effect) => PartitionedSemaphore.withPermits(locks, name, 1)(effect),
        // No `waitUntil` to hand it to: the process is the lifetime, so the
        // fiber is detached to the global scope rather than the request's.
        background: (effect) => Effect.forkDetach(effect.pipe(Effect.ignore)).pipe(Effect.asVoid),
      });
    }),
  );

/**
 * `Http.serve` is alchemy's portable server entry — the same `HttpEffect` shape
 * a Worker serves. Swap `Http.NodeHttpServer()` for `Http.BunHttpServer()` and
 * nothing else moves.
 *
 * The body of it is the whole difference between this host and Cloudflare's:
 * the Worker gets "one instance per repo, requests to it serialized" from the
 * platform, and here it is a `Map` and a `Semaphore`.
 */
export const main = (root: string) =>
  Effect.gen(function* () {
    const host = yield* RepoHost;

    /**
     * App instances, reference counted and keyed by repo.
     *
     * `RcMap` is doing real work here rather than being decoration: it builds
     * an instance on first use, shares it across concurrent requests, and
     * disposes it — closing the router scope — once nothing has referenced it
     * for a minute. A hand-rolled `Map` leaks every repository ever touched,
     * which on a Worker is invisible because the platform evicts idle DOs for
     * you.
     */
    const apps = yield* RcMap.make({
      lookup: (name: string) =>
        Effect.acquireRelease(
          Effect.sync(() =>
            App.forRepo(
              Layer.mergeAll(
                Layer.succeed(RepoHost, host),
                host.stores(name),
                platform,
                subscribers,
              ),
            ),
          ),
          (app) => Effect.promise(app.dispose),
        ),
      idleTimeToLive: "1 minute",
    });

    return yield* Http.serve(
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const name = App.repoName(request.url);
        if (name === undefined) {
          return HttpServerResponse.text("No repository in URL", { status: 400 });
        }

        const app = yield* RcMap.get(apps, name);
        return yield* host.serialize(
          name,
          Effect.promise(() => app.handler(toRequest(request))).pipe(
            Effect.map(HttpServerResponse.raw),
          ),
        );
      }),
    );
  }).pipe(
    Effect.scoped,
    Effect.provide(layer(root)),
    Effect.provide(Http.NodeHttpServer()),
    Effect.provide(platform),
  );

/** Response services + an in-process subscription store. */
declare const platform: Layer.Layer<
  Exclude<App.Env, RepoHost | import("../git/Store.ts").ServerStores>
>;
declare const subscribers: Layer.Layer<import("../server/Webhooks.ts").Subscribers>;
/** node's `HttpServerRequest` already wraps one; this is the unwrap. */
declare const toRequest: (request: HttpServerRequest.HttpServerRequest) => Request;
