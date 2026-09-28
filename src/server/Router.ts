/**
 * The routes that are not the JSON API, on the same `HttpRouter`.
 *
 * Smart-HTTP, LFS, bulk commits and archives answer on raw bodies — a pack in,
 * a pack or a tarball out — so they stay web handlers rather than `HttpApi`
 * endpoints with schemas. They used to be tried one after another by each
 * host before it fell through to the API router; registered here, every host
 * mounts one router and the path table lives in one place.
 *
 * Paths are the normalised ones (`server/Route.ts`): `/:repo/...`, suffix
 * already stripped. Matching never reads a body, so a large push or LFS upload
 * reaches its handler as the stream it arrived as.
 */
import { Context, Effect, Layer } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";

import { type GitError, statusOf } from "../git/Error.ts";
import type { Repository } from "../git/Repository.ts";
import type * as Api from "./Api.ts";
import * as Archive from "./Archive.ts";
import type * as Auth from "./Auth.ts";
import * as CommitPack from "./CommitPack.ts";
import * as Lfs from "./Lfs.ts";
import * as Protocol from "./Protocol.ts";
import { MAX_SEGMENT } from "./Route.ts";
import type { Subscribers } from "./Subscribers.ts";

const notFound = () => Response.json({ _tag: "NotFound" }, { status: 404 });

const failed = (error: GitError) =>
  Effect.succeed(Response.json({ _tag: error._tag }, { status: statusOf(error) }));

/**
 * One of the web handlers, as a route handler.
 *
 * `null` from a handler means the path matched but the request is not one it
 * serves (the dumb protocol, an archive format it does not write): 404, as the
 * router would have answered had the route not matched at all.
 */
const web = <R>(handle: (request: Request) => Effect.Effect<Response | null, never, R>) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest.pipe(
      Effect.flatMap(HttpServerRequest.toWeb),
      // Unreachable: `toWeb` returns the platform `Request` the request was
      // made from unchanged, and every host builds this router with
      // `toWebHandler`, which only ever makes requests from one.
      Effect.orDie,
    );
    return HttpServerResponse.raw((yield* handle(request)) ?? notFound());
  });

/** `/:repo/info/refs`, `git-upload-pack`, `git-receive-pack`. */
const protocol = web((request) => Protocol.handle(request).pipe(Effect.catch(failed)));

/**
 * The writes run to completion once they have started.
 *
 * A receive-pack or commit-pack interrupted after its objects landed would
 * leave some refs moved and others not, or objects no ref reaches. A client
 * that goes away mid-upload still ends the write: its body stream fails, and
 * the handler answers that failure like any other.
 */
const whole = { uninterruptible: true } as const;

export const layer = Layer.mergeAll(
  // Matched on the whole path, so sharing the `info/` prefix with the
  // advertisement needs no ordering rule. Every method, so an unsupported
  // one is LFS's own 405.
  HttpRouter.add("*", "/:repo/info/lfs/objects/*", web(Lfs.handle)),
  HttpRouter.add("GET", "/:repo/info/refs", protocol),
  HttpRouter.add("POST", "/:repo/git-upload-pack", protocol),
  HttpRouter.add("POST", "/:repo/git-receive-pack", protocol, whole),
  // Every method, so a GET is told it needs POST rather than not found.
  HttpRouter.add("*", "/:repo/commit-pack", web(CommitPack.handle), whole),
  HttpRouter.add(
    "*",
    "/:repo/archive/:name",
    web((request) => Archive.handle(request).pipe(Effect.catch(failed))),
  ),
);

/** What a host provides the router: one repository's services. */
export type Services = Repository | Lfs.LfsStore | Subscribers | Auth.AnonymousWrites;

/**
 * One repository's router — the JSON API and the routes above — as a web
 * handler.
 *
 * Built once per repository, not per request. The requester stays *out* of
 * the graph and arrives as a per-call context instead: a router built per call
 * rebuilds the whole handler tree and opens a `Scope` nobody closes, and one
 * built with the requester baked in would answer every later request as
 * whoever made the first.
 */
export const handler = <E, P = never>(options: {
  /** `Api.layer(...)`, built by the host so it names its resolver. */
  readonly api: ReturnType<typeof Api.layer>;
  /** `provideMerge`d under both, since route requirements are request-scoped. */
  readonly services: Layer.Layer<Services, E>;
  /** Services only the smart-HTTP and LFS routes see, never the JSON API. */
  readonly protocol?: Layer.Layer<P> | undefined;
  /** Host-specific routes beside these. */
  readonly routes?: Layer.Layer<never, never, HttpRouter.HttpRouter> | undefined;
}) => {
  const router = HttpRouter.toWebHandler(
    Layer.mergeAll(
      options.api,
      options.protocol === undefined
        ? layer
        : layer.pipe(HttpRouter.provideRequest(options.protocol)),
      options.routes ?? Layer.empty,
    ).pipe(Layer.provideMerge(options.services)),
    {
      disableLogger: true,
      // A repository name, or an archive's file name, is one path parameter,
      // and the router's default refuses any longer than 100 characters.
      routerConfig: { maxParamLength: MAX_SEGMENT },
      middleware: (effect) =>
        Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest;
          // Layer construction may finish after the request was aborted,
          // before the web handler registered its abort listener.
          if (request.source instanceof Request && request.source.signal.aborted) {
            return yield* Effect.interrupt;
          }
          return yield* effect;
        }),
    },
  );
  return {
    handle: (request: Request, requester: Context.Context<Auth.Requester>): Promise<Response> =>
      // SAFETY: the handler's generated declaration erases its remaining
      // request-scoped service to `unknown`; this context contains exactly
      // that `Requester` service and no value is inspected through the cast.
      router.handler(request, requester as Context.Context<unknown>),
    dispose: router.dispose,
  };
};
