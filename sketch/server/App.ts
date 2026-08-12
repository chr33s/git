/**
 * The git server, as one host-neutral value.
 *
 * Everything the server does — smart-HTTP, the JSON API, OpenAPI — is built
 * from layers that name only the storage ports and `RepoHost`. There is no
 * `Cloudflare` import in this file, and that is the point: `host/Cloudflare.ts`
 * serves it from a Durable Object, `host/Node.ts` serves it from `node:http`,
 * and a test calls the handler directly.
 */
import { type FileSystem, Layer, type Path } from "effect";
import { type Etag, type HttpPlatform, HttpRouter } from "effect/unstable/http";
import * as GitRepository from "../git/Repository.ts";
import type { ServerStores } from "../git/Store.ts";
import type { RepoHost } from "../host/Host.ts";
import * as Api from "./Api.ts";
import * as Protocol from "./Protocol.ts";
import * as Webhooks from "./Webhooks.ts";

/** Routes + domain + hooks, with storage and the host still open. */
export const layer = Layer.mergeAll(Api.layer, Protocol.routes).pipe(
  Layer.provideMerge(GitRepository.layer),
  Layer.provideMerge(Webhooks.layer),
);

/** Everything a host has to close over for one repository. */
export type Env =
  | ServerStores
  | RepoHost
  | Webhooks.Subscribers
  // `HttpApiBuilder` asks for these to build responses; every host has them.
  | HttpPlatform.HttpPlatform
  | Etag.Generator
  | FileSystem.FileSystem
  | Path.Path;

/**
 * One app instance, bound to one repository.
 *
 * The binding is not a stylistic choice — `Repository` is *constructed* from
 * `ObjectStore` and `RefStore`, so the storage has to be resolved when the
 * layer is built, not when a request arrives. Which is precisely the Durable
 * Object model: an instance per repository, alive as long as it is used. That
 * correspondence is why this ports to Workers without a shim, and what the
 * other hosts have to reproduce:
 *
 *   - Cloudflare: one DO instance per repo, one of these inside it;
 *   - node: a cache of these keyed by repo name, plus a lock around dispatch;
 *   - a test: one of these over in-memory stores, no host at all.
 */
export const forRepo = (env: Layer.Layer<Env>) =>
  HttpRouter.toWebHandler(layer.pipe(Layer.provide(env)));

/** The repo name comes out of the path once, here, instead of at three layers. */
export const repoName = (url: string) =>
  new URL(url, "http://x").pathname.match(/^\/(?:api\/)?([a-z0-9-_.]+?)(?:\.git)?(?:\/|$)/)?.[1];
