/**
 * Browser client: two halves, neither written twice. The remote half derives
 * from the same `HttpApi` the server implements, so payloads and the error
 * union cannot drift; the local half is the same `Repository` service the
 * server runs, over whichever stores the environment provides.
 *
 * Platform-neutral throughout — `Pack.ts` sits on `git/Inflate.ts` and
 * `git/Sha1.ts`, not `node:*`, which is what lets `Browser.test.ts` clone
 * inside real Chromium.
 */
import { Effect, Layer } from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/unstable/http";
import { HttpApiClient } from "effect/unstable/httpapi";

import { noPacks, type PackStore } from "../git/Packed.ts";
import * as GitRepository from "../git/Repository.ts";
import type { Repository } from "../git/Repository.ts";
import type { ObjectStore, RefStore } from "../git/Store.ts";
import * as Api from "../server/Api.ts";
import { fetchRepository, lsRemote } from "./Fetch.ts";

export { fetchRepository, lsRemote };

/**
 * A typed client for the JSON API — methods, payloads and errors all derived
 * from the server's own declaration. `token` rides as a Bearer header on
 * every request.
 */
export const remote = (baseUrl: string, options?: { readonly token?: string }) => {
  const token = options?.token;
  // `make` documents `transformClient` as optionally `undefined` and treats
  // that the same as leaving it out, so an anonymous client passes no
  // transform rather than a transform that does nothing.
  const withAuthorization =
    token === undefined
      ? undefined
      : (client: HttpClient.HttpClient) =>
          client.pipe(
            HttpClient.mapRequest(HttpClientRequest.setHeader("authorization", `Bearer ${token}`)),
          );
  return HttpApiClient.make(Api.api, { baseUrl, transformClient: withAuthorization }).pipe(
    Effect.provide(FetchHttpClient.layer),
  );
};

/**
 * The local repository over the given stores: in a browser,
 * `Opfs.stores(await navigator.storage.getDirectory())`.
 */
export const local = (
  stores: Layer.Layer<ObjectStore | RefStore>,
  /**
   * Where packs live, when they do. `Opfs.stores` carries its own (none);
   * a caller passing bare stores gets none, which makes reads loose-only and
   * a repack unavailable rather than wrong.
   */
  packs: Layer.Layer<PackStore> = noPacks,
): Layer.Layer<Repository> =>
  GitRepository.layer.pipe(
    Layer.provide(GitRepository.hooksNoop),
    Layer.provide(stores),
    Layer.provide(packs),
  );
