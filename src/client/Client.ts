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
export const remote = (baseUrl: string, options?: { readonly token?: string }) =>
  HttpApiClient.make(Api.api, {
    baseUrl,
    ...(options?.token === undefined
      ? {}
      : {
          transformClient: (client: HttpClient.HttpClient) =>
            client.pipe(
              HttpClient.mapRequest(
                HttpClientRequest.setHeader("authorization", `Bearer ${options.token}`),
              ),
            ),
        }),
  }).pipe(Effect.provide(FetchHttpClient.layer));

/**
 * The local repository over the given stores: in a browser,
 * `Opfs.stores(await navigator.storage.getDirectory())`.
 */
export const local = (stores: Layer.Layer<ObjectStore | RefStore>): Layer.Layer<Repository> =>
  GitRepository.layer.pipe(Layer.provide(GitRepository.hooksNoop), Layer.provide(stores));
