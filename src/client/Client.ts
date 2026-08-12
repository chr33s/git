/**
 * Browser client.
 *
 * Two halves, neither written twice. The remote half derives from the same
 * `HttpApi` value the server implements (`server/Api.ts`), so payloads and
 * the tagged error union cannot drift. The local half is the *same*
 * `Repository` service the server runs, over whichever stores the
 * environment provides — OPFS in a browser (`adapters/Opfs.ts`), anything
 * else in a test.
 *
 * Everything here — including the re-exported smart-HTTP `fetchRepository`
 * — is platform-neutral: `Pack.ts` sits on `git/Inflate.ts` and `git/Sha1.ts`
 * rather than `node:*`, and `Browser.test.ts` clones inside real Chromium to
 * prove it.
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
