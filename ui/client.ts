/**
 * The API client, derived instead of written.
 *
 * `src/server/Api.ts` declares every endpoint — path, method, payload,
 * success and error schemas — as one `HttpApi` value, and the server's own
 * handlers are checked against it. This module derives the browser's client
 * from that same value, so there is no second statement of those facts to
 * drift: rename a query parameter in the declaration and every call site
 * here fails to compile rather than answering 400 at runtime.
 *
 * `AtomHttpApi.Service` wraps the derived client in reactivity: `query`
 * returns an atom tracking an endpoint's `AsyncResult` (initial → waiting →
 * success/failure), memoized per request, and `mutation` returns a writable
 * atom that runs the call and can invalidate reactivity keys. Screens
 * subscribe through `ui/atoms.ts`.
 *
 * The hand-written `ui/api.ts` remains beside this for the screens that
 * predate it; new surfaces (the hub) start here. The two share the schemas
 * in `src/server/ApiContract.ts` either way.
 */
import { Layer } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { AtomHttpApi } from "effect/unstable/reactivity";

import { api } from "../src/server/Api.ts";

/**
 * Where the API lives, read from the page exactly as `ui/api.ts` reads it:
 *   <meta name="gp-api-base" content="https://git.example.com">
 * An empty or absent base means same-origin, which is the deployed case.
 */
export const apiBase = (): string | undefined => {
  const content = document
    .querySelector('meta[name="gp-api-base"]')
    ?.getAttribute("content")
    ?.replace(/\/$/, "");
  return content === undefined || content === "" ? undefined : content;
};

/** The repository the page is about, from `<meta name="gp-repo">`. */
export const repoFromDocument = (): string =>
  document.querySelector('meta[name="gp-repo"]')?.getAttribute("content") ?? "core";

/**
 * `fetch`, taught the native challenge: a 401 with a `Hub-SSH-v1` nonce is
 * retried once under a signed envelope from the browser's key (`identity.ts`,
 * loaded lazily so anonymous pages never pay for it). This is the same rule
 * `ui/api.ts` applies in `#json`, provided here as the transport under the
 * derived client — so the hub's reads and writes answer a private
 * repository's challenge exactly as every other request does.
 */
const challengedFetch: typeof globalThis.fetch = async (input, init) => {
  const response = await globalThis.fetch(input, init);
  if (response.status !== 401) return response;
  const url =
    input instanceof URL ? input.toString() : input instanceof Request ? input.url : input;
  const retried = await import("./identity.ts")
    .then((identity) => identity.retryAuthorized(url, init ?? {}, response))
    .catch((): Response | null => null);
  return retried ?? response;
};

export class GitPlusApi extends AtomHttpApi.Service<GitPlusApi>()("GitPlusApi", {
  api,
  httpClient: FetchHttpClient.layer.pipe(
    Layer.provide(Layer.succeed(FetchHttpClient.Fetch)(challengedFetch)),
  ),
  baseUrl: apiBase(),
}) {}
