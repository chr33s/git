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
import { FetchHttpClient } from "effect/unstable/http";
import { AtomHttpApi } from "effect/unstable/reactivity";

import { api } from "../src/server/Api.ts";

/**
 * Where the API lives, read from the page exactly as `ui/api.ts` reads it:
 *   <meta name="gp-api-base" content="https://git.example.com">
 * An empty or absent base means same-origin, which is the deployed case.
 */
const base = (): string | undefined => {
  const content = document
    .querySelector('meta[name="gp-api-base"]')
    ?.getAttribute("content")
    ?.replace(/\/$/, "");
  return content === undefined || content === "" ? undefined : content;
};

/** The repository the page is about, from `<meta name="gp-repo">`. */
export const repoFromDocument = (): string =>
  document.querySelector('meta[name="gp-repo"]')?.getAttribute("content") ?? "core";

export class GitPlusApi extends AtomHttpApi.Service<GitPlusApi>()("GitPlusApi", {
  api,
  httpClient: FetchHttpClient.layer,
  baseUrl: base(),
}) {}
