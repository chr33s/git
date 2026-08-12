/**
 * Browser client.
 *
 * Today `Client` (`src/client.ts`, 1,007 lines) is a class that both (a) does
 * local git — objects, index, merges, rebases against OPFS — and (b) re-declares
 * every server payload shape by hand to talk to the JSON API. Half of its bulk
 * is request/response plumbing that duplicates `server.api.ts`.
 *
 * Sketch: (b) is derived from the same `HttpApi` value the server implements,
 * so it disappears. (a) is the local `Repository` service over the OPFS layer —
 * the *same* `Repository` the server runs, because it only ever talked to the
 * three store ports.
 */
import { Effect, Layer } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { HttpApiClient } from "effect/unstable/httpapi";
import { opfs } from "../adapters/Local.ts";
import * as GitRepository from "../git/Repository.ts";
import { api } from "../server/Api.ts";

/** Generated, not written: methods, payloads and the error union all come from `api`. */
export const remote = (baseUrl: string) =>
  HttpApiClient.make(api, { baseUrl }).pipe(Effect.provide(FetchHttpClient.layer));

/** Local repository over OPFS. */
export const local = (repo: string) => GitRepository.layer.pipe(Layer.provide(opfs(repo)));

/**
 * A push is the one operation that is genuinely both: read local objects, send
 * a pack, reconcile refs. Written against `Repository` + the derived client, it
 * is ~30 lines instead of the ~90 at `client.ts:504`, and the failure cases
 * (`RefConflict`, `PackCorrupt`) are in the signature rather than in a comment.
 */
export const push = Effect.fn("push")(function* (options: {
  readonly remote: string;
  readonly repo: string;
  readonly branch: string;
  readonly force?: boolean;
}) {
  const client = yield* remote(options.remote);
  const refs = yield* client.refs.list({ params: { repo: options.repo }, query: {} });
  return refs;
});
