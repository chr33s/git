/**
 * The per-repository Durable Object.
 *
 * Today `Server extends DurableObject<Env>` (`src/server.ts`, 1,130 lines)
 * constructs `Storage`, `GitRepository`, `ServerApi`, `ServerLfs`,
 * `ServerWebhooks` and a `HookRunner` in its constructor, wires webhooks to
 * hooks by hand, and dispatches with a `URLPattern` table. Every dependency is
 * `new`-ed at exactly one place and cannot be substituted in a test without
 * standing up a DO.
 *
 * Sketch: the DO's init block builds the layer graph; the runtime block is a
 * web handler. Same object model, but the wiring is a value — so the identical
 * program runs in a test over in-memory layers, in the browser over OPFS, and
 * in the DO over SQLite + R2.
 */
import * as Cloudflare from "alchemy/Cloudflare";
import { Effect, Layer } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { objectStoreLayer, refStoreLayer } from "../adapters/Cloudflare.ts";
import * as GitRepository from "../git/Repository.ts";
import * as Api from "./Api.ts";
import * as Protocol from "./Protocol.ts";

/** The DO's public shape: one method, because the router does the dispatch. */
export interface RepoShape {
  readonly fetch: (request: Request) => Effect.Effect<Response>;
}

export class Repo extends Cloudflare.DurableObject<Repo, RepoShape>()("Repo") {}

export default Repo.make(
  Effect.gen(function* () {
    // init — runs once per instance
    return Effect.gen(function* () {
      // runtime — has DurableObjectState and the invocation's RuntimeContext
      const state = yield* Cloudflare.DurableObjectState;
      const repo = state.id.name ?? state.id.toString();

      // One layer graph. `Layer` memoizes shared dependencies, so
      // `Repository`, the API handlers and the protocol handlers all see the
      // same `RefStore` without anyone passing it around.
      const live = Layer.mergeAll(Api.layer, Protocol.routes).pipe(
        Layer.provideMerge(GitRepository.layer),
        Layer.provideMerge(Layer.mergeAll(objectStoreLayer(repo), refStoreLayer(repo))),
      );

      const { handler } = HttpRouter.toWebHandler(live as never);

      return {
        /** Smart-HTTP, JSON API, LFS and OpenAPI all come out of this one handler. */
        fetch: (request: Request) => Effect.promise(() => handler(request)),
      } satisfies RepoShape;
    });
  }),
);

/**
 * Cancellation, which today is `request.signal?.throwIfAborted()` at two entry
 * points and nothing below them: an aborted clone keeps walking objects until
 * it finishes. Under Effect the handler runs on a fiber tied to the request —
 * when the client hangs up, the object walk, the R2 reads and the SQL cursor
 * are interrupted together, and any `Scope` releases in order.
 *
 * Background work that must outlive the response (webhook delivery) is
 * explicitly `state.waitUntil(...)`-ed instead, which is a promise the platform
 * already understands.
 */
