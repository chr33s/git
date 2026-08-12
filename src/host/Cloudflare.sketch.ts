/**
 * Cloudflare host: `Alchemy.Worker` in front, `Alchemy.DurableObject` per repo.
 *
 * `Alchemy` here is `alchemy/Cloudflare` — `Worker` and `DurableObject` are
 * Cloudflare resources, not provider-neutral ones (see `Host.ts`). Confining
 * them to this module is what keeps the rest of the sketch portable.
 *
 * Today `Server extends DurableObject<Env>` (`src/server.ts`, 1,130 lines)
 * constructs `Storage`, `GitRepository`, `ServerApi`, `ServerLfs`,
 * `ServerWebhooks` and a `HookRunner` in its constructor, wires webhooks to
 * hooks by hand, and dispatches with a `URLPattern` table. Every dependency is
 * `new`-ed at exactly one place and cannot be substituted in a test without
 * standing up a DO.
 *
 * Sketch: the DO is a host, not an application. It supplies isolation,
 * serialization and background work; the application it serves is
 * `server/App.ts`, unchanged from what runs under node.
 */
import * as Alchemy from "alchemy/Cloudflare";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { type Context, Effect, FileSystem, Layer, Path } from "effect";
import { Etag, HttpPlatform } from "effect/unstable/http";
import { objectStoreLayer, refStoreLayer } from "../adapters/Cloudflare.sketch.ts";
import * as App from "../server/App.sketch.ts";
import type * as Webhooks from "../server/Webhooks.sketch.ts";
import { RepoHost } from "./Host.sketch.ts";

/** The DO's public shape: one method, because the router does the dispatch. */
export interface RepoShape {
  readonly fetch: (request: Request) => Effect.Effect<Response>;
}

export class Repo extends Alchemy.DurableObject<Repo, RepoShape>()("Repo") {}

/**
 * Inside a DO the instance *is* the repository, and the input gate already
 * serializes every call to it — so `serialize` is the identity here. That is
 * the guarantee the other hosts have to reproduce, and the reason it is named
 * in the port rather than assumed.
 */
const hostLayer = Layer.effect(
  RepoHost,
  Effect.gen(function* () {
    const state = yield* Alchemy.DurableObjectState;
    return RepoHost.of({
      stores: storesFor,
      serialize: (_repo, effect) => effect,
      background: (effect) =>
        state.waitUntil(effect).pipe(Effect.provideContext(runtime), Effect.ignore),
    });
  }),
);

export default Repo.make(
  Effect.gen(function* () {
    // init — runs once per instance
    return Effect.gen(function* () {
      // runtime — has DurableObjectState and the invocation's RuntimeContext
      const state = yield* Alchemy.DurableObjectState;
      const repo = state.id.name ?? state.id.toString();

      const { handler } = App.forRepo(
        Layer.mergeAll(hostLayer, subscribers, storesFor(repo), platform).pipe(
          Layer.provide(Layer.succeedContext(runtime)),
        ),
      );

      return {
        /** Smart-HTTP, JSON API, LFS and OpenAPI all come out of this one handler. */
        fetch: (request: Request) => Effect.promise(() => handler(request)),
      } satisfies RepoShape;
    });
  }),
);

/**
 * The binding layer and the invocation context are the host's business; what
 * comes out is a plain `Layer<ServerStores>` the app can provide.
 *
 * `bindings` is `ReadWriteBucket(Objects)` from `alchemy.run.ts` — one call
 * wires the R2 binding, the env var and the typed client.
 */
const storesFor = (repo: string) =>
  Layer.mergeAll(objectStoreLayer(repo), refStoreLayer(repo)).pipe(
    Layer.provide(bindings),
    Layer.provide(Layer.succeedContext(runtime)),
  );

declare const runtime: Context.Context<RuntimeContext | Alchemy.DurableObjectState>;

/**
 * The response-building services `HttpApiBuilder` asks for. A Worker has no
 * filesystem, so `FileSystem.layerNoop({})` stands in — nothing in this server
 * serves a file off disk, and the type system is the thing that made that
 * assumption explicit instead of implicit.
 */
const platform = Layer.mergeAll(HttpPlatform.layer, Etag.layer, Path.layer).pipe(
  Layer.provideMerge(FileSystem.layerNoop({})),
);
declare const bindings: Layer.Layer<Alchemy.R2.ReadWriteBucket>;
/** Webhook subscriptions, read from the same DO SQLite database as the refs. */
declare const subscribers: Layer.Layer<Webhooks.Subscribers>;

/**
 * Cancellation, which today is `request.signal?.throwIfAborted()` at two entry
 * points and nothing below them: an aborted clone keeps walking objects until
 * it finishes. Under Effect the handler runs on a fiber tied to the request —
 * when the client hangs up, the object walk, the R2 reads and the SQL cursor
 * are interrupted together, and any `Scope` releases in order.
 *
 * Background work that must outlive the response goes through
 * `RepoHost.background`, which is `state.waitUntil` here and `forkDaemon` under
 * node — the caller in `server/Webhooks.ts` does not know the difference.
 */
