/**
 * Cloudflare host: the repository as an alchemy-native Durable Object.
 *
 * `git/Durable.ts` is the same server under wrangler's test harness — one DO
 * per repository, `Protocol.handle` and `Api.layer` inside it. The difference
 * is where the bindings come from: there, `wrangler.test.json` plus a
 * generated `Env` interface; here, the R2 bucket is a value (`objects.ts`'s
 * `Objects`) and the binding, its type and the migration all follow from it.
 *
 * `DurableObjectState.storage` is the raw `DurableObjectStorage`, so the
 * backend in `git/Cloudflare.ts` plugs straight in, unchanged — the whole
 * point of having kept the ports platform-shaped.
 */
import * as Alchemy from "alchemy/Cloudflare";
import type * as Http from "alchemy/Http";
import { Config, Context, Effect, Layer } from "effect";
import {
  FetchHttpClient,
  HttpClient,
  HttpRouter,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/unstable/http";

import { stores } from "../git/Cloudflare.ts";
import { type GitError, statusOf, StorageFailure } from "../git/Error.ts";
import type { Sql } from "../git/Sql.ts";
import * as GitRepository from "../git/Repository.ts";
import type { Repository } from "../git/Repository.ts";
import * as Api from "../server/Api.ts";
import * as Auth from "../server/Auth.ts";
import * as Archive from "../server/Archive.ts";
import * as CommitPack from "../server/CommitPack.ts";
import { r2 as lfsR2 } from "../server/Lfs.cloudflare.ts";
import * as LfsCore from "../server/Lfs.ts";
import * as Policy from "../server/Policy.ts";
import * as Protocol from "../server/Protocol.ts";
import * as Snapshot from "../server/Snapshot.ts";
import { collects, normalize, routeOf, settledWithin } from "../server/Route.ts";
import * as Remotes from "../server/Remotes.ts";
import * as Sending from "../server/Sending.ts";
import * as Subscribers from "../server/Subscribers.ts";
import * as Webhooks from "../server/Webhooks.ts";
import { Objects } from "../objects.ts";

/**
 * What other scripts may call on a repository: it is an HTTP surface.
 *
 * An `HttpEffect` rather than a function of a `Request`, because that is what
 * alchemy's Durable Object bridge invokes — it hands the effect the request
 * through context (`makeRequestEffect(request, instance.fetch)`) instead of
 * calling it. A function here type-checks and then fails every request at the
 * edge, on the one path no test covers.
 */
export interface RepoBindings {
  readonly fetch: Http.HttpEffect;
}

export class Repo extends Alchemy.DurableObject<Repo, RepoBindings>()("Repo") {}

export default Repo.make(
  Effect.gen(function* () {
    // init: bind the bucket and resolve this instance's storage, once.
    const bucket = yield* Alchemy.R2.ReadWriteBucket(Objects);
    const state = yield* Alchemy.DurableObjectState;

    // The nested `Effect<Effect<…>>` is alchemy's DO contract, not a mistake:
    // the outer generator binds resources once per instance, the inner runs
    // per request with `RuntimeContext` available.
    // @effect-diagnostics-next-line returnEffectInGen:off
    return Effect.gen(function* () {
      // `raw` is RuntimeContext-coloured: available here, not at init.
      // SAFETY: Alchemy's R2 binding and the generated worker runtime types
      // disagree (their `Headers` differ) but describe the same runtime
      // object, so one conversion crosses that seam here and nowhere else.
      const r2: R2Bucket = (yield* bucket.raw) as never;

      /**
       * Built per instance, not per request: `Repository` is constructed
       * from the stores, so storage has to resolve when the layer is built.
       * That correspondence is why a repository maps onto a DO at all.
       */
      const layers = new Map<string, Layer.Layer<Repository>>();

      /**
       * The subscriber registry on this instance's own SQLite, beside the
       * refs it reports on — and serialized by the same input gate.
       */
      // SAFETY: workerd's `SqlStorage` satisfies the `Sql` port structurally
      // — same value domain, cursor with `toArray` — the declarations differ
      // only in Alchemy's wider `any[]` bindings parameter.
      const sql = state.raw.storage.sql as Sql;
      const subscribers = (repo: string) => Subscribers.sql(sql, repo);

      /** The remotes this repository fetches from, on that same SQLite. */
      const remotes = (repo: string) => Remotes.sql(sql, repo);

      const live = (repo: string): Layer.Layer<Repository> => {
        const existing = layers.get(repo);
        if (existing !== undefined) return existing;
        // Delivery runs in `waitUntil`, so a slow receiver never adds its
        // latency to the push that triggered it — and so does forwarding, for
        // the same reason and under the same rule: a remote that is down must
        // not touch the push that has already been accepted.
        const detached = <A, E>(effect: Effect.Effect<A, E>) =>
          // `runPromiseWith` rather than `runPromise`: the work is detached
          // from this fiber, not from its services, and a fresh runtime would
          // drop the ones it was handed.
          Effect.context<never>().pipe(
            Effect.map((context) => {
              state.raw.waitUntil(Effect.runPromiseWith(context)(Effect.ignore(effect)));
            }),
          );

        const afterPush = Layer.effect(
          GitRepository.Hooks,
          Effect.gen(function* () {
            const subscribed = yield* Subscribers.Subscribers;
            const client = yield* HttpClient.HttpClient;
            const registry = yield* Remotes.Remotes;
            return GitRepository.hooksAll([
              Webhooks.service({
                subscribers: subscribed,
                client,
                options: { background: detached },
              }),
              // The repository a forward pushes from is built when the push
              // lands, not when this layer is: it cannot be a dependency of
              // the hooks the repository itself depends on. See `Sending`.
              Sending.service({
                remotes: registry,
                using: (effect) =>
                  effect.pipe(
                    Effect.provide(
                      GitRepository.layer.pipe(
                        Layer.provide(GitRepository.hooksNoop),
                        Layer.provideMerge(
                          stores({ bucket: r2, repo, storage: state.raw.storage }),
                        ),
                      ),
                    ),
                  ),
                options: { background: detached },
              }),
            ]);
          }),
        ).pipe(
          Layer.provide(Layer.mergeAll(subscribers(repo), remotes(repo), FetchHttpClient.layer)),
        );

        const built = GitRepository.layer.pipe(
          Layer.provide(afterPush),
          // `provideMerge`: `provide` would swallow the `Storage` identity the
          // cross-request memos key on, and two repositories in one namespace
          // would share every entry.
          Layer.provideMerge(stores({ bucket: r2, repo, storage: state.raw.storage })),
        );
        layers.set(repo, built);
        return built;
      };

      /**
       * Response bodies still being read, per repository.
       *
       * The input gate reopens when the handler returns its `Response`, but an
       * upload-pack body reads objects as the client consumes it — so the same
       * thing is true here as on the node host, and `gc`, which is the only
       * caller that deletes, is the only one that waits. The wait is bounded:
       * a client that stalls must not postpone maintenance forever.
       */
      const delivering = new Map<string, Set<Promise<unknown>>>();
      const nothing: ReadonlySet<Promise<unknown>> = new Set();

      const track = (repo: string, response: Response): Response => {
        if (response.body === null) return response;
        let finished: () => void = () => undefined;
        const done = new Promise<void>((resolve) => {
          finished = resolve;
        });
        const pending = delivering.get(repo) ?? new Set();
        pending.add(done);
        delivering.set(repo, pending);
        void done.then(() => pending.delete(done));

        return new Response(
          response.body.pipeThrough(
            new TransformStream({
              flush: () => finished(),
              // A client that goes away cancels the stream; without this the
              // promise would never settle and only the timeout would clear it.
              cancel: () => finished(),
            }),
          ),
          response,
        );
      };

      const awaitDelivery = (repo: string) => settledWithin(delivering.get(repo) ?? nothing);

      /**
       * The JSON API's router, one per repository.
       *
       * Memoised for the same reason `live` is, and possible for the same
       * reason: the requester is kept *out* of the graph and arrives as a
       * per-request context instead. Rebuilding it per request reconstructs
       * the whole handler tree and opens a `Scope` nothing closes; baking the
       * requester in would answer every later request as the first caller.
       */
      const routers = new Map<
        string,
        (request: Request, requester: Context.Context<Auth.Requester>) => Promise<Response>
      >();

      const api = (repo: string) => {
        const existing = routers.get(repo);
        if (existing !== undefined) return existing;
        const router = HttpRouter.toWebHandler(
          Api.layer(remotes(repo)).pipe(
            Layer.provideMerge(live(repo)),
            Layer.provideMerge(subscribers(repo)),
            Layer.provideMerge(openWrites),
          ),
          { disableLogger: true },
        );
        const built = (request: Request, requester: Context.Context<Auth.Requester>) =>
          // SAFETY: the handler's generated declaration erases its remaining
          // request-scoped service to `unknown`; this context contains exactly
          // that `Requester` service and no value is inspected through the cast.
          router.handler(request, requester as Context.Context<unknown>);
        routers.set(repo, built);
        return built;
      };

      /**
       * Challenge nonces for this instance.
       *
       * Built once, here: a layer rebuilt per request would hand out a nonce
       * from one map and look for it in another, so no native client could
       * ever complete a challenge.
       */
      const nonces = Auth.noncesInMemory();

      /**
       * The last refs snapshot each repository published; see `Snapshot.ts`.
       *
       * Republished synchronously after every request that can move refs and
       * *before* its response is acknowledged, so the front Worker's
       * stateless read path — which serves anonymous clones from R2 alone —
       * is never behind an acknowledged write. When the snapshot cannot be
       * written, it is removed instead: readers then fall back to this
       * Durable Object, which is always right. Degraded, but never wrong.
       */
      const snapshots = new Map<string, Snapshot.Published>();
      const republish = (repo: string): Effect.Effect<void> =>
        Effect.gen(function* () {
          // After an eviction the in-memory cache is empty but the published
          // snapshot is not: re-reading it keeps the next entry's delta
          // honest and carries the readability verdict across restarts
          // instead of re-projecting the trust log on the first write.
          let previous = snapshots.get(repo);
          if (previous === undefined) {
            const held = yield* Effect.promise(() =>
              r2
                .get(Snapshot.keyOf(repo))
                .then(async (object) =>
                  object === null ? null : new Uint8Array(await object.arrayBuffer()),
                )
                .catch((): Uint8Array | null => null),
            );
            if (held !== null) previous = Snapshot.decode(held) ?? undefined;
          }
          const captured = yield* Snapshot.capture(previous);
          if (previous !== undefined && Snapshot.same(previous, captured)) return;

          // The journal entry first, the `latest` pointer second — the same
          // pack-before-index ordering the pack store keeps: what a reader
          // can find is never ahead of what the record explains. The
          // sequence counter lives in this Durable Object's storage, which
          // is exactly the single-writer state a monotonic counter wants.
          const seqKey = `snapshot-seq:${repo}`;
          const seq =
            ((yield* Effect.tryPromise({
              try: () => state.raw.storage.get<number>(seqKey),
              catch: (cause) => new StorageFailure({ operation: "snapshot", path: seqKey, cause }),
            })) ?? 0) + 1;
          const entry = Snapshot.entryOf(seq, captured, previous);
          yield* Effect.tryPromise({
            try: async () => {
              await r2.put(Snapshot.journalKeyOf(repo, seq), Snapshot.encodeJournal(entry));
              await state.raw.storage.put(seqKey, seq);
            },
            catch: (cause) =>
              new StorageFailure({
                operation: "snapshot",
                path: Snapshot.journalKeyOf(repo, seq),
                cause,
              }),
          });
          // Retention is one keyed delete — the entry that just left the
          // window — never a listing; a missing key is already gone.
          if (seq > Snapshot.RETAIN) {
            yield* Effect.tryPromise({
              try: () => r2.delete(Snapshot.journalKeyOf(repo, seq - Snapshot.RETAIN)),
              catch: (cause) => new StorageFailure({ operation: "snapshot", path: seqKey, cause }),
            }).pipe(Effect.ignore);
          }

          yield* Effect.tryPromise({
            try: () => r2.put(Snapshot.keyOf(repo), Snapshot.encode(captured)),
            catch: (cause) =>
              new StorageFailure({ operation: "snapshot", path: Snapshot.keyOf(repo), cause }),
          });
          snapshots.set(repo, captured);
        }).pipe(
          Effect.provide(live(repo)),
          Effect.catch(() =>
            Effect.gen(function* () {
              snapshots.delete(repo);
              yield* Effect.tryPromise({
                try: () => r2.delete(Snapshot.keyOf(repo)),
                catch: (cause) =>
                  new StorageFailure({ operation: "snapshot", path: Snapshot.keyOf(repo), cause }),
              }).pipe(Effect.ignore);
            }),
          ),
          Effect.ignoreCause,
        );

      /**
       * Whether writes to repositories with no genesis are served.
       *
       * Off unless the binding says so, like `serve --open`: such a repository
       * has no membership to authorize anybody with.
       */
      const openWrites = Policy.anonymousWrites(
        yield* Config.boolean("ALLOW_ANONYMOUS_WRITES").pipe(
          Config.withDefault(false),
          Effect.orElseSucceed(() => false),
        ),
      );

      /** The routing, over the platform request the bridge was handed. */
      const serve = (request: Request): Effect.Effect<Response> =>
        Effect.gen(function* () {
          const matched = routeOf(new URL(request.url).pathname);
          if (matched === null) {
            return Response.json({ error: "Invalid" }, { status: 400 });
          }
          const { repo, route } = matched;
          request = normalize(request, matched);

          // Auth runs here because this is where the trust state is: the
          // guard reads the repository's own genesis and membership log. The
          // Worker in front of this is a router and holds no secret — there is
          // nothing for it to authenticate with.
          const guarded = yield* Auth.guard(request).pipe(
            Effect.provide(Layer.mergeAll(live(repo), nonces, openWrites)),
            Effect.orElseSucceed(() => ({
              denied: new Response("authentication unavailable", { status: 503 }),
              authenticated: Auth.anonymous,
            })),
          );
          if (guarded.denied !== null) return guarded.denied;
          const response = yield* route_(request, repo, route, matched, guarded.authenticated);
          // Reads never move refs; anything else may have, and the snapshot
          // the stateless read path serves from must be current before this
          // response acknowledges the change.
          if (request.method !== "GET" && request.method !== "HEAD") {
            yield* republish(repo);
          }
          return response;
        });

      const route_ = (
        request: Request,
        repo: string,
        route: string,
        matched: { readonly rest: string },
        authenticated: Auth.Authenticated,
      ): Effect.Effect<Response> =>
        Effect.suspend(() => {
          // Two shapes of the same fact: the protocol paths build an effect
          // per request and take a layer, the memoised router takes a context.
          const requester = Auth.requester(authenticated);
          // LFS shares the `info/` prefix with the advertisement, so it is
          // tried first; its bodies are the large ones.
          if (route === "info" && matched.rest.includes("/lfs/")) {
            return LfsCore.handle(request).pipe(
              Effect.map(
                (response) => response ?? Response.json({ error: "NotFound" }, { status: 404 }),
              ),
              Effect.provide(Layer.mergeAll(lfsR2({ bucket: r2, repo }), requester)),
            );
          }

          // Also ahead of the JSON API: a bulk commit body is arbitrarily
          // large and is read as a stream, so nothing that would buffer it
          // may see the request first. Both hosts dispatch it here.
          if (route === "commit-pack") {
            return CommitPack.handle(request).pipe(
              Effect.map(
                (response) => response ?? Response.json({ error: "NotFound" }, { status: 404 }),
              ),
              // With the requester: `commit-pack` writes a ref and so crosses
              // the policy boundary, which has to know who is asking.
              Effect.provide(Layer.mergeAll(live(repo), requester, openWrites)),
              Effect.map((response) => track(repo, response)),
            );
          }

          if (route === "archive") {
            return Archive.handle(request).pipe(
              Effect.map(
                (response) => response ?? Response.json({ error: "NotFound" }, { status: 404 }),
              ),
              Effect.catch((error: GitError) =>
                Effect.succeed(Response.json({ _tag: error._tag }, { status: statusOf(error) })),
              ),
              Effect.provide(live(repo)),
              // An archive reads a blob per entry as the client consumes it,
              // which is the same lazy body a pack is.
              Effect.map((response) => track(repo, response)),
            );
          }

          if (route === "info" || route === "git-upload-pack" || route === "git-receive-pack") {
            return Protocol.handle(request).pipe(
              Effect.map(
                (response) => response ?? Response.json({ error: "NotFound" }, { status: 404 }),
              ),
              Effect.catch((error: GitError) =>
                Effect.succeed(Response.json({ _tag: error._tag }, { status: statusOf(error) })),
              ),
              Effect.provide(Layer.mergeAll(live(repo), requester, openWrites)),
              // The pack is the body that outlives its handler.
              Effect.map((response) => track(repo, response)),
            );
          }

          return Effect.promise(async () => {
            if (collects(request)) await awaitDelivery(repo);
            return track(repo, await api(repo)(request, Auth.requesterContext(authenticated)));
          });
        });

      return {
        fetch: Effect.gen(function* () {
          const incoming = yield* HttpServerRequest.HttpServerRequest;
          // SAFETY: the platform request, headers and body intact — the effect
          // wrapper was built from it, so `source` is that very object rather
          // than a rebuild of it.
          return HttpServerResponse.raw(yield* serve(incoming.source as Request));
        }),
      };
    });
    // One binding implementation serves both phases: at deploy time it
    // registers `r2_bucket` on the host Worker, at runtime it builds the
    // client. Its own requirements are absorbed by `.make()`.
  }).pipe(Effect.provide(Alchemy.R2.ReadWriteBucketBinding)),
);
