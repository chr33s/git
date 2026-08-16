/**
 * The repository as a Durable Object — the integration harness entry point.
 * The deployable Worker is `worker.ts` via the alchemy stack; this one exists
 * so the same server can be driven end to end inside real workerd by
 * `Cloudflare.integration.ts`.
 *
 * One instance per repository is forced, not chosen: `Repository` is built
 * from `ObjectStore` and `RefStore`, so storage must resolve when the layer
 * is built rather than when a request arrives. The platform supplies two
 * things the other backends build by hand — the input gate serializes
 * `RefStore.apply`'s check-then-write, and instances are isolated by name.
 */
import { DurableObject } from "cloudflare:workers";
import { Effect, Layer } from "effect";
import { HttpRouter } from "effect/unstable/http";

import { registryContract } from "../artifacts/Registry.contract.ts";
import { sqlite } from "../artifacts/Sqlite.ts";
import * as Api from "../server/Api.ts";
import * as Auth from "../server/Auth.ts";
import * as Archive from "../server/Archive.ts";
import * as CommitPack from "../server/CommitPack.ts";
import { r2 as lfsR2 } from "../server/Lfs.cloudflare.ts";
import * as Lfs from "../server/Lfs.ts";
import * as Protocol from "../server/Protocol.ts";
import * as Remotes from "../server/Remotes.ts";
import { collects, normalize, routeOf, settledWithin } from "../server/Route.ts";
import * as Subscribers from "../server/Subscribers.ts";
import * as Webhooks from "../server/Webhooks.ts";
import { stores } from "./Cloudflare.ts";
import { collector } from "./Conformance.ts";
import { type GitError, statusOf } from "./Error.ts";
import * as GitRepository from "./Repository.ts";
import { Repository } from "./Repository.ts";
import { storeContract } from "./Store.contract.ts";

/**
 * This worker's bindings, as `wrangler.test.json` provides them. Declared by
 * hand rather than using the generated `Env` so the dependency points the
 * right way: the types follow the config this entry point actually runs
 * under.
 */
interface TestEnv {
  readonly ENABLE_CONFORMANCE?: string;
  readonly GIT_OBJECTS: R2Bucket;
  readonly GIT_REPO: DurableObjectNamespace<GitRepo>;
}

export class GitRepo extends DurableObject<TestEnv> {
  #layer: Layer.Layer<Repository> | null = null;

  #subscribers: Layer.Layer<Subscribers.Subscribers> | null = null;
  #remotes: Layer.Layer<Remotes.Remotes> | null = null;
  #nonceStore: Layer.Layer<Auth.Nonces> | null = null;
  /** The requester of the request being handled; the DO serializes them. */

  /** The registry on this instance's own SQLite, beside the refs. */
  #registry(repo: string): Layer.Layer<Subscribers.Subscribers> {
    this.#subscribers ??= Subscribers.sql(this.ctx.storage.sql, repo);
    return this.#subscribers;
  }

  /** The remotes this repository fetches from, on that same SQLite. */
  #remoteRegistry(repo: string): Layer.Layer<Remotes.Remotes> {
    this.#remotes ??= Remotes.sql(this.ctx.storage.sql, repo);
    return this.#remotes;
  }

  /**
   * Challenge nonces for this instance.
   *
   * In memory, and per Durable Object, which is the right scope: one instance
   * is one repository, and a nonce is only meaningful against the repository
   * that issued it. An evicted instance forgets them, and a client whose nonce
   * is no longer recognised is told to ask for another — a retry, not a
   * failure worth persisting through.
   */
  #nonces(): Layer.Layer<Auth.Nonces> {
    this.#nonceStore ??= Auth.noncesInMemory();
    return this.#nonceStore;
  }

  /** Built once per instance: the DO is the unit of isolation, not the request. */
  #live(repo: string): Layer.Layer<Repository> {
    this.#layer ??= GitRepository.layer.pipe(
      Layer.provide(
        Webhooks.hooksFetch({
          background: (effect) =>
            // `runPromiseWith` rather than `runPromise`: the delivery is
            // detached from this fiber, not from its services, and a fresh
            // runtime would drop the ones it was handed.
            Effect.context<never>().pipe(
              Effect.map((context) => {
                this.ctx.waitUntil(Effect.runPromiseWith(context)(Effect.ignore(effect)));
              }),
            ),
        }).pipe(Layer.provide(this.#registry(repo))),
      ),
      Layer.provide(stores({ bucket: this.env.GIT_OBJECTS, repo, storage: this.ctx.storage })),
    );
    return this.#layer;
  }

  /**
   * Response bodies still being read.
   *
   * The input gate reopens when a handler returns its `Response`, but a pack
   * or an archive reads objects as the client consumes it — so collection,
   * the one caller that deletes, waits for these before it starts. Bounded:
   * a client that stalls must not postpone maintenance forever.
   */
  readonly #delivering = new Set<Promise<unknown>>();

  #track(response: Response): Response {
    if (response.body === null) return response;
    let finished: () => void = () => undefined;
    const done = new Promise<void>((resolve) => {
      finished = resolve;
    });
    this.#delivering.add(done);
    void done.then(() => this.#delivering.delete(done));

    return new Response(
      response.body.pipeThrough(
        new TransformStream({
          flush: () => finished(),
          // A client that goes away cancels the stream rather than ending it.
          cancel: () => finished(),
        }),
      ),
      response,
    );
  }

  /**
   * The only place a failure becomes a status code, and it does so from the
   * error's own `httpApiStatus` annotation rather than a mapping table.
   */
  #respond(
    repo: string,
    requester: Layer.Layer<Auth.Requester>,
    effect: Effect.Effect<Response, GitError, Repository>,
  ): Promise<Response> {
    return Effect.runPromise(
      effect.pipe(
        Effect.catch((error: GitError) =>
          Effect.succeed(Response.json({ error: error._tag }, { status: statusOf(error) })),
        ),
        Effect.provide(Layer.merge(this.#live(repo), requester)),
        Effect.map((response) => this.#track(response)),
      ),
    );
  }

  override async fetch(request: Request): Promise<Response> {
    const matched = routeOf(new URL(request.url).pathname);
    if (matched === null) return Response.json({ error: "Invalid" }, { status: 400 });
    const { repo, route } = matched;
    request = normalize(request, matched);

    // Auth runs here rather than at the edge because this is where the trust
    // state is: the guard reads the repository's own genesis and membership
    // log. A repository with no genesis is not hub-enabled and stays open,
    // which is what every repository that predates this was.
    const guarded = await Effect.runPromise(
      Auth.guard(request).pipe(
        Effect.provide(Layer.merge(this.#live(repo), this.#nonces())),
        // As the other two hosts do: a repository whose identity cannot be
        // read is unavailable, not open, and not an exception out of `fetch`.
        Effect.orElseSucceed(() => ({
          denied: new Response("authentication unavailable", { status: 503 }),
          authenticated: Auth.anonymous,
        })),
      ),
    );
    if (guarded.denied !== null) return guarded.denied;
    // Who the requester is travels with the rest of the request as an
    // argument, not as instance state: the `await` before a collection reopens
    // the input gate, so a field would be whatever the *last* request through
    // the door set it to.
    const requester = Auth.requester(guarded.authenticated);

    if (route === "conformance") return this.#conformance(repo);
    if (route === "registry-conformance") return this.#registryConformance();

    // LFS shares the `info/` prefix with the advertisement, so it is tried
    // first; its bodies are the large ones.
    if (route === "info" && matched.rest.includes("/lfs/")) {
      return Effect.runPromise(
        Lfs.handle(request).pipe(
          Effect.map(
            (response) => response ?? Response.json({ error: "NotFound" }, { status: 404 }),
          ),
          Effect.provide(lfsR2({ bucket: this.env.GIT_OBJECTS, repo })),
        ),
      );
    }

    // Also ahead of the API: the body is arbitrarily large and is read as a
    // stream, so no handler that would buffer it may see the request first.
    if (route === "commit-pack") {
      return this.#respond(
        repo,
        requester,
        CommitPack.handle(request).pipe(
          Effect.map(
            (response) => response ?? Response.json({ error: "NotFound" }, { status: 404 }),
          ),
        ),
      );
    }

    if (route === "archive") {
      return this.#respond(
        repo,
        requester,
        Archive.handle(request).pipe(
          Effect.map(
            (response) => response ?? Response.json({ error: "NotFound" }, { status: 404 }),
          ),
        ),
      );
    }

    // The smart-HTTP endpoints; everything else is the JSON API.
    if (route === "info" || route === "git-upload-pack" || route === "git-receive-pack") {
      return this.#respond(
        repo,
        requester,
        Protocol.handle(request).pipe(
          Effect.map(
            (response) => response ?? Response.json({ error: "NotFound" }, { status: 404 }),
          ),
        ),
      );
    }

    // The `HttpApi` handler, built once per instance like the layer it wraps.
    // Never disposed: its lifetime is the Durable Object's. `provideMerge`
    // rather than `provide` — handler contexts are request-scoped, so the
    // router looks for `Repository` among the app layer's outputs.
    // `gc` is the request that deletes objects a body may still be reading.
    if (collects(request)) await settledWithin(this.#delivering);

    // Rebuilt per request rather than memoised: the requester is part of the
    // layer the handlers resolve, and a handler built once would answer every
    // later request as whoever made the first one.
    const handler = HttpRouter.toWebHandler(
      Api.layer(this.#remoteRegistry(repo)).pipe(
        Layer.provideMerge(this.#live(repo)),
        Layer.provideMerge(this.#registry(repo)),
        Layer.provideMerge(requester),
      ),
      { disableLogger: true },
    ).handler;
    return this.#track(await handler(request));
  }

  /**
   * Runs the storage contract against this instance's own R2 + SQLite and
   * returns the results.
   *
   * The test process lives outside workerd and cannot reach `storage.sql`, so
   * the suite runs here and the outcome crosses as JSON. Gated on a var that
   * only `wrangler.test.json` sets, so it does not exist in a real deployment.
   */
  async #conformance(repo: string): Promise<Response> {
    if (this.env.ENABLE_CONFORMANCE !== "1") {
      return Response.json({ error: "NotFound" }, { status: 404 });
    }

    const bucket = this.env.GIT_OBJECTS;
    const storage = this.ctx.storage;
    const { report, runner } = collector();

    storeContract(
      "Cloudflare",
      {
        run: (effect) =>
          Effect.runPromise(
            effect.pipe(
              // A fresh namespace per test, so the suite starts empty without
              // needing a fresh Durable Object each time.
              Effect.provide(stores({ bucket, repo: `${repo}/${crypto.randomUUID()}`, storage })),
            ),
          ),
      },
      runner,
    );

    return Response.json(await report());
  }

  /**
   * The registry/token contract against this instance's own SQLite — the
   * durable form a Workers-hosted Artifacts provider would use, proven in
   * the runtime that would host it rather than against `node:sqlite`.
   */
  async #registryConformance(): Promise<Response> {
    if (this.env.ENABLE_CONFORMANCE !== "1") {
      return Response.json({ error: "NotFound" }, { status: 404 });
    }

    const sql = this.ctx.storage.sql;
    const { report, runner } = collector();

    registryContract(
      "Durable Object SQLite",
      {
        run: (effect) => {
          // Empty tables per test — before the layer is built, since building
          // it is what creates them. One DO instance runs every case.
          sql.exec(`DROP TABLE IF EXISTS repos`);
          sql.exec(`DROP TABLE IF EXISTS tokens`);
          return Effect.runPromise(effect.pipe(Effect.provide(sqlite(sql))));
        },
      },
      runner,
    );

    return Response.json(await report());
  }
}

/** Router: resolve `/:repo` to its instance. */
export default {
  async fetch(request: Request, env: TestEnv): Promise<Response> {
    const route = routeOf(new URL(request.url).pathname);
    if (route === null) return new Response("No repository in URL", { status: 400 });
    // The instance is keyed on the stripped name, so `/repo` and `/repo.git`
    // reach the same Durable Object rather than two empty ones.
    return env.GIT_REPO.get(env.GIT_REPO.idFromName(route.repo)).fetch(normalize(request, route));
  },
} satisfies ExportedHandler<TestEnv>;
