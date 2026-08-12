/**
 * The repository as a Durable Object — phase 2.
 *
 * One instance per repository, which is not an arbitrary mapping: `Repository`
 * is constructed from `ObjectStore` and `RefStore`, so storage has to resolve
 * when the layer is built rather than when a request arrives. A DO is exactly
 * that shape — an object with durable state, addressed by name, that processes
 * one request at a time.
 *
 * This class is the Worker entry point, driven end to end by
 * `Cloudflare.integration.ts` through wrangler's test harness.
 *
 * What the platform gives us here, and what the other backends have to build:
 *
 *   - serialization: the input gate, so `RefStore.apply`'s check-then-write
 *     cannot interleave with another request's;
 *   - isolation: one instance per repo name.
 */
import { DurableObject } from "cloudflare:workers";
import { Effect, Layer } from "effect";
import { HttpRouter } from "effect/unstable/http";

import * as Api from "../server/Api.ts";
import * as Protocol from "../server/Protocol.ts";
import { stores } from "./Cloudflare.ts";
import { collector } from "./Conformance.ts";
import { type GitError, statusOf } from "./Error.ts";
import * as GitRepository from "./Repository.ts";
import { Repository } from "./Repository.ts";
import { storeContract } from "./Store.contract.ts";

/**
 * This worker's bindings, as both `wrangler.json` and `wrangler.test.json`
 * provide them. Declared by hand rather than using the generated `Env` because
 * `ENABLE_CONFORMANCE` exists only in the test config, which `wrangler types`
 * does not see.
 */
interface TestEnv {
  readonly ENABLE_CONFORMANCE?: string;
  readonly GIT_OBJECTS: R2Bucket;
  readonly GIT_REPO: DurableObjectNamespace<GitRepo>;
}

export class GitRepo extends DurableObject<TestEnv> {
  #layer: Layer.Layer<Repository> | null = null;
  #api: ((request: Request) => Promise<Response>) | null = null;

  /** Built once per instance: the DO is the unit of isolation, not the request. */
  #live(repo: string): Layer.Layer<Repository> {
    this.#layer ??= GitRepository.layer.pipe(
      Layer.provide(GitRepository.hooksNoop),
      Layer.provide(stores({ bucket: this.env.GIT_OBJECTS, repo, storage: this.ctx.storage })),
    );
    return this.#layer;
  }

  /**
   * The only place a failure becomes a status code, and it does so from the
   * error's own `httpApiStatus` annotation rather than a mapping table.
   */
  #respond(repo: string, effect: Effect.Effect<Response, GitError, Repository>): Promise<Response> {
    return Effect.runPromise(
      effect.pipe(
        Effect.catch((error: GitError) =>
          Effect.succeed(Response.json({ error: error._tag }, { status: statusOf(error) })),
        ),
        Effect.provide(this.#live(repo)),
      ),
    );
  }

  override async fetch(request: Request): Promise<Response> {
    const [, repo = "default", route = ""] = new URL(request.url).pathname.split("/");

    if (route === "conformance") return this.#conformance(repo);

    // The smart-HTTP endpoints; everything else is the JSON API.
    if (route === "info" || route === "git-upload-pack" || route === "git-receive-pack") {
      return this.#respond(
        repo,
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
    this.#api ??= HttpRouter.toWebHandler(Api.layer.pipe(Layer.provideMerge(this.#live(repo))), {
      disableLogger: true,
    }).handler;
    return this.#api(request);
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
            ) as Effect.Effect<never>,
          ),
      },
      runner,
    );

    return Response.json(await report());
  }
}

/** Router: resolve `/:repo` to its instance. */
export default {
  async fetch(request: Request, env: TestEnv): Promise<Response> {
    const repo = new URL(request.url).pathname.split("/")[1];
    if (repo === undefined || repo === "") {
      return new Response("No repository in URL", { status: 400 });
    }
    return env.GIT_REPO.get(env.GIT_REPO.idFromName(repo)).fetch(request);
  },
} satisfies ExportedHandler<TestEnv>;
