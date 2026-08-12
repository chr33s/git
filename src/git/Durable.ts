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
import * as Protocol from "../server/Protocol.ts";
import { normalize, routeOf } from "../server/Route.ts";
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
  /** When set, every request must carry an `Auth.hmacMint`-issued token. */
  readonly GIT_AUTH_SECRET?: string;
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
    const matched = routeOf(new URL(request.url).pathname);
    if (matched === null) return Response.json({ error: "Invalid" }, { status: 400 });
    const { repo, route } = matched;
    request = normalize(request, matched);

    // Stateless auth, on when the secret binding exists: nothing to store,
    // nothing to look up, and a token minted for one repo verifies nowhere else.
    const secret = this.env.GIT_AUTH_SECRET;
    if (secret !== undefined && secret.length > 0) {
      const denied = await Effect.runPromise(
        Auth.guard(request, (credential) => Auth.hmacVerify(secret, repo, credential)),
      );
      if (denied !== null) return denied;
    }

    if (route === "conformance") return this.#conformance(repo);
    if (route === "registry-conformance") return this.#registryConformance();

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
    return env.GIT_REPO.get(env.GIT_REPO.idFromName(route.repo)).fetch(
      normalize(request, route),
    );
  },
} satisfies ExportedHandler<TestEnv>;
