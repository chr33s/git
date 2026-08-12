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
import { Effect, Layer, Stream } from "effect";

import { stores } from "./Cloudflare.ts";
import { collector } from "./Conformance.ts";
import { type GitError, statusOf } from "./Error.ts";
import { EMPTY_TREE_OID, type Signature } from "./Format.ts";
import * as GitRepository from "./Repository.ts";
import { Repository } from "./Repository.ts";
import { storeContract } from "./Store.contract.ts";
import type { Oid } from "./Store.ts";

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

/**
 * The wire shape, which is not the domain shape: JSON has no `Date`, so
 * `author.at` arrives as a string and has to be parsed. Decoding at the
 * boundary is what `HttpApi` schemas do for the JSON API in the sketch; this
 * handler does it by hand until that lands.
 */
interface CommitBody {
  readonly author?: {
    readonly at?: string;
    readonly email?: string;
    readonly name?: string;
    readonly offset?: number;
  };
  readonly branch?: string;
  readonly expected?: string | null;
  readonly message?: string;
}

const signatureFrom = (author: CommitBody["author"]): Signature => ({
  name: author?.name ?? "Anonymous",
  email: author?.email ?? "anonymous@example.com",
  at: author?.at === undefined ? new Date() : new Date(author.at),
  offset: author?.offset ?? 0,
});

export class GitRepo extends DurableObject<TestEnv> {
  #layer: Layer.Layer<Repository> | null = null;

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
    const [, repo = "default", route = "refs", argument] = new URL(request.url).pathname.split("/");

    if (route === "conformance") return this.#conformance(repo);

    if (route === "commit" && request.method === "POST") {
      const body = (await request.json()) as CommitBody;
      return this.#respond(
        repo,
        Effect.gen(function* () {
          const repository = yield* Repository;
          const oid = yield* repository.commit({
            author: signatureFrom(body.author),
            branch: body.branch ?? "main",
            message: body.message ?? "",
            tree: EMPTY_TREE_OID,
            ...(body.expected === undefined ? {} : { expected: body.expected as Oid | null }),
          });
          return Response.json({ oid });
        }),
      );
    }

    if (route === "commit" && argument !== undefined) {
      return this.#respond(
        repo,
        Effect.gen(function* () {
          const repository = yield* Repository;
          const commit = yield* repository.readCommit(argument as Oid);
          return Response.json({
            message: commit.message,
            parents: commit.parents,
            tree: commit.tree,
          });
        }),
      );
    }

    if (route === "log" && argument !== undefined) {
      return this.#respond(
        repo,
        Effect.gen(function* () {
          const repository = yield* Repository;
          const commits = yield* Stream.runCollect(repository.log(argument as Oid, { limit: 50 }));
          return Response.json({
            commits: commits.map((commit) => ({ message: commit.message, oid: commit.oid })),
          });
        }),
      );
    }

    return this.#respond(
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;
        const refs = yield* repository.refs;
        return Response.json({ refs: refs.map(([name, oid]) => ({ name, oid })) });
      }),
    );
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
