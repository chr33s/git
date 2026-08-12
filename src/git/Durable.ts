/**
 * The repository as a Durable Object — phase 2.
 *
 * One instance per repository, which is not an arbitrary mapping: `Repository`
 * is constructed from `ObjectStore` and `RefStore`, so storage has to resolve
 * when the layer is built rather than when a request arrives. A DO is exactly
 * that shape — an object with durable state, addressed by name, that processes
 * one request at a time.
 *
 * The existing `Server` in `src/server.ts` is untouched and still serves
 * traffic. This class is the new path: it holds the layer graph, and the
 * integration tests drive it inside workerd against real R2 and DO SQLite.
 *
 * What the platform gives us here, and what the other backends have to build:
 *
 *   - serialization: the input gate, so `RefStore.apply`'s check-then-write
 *     cannot interleave with another request's;
 *   - isolation: one instance per repo name, so no repo prefix is needed on
 *     ref rows beyond bookkeeping.
 */
import { DurableObject } from "cloudflare:workers";
import { Effect, Layer, Stream } from "effect";

import { stores } from "./Cloudflare.ts";
import { statusOf, type GitError } from "./Error.ts";
import type { Signature } from "./Format.ts";
import { EMPTY_TREE_OID } from "./Format.ts";
import * as GitRepository from "./Repository.ts";
import { Repository } from "./Repository.ts";
import type { Oid } from "./Store.ts";

export class GitRepo extends DurableObject<Env> {
  #layer: Layer.Layer<Repository> | null = null;

  /**
   * Built once per instance and reused, which is what makes the DO the unit of
   * isolation rather than the request.
   */
  #live(repo: string): Layer.Layer<Repository> {
    this.#layer ??= GitRepository.layer.pipe(
      Layer.provide(GitRepository.hooksNoop),
      Layer.provide(
        stores({
          bucket: this.env.GIT_OBJECTS,
          repo,
          storage: this.ctx.storage,
        }),
      ),
    );
    return this.#layer;
  }

  #run<A>(repo: string, effect: Effect.Effect<A, GitError, Repository>): Promise<A> {
    return Effect.runPromise(effect.pipe(Effect.provide(this.#live(repo))));
  }

  /**
   * RPC surface. Each method returns plain data so the Worker in front stays a
   * router — the failure channel is collapsed at this boundary because RPC
   * cannot carry a tagged error across the isolate.
   */
  async refs(repo: string): Promise<Array<{ name: string; oid: string }>> {
    return this.#run(
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;
        const refs = yield* repository.refs;
        return refs.map(([name, oid]) => ({ name, oid }));
      }),
    );
  }

  async commit(
    repo: string,
    input: {
      author: Signature;
      branch: string;
      expected?: string | null;
      message: string;
      tree?: string;
    },
  ): Promise<{ oid: string }> {
    return this.#run(
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;
        const oid = yield* repository.commit({
          author: input.author,
          branch: input.branch,
          message: input.message,
          tree: (input.tree ?? EMPTY_TREE_OID) as Oid,
          ...(input.expected === undefined ? {} : { expected: input.expected as Oid | null }),
        });
        return { oid };
      }),
    );
  }

  async log(
    repo: string,
    from: string,
    limit = 50,
  ): Promise<Array<{ message: string; oid: string }>> {
    return this.#run(
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;
        const commits = yield* Stream.runCollect(repository.log(from as Oid, { limit }));
        return commits.map((commit) => ({ message: commit.message, oid: commit.oid }));
      }),
    );
  }

  async writeBlob(repo: string, data: ArrayBuffer): Promise<{ oid: string }> {
    return this.#run(
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;
        const oid = yield* repository.writeBlob(new Uint8Array(data));
        return { oid };
      }),
    );
  }

  /**
   * The HTTP surface.
   *
   * The only place a failure becomes a status code, and it does so from the
   * error's own annotation rather than a mapping table — `worker.ts` and
   * `server.api.ts` each keep their own today.
   */
  override async fetch(request: Request): Promise<Response> {
    const [, repo = "default", route = "refs", argument] = new URL(request.url).pathname.split("/");

    const handler =
      route === "commit" && argument !== undefined
        ? Effect.gen(function* () {
            const repository = yield* Repository;
            const commit = yield* repository.readCommit(argument as Oid);
            return Response.json({ message: commit.message, tree: commit.tree });
          })
        : Effect.gen(function* () {
            const repository = yield* Repository;
            const refs = yield* repository.refs;
            return Response.json({ refs: refs.map(([name, oid]) => ({ name, oid })) });
          });

    return this.#run(
      repo,
      handler.pipe(
        Effect.catch((error: GitError) =>
          Effect.succeed(Response.json({ error: error._tag }, { status: statusOf(error) })),
        ),
      ),
    );
  }
}

/** Router: resolve `/:repo` to its instance. Mirrors `src/worker.ts`. */
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const repo = new URL(request.url).pathname.split("/")[1];
    if (repo === undefined || repo === "") {
      return new Response("No repository in URL", { status: 400 });
    }
    return env.GIT_REPO.get(env.GIT_REPO.idFromName(repo)).fetch(request);
  },
} satisfies ExportedHandler<Env>;
