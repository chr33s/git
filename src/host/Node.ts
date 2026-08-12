/**
 * Node host: the same server the Durable Object runs, behind `node:http`.
 *
 * This is the file that proves the seam is real — `Protocol.handle` and
 * `Api.layer` are served here unchanged, over a directory of repositories in
 * git's own on-disk layout. Self-hosting is a supported shape, not a fork:
 *
 *   GIT_ROOT=repos PORT=8080 node src/host/Node.ts
 *   git clone http://127.0.0.1:8080/my-repo
 *
 * What the Durable Object gets from the platform has to be built here:
 *
 *   - serialization: a per-repository mutex around dispatch stands in for the
 *     DO input gate, so `RefStore.apply`'s check-then-write cannot interleave.
 *     (Not `PartitionedSemaphore` — its permits are a capacity shared across
 *     keys, fair queuing rather than per-key exclusion.)
 *   - isolation: one layer per repository name, cached for the process's
 *     lifetime the way a DO instance lives between requests.
 */
import * as http from "node:http";
import type { AddressInfo } from "node:net";
import * as path from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

import { Effect, Layer } from "effect";
import { HttpRouter } from "effect/unstable/http";

import { statusOf } from "../git/Error.ts";
import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import type { Repository } from "../git/Repository.ts";
import * as Api from "../server/Api.ts";
import * as Protocol from "../server/Protocol.ts";

/** No traversal, no hidden files; `.git` suffixes are part of the name. */
const REPO_NAME = /^[A-Za-z0-9][A-Za-z0-9._-]*$/;

export interface ServeOptions {
  /** Directory holding one bare repository per subdirectory. */
  readonly root: string;
  /** Defaults to an ephemeral port; the return value carries the real one. */
  readonly port?: number;
  readonly hostname?: string;
}

export interface Server {
  readonly url: string;
  readonly close: () => Promise<void>;
}

export const serve = async (options: ServeOptions): Promise<Server> => {
  const hostname = options.hostname ?? "127.0.0.1";

  interface RepoState {
    readonly layer: Layer.Layer<Repository>;
    readonly api: (request: Request) => Promise<Response>;
    /** The input-gate stand-in: requests to one repo run strictly in order. */
    gate: Promise<unknown>;
  }
  const repos = new Map<string, RepoState>();

  const stateFor = (repo: string): RepoState => {
    let state = repos.get(repo);
    if (state === undefined) {
      const layer = GitRepository.layer.pipe(
        Layer.provide(GitRepository.hooksNoop),
        Layer.provide(stores(path.join(options.root, repo))),
      );
      state = {
        layer,
        api: HttpRouter.toWebHandler(Api.layer.pipe(Layer.provideMerge(layer)), {
          disableLogger: true,
        }).handler,
        gate: Promise.resolve(),
      };
      repos.set(repo, state);
    }
    return state;
  };

  const dispatch = async (repo: string, request: Request): Promise<Response> => {
    const state = stateFor(repo);
    const run = async () => {
      const matched = await Effect.runPromise(
        Protocol.handle(request).pipe(
          Effect.catch((error) =>
            Effect.succeed(Response.json({ _tag: error._tag }, { status: statusOf(error) })),
          ),
          Effect.provide(state.layer),
        ) as Effect.Effect<Response | null>,
      );
      return matched ?? state.api(request);
    };
    const response = state.gate.then(run, run);
    state.gate = response.then(
      () => undefined,
      () => undefined,
    );
    return response;
  };

  const server = http.createServer((incoming, outgoing) => {
    void (async () => {
      const url = new URL(incoming.url ?? "/", `http://${hostname}`);
      const repo = url.pathname.split("/")[1] ?? "";
      if (!REPO_NAME.test(repo) || repo.includes("..")) {
        outgoing.writeHead(400);
        outgoing.end("bad repository name");
        return;
      }

      const headers = new Headers();
      for (const name of ["content-type", "content-encoding", "accept"]) {
        const value = incoming.headers[name];
        if (typeof value === "string") headers.set(name, value);
      }
      const method = incoming.method ?? "GET";
      const request = new Request(url, {
        method,
        headers,
        // Streamed, not buffered: a push flows straight into the pack parser.
        ...(method === "GET" || method === "HEAD"
          ? {}
          : { body: Readable.toWeb(incoming) as ReadableStream<Uint8Array>, duplex: "half" }),
      } as RequestInit);

      const response = await dispatch(repo, request);
      outgoing.writeHead(response.status, Object.fromEntries(response.headers.entries()));
      if (response.body === null) outgoing.end();
      else await pipeline(Readable.fromWeb(response.body as never), outgoing);
    })().catch((error: unknown) => {
      if (!outgoing.headersSent) outgoing.writeHead(500);
      outgoing.end(String(error));
    });
  });

  await new Promise<void>((resolve) => {
    server.listen(options.port ?? 0, hostname, resolve);
  });

  return {
    url: `http://${hostname}:${(server.address() as AddressInfo).port}`,
    close: () =>
      new Promise((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      }),
  };
};

if (import.meta.main) {
  const root = process.env["GIT_ROOT"] ?? "repos";
  const { url } = await serve({
    root,
    port: Number(process.env["PORT"] ?? 8080),
    hostname: process.env["HOSTNAME"] ?? "127.0.0.1",
  });
  console.log(`git smart-HTTP server on ${url}, repositories under ${root}/`);
}
