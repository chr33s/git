/**
 * Node host: `Protocol.handle` and `Api.layer` unchanged, behind `node:http`,
 * over a directory of repositories in git's on-disk layout.
 *
 *   GIT_ROOT=repos PORT=8080 node src/host/Node.ts
 *
 * What the Durable Object gets free is built here: a per-repository mutex
 * stands in for the input gate (not `PartitionedSemaphore` — its permits are
 * a capacity shared across keys, not per-key exclusion), and one cached layer
 * per repository name stands in for instance isolation.
 */
import * as http from "node:http";
import type { AddressInfo } from "node:net";
import * as path from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

import { Config, Effect, Layer } from "effect";
import { HttpRouter } from "effect/unstable/http";

import { statusOf } from "../git/Error.ts";
import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import type { Repository } from "../git/Repository.ts";
import * as Api from "../server/Api.ts";
import * as Auth from "../server/Auth.ts";
import * as Archive from "../server/Archive.ts";
import * as CommitPack from "../server/CommitPack.ts";
import { file as lfsFile } from "../server/Lfs.node.ts";
import * as Lfs from "../server/Lfs.ts";
import * as Protocol from "../server/Protocol.ts";
import { file as remotesFile } from "../server/Remotes.node.ts";
import { routeOf } from "../server/Route.ts";
import { file as subscribersFile } from "../server/Subscribers.node.ts";
import * as Webhooks from "../server/Webhooks.ts";

export interface ServeOptions {
  /** Directory holding one bare repository per subdirectory. */
  readonly root: string;
  /** Defaults to an ephemeral port; the return value carries the real one. */
  readonly port?: number;
  readonly hostname?: string;
  /**
   * When present, every request passes `server/Auth.ts`'s guard with this
   * verifier — `Auth.hmacVerify` for stateless tokens, or the Artifacts
   * provider's `Tokens.verify` for revocable ones. Absent means open, which
   * is what local development wants.
   */
  readonly verify?: (repo: string, credential: string | null) => Promise<Auth.Scope | null>;
}

export interface Server {
  readonly url: string;
  readonly close: () => Promise<void>;
}

export const serve = async (options: ServeOptions): Promise<Server> => {
  const hostname = options.hostname ?? "127.0.0.1";
  /** Only reached by a client that sent no `Host`, which HTTP/1.1 requires. */
  let fallbackAuthority = `${hostname}:${options.port ?? 0}`;

  interface RepoState {
    readonly layer: Layer.Layer<Repository>;
    readonly lfs: Layer.Layer<Lfs.LfsStore>;
    readonly api: (request: Request) => Promise<Response>;
    /** The input-gate stand-in: requests to one repo run strictly in order. */
    gate: Promise<unknown>;
  }
  const repos = new Map<string, RepoState>();

  const stateFor = (repo: string): RepoState => {
    const cached = repos.get(repo);
    if (cached !== undefined) return cached;

    // The registry lives beside the repository it reports on, so a webhook
    // survives a restart the same way a ref does.
    const subscribers = subscribersFile(path.join(options.root, repo, "webhooks.json"));

    // And the remotes it fetches from, for the same reason and in the same
    // place: a remote that does not survive a restart is a URL somebody has
    // to remember.
    const remotes = remotesFile(path.join(options.root, repo, "remotes.json"));

    const layer = GitRepository.layer.pipe(
      // Real hooks, not `hooksNoop`: this is what makes a push deliver.
      // `forkDetach` is the node stand-in for `waitUntil` — delivery outlives
      // the response without the push waiting on a slow receiver.
      Layer.provide(Webhooks.hooksFetch().pipe(Layer.provide(subscribers))),
      Layer.provide(stores(path.join(options.root, repo))),
    );

    const state: RepoState = {
      layer,
      lfs: lfsFile(path.join(options.root, repo, "lfs")),
      api: HttpRouter.toWebHandler(
        Api.layerWith(remotes).pipe(Layer.provideMerge(layer), Layer.provideMerge(subscribers)),
        { disableLogger: true },
      ).handler,
      gate: Promise.resolve(),
    };
    repos.set(repo, state);
    return state;
  };

  const dispatch = async (repo: string, request: Request): Promise<Response> => {
    const state = stateFor(repo);
    const run = async () => {
      // LFS first: it shares the `info/` prefix with the advertisement, and
      // its bodies are the large ones, so it must not be behind a handler
      // that would read them.
      const lfs = await Effect.runPromise(
        Lfs.handle(request).pipe(Effect.provide(state.lfs)) as Effect.Effect<Response | null>,
      );
      if (lfs !== null) return lfs;

      // Also ahead of the API: a bulk commit body is arbitrarily large and is
      // consumed as a stream, so nothing that would buffer it may see it first.
      const bulk = await Effect.runPromise(
        CommitPack.handle(request).pipe(
          Effect.provide(state.layer),
        ) as Effect.Effect<Response | null>,
      );
      if (bulk !== null) return bulk;

      const exported = await Effect.runPromise(
        Archive.handle(request).pipe(Effect.provide(state.layer)) as Effect.Effect<Response | null>,
      );
      if (exported !== null) return exported;

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
      // The `Host` header, not the bind address: a handler that has to hand
      // a client an absolute URL back — the LFS batch API does — can only
      // build one that works from the authority the client actually used.
      const authority = incoming.headers.host ?? fallbackAuthority;
      const url = new URL(incoming.url ?? "/", `http://${authority}`);
      const matched = routeOf(url.pathname);
      if (matched === null) {
        outgoing.writeHead(400);
        outgoing.end("bad repository name");
        return;
      }
      const repo = matched.repo;
      // `/repo.git/info/refs` and `/repo/info/refs` are the same request;
      // the handlers see the second spelling either way.
      url.pathname = `/${repo}${matched.rest === "" ? "" : `/${matched.rest}`}`;

      /**
       * Everything but the hop-by-hop headers.
       *
       * This was an allowlist, and the allowlist was a bug generator: it
       * silently dropped `Git-Protocol`, so a client asking for protocol v2
       * got a v0 advertisement and quietly fell back — the failure mode of
       * an omission here is a feature that looks like it works.
       */
      const headers = new Headers();
      const hopByHop = new Set([
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "host",
      ]);
      for (const [name, value] of Object.entries(incoming.headers)) {
        if (hopByHop.has(name)) continue;
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

      const verify = options.verify;
      const denied =
        verify === undefined
          ? null
          : await Effect.runPromise(
              Auth.guard(request, (credential) => Effect.promise(() => verify(repo, credential))),
            );
      const response = denied ?? (await dispatch(repo, request));
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

  // Known only now, because port 0 means "whichever one is free".
  const bound = (server.address() as AddressInfo).port;
  fallbackAuthority = `${hostname}:${bound}`;

  return {
    url: `http://${hostname}:${bound}`,
    close: () =>
      new Promise((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      }),
  };
};

/**
 * Startup configuration, read through `Config` rather than `process.env`:
 * the provider is swappable (a test can supply `ConfigProvider.fromUnknown`),
 * a malformed `PORT` fails with a config error naming the variable instead of
 * silently becoming `NaN`, and the defaults live in one place.
 */
export const configuration = Effect.gen(function* () {
  return {
    root: yield* Config.string("GIT_ROOT").pipe(Config.withDefault("repos")),
    port: yield* Config.number("PORT").pipe(Config.withDefault(8080)),
    hostname: yield* Config.string("HOSTNAME").pipe(Config.withDefault("127.0.0.1")),
  };
});

if (import.meta.main) {
  const options = await Effect.runPromise(configuration.pipe(Effect.orDie));
  const { url } = await serve(options);
  console.log(`git smart-HTTP server on ${url}, repositories under ${options.root}/`);
}
