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
import type { ReadableStream as WebReadableStream } from "node:stream/web";

import { Config, Context, Effect, Layer, Predicate } from "effect";
import { FetchHttpClient, HttpClient, HttpRouter } from "effect/unstable/http";

import { statusOf } from "../git/Error.ts";
import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import type { Repository } from "../git/Repository.ts";
import * as Api from "../server/Api.ts";
import * as Auth from "../server/Auth.ts";
import * as Policy from "../server/Policy.ts";
import * as Archive from "../server/Archive.ts";
import * as CommitPack from "../server/CommitPack.ts";
import { file as lfsFile } from "../server/Lfs.node.ts";
import * as Lfs from "../server/Lfs.ts";
import * as Protocol from "../server/Protocol.ts";
import { file as remotesFile } from "../server/Remotes.node.ts";
import * as Remotes from "../server/Remotes.ts";
import { collects, routeOf, settledWithin } from "../server/Route.ts";
import * as Sending from "../server/Sending.ts";
import { file as subscribersFile } from "../server/Subscribers.node.ts";
import * as Subscribers from "../server/Subscribers.ts";
import * as Webhooks from "../server/Webhooks.ts";

export interface ServeOptions {
  /** Directory holding one bare repository per subdirectory. */
  readonly root: string;
  /** Defaults to an ephemeral port; the return value carries the real one. */
  readonly port?: number;
  readonly hostname?: string;
  /**
   * Serve writes to repositories that have no genesis.
   *
   * Off by default: such a repository has no membership to authorize anybody,
   * and "no policy" must not read as "no protection". On for a scratch server
   * where saying so out loud is the point.
   */
  readonly allowAnonymousWrites?: boolean;
}

export interface Server {
  readonly url: string;
  readonly close: () => Promise<void>;
}

/**
 * `RequestInit` plus the `duplex` member undici requires whenever the body is
 * a stream — the lib declaration has not caught up with the fetch spec.
 */
interface StreamingRequestInit extends RequestInit {
  duplex?: "half";
}

export const serve = async (options: ServeOptions): Promise<Server> => {
  const hostname = options.hostname ?? "127.0.0.1";
  /** Only reached by a client that sent no `Host`, which HTTP/1.1 requires. */
  let fallbackAuthority = `${hostname}:${options.port ?? 0}`;

  interface RepoState {
    readonly layer: Layer.Layer<Repository>;
    readonly lfs: Layer.Layer<Lfs.LfsStore>;
    readonly api: (
      request: Request,
      requester: Context.Context<Auth.Requester>,
    ) => Promise<Response>;
    /** Closes the router's scope — the layers it built are finalized here. */
    readonly disposeApi: () => Promise<void>;
    /** The input-gate stand-in: requests to one repo run strictly in order. */
    gate: Promise<unknown>;
    /**
     * Responses whose bodies are still being written.
     *
     * An upload-pack body reads objects as the client consumes it, so it is
     * still reading the store after its handler has returned. Collection is
     * the one operation that cannot run alongside that, and it is the only one
     * that waits — making every request wait would let a client that stops
     * reading its socket wedge the repository for everyone else.
     */
    readonly delivering: Set<Promise<unknown>>;
    /** Requests inside the gate right now; an evictable entry has none. */
    active: number;
  }
  const repos = new Map<string, RepoState>();

  /**
   * Challenge nonces, one store for the process.
   *
   * Shared across repositories rather than one store each: a nonce is a
   * one-shot value, and the envelope that carries it names the repository
   * inside the signed bytes — so a nonce cannot be moved between repositories
   * even though the pool is common.
   */
  const nonces = Auth.noncesInMemory();

  /** One value for the process; see `Policy.AnonymousWrites`. */
  const openWrites = Policy.anonymousWrites(options.allowAnonymousWrites === true);

  /**
   * How many repositories keep a built layer.
   *
   * One entry per name ever asked for is a leak with a name: a scan for
   * `/aaaa/info/refs`, `/aaab/…` would grow the map without bound, and none of
   * those repositories need exist. Eviction only ever takes an entry with
   * nothing in flight, so it cannot break the serialization the gate provides.
   */
  const REPO_CACHE = 256;

  const evict = () => {
    if (repos.size <= REPO_CACHE) return;
    for (const [name, state] of repos) {
      if (state.active > 0 || state.delivering.size > 0) continue;
      repos.delete(name);
      // The router holds a `Scope`: the layers it built — stores, hooks, the
      // webhook registry — have finalizers, and dropping the entry without
      // running them leaks a file handle per evicted repository.
      //
      // Caught, not merely detached. A finalizer that rejects becomes an
      // unhandled rejection, and node's default is to turn that into a throw
      // that takes the whole server down — so a file handle this host could
      // not close would stop it serving every repository it holds. Eviction is
      // housekeeping; it says so and carries on.
      state.disposeApi().catch((cause: unknown) => {
        console.error(`could not release ${name}: ${String(cause)}`);
      });
      if (repos.size <= REPO_CACHE) return;
    }
  };

  /**
   * Just enough of a repository to check who is asking.
   *
   * `Auth.guard` reads the genesis and the trust log, and nothing else — no
   * hooks, no webhook registry, no API router. Answering it from the cached
   * per-repository state meant every request built that state *before* it was
   * allowed, so an unauthenticated scan over names that need not exist opened
   * a `Scope` apiece and evicted live repositories' routers out of the cache
   * it was thrashing. Built per request and thrown away: the guard is one read
   * of two refs, and the repositories that pass it go on to be cached below.
   */
  const guardLayer = (repo: string) =>
    GitRepository.layer.pipe(
      Layer.provide(GitRepository.hooksNoop),
      // `provideMerge`, not `provide`: the stores carry the repository's
      // `Storage` identity, and `provide` consumes a layer's outputs without
      // re-exporting them — so the memos keyed on it saw `null` on every host,
      // and an origin and its mirror under one root shared every entry again.
      // The aliasing was invisible because nothing fails: the wrong answer is
      // a well-formed one.
      Layer.provideMerge(stores(path.join(options.root, repo))),
    );

  const stateFor = (repo: string): RepoState => {
    const cached = repos.get(repo);
    if (cached !== undefined) {
      // Re-inserted so iteration order is least-recently-used first.
      repos.delete(repo);
      repos.set(repo, cached);
      return cached;
    }
    evict();

    // The registry lives beside the repository it reports on, so a webhook
    // survives a restart the same way a ref does.
    const subscribers = subscribersFile(path.join(options.root, repo, "webhooks.json"));

    // And the remotes it fetches from, for the same reason and in the same
    // place: a remote that does not survive a restart is a URL somebody has
    // to remember.
    const remotes = remotesFile(path.join(options.root, repo, "remotes.json"));

    // What happens after a push lands: deliver to whoever subscribed, and
    // forward to whoever this repository is configured to send to. Both, not
    // one — `Hooks` is a single service, so the two are combined rather than
    // chosen between.
    //
    // The forwarder gets a repository built with no hooks at all. Handed the
    // one it is installed on, a forward would be its own trigger: a push
    // forwards, the forward is a push, and that one forwards again.
    const afterPush = Layer.effect(
      GitRepository.Hooks,
      Effect.gen(function* () {
        const subscribed = yield* Subscribers.Subscribers;
        const client = yield* HttpClient.HttpClient;
        const registry = yield* Remotes.Remotes;
        return GitRepository.hooksAll([
          Webhooks.service({ subscribers: subscribed, client }),
          // The repository to push *from* is built when a push lands, not
          // when this layer is: it cannot be a dependency of the hooks the
          // repository itself depends on. `guardLayer` is the no-hooks one.
          Sending.service({
            remotes: registry,
            using: (effect) => effect.pipe(Effect.provide(guardLayer(repo))),
          }),
        ]);
      }),
    ).pipe(Layer.provide(Layer.mergeAll(subscribers, remotes, FetchHttpClient.layer)));

    const layer = GitRepository.layer.pipe(
      // Real hooks, not `hooksNoop`: this is what makes a push deliver.
      // `forkDetach` is the node stand-in for `waitUntil` — delivery outlives
      // the response without the push waiting on a slow receiver.
      Layer.provide(afterPush),
      // As `guardLayer` above: `provide` would swallow `Storage`.
      Layer.provideMerge(stores(path.join(options.root, repo))),
    );

    // Built once per repository, not once per request. The requester stays
    // *out* of the graph and arrives as a per-request context instead, which
    // is what `toWebHandler`'s second argument is for: a router built per
    // call rebuilds the whole API handler tree and opens a `Scope` nobody
    // ever closes, and one built with the requester baked in would answer
    // every later request as whoever made the first.
    const router = HttpRouter.toWebHandler(
      Api.layer(remotes).pipe(
        Layer.provideMerge(layer),
        Layer.provideMerge(subscribers),
        Layer.provideMerge(openWrites),
      ),
      { disableLogger: true },
    );

    const state: RepoState = {
      layer,
      lfs: lfsFile(path.join(options.root, repo, "lfs")),
      api: (request: Request, requester: Context.Context<Auth.Requester>) =>
        router.handler(request, requester),
      disposeApi: router.dispose,
      gate: Promise.resolve(),
      delivering: new Set(),
      active: 0,
    };
    repos.set(repo, state);
    return state;
  };

  /**
   * One repository's requests, strictly in order.
   *
   * The gate spans the handler, which is what makes a push's write-objects-
   * then-move-the-ref window indivisible. It deliberately does not span
   * writing the response body: that finishes at the client's pace, and a
   * client that stops reading would otherwise hold the whole repository.
   *
   * Collection is the exception, because a pack body reads objects long after
   * its handler returned — `Repository.gc` relies on this, so `gc` waits for
   * the bodies still in flight before it starts deleting.
   */
  const dispatch = async (
    repo: string,
    request: Request,
    deliver: (response: Response) => Promise<void>,
    authenticated: Auth.Authenticated,
  ): Promise<void> => {
    const state = stateFor(repo);
    state.active += 1;

    // Two shapes of the same fact. The protocol and bulk paths build their own
    // effect per request and take a layer; the API router is built once and
    // takes a context per call, which is what keeps it memoisable.
    const requester = Auth.requester(authenticated);
    const asked = Auth.requesterContext(authenticated);

    // Outside the gate, deliberately: a collection waits on bodies that finish
    // at their clients' pace, and waiting for them with the gate held would
    // stall every other request to this repository for the whole bound.
    if (collects(request)) await settledWithin(state.delivering);

    const answer = async (): Promise<Response> => {
      // And once more with the gate held, briefly: the wait above lets go of
      // the backlog without holding anyone up, but a body that started while
      // it was waiting would otherwise still be reading objects. Short,
      // because by here the queue is short.
      if (collects(request)) await settledWithin(state.delivering, 2_000);

      // LFS first: it shares the `info/` prefix with the advertisement, and
      // its bodies are the large ones, so it must not be behind a handler
      // that would read them.
      const lfs = await Effect.runPromise(
        Lfs.handle(request).pipe(Effect.provide(Layer.mergeAll(state.lfs, requester))),
      );
      if (lfs !== null) return lfs;

      // Also ahead of the API: a bulk commit body is arbitrarily large and is
      // consumed as a stream, so nothing that would buffer it may see it first.
      const bulk = await Effect.runPromise(
        CommitPack.handle(request).pipe(
          Effect.provide(Layer.mergeAll(state.layer, requester, openWrites)),
        ),
      );
      if (bulk !== null) return bulk;

      const exported = await Effect.runPromise(
        Archive.handle(request).pipe(Effect.provide(state.layer)),
      );
      if (exported !== null) return exported;

      const matched = await Effect.runPromise(
        Protocol.handle(request).pipe(
          Effect.catch((error) =>
            Effect.succeed(Response.json({ _tag: error._tag }, { status: statusOf(error) })),
          ),
          Effect.provide(Layer.mergeAll(state.layer, requester, openWrites)),
        ),
      );
      return matched ?? (await state.api(request, asked));
    };

    const answered = state.gate.then(answer, answer);
    state.gate = answered.then(
      () => undefined,
      () => undefined,
    );

    let response: Response;
    try {
      response = await answered;
    } finally {
      state.active -= 1;
    }

    const delivery = deliver(response);
    // Registered before it is awaited, so a `gc` that arrives mid-body sees it.
    state.delivering.add(delivery);
    try {
      await delivery;
    } finally {
      state.delivering.delete(delivery);
    }
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
        if (Predicate.isString(value)) headers.set(name, value);
      }
      const method = incoming.method ?? "GET";
      const init: StreamingRequestInit = { method, headers };
      if (method !== "GET" && method !== "HEAD") {
        // Streamed, not buffered: a push flows straight into the pack parser.
        // SAFETY: node's web stream and the fetch body type are the same
        // class at runtime; only the lib declarations disagree.
        init.body = Readable.toWeb(incoming) as ReadableStream<Uint8Array>;
        init.duplex = "half";
      }
      const request = new Request(url, init);

      // Whether a repository is guarded is the repository's own answer: one
      // with a genesis has members, and one without is a plain git repository
      // that nothing here should start refusing to serve. There is no server
      // secret to configure any more, so there is nothing to leave off.
      const denied = await Effect.runPromise(
        Auth.guard(request).pipe(
          Effect.provide(Layer.mergeAll(guardLayer(repo), nonces, openWrites)),
          // A repository whose identity cannot be read is not a repository
          // with no members: it is one nobody can be checked against, and the
          // honest answer is that the service is unavailable.
          Effect.orElseSucceed(() => ({
            denied: new Response("authentication unavailable", { status: 503 }),
            authenticated: Auth.anonymous,
          })),
        ),
      );
      const deliver = async (response: Response) => {
        outgoing.writeHead(response.status, Object.fromEntries(response.headers.entries()));
        if (response.body === null) {
          outgoing.end();
        } else {
          // SAFETY: a fetch `Response` body is the same web stream class
          // node's `fromWeb` consumes; only the lib declarations disagree.
          await pipeline(Readable.fromWeb(response.body as WebReadableStream), outgoing);
        }
      };
      if (denied.denied !== null) await deliver(denied.denied);
      else await dispatch(repo, request, deliver, denied.authenticated);
    })().catch((cause: unknown) => {
      if (!outgoing.headersSent) outgoing.writeHead(500);
      outgoing.end(String(cause));
    });
  });

  await new Promise<void>((resolve) => {
    server.listen(options.port ?? 0, hostname, resolve);
  });

  // Known only now, because port 0 means "whichever one is free".
  // SAFETY: the server listens on a TCP port, never a pipe, so `address()`
  // returns an `AddressInfo` once `listen` has resolved.
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
