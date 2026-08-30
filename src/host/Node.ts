/**
 * Node host: `Protocol.handle` and `Api.layer` unchanged, behind `node:http`,
 * over a directory of repositories in git's on-disk layout.
 *
 *   GIT_ROOT=repos PORT=8080 node src/host/Node.ts
 *
 * What the Durable Object gets free is built here: a per-repository mutex
 * stands in for the input gate (not `PartitionedSemaphore` — its permits are
 * a capacity shared across keys, not per-key exclusion), and an `RcMap` of
 * per-repository layers stands in for instance isolation.
 */
import * as http from "node:http";
import * as fs from "node:fs/promises";
import type { AddressInfo } from "node:net";
import * as path from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as WebReadableStream } from "node:stream/web";

import { Context, Effect, Exit, Layer, Predicate, RcMap, Scope } from "effect";
import { HttpRouter } from "effect/unstable/http";

import { statusOf } from "../git/Error.ts";
import { stores } from "../git/Node.ts";
import * as Pack from "../git/Pack.ts";
import * as GitRepository from "../git/Repository.ts";
import type { Repository } from "../git/Repository.ts";
import * as SocialLog from "../social/Log.ts";
import { SocialWeb } from "../social/Projection.ts";
import { readGenesis } from "../trust/Genesis.ts";
import {
  Identities,
  principalId,
  type PrincipalId,
  type ResolvedIdentity,
} from "../trust/Principal.ts";
import { project as projectTrust } from "../trust/Projection.ts";
import * as AfterPush from "../server/AfterPush.node.ts";
import * as Api from "../server/Api.ts";
import * as Auth from "../server/Auth.ts";
import * as Policy from "../server/Policy.ts";
import * as Archive from "../server/Archive.ts";
import * as CommitPack from "../server/CommitPack.ts";
import { file as lfsFile } from "../server/Lfs.node.ts";
import * as Lfs from "../server/Lfs.ts";
import * as Protocol from "../server/Protocol.ts";
import { file as remotesFile } from "../server/Remotes.node.ts";
import { collects, routeOf, settledWithin } from "../server/Route.ts";
import { assetResponse } from "../server/Static.ts";
import { file as subscribersFile } from "../server/Subscribers.node.ts";
import { resolve as resolveConfiguration, type ServeConfig } from "./ServeConfig.ts";

/**
 * A development asset server mounted before the Git routes.
 *
 * The callback follows Connect's `next` convention without making the host
 * depend on Connect (or Vite): served assets end the response; a miss falls
 * through unchanged to the streaming Git handlers.
 */
export interface DevelopmentMiddleware {
  readonly handle: (
    request: http.IncomingMessage,
    response: http.ServerResponse,
    next: (cause?: unknown) => void,
  ) => void;
  readonly close: () => Promise<void>;
}

export interface ServeOptions extends Omit<ServeConfig, "port" | "hostname" | "hosts"> {
  /** Defaults to an ephemeral port; the return value carries the real one. */
  readonly port?: number;
  readonly hostname?: string;
  /**
   * Public authorities this server answers to; see `ServeConfig.hosts`.
   *
   * Optional: the server's own bound `hostname:port` is always trusted, so a
   * server reached at the address it binds needs none. Set it only where the
   * public name differs from the bind address (behind a reverse proxy).
   */
  readonly hosts?: ReadonlyArray<string>;
  /**
   * Serve writes to repositories that have no genesis.
   *
   * Off by default: such a repository has no membership to authorize anybody,
   * and "no policy" must not read as "no protection". On for a scratch server
   * where saying so out loud is the point.
   */
  readonly allowAnonymousWrites?: boolean;
  /**
   * Directory of a built UI to serve from this origin, if any.
   *
   * The page and the API have to share an origin for the browser to let them
   * talk; see `server/Static.ts`. Unset serves the git API alone, which is
   * what a host with no interest in the browser half wants.
   */
  readonly ui?: string;
  /**
   * Development assets mounted on this server's origin.
   *
   * Kept as a factory so Vite can attach its HMR websocket to the already
   * created HTTP server. Production never constructs it and keeps serving
   * `ui` from disk.
   */
  readonly development?: (server: http.Server) => Promise<DevelopmentMiddleware>;
  /**
   * Run each repository's `wake.json` rules when a push moves its hub refs.
   *
   * Off by default, because it is the one option that makes this server start
   * processes. The rules are the operator's own file beside the repository, so
   * nothing a pusher writes chooses what runs — but a server that will run
   * commands at all should have been told to.
   */
  readonly wake?: boolean;
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

/** Header dictionary accepted by Node's `ServerResponse.writeHead`. */
interface NodeHeaders {
  [name: string]: string;
}

/** How many distinct unknown `Host` authorities are worth a log line. */
const REPORT_LIMIT = 16;

/** Addresses that name no particular host: bound to every interface there is. */
const WILDCARD: ReadonlySet<string> = new Set(["0.0.0.0", "::", "[::]", "::0", "[::0]"]);

/**
 * `host:port` as a `Host` header writes it.
 *
 * Bracketed for an IPv6 literal, because that is the form a client sends and
 * the only one `new URL` can parse back: unbracketed, `::1` and port 8080
 * concatenate to `::1:8080`, which matches no header any client will ever send
 * — so a server bound to an IPv6 address could not recognise even its own
 * address as its own.
 */
const authorityOf = (host: string, port: number): string =>
  `${host.includes(":") && !host.startsWith("[") ? `[${host}]` : host}:${port}`;

const nodeHeaders = (headers: Headers): NodeHeaders => {
  const values: NodeHeaders = {};
  headers.forEach((value, name) => {
    values[name] = value;
  });
  return values;
};

export const serve = async (options: ServeOptions): Promise<Server> => {
  const hostname = options.hostname ?? "127.0.0.1";
  /** Only reached by a client that sent no `Host`, which HTTP/1.1 requires. */
  let fallbackAuthority = authorityOf(hostname, options.port ?? 0);

  /**
   * Every public authority a host-bound credential may name, lowercased for
   * the case-insensitive match a `Host` header needs. See
   * `Auth.RequestAudience`: the `Host` header cannot be trusted to say what
   * host a request arrived at, so a delegation's audience is checked against
   * this allowlist rather than the header alone.
   *
   * The server's own bound authority joins it once `listen` has settled the
   * real port, so the two conceptually-one trust sources stay one set rather
   * than a pair of branches a later reader has to reconcile. Configuring none
   * is the common case: a client cloning straight from this server names the
   * bound authority.
   */
  const trustedHosts = new Set((options.hosts ?? []).map((host) => host.toLowerCase()));

  /**
   * Authorities already complained about, so that a client choosing a fresh
   * `Host` per request cannot flood the log — and cannot grow this set without
   * bound either, which is why it stops remembering after `REPORT_LIMIT`.
   */
  const reported = new Set<string>();

  /**
   * Says once, per authority, why a credential is about to be refused.
   *
   * The refusal itself is a generic 401 from the guard — a credential that did
   * not verify — which on its own is indistinguishable from a wrong key and
   * leaves an operator behind a proxy with nothing to go on. This is the line
   * that names the header, so the fix (`--hosts`/`GIT_HOSTS`) is one message
   * away rather than a bisect.
   */
  const reportUnknown = (authority: string): void => {
    if (reported.has(authority) || reported.size > REPORT_LIMIT) return;
    reported.add(authority);
    console.error(
      reported.size > REPORT_LIMIT
        ? `unknown Host authorities are no longer being reported; ${REPORT_LIMIT} were`
        : `refusing host-bound credentials for Host '${authority}': this server answers to ` +
            `${[...trustedHosts].join(", ")}. Pass --hosts (or GIT_HOSTS) naming the public ` +
            `authority if this server is reached under a name it does not bind`,
    );
  };

  /**
   * The audience to check a host-bound credential against: the `Host` header
   * only when it names an authority this server actually answers to, and
   * `null` otherwise — which refuses the credential rather than trusting a
   * header the client (a mirror, in the case that matters) chose.
   *
   * "Actually answers to" is the server's own bound authority — its real
   * `hostname:port`, which it controls, not the client — plus any public
   * authorities configured for a server reached under a different name (behind
   * a proxy, say). A mirror's host is neither, so its replay still fails.
   *
   * A request carrying no `Host` at all is not a client naming a host to be
   * distrusted: HTTP/1.1 requires the header, so this is HTTP/1.0, and the
   * only authority such a request can have arrived at is the one this server
   * bound — the same reading `authority` below already takes of the same
   * condition, and the one the pre-audience guard took by way of the URL.
   */
  const arrivedAudience = (host: string | undefined): string | null => {
    const normalized = (host ?? fallbackAuthority).toLowerCase();
    if (trustedHosts.has(normalized)) return normalized;
    reportUnknown(normalized);
    return null;
  };

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
  }

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
   * What one object may inflate to here; see `Pack.MaxObject`.
   *
   * The portable default is sized for the smallest host there is — a Durable
   * Object's 128 MiB, halved again for the decoder's doubling — and a
   * self-hosted server is not that. Declared rather than inherited, so the
   * first import of a repository with a large blob in its history is not
   * refused by a budget belonging to a machine this is not running on.
   */
  const objectSize = Pack.maxObject(128 * 1024 * 1024);

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

  /**
   * Identity repositories and social logs this host already holds.
   *
   * A target repository's policy cannot reach through its own `Repository`
   * service into a sibling repository. The host owns that composition: each
   * sibling is opened through the same no-hooks layer used by authentication,
   * verified independently, and only then exposed to the policy fold. Missing
   * or malformed repositories are absence (and therefore quarantine), never a
   * reason to take the target repository down.
   */
  const localRepositories = Effect.fn("host.Node.localRepositories")(function* () {
    const entries = yield* Effect.promise(() =>
      fs.readdir(options.root, { withFileTypes: true }).catch(() => []),
    );
    return entries
      .filter((entry) => entry.isDirectory())
      .map((entry) => entry.name)
      .sort()
      .slice(0, 4096);
  });

  interface FederationSnapshot {
    readonly identities: ReadonlyMap<PrincipalId, ResolvedIdentity>;
    readonly logs: ReadonlyArray<SocialLog.VerifiedLog>;
  }

  /**
   * A sibling's identity, on its own runtime.
   *
   * Nested `Effect.provide` merges with the caller's `Repository`, so the
   * ambient one would win and every sibling would look like the target. A
   * fresh runtime is the isolation boundary; this is not inside an Effect
   * generator for that reason.
   */
  const readSibling = (name: string) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const stored = yield* readGenesis();
        if (stored === null) return null;
        const projection = yield* projectTrust(stored.genesis);
        const principal = principalId(stored.genesis.repoId);
        return {
          identity: { principal, projection, head: projection.head } satisfies ResolvedIdentity,
          log: yield* SocialLog.verified(stored.genesis, projection),
        };
      }).pipe(
        Effect.provide(guardLayer(name)),
        Effect.orElseSucceed(() => null),
      ),
    );

  /** Read one sibling once; both federation consumers share this result. */
  const federationEntry = Effect.fn("host.Node.federationEntry")(function* (name: string) {
    return yield* Effect.promise(() => readSibling(name));
  });

  const loadFederation = Effect.fn("host.Node.loadFederation")(function* () {
    const entries = yield* Effect.forEach(yield* localRepositories(), federationEntry, {
      concurrency: 16,
    });
    const identities = new Map<PrincipalId, ResolvedIdentity>();
    const logs: SocialLog.VerifiedLog[] = [];
    for (const entry of entries) {
      if (entry === null) continue;
      // Preserve the resolver's existing first-directory-wins behavior for a
      // duplicate identity without dropping either log from the social view.
      if (!identities.has(entry.identity.principal)) {
        identities.set(entry.identity.principal, entry.identity);
      }
      logs.push(entry.log);
    }
    return { identities, logs } satisfies FederationSnapshot;
  });

  const startFederationLoad = () => Effect.runPromise(loadFederation());

  let federationEpoch = 0;
  let cachedFederation: { readonly epoch: number; readonly value: FederationSnapshot } | null =
    null;
  let loadingFederation: {
    readonly epoch: number;
    readonly value: Promise<FederationSnapshot>;
  } | null = null;

  /**
   * One shared, immutable index for all requests until a mutating request
   * completes. It replaces repeated sibling scans during authentication and
   * policy evaluation while keeping subsequent writes immediately visible.
   */
  const federationSnapshot = Effect.fn("host.Node.federationSnapshot")(function* () {
    const epoch = federationEpoch;
    if (cachedFederation?.epoch === epoch) return cachedFederation.value;
    if (loadingFederation?.epoch === epoch)
      return yield* Effect.promise(() => loadingFederation!.value);

    // Started outside this generator so the in-flight Promise can be shared
    // without `runPromise` appearing inside an Effect context.
    const loading = startFederationLoad();
    loadingFederation = { epoch, value: loading };
    const value = yield* Effect.promise(async () => {
      try {
        const loaded = await loading;
        if (federationEpoch === epoch) cachedFederation = { epoch, value: loaded };
        return loaded;
      } finally {
        if (loadingFederation?.value === loading) loadingFederation = null;
      }
    });
    return value;
  });

  const invalidateFederation = () => {
    federationEpoch++;
    cachedFederation = null;
  };

  const federation = Layer.merge(
    Layer.succeed(Identities)({
      resolve: (wanted) =>
        federationSnapshot().pipe(
          Effect.map((snapshot) => snapshot.identities.get(wanted) ?? null),
        ),
    }),
    Layer.succeed(SocialWeb)({
      logs: federationSnapshot().pipe(Effect.map((snapshot) => snapshot.logs)),
    }),
  );

  /**
   * One repository's layers and its input-gate stand-in.
   *
   * Built the first time a request holds this name, shared with every other
   * in-flight request (including bodies still being written), and released
   * when the last one finishes. A scan over names that need not exist cannot
   * grow a resident set: an idle entry is already gone. Closing the server
   * closes the map's scope, which is what actually runs the router finalizers
   * — dropping the entry without that leaks a file handle per repository.
   */
  const openRepo = (repo: string): RepoState => {
    // The registry lives beside the repository it reports on, so a webhook
    // survives a restart the same way a ref does.
    const subscribers = subscribersFile(path.join(options.root, repo, "webhooks.json"));

    // And the remotes it fetches from, for the same reason and in the same
    // place: a remote that does not survive a restart is a URL somebody has
    // to remember.
    const remotes = remotesFile(path.join(options.root, repo, "remotes.json"));

    // What happens after a push lands: deliver to whoever subscribed, forward
    // to whoever this repository is configured to send to, and — where the
    // operator asked for it — run its wake rules. Built from the root and the
    // repository name in `AfterPush`, because everything it reads lives inside
    // the repository and the CLI needs the same chain: a ref landed by
    // `git+ queue run` is a ref a mirror should hear about too.
    //
    // Detaching, which is this side's answer and the CLI's opposite: delivery
    // must outlive the response rather than hold a push open behind a slow
    // receiver, and this process is not about to exit.
    const directory = path.join(options.root, repo);
    const afterPush =
      options.wake === true
        ? AfterPush.chain({ root: options.root, repo, wake: true })
        : AfterPush.chain({ root: options.root, repo });

    const layer = GitRepository.layer.pipe(
      // Real hooks, not `hooksNoop`: this is what makes a push deliver.
      // `forkDetach` is the node stand-in for `waitUntil` — delivery outlives
      // the response without the push waiting on a slow receiver.
      Layer.provide(afterPush),
      // As `guardLayer` above: `provide` would swallow `Storage`.
      Layer.provideMerge(stores(directory)),
    );

    // Built once while this repository has a request in flight, not once per
    // call. The requester stays *out* of the graph and arrives as a per-request
    // context instead, which is what `toWebHandler`'s second argument is for:
    // a router built per call rebuilds the whole API handler tree and opens a
    // `Scope` nobody ever closes, and one built with the requester baked in
    // would answer every later request as whoever made the first.
    const router = HttpRouter.toWebHandler(
      Api.layer(remotes).pipe(
        Layer.provideMerge(layer),
        Layer.provideMerge(subscribers),
        Layer.provideMerge(openWrites),
      ),
      { disableLogger: true },
    );

    return {
      layer,
      lfs: lfsFile(path.join(options.root, repo, "lfs")),
      api: (request: Request, requester: Context.Context<Auth.Requester>) =>
        // SAFETY: the handler's generated declaration erases its remaining
        // request-scoped service to `unknown`; this context contains exactly
        // that `Requester` service and no value is inspected through the cast.
        router.handler(request, requester as Context.Context<unknown>),
      disposeApi: router.dispose,
      gate: Promise.resolve(),
      delivering: new Set(),
    };
  };

  const { scope, repos } = await Effect.runPromise(
    Effect.gen(function* () {
      const scope = yield* Scope.make();
      const repos = yield* RcMap.make({
        lookup: (repo: string) =>
          Effect.acquireRelease(
            Effect.sync(() => openRepo(repo)),
            (state) =>
              // Caught, not merely detached. A finalizer that rejects becomes
              // an unhandled rejection, and node's default is to turn that
              // into a throw that takes the whole server down — so a file
              // handle this host could not close would stop it serving every
              // repository it holds. Release is housekeeping; it says so and
              // carries on.
              Effect.promise(() =>
                state.disposeApi().catch((cause: unknown) => {
                  console.error(`could not release ${repo}: ${String(cause)}`);
                }),
              ),
          ),
        idleTimeToLive: 0,
      }).pipe(Effect.provideService(Scope.Scope, scope));
      return { scope, repos };
    }),
  );

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
    // The lease that keeps this repository's layers alive: acquired here and
    // released in `finally`, so it spans the handler *and* writing the body.
    // An upload-pack still reads objects as the client consumes it; dropping
    // the router in between would close that store out from under the stream.
    const held = await Effect.runPromise(Scope.make());
    try {
      const state = await Effect.runPromise(
        RcMap.get(repos, repo).pipe(Effect.provideService(Scope.Scope, held)),
      );

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
            Effect.provide(
              Layer.mergeAll(state.layer, requester, openWrites, federation, objectSize),
            ),
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
            Effect.provide(
              Layer.mergeAll(state.layer, requester, openWrites, federation, objectSize),
            ),
          ),
        );
        return matched ?? (await state.api(request, asked));
      };

      const answered = state.gate.then(answer, answer);
      state.gate = answered.then(
        () => undefined,
        () => undefined,
      );

      const response = await answered;
      // Git writes and JSON mutations are POST/PUT/PATCH/DELETE requests.
      // Invalidating after any non-safe request is deliberately conservative:
      // read-only POST endpoints merely refresh the index next time, while a
      // ref update is visible to every following request immediately.
      if (request.method !== "GET" && request.method !== "HEAD") invalidateFederation();

      const delivery = deliver(response);
      // Registered before it is awaited, so a `gc` that arrives mid-body sees it.
      state.delivering.add(delivery);
      try {
        await delivery;
      } finally {
        state.delivering.delete(delivery);
      }
    } finally {
      await Effect.runPromise(Scope.close(held, Exit.void));
    }
  };

  let development: DevelopmentMiddleware | undefined;
  const handleIncoming = (incoming: http.IncomingMessage, outgoing: http.ServerResponse): void => {
    void (async () => {
      // The `Host` header, not the bind address: a handler that has to hand
      // a client an absolute URL back — the LFS batch API does — can only
      // build one that works from the authority the client actually used.
      const authority = incoming.headers.host ?? fallbackAuthority;
      const url = new URL(incoming.url ?? "/", `http://${authority}`);

      // The built UI first, and only where it actually has the file. A miss
      // falls through to the repository routing below, so the API keeps every
      // path it owns and no list of them has to be maintained here. A
      // repository whose name collides with a built asset is shadowed by it —
      // the assets are hashed bundle names and `index.html`, so that is a
      // repository called `index.html`.
      const asset =
        options.ui === undefined
          ? null
          : await assetResponse(options.ui, new Request(url, { method: incoming.method ?? "GET" }));
      if (asset !== null) {
        outgoing.writeHead(asset.status, nodeHeaders(asset.headers));
        outgoing.end(Buffer.from(await asset.arrayBuffer()));
        return;
      }

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
          Effect.provide(
            Layer.mergeAll(
              guardLayer(repo),
              nonces,
              openWrites,
              federation,
              // The audience a host-bound credential is verified against — the
              // `Host` header only where it matches a configured authority,
              // never the raw header, which the client controls.
              Auth.requestAudience(arrivedAudience(incoming.headers.host)),
            ),
          ),
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
        outgoing.writeHead(response.status, nodeHeaders(response.headers));
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
  };

  const server = http.createServer((incoming, outgoing) => {
    if (development !== undefined && (incoming.method === "GET" || incoming.method === "HEAD")) {
      development.handle(incoming, outgoing, (cause) => {
        if (cause === undefined) {
          handleIncoming(incoming, outgoing);
        } else {
          if (!outgoing.headersSent) outgoing.writeHead(500);
          outgoing.end(cause instanceof Error ? cause.message : "development middleware failure");
        }
      });
      return;
    }
    handleIncoming(incoming, outgoing);
  });
  try {
    development = options.development === undefined ? undefined : await options.development(server);

    await new Promise<void>((resolve, reject) => {
      // Rejected on `error` as well as resolved on `listening`. A port already
      // in use — the ordinary mistake, and what a second `serve` on a fixed port
      // does — emitted an `error` nobody had subscribed to: node turned that
      // into an uncaught exception that killed the process, and the promise this
      // awaited never settled either way. Now the caller is told which port and
      // why, and `serve` fails like anything else that cannot start.
      server.once("error", reject);
      server.listen(options.port ?? 0, hostname, () => {
        server.removeListener("error", reject);
        resolve();
      });
    });
  } catch (cause: unknown) {
    await Effect.runPromise(Scope.close(scope, Exit.void));
    throw cause;
  }

  // Known only now, because port 0 means "whichever one is free".
  // SAFETY: the server listens on a TCP port, never a pipe, so `address()`
  // returns an `AddressInfo` once `listen` has resolved.
  const bound = (server.address() as AddressInfo).port;
  fallbackAuthority = authorityOf(hostname, bound);
  // Trusted from here rather than checked separately in `arrivedAudience`:
  // one allowlist, and nothing can have arrived yet to consult it early.
  trustedHosts.add(fallbackAuthority.toLowerCase());

  // A wildcard bind answers to every name the network reaches it by and knows
  // none of them, so the bound authority above is a placeholder no client will
  // ever send — the container-behind-a-proxy topology, where every host-bound
  // credential would otherwise be refused with the reason only visible here.
  if (WILDCARD.has(hostname.toLowerCase()) && trustedHosts.size === 1) {
    console.error(
      `bound to ${hostname}, which names no host a client can send: host-bound credentials ` +
        "will be refused until --hosts (or GIT_HOSTS) names the authority this server is " +
        "reached at, e.g. GIT_HOSTS=git.example.com",
    );
  }

  return {
    url: `http://${authorityOf(hostname, bound)}`,
    close: async () => {
      try {
        await development?.close();
        await new Promise<void>((resolve, reject) => {
          server.close((error) => (error ? reject(error) : resolve()));
        });
      } finally {
        // The routers' scopes live here, not on the HTTP server. Closing
        // without this drops every still-held entry without running its
        // finalizer: one file handle per repository this process opened.
        await Effect.runPromise(Scope.close(scope, Exit.void));
      }
    },
  };
};

export { configuration, resolve as resolveConfiguration } from "./ServeConfig.ts";

if (import.meta.main) {
  // `resolve` rather than `configuration`: it is where `GIT_HOSTS` is parsed
  // and refused, so this entry point and `git+ serve` reject the same value
  // with the same message rather than one of them starting on a list no
  // request can match.
  const options = await Effect.runPromise(resolveConfiguration({}).pipe(Effect.orDie));
  const { url } = await serve(options);
  console.log(`git smart-HTTP server on ${url}, repositories under ${options.root}/`);
}
