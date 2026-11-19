/**
 * The Worker: a router that resolves `/:repo` to a DO stub and forwards.
 * Same job as `git/Durable.ts`'s default export, with one difference —
 * `repos` is a typed stub derived from the DO class, so a renamed method is
 * a compile error rather than a 500 at the edge.
 *
 * Its own module because the bundler requires this file's *default export*
 * to be the worker layer, and `alchemy.run.ts`'s default export must be the
 * stack. The class stays importable on its own; consumers that bind it do
 * not pull in `.make()` — the bundler tree-shakes it.
 */
import type { RuntimeContext } from "alchemy";
import * as Alchemy from "@alchemy.run/cloudflare";
import { Config, Effect, Layer, Option, Schema } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/http";

import { objectStore, packStore } from "./git/Cloudflare.ts";
import * as GitRepository from "./git/Repository.ts";
import { Storage } from "./git/Store.ts";
import Repos from "./host/Cloudflare.ts";
import { Repo } from "./host/Cloudflare.ts";
import * as CloudflareRequest from "./host/CloudflareRequest.ts";
import * as CloudflareAdmission from "./host/CloudflareAdmission.ts";
import * as CloudflareRate from "./host/CloudflareRate.ts";
import * as BackupAdmin from "./host/CloudflareBackupAdmin.ts";
import { Invalid } from "./git/Error.ts";
import * as RequestUpload from "./host/RequestUpload.ts";
import { normalize, routeOf, UI_HOME, UI_PREFIX } from "./server/Route.ts";
import * as SnapshotCapacity from "./server/SnapshotCapacity.ts";
import * as Snapshot from "./server/Snapshot.ts";
import * as RequestLimits from "./server/RequestLimits.ts";
import { Objects } from "./objects.ts";
import { keyOf as provisioningKey } from "./server/Provisioning.ts";

/**
 * Public requests use fetch. Backup administration is private service-binding
 * RPC only; the HTTP router never dispatches this method.
 *
 * `RuntimeContext` is in the requirements because the stateless read path
 * resolves the R2 binding per request (`bucket.raw` is handler-coloured by
 * design); the bridge provides it, exactly as it does inside the DO.
 */
export type GitBindings = {
  readonly backup: (request: BackupAdmin.Request) => Effect.Effect<string, Invalid, RuntimeContext>;
  readonly fetch: Effect.Effect<
    HttpServerResponse.HttpServerResponse,
    never,
    HttpServerRequest.HttpServerRequest | RuntimeContext
  >;
};

/** `Repo` in the third slot: this Worker hosts the DO, so it is contract. */
export class Git extends Alchemy.Worker<Git, GitBindings, Repo>()("git") {}

/**
 * Pinned here and asserted against `wrangler.test.json` in
 * `alchemy.run.test.ts`: the runtime the integration suite proves must be
 * the runtime this Worker deploys.
 */
export const compatibility = {
  date: "2025-12-10",
  // Even an allowlisted same-zone URL must use the public security/routing path.
  flags: ["nodejs_compat", "global_fetch_strictly_public"],
};

/** Retain application signals without automatic request URL/header logging. */
export const observability = {
  enabled: true,
  redactQueryString: true,
  logs: { enabled: true, invocationLogs: false, headSamplingRate: 1, persist: true },
  traces: { enabled: false },
};

/** Vite's `base`, and the asset manifest's prefix. Trailing slash required. */
const UI_BASE = `${UI_PREFIX}/`;

/**
 * The one page every client route under `/hub` resolves to.
 *
 * The directory, not `index.html` by name: the asset layer's default
 * `html_handling` (`auto-trailing-slash`) answers `/hub/index.html` with a 307
 * to `/hub/`, and handing that redirect back would send every deep link to
 * the default screen instead of the one it names.
 */
const UI_ENTRY = UI_BASE;

/**
 * What the page at `/` loaded before the UI moved under `/hub`.
 *
 * That page was served `immutable` for a year, so a returning browser still
 * holds it and still asks for its bundle here. Answered with a module that
 * sends the tab to the new home — hash included, which `index.html` rewrites
 * from the old `#/screen` form — rather than routing `main.js` to a repository
 * of that name. Only for a script load, so a git client addressing a
 * repository called `main.js` is unaffected.
 */
const LEGACY_ENTRY = "/main.js";
const LEGACY_REDIRECT = `location.replace(${JSON.stringify(UI_HOME)} + location.search + location.hash);\n`;

/**
 * The static-asset fetcher, bound because this Worker declares `assets`.
 *
 * Declared here rather than threaded through `GitBindings`: the binding is
 * the asset layer's own, added by the platform when assets are configured,
 * and this handler needs exactly one method from it.
 */
interface AssetFetcher {
  readonly fetch: (request: Request) => Promise<Response>;
}

export default Git.make(
  {
    main: import.meta.url,
    dev: {
      port: Config.schema(
        Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(65535)),
        "GIT_DEV_PORT",
      ).pipe(Config.withDefault(1337)),
      strictPort: Config.Boolean("GIT_DEV_STRICT_PORT").pipe(Config.withDefault(false)),
    },
    compatibility,
    observability,
    // The Vite+ UI build (`vp build` → `dist/ui`) rides along as the
    // Worker's static assets, so the page and its requests to `/:repo/...`
    // stay same-origin — the arrangement the UI's readme promises. Routing
    // is assets-first: a request matching a file (the entry page, `main.js`,
    // a hashed chunk) is answered by the asset layer and never invokes this
    // script; everything else falls through to the router below. Cache rules
    // ship as `dist/ui/_headers`, written by the build. The build fails if
    // the entry outputs are missing, so a deploy cannot publish the API with
    // no UI behind it.
    //
    // The generated directory contains hub/* and root _headers. This makes
    // the public paths identical in Alchemy's deploy and local runtimes; the
    // local asset provider does not implement the deploy manifest's `base`.
    // Keep SPA fallback disabled so Git paths cannot become HTML responses.
    assets: { directory: "dist/worker-assets" },
  },
  Effect.gen(function* () {
    const repos = yield* Repo;
    const rate = yield* CloudflareRate.configuration().pipe(Effect.orDie);
    const limiter = Option.isSome(rate.namespace)
      ? yield* Alchemy.Workers.RateLimit("GIT_CLIENT_RATE", {
          namespaceId: rate.namespace.value,
          simple: { limit: rate.limit, period: 60 },
        })
      : undefined;
    const snapshotBytes = yield* SnapshotCapacity.configuration.pipe(Effect.orDie);
    const budgets = yield* RequestLimits.configuration().pipe(Effect.orDie);
    const requestLimits = Layer.succeed(RequestLimits.RequestLimits)(budgets);
    const admit = yield* CloudflareAdmission.make("CloudflareEdge").pipe(Effect.orDie);
    const uploadIdleMs = yield* RequestUpload.configuration().pipe(Effect.orDie);
    const bucket = yield* Alchemy.R2.ReadWriteBucket(Objects);

    // Read once, with the other bindings, rather than per request: this is
    // the Worker's own environment and it does not change between them.
    // SAFETY: `assets` is configured above, so the platform binds `ASSETS`;
    // `WorkerEnvironment` is the untyped binding record it arrives in, and
    // `AssetFetcher` is the one method of it this handler calls.
    const assets = (yield* Alchemy.WorkerEnvironment)["ASSETS"] as AssetFetcher;

    /**
     * Serve an anonymous `git-upload-pack` read from R2 alone, or `null`.
     *
     * The refs come from the snapshot the Durable Object publishes before it
     * acknowledges any ref-moving request (`Snapshot.ts`); the objects and
     * packs are already in R2. Between them this Worker can answer the
     * advertisement and cut the pack without ever waking the repository's
     * single writer — which is where clone traffic at agent scale stops
     * being the writer's problem. Anything this path cannot answer — no
     * snapshot yet, restricted reads, a credential, a write, any failure at
     * all — falls through to the Durable Object, which is always right.
     */
    const fromSnapshot = (
      r2: R2Bucket,
      repo: string,
      request: Request,
    ): Effect.Effect<Response | null> =>
      Effect.gen(function* () {
        // An operator-supplied bootstrap artifact must reach the writer even
        // when an earlier anonymous read published an empty public snapshot.
        if ((yield* Effect.promise(() => r2.head(provisioningKey(repo)))) !== null) return null;
        const snapshot = yield* Snapshot.load(
          Effect.promise(async () => {
            const held = await r2.get(Snapshot.keyOf(repo));
            return held === null ? null : await SnapshotCapacity.read(held, snapshotBytes);
          }),
        );
        if (snapshot === null || !snapshot.anonymousRead) return null;

        const stores = Layer.mergeAll(
          objectStore(r2, repo),
          Snapshot.refStore(snapshot),
          Layer.succeed(Storage)(repo),
        ).pipe(Layer.provideMerge(packStore(r2, repo)));
        return yield* Snapshot.serve(request).pipe(
          Effect.provide(
            GitRepository.layer.pipe(
              Layer.provide(GitRepository.hooksNoop),
              Layer.provideMerge(stores),
            ),
          ),
        );
      }).pipe(
        Effect.provide(requestLimits),
        // Exhausting a request budget is a refusal, not a reason to repeat
        // the same traversal in the Durable Object.
        Effect.catchTag("RequestLimitExceeded", (error) =>
          Effect.succeed(
            Response.json(
              {
                _tag: error._tag,
                budget: error.budget,
                limit: error.limit,
              },
              { status: 413, headers: { "cache-control": "no-store" } },
            ),
          ),
        ),
        // Failures before a Response is returned fall back to the writer.
        // A later streaming-body failure cannot safely replay that response.
        Effect.catchCause(() =>
          Effect.logWarning("snapshot response failed; authoritative reads required", {
            component: "SnapshotReader",
            operation: "serve",
          }).pipe(Effect.as(null)),
        ),
      );

    return {
      backup: Effect.fn("Cloudflare.backup")(function* (input: BackupAdmin.Request) {
        const invalid = () => new Invalid({ field: "backup", reason: "backup proxy rejected" });
        const request = yield* Schema.decodeEffect(BackupAdmin.Request)(input).pipe(
          Effect.mapError(invalid),
        );
        const target = yield* Effect.try({
          try: () => repos.get(repos.idFromString(request.objectId)),
          catch: invalid,
        });
        return yield* target.backup(request).pipe(Effect.mapError(invalid));
      }),
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const url = new URL(request.url, "http://x");

        // The UI routes on the client, so only its entry page and its bundles
        // exist as files. Every deeper address — a file path on the Code
        // screen, a Change Request id — matches no asset and arrives here,
        // and is answered with that same page: the bundle reads
        // `location.pathname` and renders the screen the address names.
        if (url.pathname === UI_PREFIX || url.pathname.startsWith(UI_BASE)) {
          const page = yield* Effect.promise(() =>
            assets.fetch(new Request(new URL(UI_ENTRY, url), { headers: request.headers })),
          );
          return HttpServerResponse.raw(page);
        }

        if (
          url.pathname === LEGACY_ENTRY &&
          request.method === "GET" &&
          request.headers["sec-fetch-dest"] === "script"
        ) {
          return HttpServerResponse.text(LEGACY_REDIRECT, {
            contentType: "text/javascript",
            headers: { "cache-control": "no-store" },
          });
        }

        const route = routeOf(url.pathname);
        if (route === null) {
          // The root belongs to the UI, not to a repository: send it to the
          // screen the shell opens by default rather than 404 at the door.
          if (url.pathname === "/") {
            return HttpServerResponse.empty({
              status: 302,
              headers: { location: UI_HOME },
            });
          }
          // The asset layer already served everything that matches a file, so
          // a path naming neither an asset nor a repository is simply not
          // found, not a malformed API call.
          return HttpServerResponse.text("not found", { status: 404 });
        }

        // SAFETY: the effect wrapper was built from the platform request
        // (`HttpServerRequest.fromWeb`), so `source` is that same `Request`,
        // headers and body intact — not a reconstruction. Normalised so the
        // DO sees one spelling of the path whichever the client used.
        const raw = normalize(request.source as Request, route);

        // Anonymous clone and fetch traffic is served from R2 when the
        // repository's published snapshot allows it; see `fromSnapshot`.
        if (Snapshot.readable(raw)) {
          // SAFETY: Alchemy's R2 binding and the generated worker runtime
          // types disagree but describe the same runtime object — the same
          // seam `host/Cloudflare.ts` crosses, in the same single place.
          const r2: R2Bucket = (yield* bucket.raw) as never;
          const served = yield* fromSnapshot(r2, route.repo, raw);
          if (served !== null) return HttpServerResponse.raw(served);
        }

        // Auth no longer lives at the edge. It used to, because a shared
        // secret is something an edge can hold; repository authority is not —
        // it is the genesis and membership log inside the repository, which
        // only the Durable Object can read. So the guard moved in there, and
        // this Worker is a router again.

        return yield* repos.getByName(route.repo).fetch(HttpServerRequest.fromWeb(raw));
      }).pipe((request) =>
        Effect.gen(function* () {
          const incoming = yield* HttpServerRequest.HttpServerRequest;
          // SAFETY: the bridge retains the platform Request as source.
          const original = incoming.source as Request;
          // oxlint-disable-next-line effecttsgo/abort-controller-in-effect -- Aborted on demand by the admission callback, not at scope close, so Effect.abortSignal does not apply.
          const cancellation = new AbortController();
          const upload = RequestUpload.open(
            original,
            AbortSignal.any([original.signal, cancellation.signal]),
            uploadIdleMs,
          );
          const bounded = upload.request;
          const signal = bounded.signal;
          const context = yield* Effect.context<never>();
          return yield* CloudflareRequest.respond(
            admit(
              Effect.gen(function* () {
                if (limiter !== undefined) {
                  const refused = yield* CloudflareRate.check(bounded, yield* limiter.raw);
                  if (refused !== null) return HttpServerResponse.raw(refused);
                }
                return yield* request;
              }).pipe(
                Effect.provideService(
                  HttpServerRequest.HttpServerRequest,
                  HttpServerRequest.fromWeb(bounded),
                ),
                Effect.map((response) =>
                  HttpServerResponse.toWeb(response, {
                    signal,
                    context,
                    withoutBody: incoming.method === "HEAD",
                  }),
                ),
              ),
              () => cancellation.abort(),
              Effect.promise(() => upload.idle),
            ).pipe(Effect.ensuring(Effect.sync(upload.close)), Effect.map(HttpServerResponse.raw)),
            signal,
            "CloudflareEdge",
          );
        }),
      ),
    };
    // The host Worker's layer also provides its DO's live implementation —
    // that is what makes `yield* Repo` above resolve — and the bucket binding
    // implementation serves both phases, exactly as it does inside the DO.
  }).pipe(
    Effect.provide([Repos, Alchemy.R2.ReadWriteBucketBinding, Alchemy.Workers.RateLimitBinding]),
  ),
);
