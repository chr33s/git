/**
 * A Cloudflare Artifacts binding implementation, backed by this server.
 *
 * See [`docs/artifacts-provider.md`](../../docs/artifacts-provider.md) for the
 * evaluation. In short: Artifacts is a git host and so is this, so the protocol
 * half is free; what is missing is a namespace registry, tokens, and fork.
 * Those are named here as ports (`Registry`, `Tokens`) rather than glossed —
 * the point of the sketch is to show exactly how much is *not* git.
 *
 * Alchemy ships three implementations of the R2 binding (native, HTTP, local)
 * and exactly one of Artifacts — the native one. This would be the local and
 * self-hosted one.
 *
 * Requires `@cloudflare/workers-types/experimental` for the `ArtifactsRepo` /
 * `ArtifactsCreateRepoResult` shapes, which alchemy already depends on.
 */
import * as Alchemy from "alchemy/Cloudflare";
import { Context, Effect, Layer } from "effect";
import { RepoHost } from "../host/Host.ts";

/**
 * The registry Cloudflare has implicitly and we do not.
 *
 * Repos are Durable Objects addressed by name, and DO namespaces cannot be
 * enumerated — so `list({ limit, cursor })` has nothing to read from. One index
 * DO (or a D1 table) holding a row per repo closes the gap and gives the
 * metadata fields somewhere to live at the same time.
 */
export class Registry extends Context.Service<
  Registry,
  {
    readonly create: (
      name: string,
      meta: RepoMeta,
    ) => Effect.Effect<RepoRecord, AlreadyExists | Invalid>;
    readonly get: (name: string) => Effect.Effect<RepoRecord | null>;
    readonly list: (options?: {
      readonly limit?: number;
      readonly cursor?: string;
    }) => Effect.Effect<{
      readonly repos: ReadonlyArray<RepoRecord>;
      readonly total: number;
      readonly cursor?: string;
    }>;
    readonly delete: (name: string) => Effect.Effect<boolean>;
    readonly touch: (name: string, at: Date) => Effect.Effect<void>;
  }
>()("artifacts/Registry") {}

export interface RepoMeta {
  readonly description: string | null;
  readonly defaultBranch: string;
  readonly readOnly: boolean;
  /** `github:owner/repo` for imports, `artifacts:ns/repo` for forks, else null. */
  readonly source: string | null;
}

export interface RepoRecord extends RepoMeta {
  readonly id: string;
  readonly name: string;
  readonly createdAt: Date;
  readonly updatedAt: Date;
  readonly lastPushAt: Date | null;
}

/**
 * The other thing Cloudflare has and we have none of: auth.
 *
 * Today nothing in `src/` reads an `Authorization` header — the server is open
 * by construction. Artifacts wants scoped, TTL'd, revocable per-repo tokens
 * returned in plaintext exactly once.
 *
 * The compatibility detail worth writing down: `git` sends credentials as HTTP
 * Basic, so verification has to accept the token as the *password* field, not
 * only as a bearer token. Miss it and `git clone` fails while `curl` works.
 */
export class Tokens extends Context.Service<
  Tokens,
  {
    readonly issue: (
      repo: string,
      scope: "read" | "write",
      ttlSeconds: number,
    ) => Effect.Effect<{ readonly id: string; readonly plaintext: string; readonly expiresAt: Date }>;
    readonly list: (
      repo: string,
    ) => Effect.Effect<ReadonlyArray<{ readonly id: string; readonly scope: "read" | "write" }>>;
    readonly revoke: (repo: string, tokenOrId: string) => Effect.Effect<boolean>;
    /** Used by the middleware on both the JSON API and the smart-HTTP routes. */
    readonly verify: (
      repo: string,
      presented: string,
    ) => Effect.Effect<"read" | "write" | null>;
  }
>()("artifacts/Tokens") {}

/**
 * `fork`, the cheap way: git's own alternates.
 *
 * The fork's row points at its parent and its object store starts empty, with
 * reads falling through until the first write. In the sketch's terms that is an
 * `ObjectStore` decorator — nothing above the port changes, and
 * `defaultBranchOnly` becomes "copy one ref instead of all of them".
 */
export declare const alternates: (parent: string) => Layer.Layer<never>;

/**
 * The binding implementation.
 *
 * `Layer.effect(ReadWriteNamespace, ...)` is the same shape alchemy's own
 * `ReadWriteBucketLocal` uses, so this drops into a stack as
 * `Effect.provide(Artifacts.ReadWriteNamespaceLocal)`.
 */
export declare const ReadWriteNamespaceLocal: Layer.Layer<
  Alchemy.Artifacts.ReadWriteNamespace,
  never,
  Registry | Tokens | RepoHost
>;

/**
 * What the implementation looks like per method — written out because the
 * mapping, not the code, is the deliverable of this sketch.
 *
 *   create(name, opts)  -> Registry.create + init the repo at opts.setDefaultBranch
 *                          + Tokens.issue(name, "write", 86400)
 *   get(name)           -> Registry.get, NOT_FOUND when null
 *   list(opts)          -> Registry.list, cursor passed straight through
 *   delete(name)        -> Registry.delete + purge the repo's R2 prefix and DO
 *   import({source})    -> create, then the existing clone/fetch path with
 *                          depth/branch, tracked as IMPORT_IN_PROGRESS
 *   repo.fork(name)     -> Registry.create with source, alternates(parent)
 *   repo.createToken    -> Tokens.issue
 *   repo.listTokens     -> Tokens.list
 *   repo.revokeToken    -> Tokens.revoke
 *
 * Two `raw` fields cannot be honoured off-platform. The namespace-level one is
 * an `Effect`, so it dies with guidance — the precedent is alchemy's own
 * `LocalR2Gateway`, which does exactly that. `RepoClient.raw` is an eager
 * property of type `ArtifactsRepo`, which no third-party provider can produce
 * and cannot defer either; making it an `Effect` like its sibling is the one
 * upstream change this needs.
 */
/** Both would be `Schema.TaggedError`s alongside the others in `git/Error.ts`. */
declare class AlreadyExists {
  readonly _tag: "AlreadyExists";
}
declare class Invalid {
  readonly _tag: "Invalid";
}
