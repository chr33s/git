/**
 * The host port.
 *
 * Worth stating plainly, because it shapes everything below: alchemy@next has
 * no provider-neutral `Alchemy.Worker` / `Alchemy.DurableObject`. Both are
 * Cloudflare resources — `Alchemy.Worker(...)` and `Alchemy.DurableObject(...)`
 * come from `alchemy/Cloudflare`. What alchemy *does* give you across providers
 * is the request shape: a Worker's `serve` and `alchemy/Http`'s `NodeHttpServer`
 * / `BunHttpServer` both take the same `HttpEffect`.
 *
 * So portability is not something the framework hands over — it is this file.
 * `RepoHost` names the three things a git server actually needs from its host,
 * and each host implements them with whatever it has:
 *
 *   | capability | Cloudflare                    | node / bun                  |
 *   | ---------- | ----------------------------- | --------------------------- |
 *   | isolation  | one Durable Object per repo   | one directory per repo      |
 *   | serialize  | the DO input gate (free)      | a `Semaphore` per repo      |
 *   | background | `state.waitUntil`             | `Effect.forkDaemon`         |
 *
 * Above this line — `server/App.ts`, `git/*` — nothing names a provider.
 */
import { Context, type Effect, type Layer } from "effect";
import type { ServerStores } from "../git/Store.ts";

export class RepoHost extends Context.Service<
  RepoHost,
  {
    /** One repository's storage, as a layer the caller provides where it likes. */
    readonly stores: (name: string) => Layer.Layer<ServerStores>;

    /**
     * Serialize an effect against every other caller for the same repository.
     *
     * This is the requirement that makes a git server a git server: two
     * concurrent pushes to one ref must not interleave between the compare and
     * the swap. On Workers the DO input gate already provides it, so this is
     * the identity function; elsewhere it costs a semaphore.
     *
     * It is deliberately separate from `stores`: isolation and mutual exclusion
     * are different guarantees, and a host that gets one for free should not
     * have to fake the other.
     */
    readonly serialize: <A, E, R>(
      name: string,
      effect: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E, R>;

    /**
     * Work that must outlive the response — webhook delivery, gc sweeps.
     * The host decides what keeps it alive.
     */
    readonly background: <A, E>(effect: Effect.Effect<A, E>) => Effect.Effect<void>;
  }
>()("host/RepoHost") {}

/** What a host module exports. Cloudflare and node both satisfy this. */
export type Host = Layer.Layer<RepoHost>;
