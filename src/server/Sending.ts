/**
 * What a repository sends on, on its own, when a push lands.
 *
 * §25's standing instruction: a remote configured `push` or `mirror` gets the
 * refs this repository just accepted, without anybody asking. The trigger is
 * `post-receive` — the one hook that runs after the refs are durable, which is
 * exactly when there is something true to send.
 *
 * Its own module rather than part of `Replication.ts` for the reason
 * `Webhooks.ts` is separate: this is a *hook layer*, and a hook layer owns a
 * fork and an outbound request, while `Replication` is the pure half a caller
 * drives. Keeping them apart is what lets a test drive replication without a
 * scheduler and configure a scheduler without a network.
 *
 * Replication failure never rolls back the write that caused it. A push that
 * has been accepted is accepted: the refs are durable, the client has been
 * told so, and a remote that is down is the remote's problem. So this runs
 * detached and logs, exactly as delivery does.
 */
import { Effect, Layer } from "effect";

import { push, type PushRef } from "../client/Push.ts";
import { Hooks, type ReceiveResult, Repository } from "../git/Repository.ts";
import { type Remote, Remotes, sends } from "./Remotes.ts";

/**
 * Which of a push's refs a remote's standing instruction covers.
 *
 * An empty pattern list is everything, which is what makes `{mode: "push"}`
 * mean what it looks like it means. A ref this push *deleted* is carried too:
 * a mirror that only ever adds is not a mirror, it is a graveyard.
 */
const matches = (pattern: string, ref: string): boolean =>
  pattern.endsWith("*") ? ref.startsWith(pattern.slice(0, -1)) : ref === pattern;

export const covered = (
  remote: Remote,
  results: ReadonlyArray<ReceiveResult>,
): ReadonlyArray<ReceiveResult> => {
  const patterns = remote.sync?.refs ?? [];
  return results.filter(
    (result) =>
      result.ok &&
      (patterns.length === 0 || patterns.some((pattern) => matches(pattern, result.ref))),
  );
};

/** A `push` call under construction: a credential is present or it is not. */
interface PushRequest {
  url: string;
  refs: ReadonlyArray<PushRef>;
  force: boolean;
  token?: string;
}

/** Send one push's worth of refs to one remote. */
const forward = Effect.fn("Sending.forward")(function* (
  remote: Remote,
  results: ReadonlyArray<ReceiveResult>,
) {
  const carried = covered(remote, results);
  if (carried.length === 0) return;

  // Sent as the *value* the push landed, not as the ref's name. A forward is
  // detached from the push that caused it, so by the time it runs a ref may
  // have moved again or been deleted — and `push` resolves a name at send
  // time and fails the whole call when one is gone, which dropped every other
  // ref in the batch on account of one. An oid cannot go stale that way: what
  // is forwarded is exactly what was accepted, and a later push forwards the
  // later value.
  const refs: PushRef[] = carried.map((result) =>
    result.to === null
      ? { local: result.ref, remote: result.ref, delete: true }
      : { local: result.to, remote: result.ref },
  );

  // Never forced. A standing instruction is not a licence to overwrite what
  // the other side has: a ref that will not fast-forward there is a divergence
  // for a person, which is the same answer `reconcile` gives a branch.
  const request: PushRequest = { url: remote.url, refs, force: false };
  if (remote.credential !== null) request.token = remote.credential;

  yield* push(request);
});

export interface SendingOptions {
  /** Swapped in tests for something that runs inline and can be awaited. */
  readonly background?: <A, E>(effect: Effect.Effect<A, E>) => Effect.Effect<void>;
}

/**
 * Hooks that forward on `post-receive`.
 *
 * Composed with whatever other hooks a host wants: `Hooks` is one service, so
 * a host that also delivers webhooks merges the two rather than choosing.
 */
/**
 * The forwarder itself, for a host that composes it with other hooks.
 *
 * Taken as values rather than from context because `Hooks` is one service: a
 * host that also delivers webhooks has to build both and combine them, and it
 * cannot do that by providing two layers.
 *
 * `using` supplies the repository to push *from*, and supplies it at
 * post-receive time rather than at layer-build time. Both halves of that
 * matter. The repository must be one built with **no hooks** — handed the one
 * this is installed on, a forward would be its own trigger — and it must not
 * be a dependency of the hook layer, because the hook layer is a dependency of
 * the repository: asking for one while building the other is a knot Effect
 * unties by giving somebody a second instance, and the webhook registry went
 * quiet on the side that lost.
 */
export const service = (input: {
  readonly remotes: Remotes["Service"];
  readonly using: <A, E>(effect: Effect.Effect<A, E, Repository>) => Effect.Effect<A, E>;
  readonly options?: SendingOptions;
}): Hooks["Service"] => {
  const { options, remotes: registry, using } = input;
  const background =
    options?.background ??
    (<A, E>(effect: Effect.Effect<A, E>) => Effect.forkDetach(effect).pipe(Effect.asVoid));

  return Hooks.of({
    preReceive: () => Effect.void,
    update: () => Effect.void,
    postReceive: (results) =>
      background(
        Effect.gen(function* () {
          const configured = (yield* registry.list.pipe(Effect.orElseSucceed(() => []))).filter(
            sends,
          );
          yield* Effect.forEach(
            configured,
            (remote) =>
              forward(remote, results).pipe(
                Effect.catchCause((cause) =>
                  Effect.logWarning(`replication to ${remote.name} failed`, cause),
                ),
              ),
            { concurrency: 4, discard: true },
          );
        }).pipe(
          using,
          Effect.catchCause((cause) => Effect.logWarning("replication failed", cause)),
        ),
      ),
  });
};

/**
 * The forwarder as a layer, for a caller with a repository already in hand.
 *
 * Not what a host installs — see `service` for why it cannot be — but exactly
 * what a test wants, where the repository being pushed from is the one under
 * test and there is no cycle to tie.
 */
export const hooks = (options?: SendingOptions): Layer.Layer<Hooks, never, Remotes | Repository> =>
  Layer.effect(
    Hooks,
    Effect.gen(function* () {
      const registry = yield* Remotes;
      const repository = yield* Repository;
      const using = <A, E>(effect: Effect.Effect<A, E, Repository>) =>
        effect.pipe(Effect.provideService(Repository, repository));
      if (options === undefined) return service({ remotes: registry, using });
      return service({ remotes: registry, using, options });
    }),
  );
