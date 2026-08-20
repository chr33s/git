/**
 * What happens after a ref lands, as one layer.
 *
 * `Hooks` is a single service, so a repository that wants two things to happen
 * after a push — deliver to whoever subscribed *and* forward to a mirror —
 * cannot provide two layers and hope. This is where they are combined, once,
 * for everything that lands a ref through `Repository.receive`.
 *
 * Built from a root and a repository name and nothing else. The subscribers and
 * the remotes are files inside the repository, and the wake rules are too, so a
 * caller that knows where the repository is knows everything this needs — which
 * is what lets the server and the CLI share it instead of the CLI having no
 * chain at all.
 */
import * as path from "node:path";

import { Effect, Fiber, Layer } from "effect";
import { FetchHttpClient, HttpClient } from "effect/unstable/http";

import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { file as remotesFile } from "./Remotes.node.ts";
import * as Remotes from "./Remotes.ts";
import * as Sending from "./Sending.ts";
import { file as subscribersFile } from "./Subscribers.node.ts";
import * as Subscribers from "./Subscribers.ts";
import * as Wake from "./Wake.node.ts";
import * as Webhooks from "./Webhooks.ts";

export interface ChainOptions {
  /** Directory holding one bare repository per subdirectory. */
  readonly root: string;
  readonly repo: string;
  /**
   * Whether landing a ref also runs this repository's wake rules.
   *
   * Off by default, and the default is the interesting half. A wake pass runs
   * the repository's own verbs — `git+ queue run` among them — so a verb that
   * woke the rules that ran it is a cycle, broken only by the dispatcher's
   * bookmark. A server receiving a push from outside is not in that cycle and
   * turns this on; a CLI verb *is* the thing wake would have started, and
   * leaves it off.
   */
  readonly wake?: boolean;
  /**
   * How a receiver's work is run.
   *
   * The default detaches, because on a server delivery must outlive the
   * response rather than hold a push open behind a slow receiver. A process
   * that *exits* wants something else — a detached fork dies with it, so a CLI
   * verb that took the default would report a landing whose webhook was never
   * sent and whose mirror never heard.
   *
   * What it must not be is anything that *waits here*. This runs inside
   * `Repository.receive`, between the ref moving and whatever the caller does
   * next, so a receiver awaited in place holds that window open with the ref
   * already swapped. `deliveries` is the shape that fits: start it here, join
   * it when the verb is done.
   */
  readonly background?: <A, E>(effect: Effect.Effect<A, E>) => Effect.Effect<void>;
}

/**
 * The repository a forward pushes *from*, which must have no hooks of its own.
 *
 * Handed the one it is installed on, a forward would be its own trigger: a push
 * forwards, the forward is a push, and that one forwards again.
 */
const unhooked = (directory: string) =>
  GitRepository.layer.pipe(
    Layer.provide(GitRepository.hooksNoop),
    // `provideMerge`, not `provide`: the stores carry the repository's
    // `Storage` identity, and `provide` consumes a layer's outputs without
    // re-exporting them — so memos keyed on it see `null`, and an origin and
    // its mirror under one root share every entry.
    Layer.provideMerge(stores(directory)),
  );

/**
 * Deliver, forward, and — where the caller asked for it — wake.
 *
 * All of them, not one. Inert where the repository configures none of them:
 * no `webhooks.json` is no subscriber, no `remotes.json` is no mirror, and the
 * chain then costs a list of nothing per landing. That is what makes providing
 * it by default safe for a clone on somebody's laptop.
 */
export const chain = (options: ChainOptions): Layer.Layer<GitRepository.Hooks> => {
  const directory = path.join(options.root, options.repo);
  // Named once and passed to both, rather than left out and defaulted twice
  // inside them: whether a landing waits for its receivers is one decision, and
  // a chain where the webhook waits and the mirror does not is neither answer.
  const background =
    options.background ??
    (<A, E>(effect: Effect.Effect<A, E>) => Effect.forkDetach(effect).pipe(Effect.asVoid));
  return Layer.effect(
    GitRepository.Hooks,
    Effect.gen(function* () {
      const subscribed = yield* Subscribers.Subscribers;
      const client = yield* HttpClient.HttpClient;
      const registry = yield* Remotes.Remotes;
      return GitRepository.hooksAll([
        Webhooks.service({ subscribers: subscribed, client, options: { background } }),
        // The repository a forward pushes from is built when the push lands,
        // not when this layer is: it cannot be a dependency of the hooks the
        // repository itself depends on.
        Sending.service({
          remotes: registry,
          using: (effect) => effect.pipe(Effect.provide(unhooked(directory))),
          options: { background },
        }),
        ...(options.wake === true ? [Wake.service(directory, options.repo)] : []),
      ]);
    }),
  ).pipe(
    Layer.provide(
      Layer.mergeAll(
        subscribersFile(path.join(directory, "webhooks.json")),
        remotesFile(path.join(directory, "remotes.json")),
        FetchHttpClient.layer,
      ),
    ),
  );
};

export interface Collected {
  /** Provide this in the chain's place; it holds what the chain would send. */
  readonly layer: Layer.Layer<GitRepository.Hooks>;
  /** Send everything held, as one. Nothing held is nothing sent. */
  readonly flush: Effect.Effect<void>;
}

/**
 * One announcement per verb, instead of one per ref that moved.
 *
 * A verb can land refs in more than one place — `git+ queue run` moves the
 * branch through `receive` and then appends `pr.merged` and `queue.leave`,
 * which are `setRef`s and announce themselves — and each announcement is a
 * separate push to every mirror. Separate pushes can be separately lost: the
 * first arrives, the second is cut, and the mirror is left holding the merge
 * commit while still showing the pull request that carried it as open. Wrong,
 * rather than behind, and with nothing to retry it — the pass announces only
 * what it changed, and by the next pass the pull request has left the queue.
 *
 * So hold them and send once. What a verb did is one thing that happened, and
 * a receiver hearing it as one thing either has all of it or none of it.
 */
export const collected = (inner: Layer.Layer<GitRepository.Hooks>): Collected => {
  const held: Array<GitRepository.ReceiveResult> = [];
  /**
   * The chain underneath, captured when the layer is built.
   *
   * `flush` runs after the verb, outside the context that held it, so it cannot
   * ask for it then. `null` until something has been built — a verb that never
   * touched the repository has nothing to send and nothing to send it with.
   */
  let downstream: GitRepository.Hooks["Service"] | null = null;

  const layer = Layer.effect(
    GitRepository.Hooks,
    Effect.gen(function* () {
      const real = yield* GitRepository.Hooks;
      downstream = real;
      return {
        // Refusals are not deferred: they decide whether the write happens at
        // all, so they have to be asked while it still can be refused.
        preReceive: real.preReceive,
        update: real.update,
        postReceive: (results: ReadonlyArray<GitRepository.ReceiveResult>) =>
          Effect.sync(() => {
            held.push(...results);
          }),
      };
    }),
  ).pipe(Layer.provide(inner));

  const flush = Effect.suspend(() => {
    const batch = held.splice(0);
    const sink = downstream;
    return batch.length === 0 || sink === null ? Effect.void : sink.postReceive(batch);
  });

  return { layer, flush };
};

export interface Deliveries {
  /** Hand this to `chain` as its `background`. */
  readonly background: <A, E>(effect: Effect.Effect<A, E>) => Effect.Effect<void>;
  /** Wait for what has been started, and stop waiting after `within`. */
  readonly settle: Effect.Effect<void>;
}

/**
 * Start a receiver's work now and wait for it at the end, rather than either.
 *
 * A process that exits cannot detach and forget — the fork dies with the verb,
 * and the landing is reported with nobody told. But it must not simply *await*
 * inside the hook either, and that is the sharper half: `postReceive` runs
 * inside `Repository.receive`, between the ref moving and everything the caller
 * does after it. `queue run` writes `pr.merged` and `queue.leave` there, so a
 * delivery awaited in the hook holds open exactly the window a pass is designed
 * to be interruptible in — and a mirror that black-holes would hold it for as
 * long as the socket lasts, with the branch already swapped and the queue not
 * yet told. The next pass then reads its own landing as somebody else's push.
 *
 * So: fork, which returns immediately and closes that window, and join what was
 * forked once the verb is done. `within` bounds the wait, because a receiver
 * that never answers is a verb that never returns, and this one runs on a wake.
 *
 * Above what the receivers schedule for themselves, so the bound only ever cuts
 * something that is not making progress: webhook delivery is four attempts at a
 * ten-second timeout with jittered backoff between them, which is a little over
 * forty seconds for a subscriber that is answering slowly rather than not at
 * all. A mirror push has no budget of its own — it is a socket — so this is the
 * only thing standing between a dead mirror and a queue that stops.
 */
export const deliveries = (within: `${number} millis` = "45000 millis"): Deliveries => {
  const started: Array<Fiber.Fiber<void, never>> = [];
  /**
   * How far `drain` has got, rather than taking fibers off the list.
   *
   * The list has to survive the wait: giving up on a receiver means
   * *interrupting* it, and a fiber this had already removed to join is one
   * nothing can interrupt afterwards — which is how a bounded wait still left
   * a live socket holding the process open past the verb that printed its
   * result.
   */
  let joined = 0;

  /**
   * Until there is nothing left, not once. A receiver's work can land a ref of
   * its own — a forward is a push — so joining the batch this took can be what
   * starts the next one, and a single pass would exit with that one detached.
   */
  const drain = (): Effect.Effect<void> =>
    Effect.suspend(() => {
      const batch = started.slice(joined);
      joined = started.length;
      return batch.length === 0
        ? Effect.void
        : Effect.forEach(batch, Fiber.join, {
            discard: true,
            concurrency: "unbounded",
          }).pipe(Effect.flatMap(drain));
    });

  return {
    background: (effect) =>
      Effect.forkDetach(
        effect.pipe(
          // Caught here rather than left to the join: a delivery that failed is
          // a warning, and the verb that landed the ref still succeeded.
          Effect.catchCause((cause) => Effect.logWarning("delivery failed", cause)),
          Effect.asVoid,
        ),
      ).pipe(
        Effect.flatMap((fiber) =>
          Effect.sync(() => {
            started.push(fiber);
          }),
        ),
      ),
    settle: drain().pipe(
      Effect.timeout(within),
      // Interrupted, not merely stopped waiting for. A timeout that only
      // abandons the *wait* leaves the fiber running, and a fiber holding a
      // socket open holds the process open with it — so the verb printed its
      // result and then hung anyway, which is the thing this bound exists to
      // prevent. Interrupting is what closes the socket, and it is why the push
      // path passes its abort signal through to `fetch`.
      Effect.catchCause(() =>
        Effect.forEach(started, Fiber.interrupt, {
          discard: true,
          concurrency: "unbounded",
        }).pipe(
          Effect.flatMap(() => Effect.logWarning(`gave up waiting for delivery after ${within}`)),
        ),
      ),
      Effect.asVoid,
    ),
  };
};
