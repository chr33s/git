/**
 * The queue's own records, and what a reader makes of them.
 *
 * Nothing here authorizes anything — the boundary re-derives every merge it
 * accepts and never reads these refs — so what is worth checking is that the
 * record says what happened: which entries are live, in what order, and which
 * candidate belongs to which entry after a head moves or the target does.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { generate, type PrivateKey } from "../crypto/SshSignature.ts";
import { EMPTY_TREE_OID, type Signature } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { ObjectStore } from "../git/Store.ts";
import * as Event from "./Event.ts";
import * as Queue from "./Queue.ts";

const scenario = <A, E>(effect: Effect.Effect<A, E, Repository | ObjectStore>) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provideMerge(stores),
        ),
      ),
    ),
  );

const author: Signature = {
  name: "Runner",
  email: "runner@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const REPO = "SHA256:test";

/** A commit to point events at; what it contains is beside the point here. */
const commitOf = (message: string) =>
  Effect.flatMap(Repository, (repository) =>
    repository.commitTree({ tree: EMPTY_TREE_OID, parents: [], message, author }),
  );

interface World {
  readonly key: PrivateKey;
  readonly queue: string;
}

const opened = Effect.fn("test.opened")(function* (target = "refs/heads/main") {
  const key = yield* generate("runner@example.com");
  const { queue } = yield* Queue.open({ repo: REPO, target, key });
  return { key, queue } satisfies World;
});

describe("hub Queue", () => {
  it("records what a queue is for", async () => {
    const state = await scenario(
      Effect.gen(function* () {
        const world = yield* opened();
        return yield* Queue.project(world.queue);
      }),
    );
    assert.equal(state.exists, true);
    assert.equal(state.target, "refs/heads/main");
    assert.deepEqual(state.entries, []);
  });

  it("keeps entries in the order they were entered", async () => {
    const state = await scenario(
      Effect.gen(function* () {
        const world = yield* opened();
        const first = Event.newId();
        const second = Event.newId();
        for (const [pr, message] of [
          [first, "a"],
          [second, "b"],
        ] as const) {
          yield* Queue.enter({
            repo: REPO,
            queue: world.queue,
            pr,
            head: yield* commitOf(message),
            key: world.key,
          });
        }
        return { state: yield* Queue.project(world.queue), first, second };
      }),
    );
    assert.deepEqual(
      state.state.entries.map((entry) => entry.pr),
      [state.first, state.second],
    );
  });

  it("drops an entry's candidate when its head moves", async () => {
    // A candidate built from the revision before the move is exactly what a
    // moved head invalidates, so re-entering must not leave it attached.
    const state = await scenario(
      Effect.gen(function* () {
        const world = yield* opened();
        const pr = Event.newId();
        const head = yield* commitOf("first");
        yield* Queue.enter({ repo: REPO, queue: world.queue, pr, head, key: world.key });
        yield* Queue.candidate({
          repo: REPO,
          queue: world.queue,
          pr,
          commit: yield* commitOf("candidate"),
          onto: yield* commitOf("onto"),
          branch: "refs/heads/queue/main/1",
          key: world.key,
        });
        const built = yield* Queue.project(world.queue);

        const moved = yield* commitOf("second");
        yield* Queue.enter({ repo: REPO, queue: world.queue, pr, head: moved, key: world.key });
        return { built, after: yield* Queue.project(world.queue), moved };
      }),
    );
    assert.notEqual(state.built.entries[0]?.candidate, null);
    assert.equal(state.after.entries.length, 1);
    assert.equal(state.after.entries[0]?.head, state.moved);
    assert.equal(state.after.entries[0]?.candidate, null);
  });

  it("clears candidates on a reset and keeps the entries", async () => {
    // What a reset invalidates is the chain, not anybody's intention to land.
    const state = await scenario(
      Effect.gen(function* () {
        const world = yield* opened();
        const pr = Event.newId();
        yield* Queue.enter({
          repo: REPO,
          queue: world.queue,
          pr,
          head: yield* commitOf("head"),
          key: world.key,
        });
        yield* Queue.candidate({
          repo: REPO,
          queue: world.queue,
          pr,
          commit: yield* commitOf("candidate"),
          onto: yield* commitOf("onto"),
          branch: "refs/heads/queue/main/1",
          key: world.key,
        });
        yield* Queue.reset({
          repo: REPO,
          queue: world.queue,
          at: yield* commitOf("moved"),
          key: world.key,
        });
        return yield* Queue.project(world.queue);
      }),
    );
    assert.equal(state.entries.length, 1, "still queued");
    assert.equal(state.entries[0]?.candidate, null, "but nothing built on the old tip");
    assert.equal(state.resets, 1);
  });

  it("takes an entry out, and puts it back when it is entered again", async () => {
    const state = await scenario(
      Effect.gen(function* () {
        const world = yield* opened();
        const pr = Event.newId();
        const head = yield* commitOf("head");
        yield* Queue.enter({ repo: REPO, queue: world.queue, pr, head, key: world.key });
        yield* Queue.leave({
          repo: REPO,
          queue: world.queue,
          pr,
          reason: "withdrawn",
          key: world.key,
        });
        const gone = yield* Queue.project(world.queue);
        yield* Queue.enter({ repo: REPO, queue: world.queue, pr, head, key: world.key });
        return { gone, back: yield* Queue.project(world.queue) };
      }),
    );
    assert.deepEqual(state.gone.entries, []);
    assert.deepEqual(state.gone.left, [{ pr: state.gone.left[0]?.pr ?? "", reason: "withdrawn" }]);
    assert.equal(state.back.entries.length, 1, "leaving is undone by saying the opposite");
  });

  it("ignores a candidate for something that is not queued", async () => {
    const state = await scenario(
      Effect.gen(function* () {
        const world = yield* opened();
        yield* Queue.candidate({
          repo: REPO,
          queue: world.queue,
          pr: Event.newId(),
          commit: yield* commitOf("candidate"),
          onto: yield* commitOf("onto"),
          branch: "refs/heads/queue/main/1",
          key: world.key,
        });
        return yield* Queue.project(world.queue);
      }),
    );
    assert.deepEqual(state.entries, []);
    assert.equal(state.ignored.length, 1, "said out loud rather than swallowed");
  });

  it("refuses a record naming something that cannot be an object id", async () => {
    const failure = await scenario(
      Effect.gen(function* () {
        const world = yield* opened();
        const base = yield* Queue.context(REPO, world.queue);
        return yield* Queue.issue(
          { ...base, type: "queue.entered", pr: Event.newId(), head: "../HEAD" },
          world.key,
        ).pipe(Effect.flip);
      }),
    );
    assert.equal(failure._tag, "Invalid");
    assert.match(failure.reason, /hash-qualified object id/);
  });

  it("refuses a record naming something that cannot be a pull request", async () => {
    const failure = await scenario(
      Effect.gen(function* () {
        const world = yield* opened();
        const base = yield* Queue.context(REPO, world.queue);
        return yield* Queue.issue(
          {
            ...base,
            type: "queue.entered",
            pr: "not/one/component",
            head: Event.qualify(yield* commitOf("head")),
          },
          world.key,
        ).pipe(Effect.flip);
      }),
    );
    assert.equal(failure._tag, "Invalid");
    assert.match(failure.reason, /one ref path component/);
  });

  it("finds a queue by the branch it serves", async () => {
    const found = await scenario(
      Effect.gen(function* () {
        const world = yield* opened("refs/heads/release");
        return {
          matching: yield* Queue.forTarget("refs/heads/release"),
          other: yield* Queue.forTarget("refs/heads/main"),
          queue: world.queue,
        };
      }),
    );
    assert.equal(found.matching?.queue, found.queue);
    assert.equal(found.other, null);
  });

  it("names the ref a queue lives on, and reads it back", () => {
    const id = Event.newId();
    assert.equal(Queue.refOf(id), `refs/hub/queue/${id}`);
    assert.equal(Queue.queueOf(Queue.refOf(id)), id);
    assert.equal(Queue.queueOf("refs/hub/pr/x"), null);
    assert.equal(Queue.queueOf("refs/hub/queue/two/parts"), null);
  });

  it("ignores every record nobody signed", async () => {
    // A record nobody signed decides nothing, here as everywhere: an append to
    // this ref needs `hub.queue`, but what the fold acts on is the signature.
    const state = await scenario(
      Effect.gen(function* () {
        const world = yield* opened();
        const base = yield* Queue.context(REPO, world.queue);
        const payload: Queue.QueuePayload = {
          ...base,
          type: "queue.entered",
          pr: Event.newId(),
          head: Event.qualify(yield* commitOf("head")),
        };
        yield* Event.appendTo({
          ref: Queue.refOf(world.queue),
          message: "queue.entered\n",
          payload: Queue.encode(payload),
          signatures: [],
        });
        return yield* Queue.project(world.queue);
      }),
    );
    assert.deepEqual(state.entries, []);
    assert.equal(state.ignored.length, 1);
  });
});
