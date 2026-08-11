/**
 * Repository operations.
 *
 * Today: `GitRepository` (874 lines) is constructed with a storage instance and
 * a config object, and every one of its ~50 methods is `async` returning a
 * value or throwing. Callers cannot tell from a signature whether `commit` can
 * fail with a ref conflict, and the receive-pack path re-implements
 * compare-and-swap because `writeRef` does not.
 *
 * Sketch: a service whose methods declare what they can fail with. The
 * interesting change is not the syntax — it is that ref updates, hooks and
 * webhooks become one transactional pipeline instead of three sequential
 * `await`s with partial-failure holes between them.
 */
import { Context, Effect, Layer, Option, Schedule, Stream } from "effect";
import {
  type HookRejected,
  type Invalid,
  ObjectNotFound,
  RefConflict,
  type StorageFailure,
} from "./Error.ts";
import { decode, type CommitInfo, type Signature, parseCommit } from "./Format.ts";
import { ObjectStore, type Oid, RefStore, type RefUpdate } from "./Store.ts";

export class Repository extends Context.Service<
  Repository,
  {
    readonly commit: (input: {
      readonly branch: string;
      readonly tree: Oid;
      readonly message: string;
      readonly author: Signature;
      readonly expected?: Oid | null;
    }) => Effect.Effect<Oid, RefConflict | ObjectNotFound | StorageFailure | Invalid>;

    readonly log: (
      from: Oid,
      options?: { readonly limit?: number },
    ) => Stream.Stream<CommitInfo & { readonly oid: Oid }, ObjectNotFound | StorageFailure>;

    /** receive-pack's ref update phase; runs hooks, all-or-nothing when atomic. */
    readonly receive: (
      updates: ReadonlyArray<RefUpdate>,
      options?: { readonly atomic?: boolean },
    ) => Effect.Effect<ReadonlyArray<ReceiveResult>, HookRejected | StorageFailure | Invalid>;

    readonly gc: (options?: {
      readonly grace?: `${number} minutes`;
    }) => Effect.Effect<{ readonly removed: number }, StorageFailure>;
  }
>()("git/Repository") {}

export interface ReceiveResult {
  readonly ref: string;
  readonly from: Oid | null;
  readonly to: Oid | null;
  readonly ok: boolean;
  readonly reason?: string;
}

/** Hooks are a service, not a mutable registry on the DO instance. */
export class Hooks extends Context.Service<
  Hooks,
  {
    readonly preReceive: (
      updates: ReadonlyArray<RefUpdate>,
    ) => Effect.Effect<void, HookRejected>;
    readonly update: (update: RefUpdate) => Effect.Effect<void, HookRejected>;
    readonly postReceive: (
      results: ReadonlyArray<ReceiveResult>,
    ) => Effect.Effect<void, never>;
  }
>()("git/Hooks") {}

export const layer = Layer.effect(
  Repository,
  Effect.gen(function* () {
    const objects = yield* ObjectStore;
    const refs = yield* RefStore;
    const hooks = yield* Hooks;

    const readCommit = (oid: Oid) =>
      objects.read(oid).pipe(
        Effect.flatMap((object) =>
          object.type === "commit"
            ? decode(parseCommit(object.data)).pipe(
                // A commit that will not parse is indistinguishable from a
                // missing one as far as a caller is concerned.
                Effect.mapError(() => new ObjectNotFound({ oid })),
              )
            : Effect.fail(new ObjectNotFound({ oid })),
        ),
      );

    return {
      commit: ({ author, branch, expected, message, tree }) =>
        Effect.gen(function* () {
          const ref = `refs/heads/${branch}`;
          const parent = yield* refs.read(ref);
          const data = encodeCommitBytes({
            tree,
            parents: parent ? [parent] : [],
            author,
            committer: author,
            message,
          });
          const oid = yield* objects.write({ type: "commit", data });

          const [result] = yield* refs.apply([
            { name: ref, value: oid, expected: expected ?? parent, reason: `commit: ${message}` },
          ]);
          // The conflict is a value here, so the retry policy below is a
          // composition rather than a hand-rolled loop.
          if (!result?.applied) {
            return yield* new RefConflict({
              ref,
              expected: expected ?? parent,
              actual: result?.current ?? null,
            });
          }
          return oid;
        }).pipe(
          // Optimistic concurrency: re-read the parent and retry a few times
          // before surfacing the conflict. Today this is a caller problem.
          Effect.retry({
            while: (error) => error._tag === "RefConflict",
            times: 3,
            schedule: Schedule.exponential("10 millis"),
          }),
        ),

      log: (from, options) =>
        Stream.paginate(from, (oid) =>
          readCommit(oid).pipe(
            Effect.map(
              (commit) =>
                [[{ ...commit, oid }], Option.fromNullishOr(commit.parents[0])] as const,
            ),
          ),
        ).pipe(options?.limit === undefined ? (self) => self : Stream.take(options.limit)),

      receive: (updates, options) =>
        Effect.gen(function* () {
          yield* hooks.preReceive(updates);
          // Per-ref `update` hooks run concurrently — they are pure policy
          // checks — but a rejection cancels its siblings.
          yield* Effect.forEach(updates, hooks.update, { concurrency: "unbounded" });

          const applied = yield* refs.apply(updates, options);
          const results = applied.map((result, index): ReceiveResult => ({
            ref: result.name,
            from: updates[index]?.expected ?? null,
            to: result.current,
            ok: result.applied,
          }));

          // post-receive (webhooks, notifications) must not hold the
          // response. Forking into the request scope keeps the work alive
          // past the reply without blocking the client.
          yield* Effect.forkScoped(hooks.postReceive(results));
          return results;
        }).pipe(Effect.scoped),

      gc: () => Effect.succeed({ removed: 0 }),
    };
  }),
);

declare const encodeCommitBytes: (commit: CommitInfo) => Uint8Array;
