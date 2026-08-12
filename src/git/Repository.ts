/**
 * Repository operations.
 *
 * Replaces `GitRepository` (`src/git.repository.ts`), whose ~50 `async` methods
 * throw on failure, so no caller can tell from a signature that `commit` can
 * lose a race. Here every failure is in the type, ref updates go through one
 * compare-and-swap path, and the hooks run inside the same pipeline as the ref
 * update instead of beside it.
 *
 * Storage reads go through this service rather than the stores directly — that
 * is what keeps `ObjectStore`/`RefStore` out of the HTTP handlers' requirements
 * later on. See `docs/rewrite.md`.
 */
import { Context, Effect, Layer, Option, Schedule, Stream } from "effect";
import {
  type HookRejected,
  Invalid,
  ObjectNotFound,
  RefConflict,
  type StorageFailure,
} from "./Error.ts";
import {
  type CommitInfo,
  EMPTY_TREE_OID,
  encodeCommit,
  encodeTree,
  parseCommit,
  parseTree,
  type Signature,
  type TreeEntry,
} from "./Format.ts";
import { ObjectStore, type Oid, RefStore, type RefUpdate } from "./Store.ts";

export interface Commit extends CommitInfo {
  readonly oid: Oid;
}

export interface ReceiveResult {
  readonly ref: string;
  readonly from: Oid | null;
  readonly to: Oid | null;
  readonly ok: boolean;
  readonly reason?: string;
}

export class Repository extends Context.Service<
  Repository,
  {
    readonly refs: Effect.Effect<ReadonlyArray<readonly [string, Oid]>, StorageFailure>;
    readonly resolve: (name: string) => Effect.Effect<Oid | null, StorageFailure>;
    readonly head: Effect.Effect<string, StorageFailure>;

    readonly readCommit: (oid: Oid) => Effect.Effect<CommitInfo, ObjectNotFound | StorageFailure>;
    readonly readTree: (
      oid: Oid,
    ) => Effect.Effect<ReadonlyArray<TreeEntry>, ObjectNotFound | StorageFailure>;

    readonly writeTree: (entries: ReadonlyArray<TreeEntry>) => Effect.Effect<Oid, StorageFailure>;
    readonly writeBlob: (data: Uint8Array) => Effect.Effect<Oid, StorageFailure>;

    readonly commit: (input: {
      readonly branch: string;
      readonly tree: Oid;
      readonly message: string;
      readonly author: Signature;
      readonly committer?: Signature;
      /** `undefined` = whatever the branch is now; `null` = must not exist. */
      readonly expected?: Oid | null;
    }) => Effect.Effect<Oid, RefConflict | ObjectNotFound | StorageFailure | Invalid>;

    readonly log: (
      from: Oid,
      options?: { readonly limit?: number },
    ) => Stream.Stream<Commit, ObjectNotFound | StorageFailure>;

    readonly branch: (input: {
      readonly name: string;
      readonly base: string;
    }) => Effect.Effect<Oid, RefConflict | Invalid | StorageFailure>;

    /** receive-pack's ref phase: hooks, then one all-or-nothing ref update. */
    readonly receive: (
      updates: ReadonlyArray<RefUpdate>,
      options?: { readonly atomic?: boolean },
    ) => Effect.Effect<ReadonlyArray<ReceiveResult>, HookRejected | StorageFailure | Invalid>;
  }
>()("git/Repository") {}

/** Hooks as a service, rather than a mutable registry on the server instance. */
export class Hooks extends Context.Service<
  Hooks,
  {
    readonly preReceive: (updates: ReadonlyArray<RefUpdate>) => Effect.Effect<void, HookRejected>;
    readonly update: (update: RefUpdate) => Effect.Effect<void, HookRejected>;
    readonly postReceive: (results: ReadonlyArray<ReceiveResult>) => Effect.Effect<void>;
  }
>()("git/Hooks") {}

/** No-op hooks, which is what a server without policy wants. */
export const hooksNoop = Layer.succeed(Hooks, {
  preReceive: () => Effect.void,
  update: () => Effect.void,
  postReceive: () => Effect.void,
});

export const layer = Layer.effect(
  Repository,
  Effect.gen(function* () {
    const objects = yield* ObjectStore;
    const refs = yield* RefStore;
    const hooks = yield* Hooks;

    const readTyped = <A>(
      oid: Oid,
      type: "commit" | "tree",
      parse: (data: Uint8Array) => import("effect").Result.Result<A, Invalid>,
    ) =>
      objects.read(oid).pipe(
        Effect.flatMap((object) =>
          object.type === type
            ? Effect.fromResult(parse(object.data)).pipe(
                // A malformed object is indistinguishable from a missing one as
                // far as a caller can act on it.
                Effect.mapError(() => new ObjectNotFound({ oid })),
              )
            : Effect.fail(new ObjectNotFound({ oid })),
        ),
      );

    const readCommit = (oid: Oid) => readTyped(oid, "commit", parseCommit);

    return Repository.of({
      refs: refs.list("refs/"),
      resolve: refs.resolve,
      head: refs.head,

      readCommit,
      readTree: (oid) =>
        oid === EMPTY_TREE_OID ? Effect.succeed([]) : readTyped(oid, "tree", parseTree),

      writeTree: (entries) => objects.write({ type: "tree", data: encodeTree(entries) }),
      writeBlob: (data) => objects.write({ type: "blob", data }),

      commit: Effect.fn("Repository.commit")(
        function* ({ author, branch, committer, expected, message, tree }) {
          const ref = branch.startsWith("refs/") ? branch : `refs/heads/${branch}`;
          const parent = yield* refs.read(ref);

          const oid = yield* objects.write({
            type: "commit",
            data: encodeCommit({
              tree,
              parents: parent === null ? [] : [parent],
              author,
              committer: committer ?? author,
              message,
            }),
          });

          const [result] = yield* refs.apply([
            {
              name: ref,
              value: oid,
              expected: expected === undefined ? parent : expected,
              reason: `commit: ${message.split("\n")[0]}`,
            },
          ]);

          if (result === undefined || !result.applied) {
            return yield* new RefConflict({
              ref,
              expected: expected === undefined ? parent : expected,
              actual: result?.current ?? null,
            });
          }

          return oid;
        },
        // Optimistic concurrency: a caller that did not pin `expected` is
        // saying "append to the branch", so a lost race is retried rather than
        // surfaced. A caller that did pin one gets the conflict.
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
              (commit) => [[{ ...commit, oid }], Option.fromNullishOr(commit.parents[0])] as const,
            ),
          ),
        ).pipe(options?.limit === undefined ? (self) => self : Stream.take(options.limit)),

      branch: Effect.fn("Repository.branch")(function* ({ base, name }) {
        const from = yield* refs.resolve(base);
        if (from === null) {
          return yield* new Invalid({ field: "base", reason: `unknown ref '${base}'` });
        }

        const ref = `refs/heads/${name}`;
        const [result] = yield* refs
          .apply([{ name: ref, value: from, expected: null, reason: `branch: from ${base}` }])
          .pipe(Effect.catchTag("Invalid", (error) => Effect.fail(error)));

        if (result === undefined || !result.applied) {
          return yield* new RefConflict({
            ref,
            expected: null,
            actual: result?.current ?? null,
          });
        }

        return from;
      }),

      receive: Effect.fn("Repository.receive")(function* (updates, options) {
        yield* hooks.preReceive(updates);
        // Per-ref checks are independent, so they run together; one rejection
        // interrupts the rest.
        yield* Effect.forEach(updates, hooks.update, { concurrency: "unbounded" });

        const applied = yield* refs.apply(updates, options);
        const results = applied.map(
          (result, index): ReceiveResult => ({
            ref: result.name,
            from: updates[index]?.expected ?? null,
            to: result.current,
            ok: result.applied,
            ...(result.applied ? {} : { reason: "ref moved" }),
          }),
        );

        yield* hooks.postReceive(results);
        return results;
      }),
    });
  }),
);
