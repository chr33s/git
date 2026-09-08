import { Context, Effect, Layer } from "effect";
import type { Invalid, StorageFailure } from "./Error.ts";
import type { Oid } from "./Store.ts";

/** Pending merge metadata belongs to a checkout, including a linked worktree. */
export class MergeState extends Context.Service<
  MergeState,
  {
    readonly heads: Effect.Effect<ReadonlyArray<Oid>, Invalid | StorageFailure>;
    /** Clear pending metadata only after publishing the merge commit. */
    readonly clear: Effect.Effect<void, StorageFailure>;
  }
>()("git/MergeState") {}

/** Workspaces without a pending merge, such as the in-memory checkout. */
export const none = Layer.succeed(
  MergeState,
  MergeState.of({
    heads: Effect.succeed([]),
    clear: Effect.void,
  }),
);
