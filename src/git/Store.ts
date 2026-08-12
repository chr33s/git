/**
 * Storage ports.
 *
 * The ports are addressed by git concepts rather than file paths, and atomic
 * ref update is part of `RefStore` rather than an optional extra — so every
 * backend has to answer for it and callers never branch on whether a method
 * exists.
 */
import { Context, type Effect, type Layer, type Stream } from "effect";
import type { Invalid, ObjectNotFound, StorageFailure } from "./Error.ts";

/** A 40-char lowercase hex object id. Branded so a ref name cannot pass as one. */
export type Oid = string & { readonly Oid: unique symbol };
export type ObjectType = "blob" | "tree" | "commit" | "tag";

export const isOid = (value: string): value is Oid => /^[0-9a-f]{40}$/.test(value);

export interface RawObject {
  readonly type: ObjectType;
  readonly data: Uint8Array;
}

/** Content-addressed object storage. Immutable, so no compare-and-swap needed. */
export class ObjectStore extends Context.Service<
  ObjectStore,
  {
    readonly read: (oid: Oid) => Effect.Effect<RawObject, ObjectNotFound | StorageFailure>;
    readonly readStream: (
      oid: Oid,
    ) => Effect.Effect<Stream.Stream<Uint8Array, StorageFailure>, ObjectNotFound | StorageFailure>;
    readonly write: (object: RawObject) => Effect.Effect<Oid, StorageFailure>;
    readonly has: (oid: Oid) => Effect.Effect<boolean, StorageFailure>;
    readonly delete: (oid: Oid) => Effect.Effect<void, StorageFailure>;
    readonly list: () => Stream.Stream<Oid, StorageFailure>;
  }
>()("git/ObjectStore") {}

export interface RefUpdate {
  readonly name: string;
  /** `null` deletes the ref. */
  readonly value: Oid | null;
  /** `undefined` means "don't care"; `null` means "must not exist". */
  readonly expected?: Oid | null;
  readonly reason?: string;
}

export interface RefUpdateResult {
  readonly name: string;
  readonly applied: boolean;
  readonly current: Oid | null;
}

export interface ReflogEntry {
  readonly from: Oid | null;
  readonly to: Oid | null;
  readonly at: Date;
  readonly message: string;
}

/**
 * Mutable ref namespace.
 *
 * `apply` is the only writer and is all-or-nothing when `atomic` is set, which
 * is what turns receive-pack's atomic capability into a parameter instead of a
 * second code path.
 */
export class RefStore extends Context.Service<
  RefStore,
  {
    readonly read: (name: string) => Effect.Effect<Oid | null, StorageFailure>;
    /** Follows symbolic refs (`HEAD` -> `refs/heads/main` -> oid). */
    readonly resolve: (name: string) => Effect.Effect<Oid | null, StorageFailure>;
    readonly list: (
      prefix?: string,
    ) => Effect.Effect<ReadonlyArray<readonly [string, Oid]>, StorageFailure>;
    readonly apply: (
      updates: ReadonlyArray<RefUpdate>,
      options?: { readonly atomic?: boolean },
    ) => Effect.Effect<ReadonlyArray<RefUpdateResult>, StorageFailure | Invalid>;
    readonly head: Effect.Effect<string, StorageFailure>;
    readonly setHead: (target: string) => Effect.Effect<void, StorageFailure>;
    readonly reflog: (name: string) => Effect.Effect<ReadonlyArray<ReflogEntry>, StorageFailure>;
  }
>()("git/RefStore") {}

/** What a server needs. No staging area: a bare repository has no work tree. */
export type ServerStores = ObjectStore | RefStore;
export type ServerStoreLayer = Layer.Layer<ServerStores>;
