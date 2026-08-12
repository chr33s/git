/**
 * Storage ports.
 *
 * Three narrow services, addressed by git concepts instead of paths. Atomic
 * ref update is part of `RefStore`, not an optional extra, so the OPFS/node
 * layers have to answer for it and callers never branch on whether a method
 * exists.
 */
import { Context, type Effect, type Layer, type Stream } from "effect";
import type { Invalid, ObjectNotFound, StorageFailure } from "./Error.sketch.ts";

/**
 * The ports are deliberately `R = never`.
 *
 * Alchemy's Cloudflare bindings return effects requiring `RuntimeContext` — it
 * carries the `env`/`ExecutionContext` of the workerd invocation. Threading
 * that through the port types was the first thing tried here, and the
 * typechecker rejected it for the right reason: it followed `Repository` into
 * the CLI and the test suite, neither of which runs on Workers.
 *
 * So the Cloudflare layer discharges it instead — see
 * `adapters/Cloudflare.ts`, which captures the invocation context once and
 * provides it to every method. The cost is that the layer is built per
 * invocation rather than per instance; the benefit is that nothing above the
 * adapter knows Cloudflare exists.
 */

export type Oid = string & { readonly Oid: unique symbol };
export type ObjectType = "blob" | "tree" | "commit" | "tag";

export interface RawObject {
  readonly type: ObjectType;
  readonly data: Uint8Array;
}

/** Content-addressed object storage. Immutable; no CAS needed. */
export class ObjectStore extends Context.Service<
  ObjectStore,
  {
    readonly read: (oid: Oid) => Effect.Effect<RawObject, ObjectNotFound | StorageFailure>;
    /** Large blobs never materialize: R2 hands back a stream, OPFS a file stream. */
    readonly readStream: (
      oid: Oid,
    ) => Effect.Effect<Stream.Stream<Uint8Array, StorageFailure>, ObjectNotFound | StorageFailure>;
    readonly write: (object: RawObject) => Effect.Effect<Oid, StorageFailure>;
    readonly writeStream: (
      type: ObjectType,
      body: Stream.Stream<Uint8Array, StorageFailure>,
    ) => Effect.Effect<Oid, StorageFailure>;
    readonly has: (oid: Oid) => Effect.Effect<boolean, StorageFailure>;
    readonly delete: (oid: Oid) => Effect.Effect<void, StorageFailure>;
    readonly list: () => Stream.Stream<Oid, StorageFailure>;
  }
>()("git/ObjectStore") {}

export interface RefUpdate {
  readonly name: string;
  /** `null` deletes. */
  readonly value: Oid | null;
  /** `undefined` = don't care, `null` = must not exist. */
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
 * Mutable ref namespace. `apply` is the only writer, and it is all-or-nothing:
 * receive-pack's `atomic` capability becomes a parameter rather than a
 * different code path.
 */
export class RefStore extends Context.Service<
  RefStore,
  {
    readonly read: (name: string) => Effect.Effect<Oid | null, StorageFailure>;
    readonly resolve: (name: string) => Effect.Effect<Oid | null, StorageFailure>;
    readonly list: (prefix?: string) => Effect.Effect<ReadonlyArray<[string, Oid]>, StorageFailure>;
    readonly apply: (
      updates: ReadonlyArray<RefUpdate>,
      options?: { readonly atomic?: boolean },
    ) => Effect.Effect<ReadonlyArray<RefUpdateResult>, StorageFailure | Invalid>;
    readonly head: Effect.Effect<string, StorageFailure>;
    readonly setHead: (target: string) => Effect.Effect<void, StorageFailure>;
    readonly reflog: (name: string) => Effect.Effect<ReadonlyArray<ReflogEntry>, StorageFailure>;
  }
>()("git/RefStore") {}

/** Staging area. Kept separate because the server never needs it. */
export class IndexStore extends Context.Service<
  IndexStore,
  {
    readonly entries: Effect.Effect<ReadonlyArray<IndexEntry>, StorageFailure>;
    readonly stage: (entry: IndexEntry) => Effect.Effect<void, StorageFailure>;
    readonly unstage: (path: string) => Effect.Effect<void, StorageFailure>;
    readonly clear: Effect.Effect<void, StorageFailure>;
  }
>()("git/IndexStore") {}

export interface IndexEntry {
  readonly path: string;
  readonly oid: Oid;
  readonly mode: number;
  readonly size: number;
  readonly mtime: Date;
}

/**
 * The layer set every runtime has to supply.
 *
 * `ServerStores` is the subset a server needs — no staging area, because a bare
 * repository has no work tree. Splitting it keeps the Cloudflare host from
 * having to invent an `IndexStore` it would never call.
 */
export type ServerStores = ObjectStore | RefStore;
export type Stores = ServerStores | IndexStore;
export type StoreLayer<E = never> = Layer.Layer<Stores, E>;
