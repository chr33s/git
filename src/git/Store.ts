/**
 * Storage ports.
 *
 * The ports are addressed by git concepts rather than file paths, and atomic
 * ref update is part of `RefStore` rather than an optional extra — so every
 * backend has to answer for it and callers never branch on whether a method
 * exists.
 */
import { Context, Effect, type Layer, Option, Stream } from "effect";
import { Invalid, type ObjectNotFound, type StorageFailure } from "./Error.ts";

/** A 40-char lowercase hex object id. Branded so a ref name cannot pass as one. */
export type Oid = string & { readonly Oid: unique symbol };
export type ObjectType = "blob" | "tree" | "commit" | "tag";

export const isOid = (value: string): value is Oid => /^[0-9a-f]{40}$/.test(value);

export interface RawObject {
  readonly type: ObjectType;
  readonly data: Uint8Array;
}

/**
 * Which repository this store *is*, as far as the host is concerned.
 *
 * Not its identity in the trust sense — that is the genesis, and a mirror
 * shares it. This is the thing that differs between an origin and its mirror
 * sitting side by side under one `serve --root`: the directory, the Durable
 * Object, the map. Anything memoised across requests on a host that serves
 * both has to key on it, because everything else about them can agree — the
 * genesis bytes, the RepoID, the ref names, and right after a replication the
 * ref values too — while what they can actually *read* does not, since refs
 * are applied without a connectivity check. Cached under a shared key, the
 * answer computed for the replica that is missing objects was served for the
 * origin: a revocation folded away, an exclusion set computed for the wrong
 * repository, an approved pull request filtered out of a protected-branch push.
 */
export class Storage extends Context.Service<Storage, string>()("git/Storage") {}

/**
 * The storage identity in force, or `null` where none was provided.
 *
 * `null` is what every store layer here supplies one to avoid, and it keeps a
 * caller that builds a `Repository` by hand working exactly as it did — one
 * unnamed repository per process, which is the assumption that was already
 * being made.
 */
export const storageOf = Effect.fnUntraced(function* () {
  return Option.getOrElse(yield* Effect.serviceOption(Storage), () => null);
});

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
    readonly list: Stream.Stream<Oid, StorageFailure>;
    /**
     * Object storage this repository lends to, or borrows from, others.
     *
     * Optional because only a backend that can share objects has an answer —
     * `Node` through git's `alternates`, nothing else today. `gc` is the one
     * caller: it must not collect what a borrower still reaches, and it must
     * not repack what it only borrowed.
     */
    readonly shared?: Effect.Effect<
      { readonly borrowers: ReadonlyArray<string>; readonly alternates: ReadonlyArray<string> },
      StorageFailure
    >;
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
  /**
   * Why it was not applied, when that is not "somebody else moved it".
   *
   * A ref the store could not write — `refs/heads/x` where `refs/heads/x/y`
   * is a directory, a full disk — reads as a lost race without this, and the
   * client retries a push that cannot succeed.
   */
  readonly reason?: string;
}

export interface ReflogEntry {
  readonly from: Oid | null;
  readonly to: Oid | null;
  readonly at: Date;
  readonly message: string;
}

/**
 * Whether a name can address anything other than a ref.
 *
 * This half is a boundary rather than a convention. A ref name arrives from
 * the network and reaches a backend joined onto the repository root — as a
 * path segment (`Node`, `Opfs`) or an object key (`Cloudflare`) — and that
 * root also holds `HEAD`, `objects/`, `logs/` and the host's own registries.
 * Confining the name to `refs/`, with no traversal and no empty component, is
 * what keeps a push inside the ref namespace: `..` alone would not, since
 * `HEAD` and `objects/ab/cdef…` contain none.
 */
export const checkRefAddress = (name: string): string | null => {
  if (name.length === 0) return "empty ref name";
  if (!name.startsWith("refs/") || name.length === "refs/".length) {
    return "must name something under 'refs/'";
  }
  if (name.endsWith("/") || name.includes("//")) return "empty path component";
  if (name.includes("..")) return "contains '..'";
  for (const character of name) {
    const code = character.codePointAt(0)!;
    if (code < 0x20 || code === 0x7f) return "contains a control character";
  }
  for (const component of name.split("/")) {
    if (component.startsWith(".")) return "path component starts with '.'";
  }
  return null;
};

/**
 * git's `check-ref-format`, in the port rather than in each backend, so every
 * backend refuses the same names because the rule lives in one place.
 *
 * Returns why the name is bad, or `null` when it is fine.
 */
export const checkRefName = (name: string): string | null => {
  const addresses = checkRefAddress(name);
  if (addresses !== null) return addresses;
  if (name.endsWith(".")) return "ends with '.'";
  if (name.includes("@{")) return "contains '@{'";
  // `~^:?*[` and backslash are git's reserved set; a space would make the name
  // unquotable in the protocol's own framing.
  if (/[ ~^:?*[\\]/.test(name)) return "contains a reserved character";
  for (const component of name.split("/")) {
    if (component.endsWith(".lock")) return "path component ends with '.lock'";
  }
  return null;
};

/**
 * `checkRefName` over a batch, as the failure `RefStore.apply` reports.
 *
 * A deletion is held only to the addressing rules. A name this version would
 * refuse to create may already exist — written by an older build, or by other
 * tooling in a `Node` repository — and refusing to delete it would leave a ref
 * nothing can remove, pinning every object it reaches forever.
 */
export const checkRefNames = (updates: ReadonlyArray<RefUpdate>): Effect.Effect<void, Invalid> =>
  Effect.suspend(() => {
    for (const update of updates) {
      const problem =
        update.value === null ? checkRefAddress(update.name) : checkRefName(update.name);
      if (problem !== null) {
        return Effect.fail(
          new Invalid({ field: "ref", reason: `bad ref name '${update.name}': ${problem}` }),
        );
      }
    }
    return Effect.void;
  });

/**
 * The same rules for the other writer of a ref name.
 *
 * `setHead` takes its target from a caller — a default branch in a create
 * request, an argument on the command line — and every backend turns it into
 * a path the same way `apply` does, so it needs the same guard.
 */
export const checkHeadTarget = (target: string): Effect.Effect<void, Invalid> =>
  Effect.suspend(() => {
    const problem = checkRefName(target);
    return problem === null
      ? Effect.void
      : Effect.fail(new Invalid({ field: "head", reason: `bad ref name '${target}': ${problem}` }));
  });

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
    readonly setHead: (target: string) => Effect.Effect<void, StorageFailure | Invalid>;
    readonly reflog: (name: string) => Effect.Effect<ReadonlyArray<ReflogEntry>, StorageFailure>;
    /**
     * Every ref that has a reflog, including refs `list` no longer returns.
     *
     * Deleting a branch does not delete the record of where it was, and that
     * is the whole value of the record: `gc` protects what a recent reflog
     * entry names, and the branch somebody deleted by mistake is exactly the
     * case where "which refs exist now" is the wrong question to ask.
     */
    readonly logged: Effect.Effect<ReadonlyArray<string>, StorageFailure>;
  }
>()("git/RefStore") {}

/** What a server needs. No staging area: a bare repository has no work tree. */
export type ServerStores = ObjectStore | RefStore;
export type ServerStoreLayer = Layer.Layer<ServerStores>;

/**
 * Span wrappers for a port implementation.
 *
 * The names live here rather than in each backend for two reasons: a trace
 * reads `Repository.commit → Cloudflare.RefStore.apply` with the same
 * vocabulary whichever storage is loaded, and a new backend cannot forget to
 * name itself — it wraps or it does not, and the contract suite runs the
 * wrapped form either way.
 *
 * `Effect.fn` rather than `Effect.withSpan` for the method forms: it carries
 * the call-site stack frame as well as the span. `list` is a `Stream` and
 * `head` is a plain `Effect`, so those take the combinator that fits.
 */
export const tracedObjectStore = (
  backend: string,
  store: ObjectStore["Service"],
): ObjectStore["Service"] => {
  const traced = {
    read: Effect.fn(`${backend}.ObjectStore.read`)(store.read),
    readStream: Effect.fn(`${backend}.ObjectStore.readStream`)(store.readStream),
    write: Effect.fn(`${backend}.ObjectStore.write`)(store.write),
    has: Effect.fn(`${backend}.ObjectStore.has`)(store.has),
    delete: Effect.fn(`${backend}.ObjectStore.delete`)(store.delete),
    list: store.list.pipe(Stream.withSpan(`${backend}.ObjectStore.list`)),
  };
  // `shared` is forwarded only when the backend has one: it is optional on the
  // port, and adding the key as `undefined` is not the same as leaving it out.
  return store.shared === undefined ? traced : { ...traced, shared: store.shared };
};

export const tracedRefStore = (
  backend: string,
  store: RefStore["Service"],
): RefStore["Service"] => ({
  read: Effect.fn(`${backend}.RefStore.read`)(store.read),
  resolve: Effect.fn(`${backend}.RefStore.resolve`)(store.resolve),
  list: Effect.fn(`${backend}.RefStore.list`)(store.list),
  apply: Effect.fn(`${backend}.RefStore.apply`)(store.apply),
  head: store.head.pipe(Effect.withSpan(`${backend}.RefStore.head`)),
  setHead: Effect.fn(`${backend}.RefStore.setHead`)(store.setHead),
  reflog: Effect.fn(`${backend}.RefStore.reflog`)(store.reflog),
  logged: store.logged.pipe(Effect.withSpan(`${backend}.RefStore.logged`)),
});
