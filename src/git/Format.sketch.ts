/**
 * Wire format codecs — the part that does NOT get rewritten.
 *
 * `git.object.ts`, `git.delta.ts`, `git.index.ts`, `git.merge.ts` and
 * `git.utils.ts` are ~2.6k lines of pure, synchronous byte manipulation with
 * no I/O in them. Effect adds nothing to a function that turns a `Uint8Array`
 * into a commit; wrapping them would only cost allocation per call and make
 * the diff enormous.
 *
 * So they port over as-is, with two edits:
 *   1. functions that `throw` return `Result` (or the caller wraps at the
 *      boundary with `Effect.try`), so the failure shows up in a type;
 *   2. anything that reached for storage moves out into `Repository`.
 *
 * This file is the seam: pure below, effectful above.
 */
import { Effect, Result } from "effect";
import { Invalid, PackCorrupt } from "./Error.sketch.ts";
import type { ObjectType, Oid, RawObject } from "./Store.sketch.ts";

/** Pure — same body as today's `parseCommit`, returning a Result. */
export declare const parseCommit: (data: Uint8Array) => Result.Result<CommitInfo, Invalid>;
export declare const parseTree: (
  data: Uint8Array,
) => Result.Result<ReadonlyArray<TreeEntry>, Invalid>;
export declare const encodeCommit: (commit: CommitInfo) => Uint8Array;
export declare const encodeTree: (entries: ReadonlyArray<TreeEntry>) => Uint8Array;
export declare const applyDelta: (
  base: Uint8Array,
  delta: Uint8Array,
) => Result.Result<Uint8Array, PackCorrupt>;

export interface CommitInfo {
  readonly tree: Oid;
  readonly parents: ReadonlyArray<Oid>;
  readonly author: Signature;
  readonly committer: Signature;
  readonly message: string;
}

export interface Signature {
  readonly name: string;
  readonly email: string;
  readonly at: Date;
  readonly offset: number;
}

export interface TreeEntry {
  readonly mode: number;
  readonly name: string;
  readonly oid: Oid;
}

/**
 * Hashing is the one codec that is genuinely effectful — Web Crypto is async
 * everywhere the client runs. `Effect.promise` because a SHA-1 of bytes in
 * memory cannot fail for reasons a caller could act on.
 */
export const hashObject = (object: RawObject): Effect.Effect<Oid> =>
  Effect.promise(async () => {
    const header = new TextEncoder().encode(`${object.type} ${object.data.length}\0`);
    const payload = new Uint8Array(header.length + object.data.length);
    payload.set(header);
    payload.set(object.data, header.length);
    const digest = await crypto.subtle.digest("SHA-1", payload);
    return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("") as Oid;
  });

/**
 * Lifting a pure codec into an effect is `Effect.fromResult` — no wrapper
 * needed. Call sites read `yield* Effect.fromResult(parseCommit(bytes))`.
 */

export type { ObjectType };
