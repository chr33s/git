/**
 * Derived bundle artifacts, separate from Git object storage.
 *
 * Git objects and refs are repository truth. Bundles are a cache of that
 * truth, addressed by their own keys, and a crash before the manifest is
 * published must leave only an unreferenced file — never an advertised
 * pointer at incomplete bytes.
 */
import { Context, Effect, Layer, Stream } from "effect";

import { StorageFailure } from "../git/Error.ts";
import { bytesToHex, concatBytes } from "../git/Format.ts";
import { Sha1 } from "../git/Sha1.ts";
import type { BundleManifest } from "./BundleFormat.ts";

export interface BundleWrite {
  readonly objectId: string;
  readonly bytes: number;
  readonly checksum: string;
}

export interface BundleStat {
  readonly objectId: string;
  readonly bytes: number;
  readonly checksum: string;
}

export class BundleStore extends Context.Service<
  BundleStore,
  {
    readonly list: (repo: string) => Effect.Effect<BundleManifest | null, StorageFailure>;
    readonly publish: (
      repo: string,
      manifest: BundleManifest,
    ) => Effect.Effect<void, StorageFailure>;

    readonly stat: (id: string) => Effect.Effect<BundleStat | null, StorageFailure>;
    readonly read: (
      id: string,
      range?: { readonly offset: number; readonly length: number },
    ) => Effect.Effect<Stream.Stream<Uint8Array, StorageFailure>, StorageFailure>;
    readonly write: (
      id: string,
      source: Stream.Stream<Uint8Array, StorageFailure>,
    ) => Effect.Effect<BundleWrite, StorageFailure>;
    readonly move: (from: string, to: string) => Effect.Effect<void, StorageFailure>;
    readonly delete: (id: string) => Effect.Effect<void, StorageFailure>;
    /** Every stored object id, advertised or not — what prune walks. */
    readonly listIds: (repo: string) => Effect.Effect<ReadonlyArray<string>, StorageFailure>;
  }
>()("server/BundleStore") {}

const missing = (operation: string, id: string) =>
  new StorageFailure({ operation, path: id, cause: "no such bundle artifact" });

/** In-memory backend, for the contract suite and for tests. */
export const memory = (): BundleStore["Service"] => {
  const artifacts = new Map<string, Uint8Array>();
  const checksums = new Map<string, string>();
  const manifests = new Map<string, BundleManifest>();

  return BundleStore.of({
    list: (repo) => Effect.sync(() => manifests.get(repo) ?? null),
    publish: (repo, manifest) =>
      Effect.sync(() => {
        manifests.set(repo, manifest);
      }),
    stat: (id) =>
      Effect.sync(() => {
        const bytes = artifacts.get(id);
        const checksum = checksums.get(id);
        return bytes === undefined || checksum === undefined
          ? null
          : { objectId: id, bytes: bytes.length, checksum };
      }),
    read: (id, range) =>
      Effect.sync(() => {
        const bytes = artifacts.get(id);
        if (bytes === undefined) {
          return Stream.fail(missing("bundles.read", id));
        }
        const slice =
          range === undefined ? bytes : bytes.subarray(range.offset, range.offset + range.length);
        return Stream.fromIterable([new Uint8Array(slice)]);
      }),
    write: (id, source) =>
      Effect.gen(function* () {
        const hash = new Sha1();
        const chunks: Uint8Array[] = [];
        yield* Stream.runForEach(source, (chunk) =>
          Effect.sync(() => {
            hash.update(chunk);
            chunks.push(chunk);
          }),
        );
        const bytes = concatBytes(chunks);
        const checksum = bytesToHex(hash.digest());
        artifacts.set(id, bytes);
        checksums.set(id, checksum);
        return { objectId: id, bytes: bytes.length, checksum };
      }),
    move: (from, to) =>
      Effect.sync(() => {
        if (from === to) return;
        const bytes = artifacts.get(from);
        const checksum = checksums.get(from);
        if (bytes === undefined || checksum === undefined) return;
        artifacts.set(to, bytes);
        checksums.set(to, checksum);
        artifacts.delete(from);
        checksums.delete(from);
      }),
    delete: (id) =>
      Effect.sync(() => {
        artifacts.delete(id);
        checksums.delete(id);
      }),
    listIds: () => Effect.sync(() => [...artifacts.keys()]),
  });
};

export const memoryLayer = Layer.sync(BundleStore, memory);
