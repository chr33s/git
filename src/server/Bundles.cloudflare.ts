/**
 * Bundle artifacts in R2. Manifest and maintenance state sit beside them.
 *
 *   <repo>/bundles/<family>/<token>-<checksum>.bundle
 *   <repo>/bundles/manifest.json
 *   <repo>/gitplus/state.json
 *
 * Ports stay `R = never`: the bucket is captured when the layer is built.
 */
import { Effect, Layer, Stream } from "effect";

import { StorageFailure } from "../git/Error.ts";
import { bytesToHex } from "../git/Format.ts";
import { Sha1 } from "../git/Sha1.ts";
import { decodeManifest, encodeManifest } from "./BundleFormat.ts";
import { BundleStore } from "./BundleStore.ts";
import { MaintenanceMeta, parseRecord } from "./MaintenanceRun.ts";

export interface CloudflareBundles {
  readonly bucket: R2Bucket;
  readonly repo: string;
}

const failed = (operation: string, path: string) => (cause: unknown) =>
  new StorageFailure({ operation, path, cause });

export const r2Store = (options: CloudflareBundles): BundleStore["Service"] => {
  const prefix = `${options.repo}/bundles/`;
  const manifestKey = `${prefix}manifest.json`;
  const keyOf = (id: string) => `${prefix}${id}`;

  return BundleStore.of({
    list: () =>
      Effect.tryPromise({
        try: async () => {
          const object = await options.bucket.get(manifestKey);
          if (object === null) return null;
          return decodeManifest(await object.text());
        },
        catch: failed("bundles.list", manifestKey),
      }),

    publish: (_repo, manifest) =>
      Effect.tryPromise({
        try: () => options.bucket.put(manifestKey, encodeManifest(manifest)),
        catch: failed("bundles.publish", manifestKey),
      }),

    stat: (id) =>
      Effect.tryPromise({
        try: async () => {
          const found = await options.bucket.head(keyOf(id));
          if (found === null) return null;
          const checksum =
            found.customMetadata?.["checksum"] ??
            id
              .split("-")
              .at(-1)
              ?.replace(/\.bundle$/, "") ??
            "";
          return { objectId: id, bytes: found.size, checksum };
        },
        catch: failed("bundles.stat", keyOf(id)),
      }),

    read: (id, range) =>
      Effect.tryPromise({
        try: () =>
          range === undefined
            ? options.bucket.get(keyOf(id))
            : options.bucket.get(keyOf(id), {
                range: { offset: range.offset, length: range.length },
              }),
        catch: failed("bundles.read", keyOf(id)),
      }).pipe(
        Effect.flatMap((object) =>
          object === null || object.body === null
            ? Effect.succeed(
                Stream.fail(new StorageFailure({ operation: "bundles.read", path: keyOf(id) })),
              )
            : Effect.succeed(
                Stream.fromReadableStream({
                  evaluate: () => {
                    // SAFETY: R2 serves an object's bytes; the runtime types
                    // declare `body` without a chunk type.
                    return object.body as ReadableStream<Uint8Array>;
                  },
                  onError: failed("bundles.read", keyOf(id)),
                }),
              ),
        ),
      ),

    write: (id, source) =>
      Effect.gen(function* () {
        const hash = new Sha1();
        let bytes = 0;
        const hashed = source.pipe(
          Stream.tap((chunk) =>
            Effect.sync(() => {
              hash.update(chunk);
              bytes += chunk.length;
            }),
          ),
        );
        yield* Effect.tryPromise({
          try: () => options.bucket.put(keyOf(id), Stream.toReadableStream(hashed)),
          catch: failed("bundles.write", keyOf(id)),
        });
        return { objectId: id, bytes, checksum: bytesToHex(hash.digest()) };
      }),

    move: (from, to) =>
      Effect.gen(function* () {
        if (from === to) return;
        const object = yield* Effect.tryPromise({
          try: () => options.bucket.get(keyOf(from)),
          catch: failed("bundles.move", keyOf(from)),
        });
        if (object === null || object.body === null) return;
        yield* Effect.tryPromise({
          try: () =>
            options.bucket.put(keyOf(to), object.body, {
              customMetadata: object.customMetadata,
            }),
          catch: failed("bundles.move", keyOf(to)),
        });
        yield* Effect.tryPromise({
          try: () => options.bucket.delete(keyOf(from)),
          catch: failed("bundles.move", keyOf(from)),
        });
      }),

    delete: (id) =>
      Effect.tryPromise({
        try: () => options.bucket.delete(keyOf(id)),
        catch: failed("bundles.delete", keyOf(id)),
      }),

    listIds: () =>
      Effect.tryPromise({
        try: async () => {
          const ids: string[] = [];
          let cursor: string | undefined;
          for (;;) {
            const page: R2Objects = await options.bucket.list({
              prefix,
              cursor,
            });
            for (const object of page.objects) {
              if (object.key === manifestKey) continue;
              ids.push(object.key.slice(prefix.length));
            }
            if (!page.truncated) break;
            cursor = page.cursor;
          }
          return ids;
        },
        catch: failed("bundles.listIds", prefix),
      }),
  });
};

export const r2Layer = (options: CloudflareBundles) =>
  Layer.sync(BundleStore, () => r2Store(options));

export const r2Meta = (options: CloudflareBundles): MaintenanceMeta["Service"] => {
  const key = `${options.repo}/gitplus/state.json`;
  return MaintenanceMeta.of({
    read: Effect.tryPromise({
      try: async () => {
        const object = await options.bucket.get(key);
        if (object === null) return {};
        return parseRecord(await object.text());
      },
      catch: failed("maintenance.read", key),
    }),
    write: (record) =>
      Effect.tryPromise({
        try: () => options.bucket.put(key, JSON.stringify(record)),
        catch: failed("maintenance.write", key),
      }),
  });
};

export const r2MetaLayer = (options: CloudflareBundles) =>
  Layer.sync(MaintenanceMeta, () => r2Meta(options));
