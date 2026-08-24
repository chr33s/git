/**
 * Bundle artifacts on the filesystem, under server-managed metadata.
 *
 *   <repo>/gitplus/bundles/<family>/<token>-<checksum>.bundle
 *   <repo>/gitplus/bundles.json
 *
 * git does not look here. A crash before `publish` leaves an unreferenced
 * file that the planner later prunes.
 */
import * as fs from "node:fs/promises";
import * as path from "node:path";

import { Effect, Layer, Predicate, Stream } from "effect";

import { StorageFailure } from "../git/Error.ts";
import { bytesToHex } from "../git/Format.ts";
import { Sha1 } from "../git/Sha1.ts";
import { decodeManifest, encodeManifest } from "./BundleFormat.ts";
import { BundleStore } from "./BundleStore.ts";
import { MaintenanceMeta, parseRecord } from "./MaintenanceRun.ts";

const failed = (operation: string, target: string) => (cause: unknown) =>
  new StorageFailure({ operation, path: target, cause });

const missing = (cause: unknown): boolean =>
  Predicate.hasProperty(cause, "code") && cause.code === "ENOENT";

const safeId = (id: string): string | null => {
  if (id.length === 0 || id.includes("\0") || id.includes("..")) return null;
  if (path.isAbsolute(id) || id.startsWith("/") || id.startsWith("\\")) return null;
  return id;
};

const writeFileAtomic = async (target: string, bytes: Uint8Array): Promise<void> => {
  await fs.mkdir(path.dirname(target), { recursive: true });
  const temp = `${target}.${process.pid}.${Date.now()}.tmp`;
  await fs.writeFile(temp, bytes);
  await fs.rename(temp, target);
};

export const fileStore = (root: string): BundleStore["Service"] => {
  const base = path.join(root, "gitplus");
  const artifactPath = (id: string): string => path.join(base, "bundles", id);
  const manifestPath = path.join(base, "bundles.json");

  return BundleStore.of({
    list: () =>
      Effect.tryPromise({
        try: async () => {
          const text = await fs.readFile(manifestPath, "utf8").catch((cause) => {
            if (missing(cause)) return null;
            throw cause;
          });
          return text === null ? null : decodeManifest(text);
        },
        catch: failed("bundles.list", manifestPath),
      }),

    publish: (_repo, manifest) =>
      Effect.tryPromise({
        try: () =>
          writeFileAtomic(manifestPath, new TextEncoder().encode(encodeManifest(manifest))),
        catch: failed("bundles.publish", manifestPath),
      }),

    stat: (id) =>
      Effect.tryPromise({
        try: async () => {
          const safe = safeId(id);
          if (safe === null) return null;
          const target = artifactPath(safe);
          const found = await fs.stat(target).catch(() => null);
          if (found === null || !found.isFile()) return null;
          const checksum =
            path
              .basename(safe)
              .split("-")
              .at(-1)
              ?.replace(/\.bundle$/, "") ?? "";
          return { objectId: id, bytes: found.size, checksum };
        },
        catch: failed("bundles.stat", id),
      }),

    read: (id, range) =>
      Effect.gen(function* () {
        const safe = safeId(id);
        if (safe === null) {
          return Stream.fail(new StorageFailure({ operation: "bundles.read", path: id }));
        }
        const target = artifactPath(safe);
        const handle = yield* Effect.tryPromise({
          try: () => fs.open(target, "r"),
          catch: failed("bundles.read", target),
        });
        const start = range?.offset ?? 0;
        const length = range?.length;
        return Stream.fromAsyncIterable(
          (async function* () {
            try {
              const stat = await handle.stat();
              let remaining = length ?? Math.max(0, stat.size - start);
              let offset = start;
              const chunk = new Uint8Array(64 * 1024);
              while (remaining > 0) {
                const take = Math.min(chunk.length, remaining);
                const result = await handle.read(chunk, 0, take, offset);
                if (result.bytesRead === 0) break;
                yield new Uint8Array(chunk.subarray(0, result.bytesRead));
                offset += result.bytesRead;
                remaining -= result.bytesRead;
              }
            } finally {
              await handle.close();
            }
          })(),
          failed("bundles.read", target),
        );
      }),

    write: (id, source) =>
      Effect.gen(function* () {
        const safe = safeId(id);
        if (safe === null) {
          return yield* new StorageFailure({
            operation: "bundles.write",
            path: id,
            cause: "unsafe artifact id",
          });
        }
        const target = artifactPath(safe);
        yield* Effect.tryPromise({
          try: () => fs.mkdir(path.dirname(target), { recursive: true }),
          catch: failed("bundles.write", target),
        });
        const temp = `${target}.${process.pid}.writing`;
        const handle = yield* Effect.tryPromise({
          try: () => fs.open(temp, "w"),
          catch: failed("bundles.write", temp),
        });
        const hash = new Sha1();
        let bytes = 0;
        yield* Stream.runForEach(source, (chunk) =>
          Effect.tryPromise({
            try: async () => {
              hash.update(chunk);
              bytes += chunk.length;
              await handle.write(chunk);
            },
            catch: failed("bundles.write", temp),
          }),
        ).pipe(
          Effect.ensuring(
            Effect.tryPromise({
              try: () => handle.close(),
              catch: failed("bundles.write", temp),
            }).pipe(Effect.ignore),
          ),
        );
        yield* Effect.tryPromise({
          try: () => fs.rename(temp, target),
          catch: failed("bundles.write", target),
        });
        return { objectId: id, bytes, checksum: bytesToHex(hash.digest()) };
      }),

    move: (from, to) =>
      Effect.gen(function* () {
        if (from === to) return;
        const source = safeId(from);
        const dest = safeId(to);
        if (source === null || dest === null) {
          return yield* new StorageFailure({
            operation: "bundles.move",
            path: to,
            cause: "unsafe artifact id",
          });
        }
        const fromPath = artifactPath(source);
        const toPath = artifactPath(dest);
        yield* Effect.tryPromise({
          try: async () => {
            await fs.mkdir(path.dirname(toPath), { recursive: true });
            await fs.rename(fromPath, toPath);
          },
          catch: failed("bundles.move", toPath),
        });
      }),

    delete: (id) =>
      Effect.tryPromise({
        try: async () => {
          const safe = safeId(id);
          if (safe === null) return;
          await fs.unlink(artifactPath(safe)).catch((cause) => {
            if (missing(cause)) return;
            throw cause;
          });
        },
        catch: failed("bundles.delete", id),
      }),

    listIds: () =>
      Effect.tryPromise({
        try: async () => {
          const root = path.join(base, "bundles");
          const found: string[] = [];
          const walk = async (dir: string, prefix: string) => {
            const entries = await fs.readdir(dir, { withFileTypes: true }).catch(() => []);
            for (const entry of entries) {
              const relative = prefix === "" ? entry.name : `${prefix}/${entry.name}`;
              if (entry.isDirectory()) await walk(path.join(dir, entry.name), relative);
              else if (!entry.name.endsWith(".tmp") && !entry.name.endsWith(".writing")) {
                found.push(relative);
              }
            }
          };
          await walk(root, "");
          return found;
        },
        catch: failed("bundles.listIds", base),
      }),
  });
};

export const fileLayer = (root: string) => Layer.sync(BundleStore, () => fileStore(root));

export const fileMeta = (root: string): MaintenanceMeta["Service"] => {
  const target = path.join(root, "gitplus", "state.json");
  return MaintenanceMeta.of({
    read: Effect.tryPromise({
      try: async () => {
        const text = await fs.readFile(target, "utf8").catch((cause) => {
          if (missing(cause)) return "{}";
          throw cause;
        });
        return parseRecord(text);
      },
      catch: failed("maintenance.read", target),
    }),
    write: (record) =>
      Effect.tryPromise({
        try: () => writeFileAtomic(target, new TextEncoder().encode(JSON.stringify(record))),
        catch: failed("maintenance.write", target),
      }),
  });
};

export const fileMetaLayer = (root: string) => Layer.sync(MaintenanceMeta, () => fileMeta(root));
