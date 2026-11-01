/** Shared native workerd runtime for backup recovery rehearsals. */
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { Miniflare } from "miniflare";
import type { R2Bucket } from "@cloudflare/workers-types";
import { build } from "vite-plus/pack";
import { Schema } from "effect";
import { decodeBackupFrame, encodeBackupFrame } from "../../tools/cloudflare-backup-codec.ts";

interface Fixture {
  bridgeStream(
    body: ReadableStream<Uint8Array>,
    length: number,
  ): Promise<ReadableStream<Uint8Array>>;
  fetch(url: string, init?: RequestInit): Promise<Response>;
  seed(): Promise<void>;
  capture(): Promise<string>;
  restore(frame: string): Promise<void>;
  capturePage(snapshotId: string, after: string | null, limit: number): Promise<string>;
  restorePage(frame: string): Promise<string>;
  finish(snapshotId: string): Promise<void>;
  values(): Promise<string>;
  attempt(operation: string, input: string): Promise<string>;
}

export const startBackupRuntime = async (directory: string) => {
  await build({
    entry: ["src/testing/BackupCodec.worker.ts"],
    outDir: directory,
    format: "esm",
    platform: "neutral",
    target: "es2024",
    fixedExtension: true,
    hash: false,
    dts: false,
    deps: {
      alwaysBundle: [/^effect/, /^devalue/, /^@alchemy.run/, /^alchemy/],
      neverBundle: ["cloudflare:workers"],
    },
  });
  const mf = new Miniflare({
    workers: [
      {
        config: {
          name: "backup-codec",
          compatibilityDate: "2025-12-10",
          compatibilityFlags: ["nodejs_compat", "global_fetch_strictly_public"],
          exports: { Store: { type: "durable-object", storage: "sqlite" } },
          manifest: {
            mainModule: "worker.js",
            modules: {
              "worker.js": {
                type: "esm",
                contents: await fs.readFile(path.join(directory, "BackupCodec.worker.mjs"), "utf8"),
              },
            },
          },
          env: {
            STORE: { type: "durable-object", worker: "backup-codec", exportName: "Store" },
            SOURCE: { type: "r2", name: "application-source" },
            RESTORED: { type: "r2", name: "application-restored" },
            ARCHIVE: { type: "r2", name: "namespace-archive" },
            ARCHIVE_RESTORED: { type: "r2", name: "namespace-restored" },
            HOLD_FAILURE: { type: "r2", name: "namespace-hold-failure" },
            PAGE_FAILURE: { type: "r2", name: "namespace-page-failure" },
          },
        },
      },
    ],
  });

  try {
    const bindings = await mf.getBindings<{
      STORE: { getByName(name: string): Fixture };
      SOURCE: R2Bucket;
      RESTORED: R2Bucket;
      ARCHIVE: R2Bucket;
      ARCHIVE_RESTORED: R2Bucket;
      HOLD_FAILURE: R2Bucket;
      PAGE_FAILURE: R2Bucket;
    }>();
    const guarded = (raw: Fixture) => {
      // HTTP calls release their object references; RPC sessions prevent eviction.
      const request = async (operation: string, input = "") =>
        (await raw.fetch(`https://fixture/${operation}`, { method: "POST", body: input })).text();
      const invoke = async (operation: string, input: string) => {
        const result = decodeBackupFrame(
          await request(operation, input),
          Schema.Struct({ ok: Schema.Boolean, value: Schema.String }),
        );
        if (!result.ok) throw new Error(result.value);
        return result.value;
      };
      return {
        invoke,
        capturePage: (id: string, after: string | null, limit: number) =>
          invoke("capture", encodeBackupFrame([id, after, limit])),
        restorePage: (frame: string) => invoke("restore", frame),
        finish: (id: string) => invoke("finish", id),
        values: () => request("values"),
      };
    };

    return { mf, bindings, guarded };
  } catch (error) {
    await mf.dispose();
    throw error;
  }
};
