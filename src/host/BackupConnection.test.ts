import assert from "node:assert/strict";
import { it } from "@effect/vitest";
import { ConfigProvider, Effect } from "effect";
import {
  connectCloudflareBackup,
  verifyBackupBindings,
} from "../../tools/cloudflare-backup-connection.ts";
import type { GetScriptScriptAndVersionSettingResponse } from "@distilled.cloud/cloudflare/workers";

const config = {
  account: "a".repeat(32),
  namespace: "b".repeat(32),
  worker: "git-staging",
  bucket: "git-objects",
  archiveBucket: "git-backup",
};
const namespace = {
  name: "Repo",
  type: "durable_object_namespace" as const,
  className: "Repo",
  namespaceId: config.namespace,
};
const bucket = { name: "Objects", type: "r2_bucket" as const, bucketName: config.bucket };
const settings = { bindings: [namespace, bucket] };
const rejected = { _tag: "Invalid", field: "backup", reason: "backup connection rejected" };

it.effect("accepts the expected Worker namespace and live R2 bucket", () =>
  verifyBackupBindings(config, settings),
);

it("rejects ambiguous, missing or mismatched deployment bindings", async () => {
  const invalid: Array<Pick<GetScriptScriptAndVersionSettingResponse, "bindings">> = [
    {},
    { bindings: [] },
    { bindings: [bucket] },
    { bindings: [namespace] },
    { bindings: [namespace, namespace, bucket] },
    { bindings: [namespace, bucket, bucket] },
    { bindings: [{ ...namespace, namespaceId: "c".repeat(32) }, bucket] },
    { bindings: [{ ...namespace, className: "DifferentClass" }, bucket] },
    { bindings: [{ ...namespace, scriptName: "different-worker" }, bucket] },
    { bindings: [{ ...namespace, environment: "different-environment" }, bucket] },
    { bindings: [{ ...namespace, dispatchNamespace: "different-dispatch" }, bucket] },
    { bindings: [namespace, { ...bucket, bucketName: "different-bucket" }] },
    { bindings: [namespace, { ...bucket, jurisdiction: "eu" }] },
  ];
  for (const value of invalid)
    await assert.rejects(Effect.runPromise(verifyBackupBindings(config, value)), rejected);
  await assert.rejects(
    Effect.runPromise(verifyBackupBindings({ ...config, archiveBucket: config.bucket }, settings)),
    rejected,
  );
  await assert.rejects(
    Effect.runPromise(verifyBackupBindings({ ...config, account: "invalid" }, settings)),
    rejected,
  );
  await Effect.runPromise(
    verifyBackupBindings(config, {
      bindings: [{ ...namespace, scriptName: config.worker }, bucket],
    }),
  );
  await Effect.runPromise(
    verifyBackupBindings(
      { ...config, jurisdiction: "eu" },
      {
        bindings: [namespace, { ...bucket, jurisdiction: "eu" }],
      },
    ),
  );
});

it("refuses missing or empty credentials before opening a remote connection", async () => {
  for (const values of [{}, { CLOUDFLARE_API_TOKEN: "" }])
    await assert.rejects(
      Effect.runPromise(
        connectCloudflareBackup(config).pipe(
          Effect.scoped,
          Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(values))),
        ),
      ),
      rejected,
    );
});
