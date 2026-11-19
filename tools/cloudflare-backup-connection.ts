/** Scoped remote operator connection. Opening it creates a Cloudflare preview proxy. */
import { Config, Effect, Layer, Redacted, Schema } from "effect";
import { FetchHttpClient } from "effect/http";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Credentials from "@distilled.cloud/cloudflare/Credentials";
import * as Workers from "@distilled.cloud/cloudflare/workers";
import * as RuntimeServices from "@alchemy.run/cloudflare-runtime/core/RuntimeServices";
import { getPlatformProxy } from "@alchemy.run/cloudflare-runtime/core/platform-proxy";
import { Service, R2Bucket as BucketBinding } from "@alchemy.run/cloudflare-runtime/core/bindings";
import type { R2Bucket } from "@cloudflare/workers-types";
import { Invalid } from "../src/git/Error.ts";
import { Request } from "../src/host/CloudflareBackupAdmin.ts";
import { Scope } from "./cloudflare-namespace-archive.ts";
import type { Client } from "./cloudflare-object-archive.ts";

const Name = Schema.NonEmptyString.check(Schema.isMaxLength(256));
const Jurisdiction = Schema.Literals(["eu", "fedramp", "fedramp-high", "us"]);
export const Connection = Schema.Struct({
  account: Scope.fields.account,
  namespace: Scope.fields.namespace,
  worker: Name,
  bucket: Name,
  archiveBucket: Name,
  jurisdiction: Schema.optional(Jurisdiction),
  archiveJurisdiction: Schema.optional(Jurisdiction),
});
export interface Connection extends Schema.Schema.Type<typeof Connection> {}
interface Bindings {
  OPERATOR: { backup(request: Request): Promise<string> };
  LIVE: R2Bucket;
  ARCHIVE: R2Bucket;
}
const invalid = () => new Invalid({ field: "backup", reason: "backup connection rejected" });

/** Check provider metadata before creating a remote proxy or sending backup commands. */
export const verifyBackupBindings = Effect.fn("Backup.verifyBindings")(function* (
  config: Connection,
  settings: Pick<Workers.GetScriptScriptAndVersionSettingResponse, "bindings">,
) {
  const checked = yield* Schema.decodeEffect(Connection)(config).pipe(Effect.mapError(invalid));
  if (checked.bucket === checked.archiveBucket) return yield* invalid();
  const namespaces =
    settings.bindings?.filter((binding) => binding.type === "durable_object_namespace") ?? [];
  const buckets = settings.bindings?.filter((binding) => binding.type === "r2_bucket") ?? [];
  const namespace = namespaces[0];
  const bucket = buckets[0];
  if (
    namespaces.length !== 1 ||
    buckets.length !== 1 ||
    namespace?.className !== "Repo" ||
    namespace.namespaceId !== checked.namespace ||
    (namespace.scriptName != null && namespace.scriptName !== checked.worker) ||
    namespace.dispatchNamespace != null ||
    namespace.environment != null ||
    bucket?.bucketName !== checked.bucket ||
    (bucket?.jurisdiction ?? undefined) !== checked.jurisdiction
  )
    return yield* invalid();
});

export const connectCloudflareBackup = Effect.fn("Backup.connect")(function* (input: Connection) {
  const config = yield* Schema.decodeEffect(Connection)(input).pipe(Effect.mapError(invalid));
  if (config.bucket === config.archiveBucket) return yield* invalid();
  const credentials = Credentials.fromApiToken({
    apiToken: yield* Config.schema(Schema.NonEmptyString, "CLOUDFLARE_API_TOKEN").pipe(
      Effect.map(Redacted.make),
      Effect.mapError(invalid),
    ),
  });
  const provider = Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer, credentials);
  const settings = yield* Workers.getScriptScriptAndVersionSetting({
    accountId: config.account,
    scriptName: config.worker,
  }).pipe(Effect.provide(provider), Effect.mapError(invalid));
  yield* verifyBackupBindings(config, settings);
  const services = yield* Layer.build(
    RuntimeServices.layerRuntime({ api: { accountId: config.account } }).pipe(
      Layer.provide(provider),
    ),
  ).pipe(Effect.mapError(invalid));
  const proxy = yield* Effect.acquireRelease(
    Effect.tryPromise({
      try: () =>
        getPlatformProxy<Bindings>({
          name: `backup-operator-${crypto.randomUUID()}`,
          services,
          bindings: [
            Service.remote("OPERATOR", config.worker),
            BucketBinding.remote("LIVE", config.bucket, config.jurisdiction),
            BucketBinding.remote("ARCHIVE", config.archiveBucket, config.archiveJurisdiction),
          ],
        }),
      catch: invalid,
    }),
    (held) => Effect.promise(() => held.dispose()),
  );
  const backup = Effect.fn("Backup.remoteCommand")(function* (input: Request) {
    const request = yield* Schema.decodeEffect(Request)(input).pipe(Effect.mapError(invalid));
    const response = yield* Effect.tryPromise({
      try: () => proxy.env.OPERATOR.backup(request),
      catch: invalid,
    });
    // Alchemy can return an error envelope instead of throwing through raw RPC.
    return yield* Schema.decodeEffect(Schema.String)(response).pipe(Effect.mapError(invalid));
  });
  const run = Effect.runPromiseWith(yield* Effect.context<never>());
  const client: Client = { backup: (request) => run(backup(request)) };
  return { client, bucket: proxy.env.LIVE, archiveBucket: proxy.env.ARCHIVE };
});
