/** Explicit operator commands. Isolation, deployment custody and recovery acceptance remain external. */
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { createHash } from "node:crypto";
import { Effect, Schema, Scope as EffectScope } from "effect";
import { Invalid } from "../src/git/Error.ts";
import * as Ssh from "../src/crypto/SshSignature.ts";
import { Connection, connectCloudflareBackup } from "./cloudflare-backup-connection.ts";
import { Scope, captureNamespaceArchive } from "./cloudflare-namespace-archive.ts";
import { restoreNamespaceArchive } from "./cloudflare-namespace-restore.ts";
import { releaseNamespaceArchive } from "./cloudflare-namespace-release.ts";
import { releaseCapturedSources, abandonCapturedSources } from "./cloudflare-source-release.ts";
import { readBackupFile } from "./cloudflare-backup-read.ts";
import type { Client } from "./cloudflare-object-archive.ts";
import type { Destination } from "./copy-r2-backup.ts";

const Path = Schema.NonEmptyString.check(Schema.isMaxLength(4096));
const Command = Schema.Literals([
  "capture",
  "restore",
  "release-targets",
  "release-sources",
  "abandon-sources",
]);
const common = {
  schemaVersion: Schema.Literal(1),
  connection: Connection,
  scope: Scope,
  signer: Schema.String.pipe(Schema.refine(Ssh.isFingerprint)),
  directory: Path,
};
const restore = { mappingFile: Path, journalDirectory: Path };
const Plan = Schema.Union([
  Schema.Struct({
    ...common,
    command: Schema.Literal("abandon-sources"),
    journalDirectory: Path,
    decisionFile: Path,
  }),
  Schema.Struct({
    ...common,
    command: Schema.Literal("capture"),
    inventoryDirectory: Path,
    keyFile: Path,
  }),
  Schema.Struct({ ...common, ...restore, command: Schema.Literal("restore") }),
  Schema.Struct({
    ...common,
    ...restore,
    command: Schema.Literal("release-targets"),
    verificationFile: Path,
  }),
  Schema.Struct({ ...common, command: Schema.Literal("release-sources"), journalDirectory: Path }),
]);
interface Connected {
  readonly client: Client;
  readonly bucket: Destination;
  readonly archiveBucket: Destination;
}
type Connect = (config: Connection) => Effect.Effect<Connected, Invalid, EffectScope.Scope>;
const invalid = () => new Invalid({ field: "backup", reason: "backup command failed" });
const io = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: invalid });
const canonical = async (value: string) => {
  try {
    return await fs.realpath(value);
  } catch (error) {
    if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
    return path.join(await fs.realpath(path.dirname(value)), path.basename(value));
  }
};
const outside = async (directory: string, value: string) => {
  const relative = path.relative(directory, await canonical(value));
  if (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative))
    throw invalid();
};

export const runBackupCommand = Effect.fn("Backup.command")(function* (
  command: string,
  planFile: string,
  connect: Connect = connectCloudflareBackup,
) {
  const requested = yield* Schema.decodeUnknownEffect(Command)(command).pipe(
    Effect.mapError(invalid),
  );
  const text = yield* io(() => readBackupFile(planFile, 64 * 1024, { private: true }));
  const plan = yield* Schema.decodeEffect(Schema.fromJsonString(Plan))(text).pipe(
    Effect.mapError(invalid),
  );
  if (plan.command !== requested) return yield* invalid();
  const file = yield* io(() => fs.realpath(planFile));
  const resolve = (value: string) => path.resolve(path.dirname(file), value);
  const directory = yield* io(() => canonical(resolve(plan.directory)));
  yield* io(() => outside(directory, file));
  const source =
    plan.command === "capture" ||
    plan.command === "release-sources" ||
    plan.command === "abandon-sources";
  const sameNamespace =
    plan.connection.account === plan.scope.account &&
    plan.connection.namespace === plan.scope.namespace;
  if (source !== sameNamespace || plan.connection.bucket === plan.connection.archiveBucket)
    return yield* invalid();

  // Promise-shaped transport ports retain this command's scope and configuration.
  // Connecting is delayed until coordinators have completed their local validation.
  const context = yield* Effect.context<EffectScope.Scope>();
  const connection = yield* Effect.cached(
    connect(plan.connection).pipe(Effect.provideContext(context)),
  );
  const call = <A>(use: (held: Connected) => Promise<A>) =>
    Effect.runPromise(connection.pipe(Effect.flatMap((held) => io(() => use(held)))));
  const client: Client = { backup: (request) => call((held) => held.client.backup(request)) };
  const bucket = (role: "bucket" | "archiveBucket"): Destination => ({
    list: (options) => call((held) => held[role].list(options)),
    get: (key, options) => call((held) => held[role].get(key, options)),
    put: (key, body, options) => call((held) => held[role].put(key, body, options)),
  });
  const expected = { directory, scope: plan.scope, signer: plan.signer };
  if (plan.command === "capture") {
    const pem = yield* io(() =>
      readBackupFile(resolve(plan.keyFile), 16 * 1024, { private: true }),
    );
    const key = yield* Effect.fromResult(Ssh.parsePrivateKey(pem)).pipe(Effect.mapError(invalid));
    if ((yield* Ssh.fingerprint(key.publicKey)) !== plan.signer) return yield* invalid();
    const result = yield* captureNamespaceArchive({
      ...expected,
      key,
      inventoryDirectory: resolve(plan.inventoryDirectory),
      client,
      sourceBucket: bucket("bucket"),
      destinationBucket: bucket("archiveBucket"),
    });
    return { schemaVersion: 1, command: plan.command, result };
  }
  const journalDirectory = resolve(plan.journalDirectory);
  yield* io(() => outside(directory, journalDirectory));
  if (plan.command === "abandon-sources") {
    const decisionFile = resolve(plan.decisionFile);
    yield* io(() => outside(directory, decisionFile));
    const decision = yield* io(() => readBackupFile(decisionFile, 1024 ** 2, { private: true }));
    if (decision.trim() === "") return yield* invalid();
    const result = yield* abandonCapturedSources({
      ...expected,
      journalDirectory,
      client,
      decisionSha256: createHash("sha256").update(decision).digest("hex"),
    });
    return { schemaVersion: 1, command: plan.command, result };
  }
  if (plan.command === "release-sources") {
    const result = yield* releaseCapturedSources({
      ...expected,
      journalDirectory,
      client,
      archiveBucket: bucket("archiveBucket"),
    });
    return { schemaVersion: 1, command: plan.command, result };
  }
  const mappingFile = resolve(plan.mappingFile);
  yield* io(() => outside(directory, mappingFile));
  const options = {
    ...expected,
    journalDirectory,
    mappingFile,
    client,
    target: {
      account: plan.connection.account,
      namespace: plan.connection.namespace,
      bucket: plan.connection.bucket,
    },
    targetBucket: bucket("bucket"),
  };
  if (plan.command === "restore") {
    const result = yield* restoreNamespaceArchive({
      ...options,
      archiveBucket: bucket("archiveBucket"),
    });
    return { schemaVersion: 1, command: plan.command, result };
  }
  const verificationFile = resolve(plan.verificationFile);
  yield* io(() => outside(directory, verificationFile));
  const verification = yield* io(() =>
    readBackupFile(verificationFile, 1024 ** 2, { private: true }),
  );
  if (verification.trim() === "") return yield* invalid();
  const result = yield* releaseNamespaceArchive({
    ...options,
    verificationSha256: createHash("sha256").update(verification).digest("hex"),
  });
  return { schemaVersion: 1, command: plan.command, result };
}, Effect.scoped);

if (import.meta.main) {
  const [command, planFile] = process.argv.slice(2);
  if (process.argv.length !== 4 || command === undefined || planFile === undefined) {
    console.error(
      "usage: node tools/backup-cloudflare.ts capture|restore|release-targets|release-sources|abandon-sources plan.json",
    );
    process.exitCode = 1;
  } else {
    try {
      console.info(
        JSON.stringify(await Effect.runPromise(runBackupCommand(command, planFile)), null, 2),
      );
    } catch {
      console.error("backup command failed");
      process.exitCode = 1;
    }
  }
}
