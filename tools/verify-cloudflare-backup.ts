/** Offline archive authentication. Expected scope and signer come from independent operator custody. */
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { Effect, Schema } from "effect";
import { Invalid } from "../src/git/Error.ts";
import { isFingerprint } from "../src/crypto/SshSignature.ts";
import { Scope, verifyNamespaceArchive } from "./cloudflare-namespace-archive.ts";
import { readBackupFile } from "./cloudflare-backup-read.ts";

const Expected = Schema.Struct({
  scope: Scope,
  signer: Schema.String.pipe(Schema.refine(isFingerprint)),
});
const invalid = () =>
  new Invalid({ field: "backup", reason: "offline archive verification failed" });

export const verifyCloudflareBackup = Effect.fn("Backup.verifyOfflineArchive")(function* (
  directory: string,
  expectedFile: string,
) {
  const expected = yield* Effect.tryPromise({
    try: async () => {
      const archive = await fs.realpath(directory);
      const trusted = await fs.realpath(expectedFile);
      const relative = path.relative(archive, trusted);
      // Do not accept the archive's own contents as its trust anchor.
      if (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative))
        throw invalid();
      return await readBackupFile(expectedFile, 16 * 1024);
    },
    catch: invalid,
  }).pipe(
    Effect.flatMap((text) => Schema.decodeEffect(Schema.fromJsonString(Expected))(text)),
    Effect.mapError(invalid),
  );
  const completion = yield* verifyNamespaceArchive({ directory, ...expected });
  return {
    schemaVersion: 1,
    kind: "namespace-archive-verification",
    signer: expected.signer,
    completion,
    verified: { archiveSignatures: true, discoveryConsistency: true, objectArchives: true },
    notVerified: ["R2 bucket contents", "installation-wide isolation", "application recovery"],
  };
});

if (import.meta.main) {
  const [directory, expectedFile] = process.argv.slice(2);
  if (process.argv.length !== 4 || directory === undefined || expectedFile === undefined) {
    console.error(
      "usage: node tools/verify-cloudflare-backup.ts archive-directory trusted-expected.json",
    );
    process.exitCode = 1;
  } else {
    try {
      const report = await Effect.runPromise(verifyCloudflareBackup(directory, expectedFile));
      console.info(JSON.stringify(report, null, 2));
    } catch {
      // Never print input paths, identifiers, payloads, schema diagnostics or nested RPC errors.
      console.error("offline archive verification failed");
      process.exitCode = 1;
    }
  }
}
