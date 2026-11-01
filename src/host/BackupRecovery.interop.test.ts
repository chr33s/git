import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import * as fs from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import type { Miniflare } from "miniflare";
import type { R2Object } from "@cloudflare/workers-types";
import { startBackupRuntime } from "../testing/BackupRuntime.ts";
import { gitEnv, gitIn, hasGit } from "../testing/Git.ts";
import * as Ssh from "../crypto/SshSignature.ts";
import type { Request as BackupRequest } from "./CloudflareBackupAdmin.ts";
import { decodeBackupFrame, encodeBackupFrame } from "../../tools/cloudflare-backup-codec.ts";
import { inventoryCloudflareObjects } from "../../tools/inventory-cloudflare-objects.ts";
import { captureNamespaceArchive } from "../../tools/cloudflare-namespace-archive.ts";
import { restoreNamespaceArchive } from "../../tools/cloudflare-namespace-restore.ts";
import { releaseNamespaceArchive } from "../../tools/cloudflare-namespace-release.ts";
import { releaseCapturedSources } from "../../tools/cloudflare-source-release.ts";
import { openRestoreJournal } from "../../tools/cloudflare-restore-journal.ts";

const GitState = Schema.Struct({
  head: Schema.String,
  refs: Schema.Array(Schema.Tuple([Schema.String, Schema.String])),
  shallow: Schema.Array(Schema.String),
  contents: Schema.Array(
    Schema.Struct({ oid: Schema.String, type: Schema.String, data: Schema.Uint8Array }),
  ),
});

describe.skipIf(!hasGit)("namespace recovery against stock Git", () => {
  it("validates recovered packs, refs and objects with Git before releasing either target", async () => {
    const scratch = await fs.mkdtemp(path.join(tmpdir(), "namespace-git-recovery-"));
    let mf: Miniflare | undefined;
    try {
      const runtime = await startBackupRuntime(scratch);
      mf = runtime.mf;
      const { bindings, guarded } = runtime;
      const application = guarded(bindings.STORE.getByName("git-source"));
      const authority = guarded(bindings.STORE.getByName("authority-source"));
      await application.invoke("application-hub-seed", "");
      const credentials = await authority.invoke("artifacts-seed", "");
      const original = await application.invoke("application-source", "");
      const originalHub = await application.invoke("hub-source", "");
      const originalAuthority = await authority.invoke("artifacts-read", credentials);
      const applicationId = await application.invoke("object-id", "");
      const authorityId = await authority.invoke("object-id", "");
      const sources = new Map([
        [applicationId, application],
        [authorityId, authority],
      ]);
      const sourceClient = {
        backup: (request: BackupRequest) => {
          const source = sources.get(request.objectId);
          assert.ok(source);
          return source.invoke("admin", encodeBackupFrame(request));
        },
      };
      const scope = {
        installationId: "stock-git-recovery",
        snapshotId: "snapshot-one",
        account: "a".repeat(32),
        namespace: "b".repeat(32),
        deploymentSha256: createHash("sha256").update("native-fixture-deployment").digest("hex"),
      };
      const inventoryDirectory = path.join(scratch, "inventory");
      await inventoryCloudflareObjects(
        scope.account,
        scope.namespace,
        inventoryDirectory,
        "fixture-only",
        async () =>
          Response.json({
            success: true,
            result: [...sources.keys()].map((id) => ({ id, hasStoredData: true })),
            result_info: {},
          }),
      );
      const key = await Effect.runPromise(Ssh.generate("stock-git-backup"));
      const signer = await Effect.runPromise(Ssh.fingerprint(key.publicKey));
      const directory = path.join(scratch, "archive");
      await Effect.runPromise(
        captureNamespaceArchive({
          scope,
          inventoryDirectory,
          directory,
          client: sourceClient,
          sourceBucket: bindings.SOURCE,
          destinationBucket: bindings.ARCHIVE,
          key,
        }),
      );
      const recovered = guarded(bindings.STORE.getByName("git-restored"));
      const recoveredAuthority = guarded(bindings.STORE.getByName("authority-restored"));
      const targetId = await recovered.invoke("object-id", "");
      const targetAuthorityId = await recoveredAuthority.invoke("object-id", "");
      const targets = new Map([
        [targetId, recovered],
        [targetAuthorityId, recoveredAuthority],
      ]);
      const mappingFile = path.join(scratch, "mapping.ndjson");
      await fs.writeFile(
        mappingFile,
        [
          { sourceId: applicationId, targetId },
          { sourceId: authorityId, targetId: targetAuthorityId },
        ]
          .map((row) => JSON.stringify(row) + "\n")
          .join(""),
      );
      const options = {
        directory,
        scope,
        signer,
        mappingFile,
        journalDirectory: path.join(scratch, "journal"),
        target: { account: "c".repeat(32), namespace: "d".repeat(32), bucket: "restored" },
        archiveBucket: bindings.ARCHIVE,
        targetBucket: bindings.ARCHIVE_RESTORED,
        client: {
          backup: (request: BackupRequest) => {
            const target = targets.get(request.objectId);
            assert.ok(target);
            return target.invoke("admin", encodeBackupFrame(request));
          },
        },
      };
      assert.equal((await Effect.runPromise(restoreNamespaceArchive(options))).phase, "ready");
      for (const name of ["git-restored", "authority-restored"])
        await mf.unsafeEvictDurableObject("backup-codec", "Store", { name });
      const restored = await recovered.invoke("application-archive-restored", "");
      assert.deepEqual(
        decodeBackupFrame(restored, Schema.Unknown),
        decodeBackupFrame(original, Schema.Unknown),
      );
      assert.deepEqual(
        decodeBackupFrame(
          await recoveredAuthority.invoke("artifacts-read", credentials),
          Schema.Unknown,
        ),
        decodeBackupFrame(originalAuthority, Schema.Unknown),
      );
      const restoredHub = await recovered.invoke("hub-restored", "");
      assert.deepEqual(
        decodeBackupFrame(restoredHub, Schema.Unknown),
        decodeBackupFrame(originalHub, Schema.Unknown),
      );
      const state = decodeBackupFrame(restored, GitState);
      const bare = path.join(scratch, "recovered.git");
      await fs.mkdir(bare);
      const git = gitIn(bare);
      git("init", "--bare", "-q", "-b", "main");
      const packs = await bindings.ARCHIVE_RESTORED.list({ prefix: "backup-application/pack/" });
      assert.equal(packs.truncated, false);
      assert.equal(packs.objects.length, 2, "fixture has one recovered pack/index pair");
      const exportObject = async (object: R2Object, destination: string) => {
        const body = await bindings.ARCHIVE_RESTORED.get(object.key);
        assert.ok(body);
        assert.equal(body.version, object.version);
        const file = await fs.open(destination, "wx");
        const reader = body.body.getReader();
        let bytes = 0;
        try {
          while (true) {
            const chunk = await reader.read();
            if (chunk.done) break;
            await file.writeFile(chunk.value);
            bytes += chunk.value.byteLength;
          }
          assert.equal(bytes, object.size);
          return body.customMetadata?.["type"];
        } finally {
          await reader.cancel();
          reader.releaseLock();
          await file.close();
        }
      };
      for (const object of packs.objects) {
        const name = path.basename(object.key);
        assert.match(name, /^pack-[a-f0-9]{40}\.(pack|idx)$/);
        await exportObject(object, path.join(bare, "objects", "pack", name));
      }
      // R2 loose objects store raw bytes and the Git type in custom metadata.
      // Preserve those restored bytes; Git independently adds framing and hashes them.
      const loose = await bindings.ARCHIVE_RESTORED.list({ prefix: "backup-application/objects/" });
      assert.equal(loose.truncated, false);
      for (const object of loose.objects) {
        const oid = path.basename(object.key);
        assert.match(oid, /^[a-f0-9]{40}$/);
        const file = path.join(scratch, oid);
        const type = await exportObject(object, file);
        assert.ok(type === "blob" || type === "tree" || type === "commit" || type === "tag");
        assert.equal(git("hash-object", "-w", "-t", type, file).trim(), oid);
      }
      for (const [ref, oid] of state.refs) git("update-ref", ref, oid);
      git("symbolic-ref", "HEAD", state.head);
      await fs.writeFile(path.join(bare, "shallow"), state.shallow.join("\n") + "\n");
      for (const object of packs.objects)
        if (object.key.endsWith(".idx"))
          git("verify-pack", "-v", path.join(bare, "objects", "pack", path.basename(object.key)));
      execFileSync("git", ["-C", bare, "fsck", "--full", "--strict"], {
        env: gitEnv,
        stdio: "inherit",
      });
      assert.equal(
        git("for-each-ref", "--format=%(refname) %(objectname)").trim(),
        state.refs
          .map(([ref, oid]) => ref + " " + oid)
          .sort()
          .join("\n"),
      );
      assert.equal(git("rev-parse", "--is-shallow-repository").trim(), "true");
      for (const object of state.contents) {
        const bytes = execFileSync(
          "git",
          ["--git-dir", bare, "cat-file", object.type, object.oid],
          { env: gitEnv },
        );
        assert.deepEqual(bytes, Buffer.from(object.data));
      }
      const clone = path.join(scratch, "clone");
      git("clone", "--no-local", bare, clone);
      const clonedGit = gitIn(clone);
      assert.equal(clonedGit("rev-parse", "HEAD").trim(), git("rev-parse", "HEAD").trim());
      assert.equal(
        await fs.readFile(path.join(clone, "README.md"), "utf8"),
        "Recovered repository\nUnicode: café 日本語\n",
      );
      assert.equal(
        await fs.readFile(path.join(clone, "run.sh"), "utf8"),
        "#!/bin/sh\nprintf 'recovered\\n'\n",
      );
      assert.deepEqual(
        await fs.readFile(path.join(clone, "data", "payload.bin")),
        Buffer.from([0, 255, 128, 13, 10, 0, 42]),
      );
      assert.equal(await fs.readlink(path.join(clone, "current")), "README.md");
      assert.notEqual((await fs.stat(path.join(clone, "run.sh"))).mode & 0o111, 0);
      assert.equal((await fs.stat(path.join(clone, "README.md"))).mode & 0o111, 0);
      assert.equal(clonedGit("status", "--porcelain"), "");
      clonedGit("fetch", "origin", "+refs/meta/*:refs/meta/*", "+refs/hub/*:refs/hub/*");
      assert.equal(
        clonedGit(
          "for-each-ref",
          "--format=%(refname) %(objectname)",
          "refs/meta",
          "refs/hub",
        ).trim(),
        state.refs
          .filter(([ref]) => ref.startsWith("refs/meta/") || ref.startsWith("refs/hub/"))
          .map(([ref, oid]) => ref + " " + oid)
          .sort()
          .join("\n"),
      );
      clonedGit("fsck", "--full", "--strict");
      for (const target of targets.values())
        assert.equal(
          await target.invoke("fence-request", ""),
          "503",
          "verification precedes release",
        );
      const report = JSON.stringify({
        git: execFileSync("git", ["--version"], { encoding: "utf8", env: gitEnv }).trim(),
        head: git("rev-parse", "HEAD").trim(),
        packFiles: packs.objects.map((object) => object.key),
        applicationAndAuthorityMatched: true,
        hubProjectionMatched: true,
        fsck: true,
        clone: true,
        checkoutContentsAndModes: true,
      });
      const reportFile = path.join(scratch, "verification.json");
      await fs.writeFile(reportFile, report);
      assert.equal(
        (
          await Effect.runPromise(
            releaseNamespaceArchive({
              ...options,
              verificationSha256: createHash("sha256")
                .update(await fs.readFile(reportFile))
                .digest("hex"),
            }),
          )
        ).phase,
        "released",
      );
      for (const target of targets.values())
        assert.equal(await target.invoke("fence-request", ""), "204");
      for (const source of sources.values())
        assert.equal(await source.invoke("fence-request", ""), "503");
      const sourceRelease = {
        directory,
        scope,
        signer,
        journalDirectory: path.join(scratch, "source-release-journal"),
        archiveBucket: bindings.ARCHIVE,
        client: sourceClient,
      };
      let sourceCalls = 0;
      const countedSource = {
        backup: (request: BackupRequest) => {
          sourceCalls++;
          assert.equal(request.command._tag, "Release");
          return sourceClient.backup(request);
        },
      };
      // A source release must not reuse a target's restore/release journal.
      await assert.rejects(
        Effect.runPromise(
          releaseCapturedSources({
            ...sourceRelease,
            journalDirectory: options.journalDirectory,
            client: countedSource,
          }),
        ),
      );
      assert.equal(sourceCalls, 0);
      await bindings.ARCHIVE.put("unexpected-backup-object", "reject before source release");
      await assert.rejects(
        Effect.runPromise(
          releaseCapturedSources({
            ...sourceRelease,
            client: countedSource,
          }),
        ),
      );
      assert.equal(sourceCalls, 0);
      await bindings.ARCHIVE.delete("unexpected-backup-object");
      // The first source releases remotely but its successful acknowledgement is lost.
      await assert.rejects(
        Effect.runPromise(
          releaseCapturedSources({
            ...sourceRelease,
            client: {
              backup: async (request) => {
                await countedSource.backup(request);
                throw new Error("lost source release acknowledgement");
              },
            },
          }),
        ),
      );
      assert.equal(sourceCalls, 1);
      assert.equal(await application.invoke("fence-request", ""), "204");
      assert.equal(await authority.invoke("fence-request", ""), "503");
      for (const name of ["git-source", "authority-source"])
        await mf.unsafeEvictDurableObject("backup-codec", "Store", { name });
      await assert.rejects(
        Effect.runPromise(
          releaseCapturedSources({
            ...sourceRelease,
            client: {
              backup: (request) => {
                if (request.objectId !== authorityId) return countedSource.backup(request);
                sourceCalls++;
                return Promise.resolve("held");
              },
            },
          }),
        ),
      );
      assert.equal(sourceCalls, 3);
      assert.equal(await authority.invoke("fence-request", ""), "503");
      const damaged = await openRestoreJournal(sourceRelease.journalDirectory);
      try {
        damaged.exec(
          "UPDATE release_objects SET phase = CASE sequence WHEN 0 THEN 'pending' ELSE 'released' END",
        );
      } finally {
        damaged.close();
      }
      await assert.rejects(
        Effect.runPromise(
          releaseCapturedSources({
            ...sourceRelease,
            client: countedSource,
          }),
        ),
      );
      assert.equal(sourceCalls, 3, "impossible journal ordering is rejected before RPC");
      const repaired = await openRestoreJournal(sourceRelease.journalDirectory);
      try {
        repaired.exec(
          "UPDATE release_objects SET phase = CASE sequence WHEN 0 THEN 'released' ELSE 'pending' END",
        );
      } finally {
        repaired.close();
      }
      // The retry repeats only the idempotent Release operation, never Hold or Retire.
      assert.equal(
        (
          await Effect.runPromise(
            releaseCapturedSources({
              ...sourceRelease,
              client: countedSource,
            }),
          )
        ).phase,
        "source-released",
      );
      assert.equal(sourceCalls, 4, "retry skips the durably recorded first source");
      for (const source of sources.values())
        assert.equal(await source.invoke("fence-request", ""), "204");
      const laterSnapshot = "later-source-backup";
      assert.equal(
        await sourceClient.backup({
          objectId: applicationId,
          snapshotId: laterSnapshot,
          command: { _tag: "Hold" },
        }),
        "held",
      );
      assert.equal(
        (
          await Effect.runPromise(
            releaseCapturedSources({
              ...sourceRelease,
              client: countedSource,
            }),
          )
        ).phase,
        "source-released",
      );
      assert.equal(sourceCalls, 4, "terminal retry performs no source RPC");
      assert.equal(await application.invoke("fence-request", ""), "503");
    } finally {
      await mf?.dispose();
      await fs.rm(scratch, { recursive: true, force: true });
    }
  });
});
