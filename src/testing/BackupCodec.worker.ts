/** Local-only storage transport fixture; never imported by the production Worker. */
import { DurableObject } from "cloudflare:workers";
import { Effect, Schema } from "effect";
import {
  seedApplication,
  inspectApplication,
  inspectApplicationHub,
} from "./BackupApplication.worker.ts";
import { Credentials, seedArtifacts, inspectArtifacts } from "./BackupArtifacts.worker.ts";
import * as ObjectImport from "../../tools/cloudflare-object-import.ts";
import { Resources } from "../../tools/cloudflare-backup-inventory.ts";
import * as BackupAdmin from "../host/CloudflareBackupAdmin.ts";
import * as BackupFence from "../host/CloudflareBackupFence.ts";
import { encodeBackupFrame, decodeBackupFrame } from "../../tools/cloudflare-backup-codec.ts";

import { captureKvPage, restoreKvPage, finishKvRestore } from "../../tools/cloudflare-kv-backup.ts";
import { captureSqlCatalog, captureSqlPage } from "../../tools/cloudflare-sql-backup.ts";
import {
  beginSqlRestore,
  restoreSqlPage,
  finishSqlRestore,
} from "../../tools/cloudflare-sql-restore.ts";

const Snapshot = Schema.Struct({
  rows: Schema.Array(Schema.Struct({ id: Schema.Int, body: Schema.instanceOf(ArrayBuffer) })),
  kv: Schema.Array(Schema.Tuple([Schema.String, Schema.Unknown])),
});

interface Env {
  SOURCE: R2Bucket;
  RESTORED: R2Bucket;
  ARCHIVE_RESTORED: R2Bucket;
}
export class Store extends DurableObject<Env> {
  /**
   * Put a Node-side stream into the restore bucket from inside workerd.
   *
   * R2 wants a known length, which only `FixedLengthStream` gives a stream.
   * Handing that stream back to Node for a proxied `put` sent the bytes
   * workerd → Node → workerd through two nested Miniflare proxies, and on
   * Linux the second hop's socket was intermittently closed mid-upload.
   * Writing here keeps it to the one hop the body has to make.
   */
  async putRestored(
    key: string,
    body: ReadableStream<Uint8Array>,
    length: number,
    options: R2PutOptions,
  ) {
    const fixed = new FixedLengthStream(length);
    const piping = body.pipeTo(fixed.writable);
    try {
      const put = await this.env.ARCHIVE_RESTORED.put(key, fixed.readable, options);
      return put === null
        ? null
        : {
            key: put.key,
            version: put.version,
            size: put.size,
            etag: put.etag,
            httpEtag: put.httpEtag,
            uploaded: put.uploaded,
            httpMetadata: put.httpMetadata,
            customMetadata: put.customMetadata,
            storageClass: put.storageClass,
            checksums: { md5: put.checksums.md5, sha256: put.checksums.sha256 },
          };
    } finally {
      await piping.catch(() => undefined);
    }
  }
  #checkpointFails = false;
  importStorage() {
    const storage = this.ctx.storage;
    return {
      sql: storage.sql,
      transactionSync: storage.transactionSync.bind(storage),
      sync: () =>
        this.#syncFails
          ? Promise.reject(new Error("injected private flush failure"))
          : storage.sync(),
      kv: {
        get: storage.kv.get.bind(storage.kv),
        list: storage.kv.list.bind(storage.kv),
        delete: storage.kv.delete.bind(storage.kv),
        put: <A>(key: string, value: A) => {
          if (this.#checkpointFails && key === ObjectImport.checkpointKey)
            throw new Error("injected private checkpoint failure");
          storage.kv.put(key, value);
        },
      },
    };
  }
  #syncFails = false;
  #backup = BackupFence.make({
    kv: this.ctx.storage.kv,
    sync: () =>
      this.#syncFails
        ? Promise.reject(new Error("injected private flush failure"))
        : this.ctx.storage.sync(),
  });
  #admin = BackupAdmin.make(this.ctx.storage, this.ctx.id.toString(), this.#backup);
  #leases: Array<() => void> = [];
  #held = Promise.resolve("idle");
  override async fetch(request: Request) {
    const operation = new URL(request.url).pathname.slice(1);
    if (operation === "fence-stream")
      return Effect.runPromise(
        this.#backup.respond(
          Effect.sync(
            () =>
              new Response(
                // Keeps writing, as a pack stream does. A stream that goes
                // quiet never learns its client left: on Linux, workerd sees
                // the disconnect only when its next write fails, so a single
                // chunk left the lease (and `fence-wait`) pending forever.
                new ReadableStream<Uint8Array>(
                  (() => {
                    let timer: ReturnType<typeof setInterval> | undefined;
                    return {
                      start(controller) {
                        controller.enqueue(new Uint8Array([1]));
                        timer = setInterval(() => controller.enqueue(new Uint8Array([1])), 25);
                      },
                      cancel() {
                        clearInterval(timer);
                      },
                    };
                  })(),
                ),
              ),
          ),
        ),
      );
    return new Response(
      operation === "values"
        ? await this.values()
        : await this.attempt(operation, await request.text()),
    );
  }
  // Miniflare's proxy currently misdecodes rejected async RPC responses.
  // Keep the failure explicit in this local-only fixture's transport envelope.
  async attempt(operation: string, input: string) {
    try {
      let value = "";
      if (operation === "restore") value = await this.restorePage(input);
      else if (operation === "object-id") value = this.ctx.id.toString();
      else if (operation === "snapshot") value = await this.capture();
      else if (operation === "admin")
        value = await Effect.runPromise(
          this.#admin.backup(decodeBackupFrame(input, BackupAdmin.Request)),
        );
      else if (operation === "import-fail") this.#checkpointFails = input === "on";
      else if (operation === "import-start")
        value = encodeBackupFrame(
          await Effect.runPromise(
            ObjectImport.startObjectImport(
              this.importStorage(),
              decodeBackupFrame(input, ObjectImport.Plan),
            ),
          ),
        );
      else if (operation === "import-apply")
        value = encodeBackupFrame(
          await Effect.runPromise(ObjectImport.applyObjectImport(this.importStorage(), input)),
        );
      else if (operation === "import-status")
        value = encodeBackupFrame(
          await Effect.runPromise(ObjectImport.readObjectImport(this.importStorage())),
        );
      else if (operation === "import-finish") {
        const [packet, resources] = decodeBackupFrame(
          input,
          Schema.Tuple([Schema.String, Resources]),
        );
        value = encodeBackupFrame(
          await Effect.runPromise(
            ObjectImport.finishObjectImport(this.importStorage(), packet, resources),
          ),
        );
      } else if (operation === "fence-enter") this.#leases.push(this.#backup.enter());
      else if (operation === "fence-sync-fail") this.#syncFails = input === "on";
      else if (operation === "fence-followup") this.#leases.push(this.#backup.followup());
      else if (operation === "fence-done") this.#leases.shift()?.();
      else if (operation === "fence-start")
        this.#held = this.#backup.hold(input).then(
          () => "held",
          () => "rejected",
        );
      else if (operation === "fence-wait") value = await this.#held;
      else if (operation === "fence-release") await this.#backup.release(input);
      else if (operation === "fence-status")
        value = encodeBackupFrame(this.ctx.storage.kv.get(BackupFence.key) ?? null);
      else if (operation === "fence-corrupt")
        this.ctx.storage.kv.put(BackupFence.key, "invalid private marker");
      else if (operation === "fence-request")
        value = String(
          (
            await Effect.runPromise(
              this.#backup.respond(Effect.sync(() => new Response(null, { status: 204 }))),
            )
          ).status,
        );
      else if (operation === "artifacts-seed")
        value = encodeBackupFrame(await Effect.runPromise(seedArtifacts(this.ctx.storage)));
      else if (operation === "artifacts-read" || operation === "artifacts-mutate")
        value = encodeBackupFrame(
          await Effect.runPromise(
            inspectArtifacts(
              this.ctx.storage,
              decodeBackupFrame(input, Credentials),
              operation === "artifacts-mutate",
            ),
          ),
        );
      else if (operation === "application-seed" || operation === "application-hub-seed")
        await Effect.runPromise(
          seedApplication(this.ctx.storage, this.env.SOURCE, operation === "application-hub-seed"),
        );
      else if (operation === "hub-source" || operation === "hub-restored")
        value = encodeBackupFrame(
          await Effect.runPromise(
            inspectApplicationHub(
              this.ctx.storage,
              operation === "hub-source" ? this.env.SOURCE : this.env.ARCHIVE_RESTORED,
            ),
          ),
        );
      else if (
        operation === "application-source" ||
        operation === "application-restored" ||
        operation === "application-archive-restored" ||
        operation === "application-mutate"
      )
        value = encodeBackupFrame(
          await Effect.runPromise(
            inspectApplication(
              this.ctx.storage,
              operation === "application-source"
                ? this.env.SOURCE
                : operation === "application-archive-restored"
                  ? this.env.ARCHIVE_RESTORED
                  : this.env.RESTORED,
              operation === "application-mutate",
            ),
          ),
        );
      else if (operation === "finish") await this.finish(input);
      else if (operation === "sql-catalog") value = captureSqlCatalog(this.ctx.storage.sql);
      else if (operation === "sql-begin" || operation === "sql-restore") {
        const [id, frame] = decodeBackupFrame(input, Schema.Tuple([Schema.String, Schema.String]));
        if (operation === "sql-begin") beginSqlRestore(this.ctx.storage, id, frame);
        else restoreSqlPage(this.ctx.storage, id, frame);
      } else if (operation === "sql-finish") finishSqlRestore(this.ctx.storage, input);
      else if (operation === "sql-insert")
        this.ctx.storage.sql.exec('INSERT INTO "odd""table" (value) VALUES (7)');
      else if (operation === "sql-page") {
        const [table, after, limit] = decodeBackupFrame(
          input,
          Schema.Tuple([Schema.String, Schema.NullOr(Schema.String), Schema.Int]),
        );
        value = captureSqlPage(this.ctx.storage.sql, table, after, limit);
      } else if (operation === "sql-seed") {
        const sql = this.ctx.storage.sql;
        sql.exec('CREATE TABLE "odd""table" (value, data BLOB, optional TEXT)');
        sql.exec('CREATE INDEX value_index ON "odd""table" (value)');
        sql.exec("CREATE TABLE audit (count INTEGER)");
        sql.exec("INSERT INTO audit VALUES (0)");
        sql.exec(
          'CREATE TRIGGER audit_rows AFTER INSERT ON "odd""table" BEGIN UPDATE audit SET count = count + 1; END',
        );
        sql.exec(
          'INSERT INTO "odd""table" (rowid, value, data) VALUES (-2, 9223372036854775807, ?)',
          new Uint8Array([0, 255]).buffer,
        );
        sql.exec(
          'INSERT INTO "odd""table" (rowid, value) VALUES (9007199254740993, -9223372036854775808)',
        );
        sql.exec('INSERT INTO "odd""table" (rowid, value) VALUES (9223372036854775807, 1.25)');
      } else if (operation === "capture") {
        const [id, after, limit] = decodeBackupFrame(
          input,
          Schema.Tuple([Schema.String, Schema.NullOr(Schema.String), Schema.Int]),
        );
        value = await this.capturePage(id, after, limit);
      } else throw new Error("Unknown fixture operation");
      return encodeBackupFrame({ ok: true, value });
    } catch (error) {
      return encodeBackupFrame({
        ok: false,
        value: error instanceof Error ? error.message : "Fixture operation failed",
      });
    }
  }

  capturePage(snapshotId: string, after: string | null, limit: number) {
    return captureKvPage(this.ctx.storage, snapshotId, after, limit);
  }
  async restorePage(frame: string) {
    return encodeBackupFrame(await restoreKvPage(this.ctx.storage, frame));
  }
  finish(snapshotId: string) {
    return finishKvRestore(this.ctx.storage, snapshotId);
  }
  async values() {
    return encodeBackupFrame([...(await this.ctx.storage.list())]);
  }

  async seed() {
    this.ctx.storage.sql.exec("CREATE TABLE records (id INTEGER PRIMARY KEY, body BLOB NOT NULL)");
    this.ctx.storage.sql.exec(
      "INSERT INTO records VALUES (?, ?)",
      1,
      new Uint8Array([0, 255, 128]).buffer,
    );
    interface Graph {
      label: string;
      self?: Graph;
    }
    const graph: Graph = { label: "private stored graph" };
    graph.self = graph;
    await this.ctx.storage.put("HEAD:repo", "refs/heads/main");
    await this.ctx.storage.put("snapshot-seq:repo", 17);
    await this.ctx.storage.put("typed", {
      date: new Date(0),
      map: new Map([["key", 3n]]),
      set: new Set(["value"]),
      binary: new Uint8Array([0, 255, 128]),
      negativeZero: -0,
      notANumber: NaN,
      graph,
      alias: graph,
    });
  }
  async capture() {
    return encodeBackupFrame({
      rows: this.ctx.storage.sql.exec("SELECT id, body FROM records ORDER BY id").toArray(),
      kv: [...(await this.ctx.storage.list())],
    });
  }
  async restore(frame: string) {
    const decoded = decodeBackupFrame(frame, Snapshot);
    if ((await this.ctx.storage.list()).size !== 0) throw new Error("fixture target is not empty");
    this.ctx.storage.sql.exec("CREATE TABLE records (id INTEGER PRIMARY KEY, body BLOB NOT NULL)");
    for (const row of decoded.rows)
      this.ctx.storage.sql.exec("INSERT INTO records VALUES (?, ?)", row.id, row.body);
    for (const [key, value] of decoded.kv) await this.ctx.storage.put(key, value);
  }
}
export default { fetch: () => new Response("local fixture only", { status: 404 }) };
