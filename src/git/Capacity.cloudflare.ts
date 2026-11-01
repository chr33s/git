import { Config, Effect, Schema } from "effect";
import { StorageFailure } from "./Error.ts";

export interface Limits {
  readonly bytes: number;
  readonly keys: number;
}

export const configuration = Effect.fn("GitCapacity.configuration")(function* () {
  return {
    bytes: yield* Config.schema(
      Schema.Int.check(
        Schema.isGreaterThan(0),
        Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
      ),
      "GIT_CF_RETAINED_BYTES",
    ).pipe(Config.withDefault(10 * 1024 ** 3)),
    keys: yield* Config.schema(
      Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_000_000)),
      "GIT_CF_RETAINED_KEYS",
    ).pipe(Config.withDefault(100_000)),
  };
});

/** One SQLite owner per R2 repository prefix. Never steal an abandoned reservation. */
export const bounded = (
  bucket: R2Bucket,
  storage: Pick<DurableObjectStorage, "sql" | "sync" | "transactionSync">,
  repo: string,
  limits: Limits,
): R2Bucket => {
  const prefixes = [`${repo}/objects/`, `${repo}/pack/`];
  const failed = (operation: string) => new StorageFailure({ operation, path: repo });
  const row = () => {
    const value = storage.sql
      .exec<{ bytes: number; keys: number; state: number }>(
        "SELECT bytes, keys, state FROM git_capacity WHERE repo = ?",
        repo,
      )
      .toArray()[0];
    if (
      value !== undefined &&
      (!Number.isSafeInteger(value.bytes) ||
        value.bytes < 0 ||
        !Number.isSafeInteger(value.keys) ||
        value.keys < 0 ||
        ![0, 1, 2].includes(value.state))
    )
      throw failed("quota.inspect");
    return value;
  };
  const initialize = async () => {
    storage.sql.exec(`CREATE TABLE IF NOT EXISTS git_capacity (
      repo TEXT PRIMARY KEY,
      bytes INTEGER NOT NULL CHECK(bytes >= 0),
      keys INTEGER NOT NULL CHECK(keys >= 0),
      state INTEGER NOT NULL CHECK(state IN (0, 1, 2))
    )`);
    if (row() !== undefined) return;
    // State 0 fences concurrent initialization and survives process death.
    storage.sql.exec("INSERT INTO git_capacity VALUES (?, 0, 0, 0)", repo);
    await storage.sync();
    let bytes = 0;
    let keys = 0;
    let pages = 0;
    for (const prefix of prefixes) {
      let cursor: string | undefined;
      for (;;) {
        const page = await bucket.list({ prefix, cursor, limit: 1000 });
        if (++pages > limits.keys + 2) throw failed("quota.inventory");
        for (const object of page.objects) {
          if (!Number.isSafeInteger(object.size) || object.size < 0)
            throw failed("quota.inventory");
          if (object.size > limits.bytes - bytes) throw failed("quota.bytes");
          bytes += object.size;
          if (++keys > limits.keys) throw failed("quota.keys");
        }
        if (!page.truncated) break;
        if (page.cursor === cursor) throw failed("quota.inventory");
        cursor = page.cursor;
      }
    }
    storage.sql.exec(
      "UPDATE git_capacity SET bytes = ?, keys = ?, state = 1 WHERE repo = ?",
      bytes,
      keys,
      repo,
    );
    await storage.sync();
  };
  const acquire = async (key: string) => {
    if (!prefixes.some((prefix) => key.startsWith(prefix))) throw failed("quota.key");
    await initialize();
    return storage.transactionSync(() => {
      const current = row();
      if (current === undefined || current.state !== 1) throw failed("quota.reservation");
      // State 2 is a persistent reservation, not a lease with an expiry.
      storage.sql.exec("UPDATE git_capacity SET state = 2 WHERE repo = ?", repo);
      return current;
    });
  };
  const finish = async (bytes: number, keys: number) => {
    if (!Number.isSafeInteger(bytes) || bytes < 0 || !Number.isSafeInteger(keys) || keys < 0)
      throw failed("quota.inspect");
    storage.sql.exec(
      "UPDATE git_capacity SET bytes = ?, keys = ?, state = 1 WHERE repo = ?",
      bytes,
      keys,
      repo,
    );
    await storage.sync();
  };
  const inspect = async (key: string, bytes: number, keys: number) => {
    const existing = await bucket.head(key);
    if (
      existing !== null &&
      (!Number.isSafeInteger(existing.size) ||
        existing.size < 0 ||
        existing.size > bytes ||
        keys === 0)
    )
      throw failed("quota.inspect");
    return existing;
  };
  return {
    head: bucket.head.bind(bucket),
    get: bucket.get.bind(bucket),
    list: bucket.list.bind(bucket),
    // Git storage uses only complete Uint8Array PUTs. Exposing multipart here
    // would bypass reservation sizes, so those methods fail closed.
    createMultipartUpload: async () => {
      throw failed("quota.multipart");
    },
    resumeMultipartUpload: () => {
      throw failed("quota.multipart");
    },
    put: async (key, value, options) => {
      if (!(value instanceof Uint8Array)) throw failed("quota.body");
      const current = await acquire(key);
      const previous = await inspect(key, current.bytes, current.keys);
      if (value.byteLength > limits.bytes - current.bytes || current.keys >= limits.keys) {
        // No R2 mutation was started, so a known quota refusal can safely unlock.
        await finish(current.bytes, current.keys);
        throw failed(
          value.byteLength > limits.bytes - current.bytes ? "quota.bytes" : "quota.keys",
        );
      }
      storage.sql.exec(
        "UPDATE git_capacity SET bytes = ?, keys = ? WHERE repo = ?",
        current.bytes + value.byteLength,
        current.keys + 1,
        repo,
      );
      // Persist the reservation before sending any R2 mutation. A lost response
      // leaves state 2 and the full reservation for offline reconciliation.
      await storage.sync();
      const result = await bucket.put(key, value, options);
      if (result === null) await finish(current.bytes, current.keys);
      else
        await finish(
          current.bytes + value.byteLength - (previous?.size ?? 0),
          current.keys + (previous === null ? 1 : 0),
        );
      return result;
    },
    delete: async (key) => {
      // The Git ports delete one key at a time, in explicit publication order.
      if (Array.isArray(key)) throw failed("quota.key");
      const current = await acquire(key);
      const previous = await inspect(key, current.bytes, current.keys);
      await storage.sync();
      await bucket.delete(key);
      await finish(
        current.bytes - (previous?.size ?? 0),
        current.keys - (previous === null ? 0 : 1),
      );
    },
  };
};
