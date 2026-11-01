/** Private KV transport component. Caller must quiesce writers and isolate restore targets. */
import type { DurableObjectStorage } from "@cloudflare/workers-types";
import { Result, Schema } from "effect";
import { encodeBackupFrame, decodeBackupFrame } from "./cloudflare-backup-codec.ts";

import {
  fenceKey,
  FenceMarker,
  checkpointKey,
  legacyCheckpointKey,
  restoreKey,
} from "./cloudflare-backup-control.ts";
export { restoreKey } from "./cloudflare-backup-control.ts";
const reserved = new Set([fenceKey, checkpointKey, legacyCheckpointKey, restoreKey]);
const Id = Schema.NonEmptyString.check(Schema.isMaxLength(128));
const Cursor = Schema.NullOr(Schema.String);
export const KvPage = Schema.Struct({
  kind: Schema.Literal("durable-kv"),
  snapshotId: Id,
  after: Cursor,
  next: Cursor,
  entries: Schema.Array(Schema.Tuple([Schema.String, Schema.Unknown])).check(
    Schema.isMaxLength(16),
  ),
});
export interface KvPage extends Schema.Schema.Type<typeof KvPage> {}
export const KvProgress = Schema.Struct({
  snapshotId: Id,
  after: Cursor,
  pages: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  keys: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  complete: Schema.Boolean,
});
export interface KvProgress extends Schema.Schema.Type<typeof KvProgress> {}
type Storage = Pick<DurableObjectStorage, "get" | "list" | "transaction">;
const decodeMarker = <A>(value: A) => {
  try {
    return Schema.decodeUnknownSync(KvProgress)(value);
  } catch {
    throw new Error("Invalid KV restore progress");
  }
};

export const captureKvPage = async (
  storage: Storage,
  snapshotId: string,
  after: string | null,
  limit = 8,
) => {
  if (Result.isFailure(Schema.decodeResult(Id)(snapshotId)))
    throw new Error("Invalid KV snapshot identifier");
  if (!Number.isInteger(limit) || limit < 1 || limit > 16)
    throw new Error("Invalid KV capture page size");
  if ((await storage.get(restoreKey)) !== undefined)
    throw new Error("Cannot capture an unfinished KV restore");
  if (
    (await storage.get(checkpointKey)) !== undefined ||
    (await storage.get(legacyCheckpointKey)) !== undefined
  )
    throw new Error("Cannot capture a retained import checkpoint");
  const fence = await storage.get(fenceKey);
  if (fence !== undefined) {
    const marker = Schema.decodeUnknownSync(FenceMarker)(fence);
    if (marker.phase !== "held" || marker.snapshotId !== snapshotId)
      throw new Error("KV capture fence mismatch");
  }
  // One excluded control key plus one application lookahead; never walk the full store.
  const options = after === null ? { limit: limit + 2 } : { limit: limit + 2, startAfter: after };
  const rows = [...(await storage.list<unknown>(options))].filter(([key]) => key !== fenceKey);
  const entries = rows.slice(0, limit);
  if (entries.some(([key]) => reserved.has(key)))
    throw new Error("Reserved KV restore key in source");
  const last = entries.at(-1);
  const next = rows.length > limit && last !== undefined ? last[0] : null;
  return encodeBackupFrame({ kind: "durable-kv", snapshotId, after, next, entries });
};

export const restoreKvPage = async (storage: Storage, frame: string) => {
  const page = decodeBackupFrame(frame, KvPage);
  const keys = validatePage(page);
  return storage.transaction(async (transaction) => {
    let pages = 0;
    let count = 0;
    if (page.after === null) {
      if ((await transaction.list({ limit: 1 })).size !== 0)
        throw new Error("KV restore target must be empty");
    } else {
      const progress = decodeMarker(await transaction.get(restoreKey));
      if (
        progress.complete ||
        progress.snapshotId !== page.snapshotId ||
        progress.after !== page.after
      )
        throw new Error("KV restore page is out of sequence");
      pages = progress.pages;
      count = progress.keys;
    }
    if (keys.length > 0 && (await transaction.get(keys)).size !== 0)
      throw new Error("KV restore would replace existing data");
    for (const [key, value] of page.entries) await transaction.put(key, value);
    const progress = {
      snapshotId: page.snapshotId,
      after: page.next,
      pages: pages + 1,
      keys: count + keys.length,
      complete: page.next === null,
    };
    await transaction.put(restoreKey, progress);
    return progress;
  });
};

/** Only the outer restore coordinator may call this after SQL/R2 and policy verification. */
export const finishKvRestore = async (storage: Storage, snapshotId: string) => {
  await storage.transaction(async (transaction) => {
    const progress = decodeMarker(await transaction.get(restoreKey));
    if (!progress.complete || progress.snapshotId !== snapshotId)
      throw new Error("KV restore is not ready to finish");
    await transaction.delete(restoreKey);
  });
};

const validatePage = (page: KvPage) => {
  if (page.entries.some(([, value]) => value === undefined))
    throw new Error("KV restore value is absent");
  const keys = page.entries.map(([key]) => key);
  if (new Set(keys).size !== keys.length || keys.some((key) => reserved.has(key)))
    throw new Error("Invalid or reserved KV restore keys");
  if (keys.some((key) => new TextEncoder().encode(key).byteLength > 2048))
    throw new Error("KV restore key exceeds byte budget");
  if (
    page.next !== null &&
    (page.entries.length === 0 ||
      page.next !== page.entries.at(-1)?.[0] ||
      page.next === page.after)
  )
    throw new Error("Invalid KV restore continuation");
  return keys;
};

const compareKeys = (left: string, right: string) => {
  const encoder = new TextEncoder();
  const a = encoder.encode(left);
  const b = encoder.encode(right);
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    const difference = (a.at(i) ?? 0) - (b.at(i) ?? 0);
    if (difference !== 0) return difference;
  }
  return a.length - b.length;
};

/** Requires the object's held fence and outer transaction with its inventory checkpoint. */
export const importKvPage = (
  kv: DurableObjectStorage["kv"],
  snapshotId: string,
  frame: string,
  progress: KvProgress | null,
): KvProgress => {
  const page = decodeBackupFrame(frame, KvPage);
  const keys = validatePage(page);
  if (
    page.snapshotId !== snapshotId ||
    progress?.complete ||
    page.after !== (progress?.after ?? null)
  )
    throw new Error("KV import page is out of sequence");
  let previous = page.after;
  for (const key of keys) {
    if (previous !== null && compareKeys(previous, key) >= 0)
      throw new Error("KV import keys are out of order");
    if (kv.get(key) !== undefined) throw new Error("KV import would replace existing data");
    previous = key;
  }
  for (const [key, value] of page.entries) kv.put(key, value);
  return {
    snapshotId,
    after: page.next,
    pages: (progress?.pages ?? 0) + 1,
    keys: (progress?.keys ?? 0) + keys.length,
    complete: page.next === null,
  };
};
