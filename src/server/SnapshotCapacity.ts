import { Config, Schema } from "effect";
import { StorageFailure } from "../git/Error.ts";

export const defaults = 16 * 1024 ** 2;
export const configuration = Config.schema(
  Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(64 * 1024 ** 2)),
  "GIT_CF_SNAPSHOT_BYTES",
).pipe(Config.withDefault(defaults));

const exhausted = () => new StorageFailure({ operation: "quota.snapshot.bytes", path: "snapshot" });

export const check = (bytes: number, limit: number): void => {
  if (!Number.isSafeInteger(bytes) || bytes < 0 || bytes > limit) throw exhausted();
};

/** Allocate only a validated R2 size and verify that the stream agrees with it. */
export const read = async (
  object: Pick<R2ObjectBody, "size" | "body">,
  limit: number,
): Promise<Uint8Array> => {
  try {
    check(object.size, limit);
  } catch (error) {
    await object.body.cancel().catch(() => undefined);
    throw error;
  }
  const bytes = new Uint8Array(object.size);
  const reader = object.body.getReader();
  let offset = 0;
  let complete = false;
  try {
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      if (chunk.value.byteLength > bytes.byteLength - offset) throw exhausted();
      bytes.set(chunk.value, offset);
      offset += chunk.value.byteLength;
    }
    if (offset !== bytes.byteLength)
      throw new StorageFailure({ operation: "snapshot.read", path: "snapshot" });
    complete = true;
    return bytes;
  } finally {
    if (!complete) await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
};
