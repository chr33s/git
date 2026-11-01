/** Bounded, non-following reads for private backup files. */
import * as fs from "node:fs/promises";
import { constants } from "node:fs";
import { Schema } from "effect";
import { Invalid } from "../src/git/Error.ts";
const invalid = () => new Invalid({ field: "backup", reason: "backup file rejected" });
const open = async (file: string, privateFile = false) => {
  const input = await fs.open(
    file,
    constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
  );
  try {
    const stat = await input.stat();
    if (!stat.isFile() || (privateFile && (stat.mode & 0o077) !== 0)) throw invalid();
    return input;
  } catch (error) {
    await input.close();
    throw error;
  }
};
export const readBackupFile = async (
  file: string,
  limit: number,
  options?: { readonly private: boolean },
) => {
  const input = await open(file, options?.private);
  try {
    const buffer = Buffer.alloc(limit + 1);
    let count = 0;
    while (count <= limit) {
      const result = await input.read(buffer, count, buffer.length - count, count);
      if (result.bytesRead === 0) break;
      count += result.bytesRead;
    }
    if (count > limit) throw invalid();
    return new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(
      buffer.subarray(0, count),
    );
  } finally {
    await input.close();
  }
};
export async function* readBackupLines(
  file: string,
  limit: number,
): AsyncGenerator<string, void, void> {
  const input = await open(file);
  try {
    const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
    let pending = "";
    for await (const chunk of input.createReadStream({ autoClose: false, highWaterMark: 4096 })) {
      const bytes = Schema.decodeUnknownSync(Schema.instanceOf(Uint8Array))(chunk);
      pending += decoder.decode(bytes, { stream: true });
      let end: number;
      while ((end = pending.indexOf("\n")) !== -1) {
        const line = pending.slice(0, end);
        if (Buffer.byteLength(line) > limit) throw invalid();
        yield line;
        pending = pending.slice(end + 1);
      }
      if (Buffer.byteLength(pending) > limit) throw invalid();
    }
    pending += decoder.decode();
    if (pending !== "") throw invalid();
  } finally {
    await input.close();
  }
}
