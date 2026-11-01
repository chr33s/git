import * as fs from "node:fs/promises";
import { Config, Option, Schema } from "effect";

/** Opt-in assertion about an operator-provisioned filesystem, not a quota setter. */
export const ceiling = Config.option(
  Config.schema(
    Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
    "GIT_STORAGE_FILESYSTEM_BYTES",
  ),
).pipe(Config.map(Option.getOrUndefined));

/** Run before opening listeners or creating repository state. */
export const verify = async (root: string, maximumBytes: number | undefined): Promise<void> => {
  if (maximumBytes === undefined) return;
  if (!Number.isSafeInteger(maximumBytes) || maximumBytes <= 0)
    throw new RangeError("invalid storage filesystem ceiling");
  if (process.platform !== "linux") throw new Error("storage filesystem ceiling requires Linux");
  const [directory, system, filesystem] = await Promise.all([
    fs.stat(root, { bigint: true }),
    fs.stat("/", { bigint: true }),
    fs.statfs(root, { bigint: true }),
  ]);
  if (!directory.isDirectory()) throw new Error("storage root must be an existing directory");
  if (directory.dev === system.dev)
    throw new Error("storage root must use a separate filesystem from /");
  const bytes = filesystem.blocks * filesystem.bsize;
  if (bytes <= 0n || bytes > BigInt(maximumBytes))
    throw new Error("storage filesystem exceeds configured ceiling or reports no capacity");
};
