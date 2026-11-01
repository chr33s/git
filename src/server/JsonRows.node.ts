/**
 * A list of rows in a JSON file, read and written whole.
 *
 * `Remotes.node.ts` and `Subscribers.node.ts` are the same registry with
 * different row types — a small list beside the repository, read once per
 * request and written by hand often enough that being readable matters — and
 * they had a copy each of the load-parse-revive and temp-and-rename halves.
 * The durability is worth having in one place: `rename(2)` is atomic within a
 * filesystem, so a reader sees the old list or the new one and never a
 * half-written file. An exclusive `.lock` file spans each read-modify-write
 * operation so separate hosts or CLI processes cannot overwrite an
 * acknowledged edit — contention fails before reading, and the caller can
 * retry. A lock left by a killed process needs manual removal after
 * confirming its writer exited.
 *
 * Its own `.node` module because it reaches for `node:fs`, and both callers
 * are imported by code that also builds for Workers.
 */
import * as fs from "node:fs";
import * as path from "node:path";

import { Result, Schema } from "effect";
import { StorageFailure } from "../git/Error.ts";
import type { Limits } from "./RegistryCapacity.ts";

/** Call under editRows' lock before parsing a file for an addition. */
export const checkCapacity = (file: string, limits: Limits): void => {
  let size: number;
  try {
    size = fs.statSync(file).size;
  } catch (cause) {
    if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return;
    throw cause;
  }
  if (size > limits.bytes) throw new StorageFailure({ operation: "quota.registry", path: file });
};

const syncDirectory = (directory: string): void => {
  const descriptor = fs.openSync(directory, "r");
  try {
    fs.fsyncSync(descriptor);
  } finally {
    fs.closeSync(descriptor);
  }
};

/** Existing directories can be remnants of an earlier failed creation. */
const prepareDirectory = (directory: string): void => {
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  // There is no durable marker proving which ancestor was synced before a
  // crash. Repeat the physical chain on every mutation, including reopened
  // processes; mkdir returning undefined is not evidence of persistence.
  let current = fs.realpathSync(directory);
  for (;;) {
    syncDirectory(current);
    const parent = path.dirname(current);
    if (parent === current) return;
    current = parent;
  }
};

/** Reserve the file before reading the snapshot an edit will replace. */
export const editRows = <A>(file: string, edit: () => A): A => {
  prepareDirectory(path.dirname(file));
  const lock = `${file}.lock`;
  const descriptor = fs.openSync(lock, "wx", 0o600);
  try {
    fs.closeSync(descriptor);
    return edit();
  } finally {
    // An existing lock fails at open, outside this finalizer. It belongs to
    // another writer and must survive the competing operation's failure.
    fs.unlinkSync(lock);
  }
};

/** Rows as they are stored, revived into the shape the caller works in. */
export const readRows = <Row, Stored>(
  file: string,
  schema: Schema.Codec<ReadonlyArray<Stored>, string>,
  revive: (stored: Stored) => Row,
): ReadonlyArray<Row> => {
  let text: string;
  try {
    text = fs.readFileSync(file, "utf8");
  } catch (cause) {
    if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return [];
    throw cause;
  }
  const parsed = Schema.decodeResult(schema)(text);
  // Parse diagnostics may quote a private key or webhook secret. Preserve the
  // file and name the problem without echoing any of its bytes into a response.
  if (Result.isFailure(parsed))
    throw new Error("invalid registry format; preserve the file for recovery");
  return parsed.success.map(revive);
};

/** Publish inside `editRows`, so readers see a complete old or new list. */
export const writeRows = <Row, Stored>(
  file: string,
  rows: ReadonlyArray<Row>,
  store: (row: Row) => Stored,
  limits?: Limits,
): void => {
  if (limits !== undefined && rows.length > limits.rows)
    throw new StorageFailure({ operation: "quota.registry", path: file });
  const bytes = Buffer.from(JSON.stringify(rows.map(store), null, 2));
  if (limits !== undefined && bytes.byteLength > limits.bytes)
    throw new StorageFailure({ operation: "quota.registry", path: file });
  prepareDirectory(path.dirname(file));
  const temporary = `${file}.${crypto.randomUUID()}.tmp`;
  try {
    // Buffer uses Node's write-and-fsync path rather than its UTF-8 string
    // fast path, which does not honor flush in the supported Node 26 runtime.
    fs.writeFileSync(temporary, bytes, {
      flag: "wx",
      mode: 0o600,
      flush: true,
    });
    fs.renameSync(temporary, file);
    syncDirectory(path.dirname(file));
  } finally {
    fs.rmSync(temporary, { force: true });
  }
};
