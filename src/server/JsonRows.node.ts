/**
 * A list of rows in a JSON file, read and written whole.
 *
 * `Remotes.node.ts` and `Subscribers.node.ts` are the same registry with
 * different row types — a small list beside the repository, read once per
 * request and written by hand often enough that being readable matters — and
 * they had a copy each of the load-parse-revive and temp-and-rename halves.
 * The interesting part is the durability, which is worth having in one place:
 * `rename(2)` is atomic within a filesystem, so a reader sees the old list or
 * the new one and never a half-written file.
 *
 * Its own `.node` module because it reaches for `node:fs`, and both callers
 * are imported by code that also builds for Workers.
 */
import * as fs from "node:fs";
import * as path from "node:path";

import { Result, Schema } from "effect";

const Rows = Schema.fromJsonString(Schema.Array(Schema.Unknown));

/** Rows as they are stored, revived into the shape the caller works in. */
export const readRows = <Row, Stored>(
  file: string,
  revive: (stored: Stored) => Row,
): ReadonlyArray<Row> => {
  if (!fs.existsSync(file)) return [];
  const parsed = Schema.decodeResult(Rows)(fs.readFileSync(file, "utf8"));
  // A file somebody edited into a shape this does not understand is an empty
  // list rather than a crash on every request that touches it.
  if (Result.isFailure(parsed)) return [];
  // SAFETY: this file is written only by `writeRows`, which serialises `Stored`
  // rows; a hand-edited file that lies about them surfaces at `revive`, not as
  // corruption here.
  return parsed.success.map((row) => revive(row as Stored));
};

/** Temp-and-rename, so a reader never sees a half-written list. */
export const writeRows = <Row, Stored>(
  file: string,
  rows: ReadonlyArray<Row>,
  store: (row: Row) => Stored,
): void => {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const temporary = `${file}.${crypto.randomUUID()}.tmp`;
  fs.writeFileSync(temporary, JSON.stringify(rows.map(store), null, 2));
  fs.renameSync(temporary, file);
};
