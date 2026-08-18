/**
 * The remote registry as a file beside the repository.
 *
 * Its own module because it reaches for `node:fs`, and `Remotes.ts` is
 * imported by the Worker bundle — the same reason `git/Node.ts` is separate
 * from `git/Cloudflare.ts`.
 *
 * JSON rather than SQLite, for the reason `Subscribers.node.ts` gives: the
 * node backend keeps a repository as a directory in git's own layout, and a
 * remote list is small, read once per fetch, and edited by hand often enough
 * that being able to read it matters. The credential sits in that file in
 * plain text, so the file is only as private as the repository directory it
 * lives in — which is also true of the objects it protects.
 */
import { Effect, Layer, Option } from "effect";

import { Invalid, StorageFailure } from "../git/Error.ts";
import { readRows, writeRows } from "./JsonRows.node.ts";
import { decodeSync, duplicate, type Remote, Remotes, type Sync, validate } from "./Remotes.ts";

interface Stored {
  readonly name: string;
  readonly url: string;
  readonly credential: string | null;
  /** Whatever the file says; `read` decides whether it is a `Sync`. */
  readonly sync?: unknown;
  readonly createdAt: string;
}

const read = (file: string): ReadonlyArray<Remote> =>
  readRows<Remote, Stored>(file, (row) => ({
    name: row.name,
    url: row.url,
    credential: row.credential ?? null,
    // Decoded with the same schema the SQL registry uses, not trusted as
    // written. Absent — a file from before remotes had a standing instruction
    // — reads as `manual`, which is the behaviour that file was written
    // under; so does a hand-edit the schema cannot make sense of, because the
    // alternative is a shape `Sending` walks straight into.
    sync: Option.getOrElse(decodeSync(row.sync), (): Sync | null => null),
    createdAt: new Date(row.createdAt),
  }));

const write = (file: string, rows: ReadonlyArray<Remote>): void =>
  writeRows(file, rows, (row) => ({ ...row, createdAt: row.createdAt.toISOString() }));

export const file = (location: string): Layer.Layer<Remotes> =>
  Layer.sync(Remotes, () => {
    const failed = (operation: string) => (cause: unknown) =>
      new StorageFailure({ operation, path: location, cause });

    return Remotes.of({
      list: Effect.try({ try: () => read(location), catch: failed("remotes.list") }),
      get: (name) =>
        Effect.try({
          try: () => read(location).find((row) => row.name === name) ?? null,
          catch: failed("remotes.get"),
        }),
      add: (input) =>
        validate(input).pipe(
          Effect.flatMap(() =>
            // Read and write inside one `try`, so the duplicate check cannot
            // be separated from the write it guards by anything but this
            // process's own scheduling — which the hosts serialize per
            // repository anyway.
            Effect.try({
              try: () => {
                const rows = read(location);
                if (rows.some((row) => row.name === input.name)) throw duplicate(input.name);
                const remote: Remote = {
                  name: input.name,
                  url: input.url,
                  credential: input.credential ?? null,
                  sync: input.sync ?? null,
                  createdAt: new Date(),
                };
                write(location, [...rows, remote]);
                return remote;
              },
              catch: (cause) => (cause instanceof Invalid ? cause : failed("remotes.add")(cause)),
            }),
          ),
        ),
      remove: (name) =>
        Effect.try({
          try: () => {
            const rows = read(location);
            const kept = rows.filter((row) => row.name !== name);
            if (kept.length === rows.length) return false;
            write(location, kept);
            return true;
          },
          catch: failed("remotes.remove"),
        }),
    });
  });
