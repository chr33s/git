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
import * as fs from "node:fs";
import * as path from "node:path";

import { Effect, Layer } from "effect";

import { Invalid, StorageFailure } from "../git/Error.ts";
import { duplicate, type Remote, Remotes, validate } from "./Remotes.ts";

interface Stored {
  readonly name: string;
  readonly url: string;
  readonly credential: string | null;
  readonly createdAt: string;
}

const read = (file: string): ReadonlyArray<Remote> => {
  if (!fs.existsSync(file)) return [];
  const parsed: unknown = JSON.parse(fs.readFileSync(file, "utf8"));
  if (!Array.isArray(parsed)) return [];
  return (parsed as ReadonlyArray<Stored>).map((row) => ({
    name: row.name,
    url: row.url,
    credential: row.credential ?? null,
    createdAt: new Date(row.createdAt),
  }));
};

/** Temp-and-rename, so a reader never sees a half-written list. */
const write = (file: string, rows: ReadonlyArray<Remote>): void => {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const temporary = `${file}.${crypto.randomUUID()}.tmp`;
  fs.writeFileSync(
    temporary,
    JSON.stringify(
      rows.map((row) => ({ ...row, createdAt: row.createdAt.toISOString() })),
      null,
      2,
    ),
  );
  fs.renameSync(temporary, file);
};

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
