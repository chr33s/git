/**
 * The subscriber registry as a file beside the repository.
 *
 * Its own module because it reaches for `node:fs`, and `Subscribers.ts` is
 * imported by the Worker bundle — the same reason `git/Node.ts` is separate
 * from `git/Cloudflare.ts`.
 *
 * JSON rather than SQLite: the node backend keeps a repository as a directory
 * in git's own layout, and a webhook list is small, read once per push, and
 * written by hand often enough that being able to read it matters.
 */
import * as fs from "node:fs";
import * as path from "node:path";

import { Effect, Layer } from "effect";

import { StorageFailure } from "../git/Error.ts";
import { type Subscriber, Subscribers, validate } from "./Subscribers.ts";

interface Stored {
  readonly id: string;
  readonly url: string;
  readonly secret: string;
  readonly createdAt: string;
}

const read = (file: string): ReadonlyArray<Subscriber> => {
  if (!fs.existsSync(file)) return [];
  const parsed: unknown = JSON.parse(fs.readFileSync(file, "utf8"));
  if (!Array.isArray(parsed)) return [];
  return (parsed as ReadonlyArray<Stored>).map((row) => ({
    id: row.id,
    url: row.url,
    secret: row.secret,
    createdAt: new Date(row.createdAt),
  }));
};

/** Temp-and-rename, so a reader never sees a half-written list. */
const write = (file: string, rows: ReadonlyArray<Subscriber>): void => {
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

export const file = (location: string): Layer.Layer<Subscribers> =>
  Layer.sync(Subscribers, () => {
    const failed = (operation: string) => (cause: unknown) =>
      new StorageFailure({ operation, path: location, cause });

    const all = Effect.try({ try: () => read(location), catch: failed("subscribers.list") });

    return Subscribers.of({
      forEvent: () => all.pipe(Effect.orElseSucceed(() => [])),
      list: all,
      add: (input) =>
        validate(input).pipe(
          Effect.flatMap(() =>
            Effect.try({
              try: () => {
                const subscriber: Subscriber = {
                  id: crypto.randomUUID(),
                  url: input.url,
                  secret: input.secret,
                  createdAt: new Date(),
                };
                write(location, [...read(location), subscriber]);
                return subscriber;
              },
              catch: failed("subscribers.add"),
            }),
          ),
        ),
      remove: (id) =>
        Effect.try({
          try: () => {
            const rows = read(location);
            const kept = rows.filter((row) => row.id !== id);
            if (kept.length === rows.length) return false;
            write(location, kept);
            return true;
          },
          catch: failed("subscribers.remove"),
        }),
    });
  });
