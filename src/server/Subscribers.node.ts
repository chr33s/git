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
import { Effect, Layer, Schema } from "effect";

import { StorageFailure } from "../git/Error.ts";
import { checkCapacity, editRows, readRows, writeRows } from "./JsonRows.node.ts";
import * as RegistryCapacity from "./RegistryCapacity.ts";
import { type Subscriber, Subscribers, validate } from "./Subscribers.ts";

const Rows = Schema.fromJsonString(
  Schema.Array(
    Schema.Struct({
      id: Schema.NonEmptyString,
      url: Schema.NonEmptyString,
      secret: Schema.String.check(Schema.isMinLength(16)),
      createdAt: Schema.DateFromString,
    }),
  ),
);

const read = (file: string): ReadonlyArray<Subscriber> =>
  readRows(file, Rows, (row) => ({
    id: row.id,
    url: row.url,
    secret: row.secret,
    createdAt: row.createdAt,
  }));

const write = (
  file: string,
  rows: ReadonlyArray<Subscriber>,
  limits?: RegistryCapacity.Limits,
): void =>
  writeRows(file, rows, (row) => ({ ...row, createdAt: row.createdAt.toISOString() }), limits);

export const file = (
  location: string,
  limits?: RegistryCapacity.Limits,
): Layer.Layer<Subscribers> =>
  Layer.effect(
    Subscribers,
    Effect.gen(function* () {
      const capacity = limits ?? (yield* RegistryCapacity.configuration("GIT").pipe(Effect.orDie));
      const failed = (operation: string) => (cause: unknown) =>
        cause instanceof StorageFailure
          ? cause
          : new StorageFailure({ operation, path: location, cause });

      const all = Effect.try({ try: () => read(location), catch: failed("subscribers.list") });

      return Subscribers.of({
        forEvent: () =>
          all.pipe(
            Effect.catch(() =>
              Effect.logError("webhook registry unavailable", {
                operation: "subscribers.read",
                path: location,
              }).pipe(Effect.as([])),
            ),
          ),
        list: all,
        add: Effect.fn("Subscribers.add")((input) =>
          validate(input).pipe(
            Effect.flatMap(() =>
              Effect.try({
                try: () =>
                  editRows(location, () => {
                    checkCapacity(location, capacity);
                    const subscriber: Subscriber = {
                      id: crypto.randomUUID(),
                      url: input.url,
                      secret: input.secret,
                      createdAt: new Date(),
                    };
                    write(location, [...read(location), subscriber], capacity);
                    return subscriber;
                  }),
                catch: failed("subscribers.add"),
              }).pipe(Effect.tapError(RegistryCapacity.report("webhooks"))),
            ),
          ),
        ),
        remove: Effect.fn("Subscribers.remove")((id) =>
          Effect.try({
            try: () =>
              editRows(location, () => {
                const rows = read(location);
                const kept = rows.filter((row) => row.id !== id);
                if (kept.length === rows.length) return false;
                write(location, kept);
                return true;
              }),
            catch: failed("subscribers.remove"),
          }),
        ),
      });
    }),
  );
