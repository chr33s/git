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
import { Effect, Layer, Schema } from "effect";

import { Invalid, StorageFailure } from "../git/Error.ts";
import { checkCapacity, editRows, readRows, writeRows } from "./JsonRows.node.ts";
import * as RegistryCapacity from "./RegistryCapacity.ts";
import { duplicate, type Remote, Remotes, StoredSync, validate } from "./Remotes.ts";

const Rows = Schema.fromJsonString(
  Schema.Array(
    Schema.Struct({
      name: Schema.NonEmptyString,
      url: Schema.NonEmptyString,
      credential: Schema.optional(Schema.NullOr(Schema.String)),
      key: Schema.optional(Schema.NullOr(Schema.String)),
      sync: Schema.optional(Schema.NullOr(StoredSync)),
      createdAt: Schema.DateFromString,
    }),
  ),
);

const read = (file: string): ReadonlyArray<Remote> =>
  readRows(file, Rows, (row) => ({
    name: row.name,
    url: row.url,
    credential: row.credential ?? null,
    key: row.key ?? null,
    // Absent in legacy files means manual; malformed content is an error.
    sync: row.sync ?? null,
    createdAt: row.createdAt,
  }));

const write = (file: string, rows: ReadonlyArray<Remote>, limits?: RegistryCapacity.Limits): void =>
  writeRows(file, rows, (row) => ({ ...row, createdAt: row.createdAt.toISOString() }), limits);

export const file = (location: string, limits?: RegistryCapacity.Limits): Layer.Layer<Remotes> =>
  Layer.effect(
    Remotes,
    Effect.gen(function* () {
      const capacity = limits ?? (yield* RegistryCapacity.configuration("GIT").pipe(Effect.orDie));
      const failed = (operation: string) => (cause: unknown) =>
        cause instanceof StorageFailure
          ? cause
          : new StorageFailure({ operation, path: location, cause });

      return Remotes.of({
        list: Effect.try({ try: () => read(location), catch: failed("remotes.list") }),
        get: (name) =>
          Effect.try({
            try: () => read(location).find((row) => row.name === name) ?? null,
            catch: failed("remotes.get"),
          }),
        add: Effect.fn("Remotes.add")((input) =>
          validate(input).pipe(
            Effect.flatMap(() =>
              Effect.try({
                try: () =>
                  editRows(location, () => {
                    checkCapacity(location, capacity);
                    const rows = read(location);
                    if (rows.some((row) => row.name === input.name)) throw duplicate(input.name);
                    const remote: Remote = {
                      name: input.name,
                      url: input.url,
                      credential: input.credential ?? null,
                      key: input.key ?? null,
                      sync: input.sync ?? null,
                      createdAt: new Date(),
                    };
                    write(location, [...rows, remote], capacity);
                    return remote;
                  }),
                catch: (cause) =>
                  Schema.is(Invalid)(cause) ? cause : failed("remotes.add")(cause),
              }).pipe(
                Effect.tapError((error) =>
                  error._tag === "StorageFailure"
                    ? RegistryCapacity.report("remotes")(error)
                    : Effect.void,
                ),
              ),
            ),
          ),
        ),
        remove: Effect.fn("Remotes.remove")((name) =>
          Effect.try({
            try: () =>
              editRows(location, () => {
                const rows = read(location);
                const kept = rows.filter((row) => row.name !== name);
                if (kept.length === rows.length) return false;
                write(location, kept);
                return true;
              }),
            catch: failed("remotes.remove"),
          }),
        ),
      });
    }),
  );
