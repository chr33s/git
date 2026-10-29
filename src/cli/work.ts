/**
 * Working-tree commands.
 *
 * Everything that touches files on disk: the commands that take `--work`, a
 * checkout rather than one of the bare repositories under `--root`.
 * Descriptions stay at the registration site in `main.ts`.
 */
import { Console, Effect } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";

import * as Checkout from "../git/Checkout.ts";
import { quoteListPath } from "../git/Diff.ts";
import { Repository } from "../git/Repository.ts";
import { cliSignature, mustResolve, withWork, workFlag, workPath } from "./shared.ts";

/**
 * `git status --porcelain`, deliberately.
 *
 * Two columns — index against HEAD, then disk against the index — is the
 * format every script that reads git's status already parses, so it is the
 * one worth emitting rather than a prettier private one.
 */
export const statusCommand = Command.make("status", { work: workFlag }, ({ work }) =>
  withWork(
    work,
    Effect.gen(function* () {
      const current = yield* Checkout.status();
      const letter = { added: "A", modified: "M", deleted: "D" } as const;

      const staged = new Map(current.staged.map((entry) => [entry.path, letter[entry.change]]));
      const unstaged = new Map(current.unstaged.map((entry) => [entry.path, letter[entry.change]]));
      const unmerged = new Map(current.unmerged.map((entry) => [entry.path, entry.status]));

      yield* Console.log(`## ${current.branch.replace(/^refs\/heads\//, "")}`);
      for (const path of [
        ...new Set([...staged.keys(), ...unstaged.keys(), ...unmerged.keys()]),
      ].sort()) {
        const code = unmerged.get(path) ?? `${staged.get(path) ?? " "}${unstaged.get(path) ?? " "}`;
        // Quoted as git quotes it: the format claimed above is one whose
        // readers take the path as the rest of the line, so a name carrying a
        // newline, a tab or a space has to arrive the way they expect it.
        yield* Console.log(`${code} ${quoteListPath(path)}`);
      }
      for (const path of current.untracked) yield* Console.log(`?? ${quoteListPath(path)}`);
    }),
  ),
);

export const addCommand = Command.make(
  "add",
  { work: workFlag, paths: Argument.string("paths").pipe(Argument.variadic({ min: 1 })) },
  ({ paths, work }) =>
    withWork(
      work,
      Effect.gen(function* () {
        for (const staged of yield* Checkout.add(yield* Effect.forEach(paths, workPath)))
          yield* Console.log(staged);
      }),
    ),
);

export const rm = Command.make(
  "rm",
  {
    work: workFlag,
    force: Flag.boolean("force").pipe(Flag.withDefault(false), Flag.withAlias("f")),
    cached: Flag.boolean("cached").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Unstage only, and leave the file on disk"),
    ),
    paths: Argument.string("paths").pipe(Argument.variadic({ min: 1 })),
  },
  ({ cached, force, paths, work }) =>
    withWork(
      work,
      Effect.gen(function* () {
        for (const removed of yield* Checkout.remove(yield* Effect.forEach(paths, workPath), {
          cached,
          force,
        }))
          yield* Console.log(removed);
      }),
    ),
);

export const mv = Command.make(
  "mv",
  { work: workFlag, from: Argument.string("from"), to: Argument.string("to") },
  ({ from, to, work }) =>
    withWork(
      work,
      Effect.gen(function* () {
        const moved = yield* Checkout.move(yield* workPath(from), yield* workPath(to));
        yield* Console.log(`${moved.from} -> ${moved.to}`);
      }),
    ),
);

export const restore = Command.make(
  "restore",
  {
    work: workFlag,
    staged: Flag.boolean("staged").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Restore the index rather than the work tree"),
    ),
    source: Flag.string("source").pipe(
      Flag.optional,
      Flag.withDescription("Take content from this revision instead of the index"),
    ),
    paths: Argument.string("paths").pipe(Argument.variadic({ min: 1 })),
  },
  ({ paths, source, staged, work }) =>
    withWork(
      work,
      Effect.gen(function* () {
        const options = { staged, worktree: !staged };
        const restored = yield* source._tag === "Some"
          ? Checkout.restore(yield* Effect.forEach(paths, workPath), {
              ...options,
              source: yield* mustResolve(yield* Repository, source.value),
            })
          : Checkout.restore(yield* Effect.forEach(paths, workPath), options);
        for (const path of restored) yield* Console.log(path);
      }),
    ),
);

export const switchCommand = Command.make(
  "switch",
  {
    work: workFlag,
    create: Flag.boolean("create").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Branch from HEAD first"),
    ),
    force: Flag.boolean("force").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Overwrite unstaged changes instead of refusing"),
    ),
    branch: Argument.string("branch"),
  },
  ({ branch, create, force, work }) =>
    withWork(
      work,
      Effect.gen(function* () {
        const result = yield* Checkout.checkout(branch, { create, force });
        yield* Console.log(`${result.ref} ${result.oid} (${result.files} file(s))`);
      }),
    ),
);

export const commitCommand = Command.make(
  "commit",
  {
    work: workFlag,
    message: Flag.string("message").pipe(Flag.withDescription("Commit message")),
  },
  ({ message, work }) =>
    withWork(
      work,
      Effect.gen(function* () {
        const made = yield* Checkout.commit({ message, author: yield* cliSignature() });
        yield* Console.log(`${made.oid} ${made.files} file(s)`);
      }),
    ),
);
