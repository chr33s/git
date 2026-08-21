/**
 * Working-tree commands.
 *
 * Everything that touches files on disk: the commands that take `--work`, a
 * checkout rather than one of the bare repositories under `--root`.
 * Descriptions stay at the registration site in `main.ts`.
 */
import * as path from "node:path";

import { Console, Effect, Layer } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";

import * as Checkout from "../git/Checkout.ts";
import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { IndexStore, WorkTree } from "../git/Work.ts";
import { workspace } from "../git/Work.node.ts";
import { cliSignature } from "./shared.ts";

/**
 * A checkout, rather than one of the bare repositories under `--root`.
 *
 * The working-tree commands are the only ones that need files on disk, so
 * they take `--work` — a directory whose repository is `.git` inside it,
 * which is the layout `git` itself uses and the reason the two can be
 * pointed at the same directory.
 */
const workFlag = Flag.string("work").pipe(
  Flag.withDefault("."),
  Flag.withDescription("A checkout: a work tree whose repository is .git inside it"),
);

const withWork = <A, E>(
  work: string,
  effect: Effect.Effect<A, E, Repository | WorkTree | IndexStore>,
) =>
  effect.pipe(
    Effect.provide(
      GitRepository.layer.pipe(
        Layer.provide(GitRepository.hooksNoop),
        Layer.provide(stores(path.join(work, ".git"))),
        Layer.provideMerge(workspace(work)),
      ),
    ),
  );

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

      yield* Console.log(`## ${current.branch.replace(/^refs\/heads\//, "")}`);
      for (const path of [...new Set([...staged.keys(), ...unstaged.keys()])].sort()) {
        yield* Console.log(`${staged.get(path) ?? " "}${unstaged.get(path) ?? " "} ${path}`);
      }
      for (const path of current.untracked) yield* Console.log(`?? ${path}`);
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
        for (const staged of yield* Checkout.add(paths)) yield* Console.log(staged);
      }),
    ),
);

export const rm = Command.make(
  "rm",
  {
    work: workFlag,
    cached: Flag.boolean("cached").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Unstage only, and leave the file on disk"),
    ),
    paths: Argument.string("paths").pipe(Argument.variadic({ min: 1 })),
  },
  ({ cached, paths, work }) =>
    withWork(
      work,
      Effect.gen(function* () {
        for (const removed of yield* Checkout.remove(paths, { cached }))
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
        const moved = yield* Checkout.move(from, to);
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
          ? Checkout.restore(paths, { ...options, source: source.value })
          : Checkout.restore(paths, options);
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
