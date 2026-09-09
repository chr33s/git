/**
 * Working-tree commands.
 *
 * Everything that touches files on disk: the commands that take `--work`, a
 * checkout rather than one of the bare repositories under `--root`.
 * Descriptions stay at the registration site in `main.ts`.
 */
import * as path from "node:path";

import { Console, Context, Effect, Layer } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";

import * as Checkout from "../git/Checkout.ts";
import { quoteListPath } from "../git/Diff.ts";
import { Invalid } from "../git/Error.ts";
import { MergeState } from "../git/MergeState.ts";
import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { IndexStore, WorkTree } from "../git/Work.ts";
import { workspace } from "../git/Work.node.ts";
import { GitInvocation } from "./GitCompat.ts";
import { discoverRepository } from "./GitCompat.node.ts";
import { cliSignature, mustResolve } from "./shared.ts";

/**
 * A checkout, rather than one of the bare repositories under `--root`.
 *
 * The working-tree commands are the only ones that need files on disk, so
 * they take `--work` — a directory whose repository is `.git` inside it,
 * which is the layout `git` itself uses and the reason the two can be
 * pointed at the same directory.
 */
export const workFlag = Flag.string("work").pipe(
  Flag.optional,
  Flag.withDescription("Explicit checkout selector for extension commands"),
);

class WorkPaths extends Context.Service<
  WorkPaths,
  { readonly root: string; readonly base: string }
>()("cli/WorkPaths") {}

const workPath = Effect.fn("cli.workPath")(function* (value: string) {
  const paths = yield* WorkPaths;
  return (
    path.relative(paths.root, path.resolve(paths.base, value)).split(path.sep).join("/") || "."
  );
});

export const withWork = <A, E>(
  work: { readonly _tag: "None" } | { readonly _tag: "Some"; readonly value: string },
  effect: Effect.Effect<A, E, Repository | WorkTree | IndexStore | MergeState | WorkPaths>,
) =>
  Effect.gen(function* () {
    const invocation = yield* GitInvocation;
    if (invocation.bare) {
      return yield* new Invalid({
        field: "bare",
        reason: "this command requires a work tree",
      });
    }
    let selected = invocation;
    if (invocation.workTree === undefined && work._tag === "Some") {
      const location = path.resolve(invocation.cwd, work.value);
      // The extension's checkout selector chooses a repository to discover;
      // an explicit Git directory already selects its metadata separately.
      selected =
        invocation.gitDir === undefined
          ? { ...invocation, cwd: location }
          : { ...invocation, workTree: location };
    }
    const found = yield* discoverRepository(selected);
    if (found === null || found.workTree === null) {
      return yield* new Invalid({
        field: "repository",
        reason: "not a Git work tree",
      });
    }
    const relative = path.relative(found.workTree, invocation.cwd);
    const base =
      relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)
        ? found.workTree
        : invocation.cwd;
    return yield* effect.pipe(
      Effect.provideService(WorkPaths, { root: found.workTree, base }),
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provide(stores(found.gitDir)),
          Layer.provideMerge(workspace(found.workTree, found.gitDir)),
        ),
      ),
    );
  });

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
