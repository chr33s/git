/** Clone follow-up transport commands: fetch and its branch-specific pull spelling. */
import * as path from "node:path";

import { Console, Effect } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";

import { fetchRepository } from "../client/Fetch.ts";
import { Invalid } from "../git/Error.ts";
import { stores } from "../git/Node.ts";
import { ObjectStore, RefStore } from "../git/Store.ts";
import { repoArgument, rootFlag } from "./shared.ts";

interface FetchInput {
  readonly url: string;
  readonly branch?: string | undefined;
  readonly token?: string | undefined;
}

/** One fetch setup for both `fetch` and the branch-specific `pull` alias. */
const fetchInto = Effect.fn("cli.fetchInto")(function* (input: FetchInput) {
  const target = { objects: yield* ObjectStore, refs: yield* RefStore };
  const before = new Map(yield* target.refs.list());
  const result = yield* fetchRepository({ ...input, stores: target });
  return { before, target, result };
});

export const fetchCommand = Command.make(
  "fetch",
  {
    root: rootFlag,
    token: Flag.string("token").pipe(Flag.withDefault("")),
    branch: Flag.string("branch").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Fetch one branch instead of everything advertised"),
    ),
    repo: repoArgument,
    url: Argument.string("url"),
  },
  ({ branch, repo, root, token, url }) =>
    Effect.gen(function* () {
      const { before, result } = yield* fetchInto({
        url,
        branch: branch === "" ? undefined : branch,
        token: token === "" ? undefined : token,
      });
      const moved = result.refs.filter((update) => before.get(update.name) !== update.value);
      for (const update of moved) {
        yield* Console.log(`${update.value ?? "0".repeat(40)} ${update.name}`);
      }
      for (const rejected of result.rejected) {
        yield* Console.error(`refused ${rejected.name}: not a fast-forward`);
      }
      if (moved.length === 0 && result.rejected.length === 0) yield* Console.log("up to date");
    }).pipe(Effect.provide(stores(path.join(root, repo)))),
);

/** `fetch --branch`, under the name fingers expect. */
export const pullCommand = Command.make(
  "pull",
  {
    root: rootFlag,
    token: Flag.string("token").pipe(Flag.withDefault("")),
    repo: repoArgument,
    url: Argument.string("url"),
    branch: Argument.string("branch"),
  },
  ({ branch, repo, root, token, url }) =>
    Effect.gen(function* () {
      const { before, result } = yield* fetchInto({
        url,
        branch,
        token: token === "" ? undefined : token,
      });
      const name = `refs/heads/${branch}`;
      const previous = before.get(name) ?? null;
      if (result.rejected.some((entry) => entry.name === name)) {
        return yield* new Invalid({
          field: "branch",
          reason: `${name} diverged — merge or rebase, a pull cannot guess which`,
        });
      }
      const moved = result.refs.find((update) => update.name === name && update.value !== previous);
      if (moved !== undefined) {
        yield* Console.log(`${moved.value ?? ""} ${name}`);
        return;
      }
      yield* Console.log("up to date");
    }).pipe(Effect.provide(stores(path.join(root, repo)))),
);
