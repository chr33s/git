/**
 * `@pierre/trees`, mounted.
 *
 * The explorer is path-first: it takes a flat list of blob paths and derives
 * the folder structure itself, so the `/files` response needs no reshaping.
 * It also owns its subtree, its scroll position and which folders are open,
 * which is exactly the kind of state a virtual DOM must not diff — so the view
 * declares an empty host and this attaches the library to it.
 *
 * A stream Mount rather than a one-shot: selecting a row is an event, and
 * there are as many of them as the reader cares to make.
 *
 * Which folders are open is remembered per repository. Visible rows are
 * exactly the restorable state: a folder hidden under a collapsed ancestor
 * cannot be reopened without its ancestor, so recording only what shows also
 * guarantees the saved set never names a folder whose ancestors are closed.
 * No entry at all is a first visit, which opens everything — by listing
 * everything, because the tree is always built over a "closed" baseline. The
 * sample repository stays out of storage entirely, so a sample layout never
 * shapes a real one.
 */
import { Mount } from "foldkit";
import { Effect, Queue, Schema, Stream } from "effect";

import * as Code from "./code.ts";
import { AppMessage } from "./app.message.ts";

/** The two Messages this Mount emits, and only those. */
type TreeMessage =
  | ReturnType<typeof AppMessage.ClickedFile>
  | ReturnType<typeof AppMessage.CompletedMountTree>;

const StoredExpansion = Schema.Array(Schema.String);

/** What this repository last had open, or `null` for a first visit. */
const storedExpansion = (repo: string): readonly string[] | null => {
  try {
    const raw = localStorage.getItem(Code.expansionKey(repo));
    if (raw === null) return null;
    return Schema.decodeUnknownSync(StoredExpansion)(JSON.parse(raw));
  } catch {
    // Whatever is stored is not this shape — a first visit's default beats
    // trusting it.
    return null;
  }
};

export const PierreTree = Mount.defineStream("PierreTree", {
  args: {
    paths: Schema.Array(Schema.String),
    /** Empty for the sample repository, which is not remembered. */
    repo: Schema.String,
    /** Open the folders leading to this file, so a deep link lands visible. */
    selected: Schema.NullOr(Schema.String),
    offline: Schema.Boolean,
  },
  messages: [AppMessage.ClickedFile, AppMessage.CompletedMountTree],
  execute: ({ element, paths, repo, selected, offline }) =>
    // `Stream.callback` gives the tree a queue to push into for as long as the
    // element is mounted, and a scope to release it in. The scope is the
    // element's: Foldkit closes it on unmount, which runs the cleanup below.
    Stream.callback<TreeMessage>((queue) =>
      Effect.acquireRelease(
        Effect.gen(function* () {
          const { FileTree } = yield* Effect.promise(async () => await import("@pierre/trees"));
          const stored = repo === "" ? null : storedExpansion(repo);
          const tree = new FileTree({
            paths: [...paths],
            initialExpansion: "closed",
            initialExpandedPaths: [
              ...new Set([
                ...(stored ?? Code.allDirectories(paths)),
                ...Code.allDirectories(selected === null ? [] : [selected]),
              ]),
            ],
            flattenEmptyDirectories: true,
            // The design's explorer header carries only "+" and "…";
            // repository-wide search lives in the rail's ⌘K field, so the tree
            // does not add a second search box of its own.
            search: false,
            gitStatus: offline ? [...Code.FALLBACK_STATUS] : [],
            onSelectionChange: (chosen) => {
              const path = chosen[0];
              // Directories arrive here too; they have no blob to read, and
              // `update` drops a path the view does not hold.
              if (path !== undefined) Queue.offerUnsafe(queue, AppMessage.ClickedFile({ path }));
            },
          });
          const stop =
            repo === ""
              ? null
              : tree.subscribe(() => {
                  try {
                    localStorage.setItem(
                      Code.expansionKey(repo),
                      JSON.stringify(
                        tree
                          .getVisibleRows(0, tree.getVisibleCount())
                          .filter((row) => row.kind === "directory" && row.isExpanded)
                          .map((row) => row.path),
                      ),
                    );
                  } catch {
                    // A browser refusing storage forgets which folders were
                    // open between sessions. It is not a reason to fail here.
                  }
                });
          if (element instanceof HTMLElement) tree.render({ containerWrapper: element });
          Queue.offerUnsafe(queue, AppMessage.CompletedMountTree());
          return { tree, stop };
        }),
        ({ tree, stop }) =>
          Effect.sync(() => {
            stop?.();
            tree.cleanUp();
          }),
      ),
    ),
});
