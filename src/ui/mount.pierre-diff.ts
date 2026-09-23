/**
 * `@pierre/diffs`, mounted.
 *
 * A diff renderer owns its subtree: it builds DOM, holds a Shiki highlighter,
 * and keeps state Foldkit's virtual DOM has no business diffing. So the view
 * declares an empty host element and this attaches the library to it — the
 * library owns the subtree, Foldkit owns the lifetime, and the Model owns the
 * facts (which file, which sides, which palette).
 *
 * Everything is built inside the acquire body, never before it. Foldkit's
 * Mount contract is explicit that `Effect.acquireRelease` only guarantees
 * "acquire completed, therefore release is registered": a renderer constructed
 * before the call and merely returned from it can leak if acquisition is
 * interrupted, which a fast reader clicking through Change Requests can cause.
 *
 * The import is inside too, and lazily: `@pierre/diffs` brings Shiki, whose
 * default entry carries every bundled grammar and theme. Only two screens ever
 * need it, so it loads on first mount rather than in the entry bundle.
 */
import { Mount } from "foldkit";
import { Effect, Schema } from "effect";

import { diffs } from "./highlight.ts";
import { Theme } from "./app.model.ts";
import { AppMessage } from "./app.message.ts";

export const PierreDiff = Mount.define("PierreDiff", {
  args: {
    path: Schema.String,
    /** `null` for an added file: there is no side to show it against. */
    oldContents: Schema.NullOr(Schema.String),
    /** `null` for a removed file. One of the two is always present. */
    newContents: Schema.NullOr(Schema.String),
    theme: Theme,
  },
  messages: [AppMessage.CompletedMountDiff],
  execute: ({ element, path, oldContents, newContents, theme }) =>
    Effect.gen(function* () {
      yield* Effect.acquireRelease(
        Effect.gen(function* () {
          const { FileDiff } = yield* Effect.promise(async () => await diffs());
          // Foldkit types the mounted node as `Element`; the renderer wants
          // the HTML one it always is — the view mounts this on a `div`.
          const host = element instanceof HTMLElement ? element : null;
          if (host === null) return { renderer: null, element: null };
          const renderer = new FileDiff({
            themeType: theme,
            diffStyle: "unified",
            disableFileHeader: true,
            overflow: "scroll",
          });
          const oldFile = oldContents === null ? null : { name: path, contents: oldContents };
          const newFile = newContents === null ? null : { name: path, contents: newContents };
          // A diff entry with neither side would not have been reported as a
          // change, so one of the two is always there — and the renderer's
          // two overloads want the present one named.
          if (newFile === null && oldFile !== null) {
            renderer.render({ oldFile, newFile: null, containerWrapper: host });
          } else if (newFile !== null) {
            renderer.render({ oldFile, newFile, containerWrapper: host });
          }
          return { renderer, element: host };
        }),
        ({ renderer, element: host }) =>
          Effect.sync(() => {
            // `cleanUp` is what releases the resize, interaction and scroll
            // managers and unsubscribes from theme changes — and one of those
            // keeps observed elements in a process-wide map, so skipping it
            // leaks a manager and its detached nodes per file per visit.
            renderer?.cleanUp();
            // The library built this subtree; Foldkit never owned it, so
            // emptying the host is the other half of releasing it.
            host?.replaceChildren();
          }),
      );
      return AppMessage.CompletedMountDiff({ path });
    }),
});
