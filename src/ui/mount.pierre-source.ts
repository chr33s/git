/**
 * `@pierre/diffs`'s file renderer, mounted — read-only, and editable.
 *
 * The same highlighted surface serves both: the pencil attaches the package's
 * edit mode to the already-rendered file, which is what gives the editor its
 * own undo stack over exactly the text on screen.
 *
 * The draft does not live in here. The editor reports every change back as a
 * Message and the Model holds the text, so what a commit writes is what the
 * Model says — which is the spec's rule for this integration, and the
 * difference between one source of truth and two.
 *
 * A stream Mount, because an editing session emits as long as it is open. A
 * read-only view emits once and then nothing, which costs nothing.
 */
import { Mount } from "foldkit";
import { Effect, Queue, Schema, Stream } from "effect";

import { diffs } from "./highlight.ts";
import { Theme } from "./app.model.ts";
import { AppMessage } from "./app.message.ts";

/** The Messages this Mount emits, and only those. */
type SourceMessage =
  | ReturnType<typeof AppMessage.ChangedFileDraft>
  | ReturnType<typeof AppMessage.CompletedMountTree>;

export const PierreSource = Mount.defineStream("PierreSource", {
  args: {
    /** The file's name, which is what picks its grammar. */
    name: Schema.String,
    /** The text to show. In edit mode this seeds the session, once. */
    contents: Schema.String,
    editable: Schema.Boolean,
    theme: Theme,
  },
  messages: [AppMessage.ChangedFileDraft, AppMessage.CompletedMountTree],
  execute: ({ element, name, contents, editable, theme }) =>
    Stream.callback<SourceMessage>((queue) =>
      Effect.acquireRelease(
        Effect.gen(function* () {
          const { File, Editor } = yield* Effect.promise(async () => await diffs());
          const host = element instanceof HTMLElement ? element : null;
          if (host === null) return { viewer: null, session: null, detach: null };

          // A fresh `File` appends its own `<diffs-container>` and never
          // removes a predecessor's, so the host is emptied first: without
          // this, every discarded viewer left its pane stacked above the live
          // one on a client swap or a branch switch.
          host.replaceChildren();
          const viewer = new File({
            themeType: theme,
            disableFileHeader: true,
            overflow: "scroll",
            stickyHeader: false,
          });
          viewer.render({ file: { name, contents }, containerWrapper: host });

          if (!editable) {
            Queue.offerUnsafe(queue, AppMessage.CompletedMountTree());
            return { viewer, session: null, detach: null };
          }

          const session = new Editor("file", {
            // This is the whole contract with the Model: every keystroke the
            // package resolves into a document change comes back as a
            // Message, and the Model's `draft` is what a commit writes.
            onChange: () => {
              Queue.offerUnsafe(queue, AppMessage.ChangedFileDraft({ text: session.getText() }));
            },
          });
          const detach = session.edit(viewer);
          // With a line, not bare: a bare `focus()` moves element focus
          // without placing a caret, and a contenteditable with no selection
          // swallows keystrokes. The surface materialises through the
          // package's own render queue some frames after attach, so this is
          // asked for on the next frame rather than immediately.
          requestAnimationFrame(() => {
            session.focus({ lineNumber: "first-visible" });
          });
          Queue.offerUnsafe(queue, AppMessage.CompletedMountTree());
          return { viewer, session, detach };
        }),
        ({ viewer, session, detach }) =>
          Effect.sync(() => {
            // Detaching first: the draft dies with the session, and the
            // viewer owns the pane again. Both are the library's, and the
            // host element is Foldkit's to remove.
            detach?.();
            session?.cleanUp();
            // Then the viewer itself. `cleanUp` is what releases the resize
            // and interaction managers and unsubscribes from theme changes,
            // and the resize manager holds its observed elements in a
            // process-wide map — so a viewer left uncleaned pins the file's
            // whole rendered subtree for the life of the page, once per file
            // opened.
            viewer?.cleanUp();
          }),
      ),
    ),
});
