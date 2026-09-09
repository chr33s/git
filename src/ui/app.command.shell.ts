/**
 * The shell's one-shot effects: the palette, the rail, and focus.
 *
 * Small, but they are the reason `update` can stay pure. Each writes to
 * something outside the Model — `localStorage`, the document element, the
 * focus ring — and answers with a Message, so the write is visible in the
 * same history as the click that caused it.
 */
import { Command, Dom, Navigation } from "foldkit";
import { Effect, Schema } from "effect";

import { Theme } from "./app.model.ts";
import { AppMessage } from "./app.message.ts";
import * as palette from "./theme.ts";
import { hideDialog, showDialog } from "./element.base-wc.ts";

/**
 * Pin a palette: stamps the root so `tokens.css` switches, and remembers it.
 *
 * `index.html` reads the same key synchronously before first paint, which is
 * what keeps a dark-mode reader from seeing a white flash. This Command and
 * that script are the two halves of one contract, and `theme.ts` holds both.
 */
export const ApplyTheme = Command.define("ApplyTheme", {
  args: { theme: Theme },
  messages: [AppMessage.ChangedTheme],
  execute: ({ theme }) =>
    Effect.sync(() => {
      palette.apply(theme);
      return AppMessage.ChangedTheme({ theme });
    }),
});

/** Remember the rail's width, the way `theme.ts` remembers the palette. */
export const RememberRail = Command.define("RememberRail", {
  args: { collapsed: Schema.Boolean },
  messages: [AppMessage.CompletedFocus],
  execute: ({ collapsed }) =>
    Effect.sync(() => {
      try {
        localStorage.setItem(RAIL_KEY, String(collapsed));
      } catch {
        // A browser refusing storage is a browser that forgets the rail's
        // width between sessions. It is not a reason to fail the click.
      }
      return AppMessage.CompletedFocus();
    }),
});

/** Where the rail remembers being collapsed — `theme.ts`'s pattern. */
export const RAIL_KEY = "gp-nav-collapsed";

/** What the page last remembered, for the first Model. */
export const railCollapsed = (): boolean => {
  try {
    return localStorage.getItem(RAIL_KEY) === "true";
  } catch {
    return false;
  }
};

/**
 * Move focus to the rail's search input.
 *
 * `ElementNotFound` is recovered rather than propagated: the input is behind a
 * render, and asking for it in the same turn that expanded the rail is a race
 * the reader can win. Nothing is broken when it loses.
 */
export const FocusSearch = Command.define("FocusSearch", {
  args: { selector: Schema.String },
  messages: [AppMessage.CompletedFocus],
  execute: ({ selector }) =>
    Dom.focus(selector).pipe(
      Effect.as(AppMessage.CompletedFocus()),
      Effect.orElseSucceed(() => AppMessage.CompletedFocus()),
    ),
});

/**
 * Go somewhere, as a Command.
 *
 * `Navigation.pushUrl` is an Effect the runtime understands; a Command is what
 * an `update` returns. Wrapping it here rather than at each call site keeps
 * `app.update.ts` returning one kind of thing.
 *
 * The Message it answers with is `ChangedUrl`'s cousin, not `ChangedUrl`
 * itself: the runtime dispatches that from `onUrlChange` once the address has
 * actually moved, and a second one from here would apply the route twice.
 */
export const Navigate = Command.define("Navigate", {
  args: { url: Schema.String },
  messages: [AppMessage.CompletedNavigate],
  execute: ({ url }) => Navigation.pushUrl(url).pipe(Effect.as(AppMessage.CompletedNavigate())),
});

/** Close the "New task" dialog, and say what closing it did. */
export const CloseNewTaskDialog = Command.define("CloseNewTaskDialog", {
  args: { selector: Schema.String },
  messages: [AppMessage.CompletedCloseNewTaskDialog],
  execute: ({ selector }) =>
    hideDialog(selector).pipe(
      Effect.map((outcome) => AppMessage.CompletedCloseNewTaskDialog({ outcome })),
    ),
});

/** Leave the application, for a link that points off-site. */
export const LoadUrl = Command.define("LoadUrl", {
  args: { href: Schema.String },
  messages: [AppMessage.CompletedNavigate],
  execute: ({ href }) => Navigation.load(href).pipe(Effect.as(AppMessage.CompletedNavigate())),
});

/**
 * Put text on the clipboard.
 *
 * A refusal is silent by design: a browser that denies clipboard access has
 * told the reader already, and a second notice from the page would only
 * repeat it.
 */
export const CopyText = Command.define("CopyText", {
  args: { text: Schema.String },
  messages: [AppMessage.CompletedCopy],
  execute: ({ text }) =>
    Effect.tryPromise(async () => await navigator.clipboard.writeText(text)).pipe(
      Effect.as(AppMessage.CompletedCopy()),
      Effect.orElseSucceed(() => AppMessage.CompletedCopy()),
    ),
});

/** Open a dialog that has no trigger of its own — the branch menu's. */
export const OpenDialog = Command.define("OpenDialog", {
  args: { selector: Schema.String },
  messages: [AppMessage.CompletedCloseNewTaskDialog],
  execute: ({ selector }) =>
    showDialog(selector).pipe(
      Effect.map((outcome) => AppMessage.CompletedCloseNewTaskDialog({ outcome })),
    ),
});
