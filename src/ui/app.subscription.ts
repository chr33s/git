/**
 * The external event sources the application listens to for as long as it runs.
 *
 * Two, and both are the page's rather than a screen's: the ⌘K the rail
 * advertises, and the operating system's palette preference. Browser history
 * is deliberately absent — the runtime owns `popstate` and link interception
 * and delivers them through `onUrlChange`, so a Subscription for it would be
 * a second listener racing the first.
 */
import { Subscription } from "foldkit";
import { Option, Schema } from "effect";

import { AppMessage } from "./app.message.ts";
import type { Model } from "./app.model.ts";

export const subscriptions = Subscription.make<Model, AppMessage>()((entry) => ({
  /** The ⌘K the search row advertises. Ctrl+K, for keyboards without a ⌘. */
  searchShortcut: entry(
    {},
    {
      modelToDependencies: () => ({}),
      dependenciesToStream: () =>
        Subscription.fromEventFilterMap({
          target: globalThis,
          type: "keydown",
          toMessage: (event: KeyboardEvent) => {
            if (!(event.metaKey || event.ctrlKey) || event.key.toLowerCase() !== "k") {
              return Option.none();
            }
            // Inside the mapper, so it runs in the browser's own dispatch and
            // the browser's find-in-page never opens.
            event.preventDefault();
            return Option.some(AppMessage.PressedSearchShortcut());
          },
        }),
    },
  ),

  /**
   * The operating system's palette, while the reader has not pinned one.
   *
   * Gated on the Model: once a palette is stored, the page has been told what
   * to show and the system changing its mind is not an instruction. The
   * dependency is what `theme.ts` stored, so pinning a palette closes this
   * stream and unpinning would reopen it.
   */
  systemTheme: entry(
    { pinned: Schema.Boolean },
    {
      modelToDependencies: (model: Model) => ({ pinned: model.themePinned }),
      dependenciesToStream: ({ pinned }) =>
        pinned
          ? Subscription.fromEventFilterMap({
              target: globalThis,
              type: "gp-never",
              toMessage: () => Option.none<AppMessage>(),
            })
          : Subscription.fromEvent({
              target: globalThis.matchMedia("(prefers-color-scheme: dark)"),
              type: "change",
              toMessage: (event: MediaQueryListEvent) =>
                AppMessage.ChangedTheme({ theme: event.matches ? "dark" : "light" }),
            }),
    },
  ),
}));
