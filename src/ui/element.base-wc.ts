/**
 * `@chr33s/base-wc`, as one typed Foldkit boundary.
 *
 * Every `ui-*` element the views use is declared here and nowhere else, so the
 * shape of that boundary is one file to read rather than a habit spread across
 * seven screens. Two kinds live in it:
 *
 * - **Declarative.** A property the Model owns, an event the element emits.
 *   `CustomElement.define` covers these: Foldkit diffs the property across
 *   renders and turns the `CustomEvent` into a Message.
 * - **Imperative.** A method — `dialog.hide()` — that no property expresses.
 *   Those are Commands at the bottom of this file, reached from `update`,
 *   never from a view.
 *
 * Nothing here renders into a shadow root. `base-wc`'s contract is light DOM,
 * because the global stylesheet has to reach these elements and their ARIA
 * relationships (`aria-controls`, `aria-activedescendant`) resolve against
 * elements the page owns. Foldkit's virtual DOM and these elements therefore
 * share one tree, which is fine for every element here: each owns behaviour
 * and attributes on children the view itself declares, and none of them
 * inserts, moves or removes a child the *view* declared. An element that did
 * would need a Mount instead, because the diff would fight it.
 *
 * `ui-search-field` is the one that adds a node of its own — the clear button
 * it appends after the input. That is safe because the view never declares a
 * sibling there for the diff to reconcile it against, and because the element
 * owns it for its whole life.
 */
import { CustomElement, Html } from "foldkit";
import { Effect, Schema } from "effect";

/**
 * The modal. Opening is declarative — a child carrying `data-dialog-trigger`
 * opens it, and the element wires that itself — so only closing needs a
 * Command. `open` is readable but not settable, which is why it is absent
 * here: a property the view cannot drive is not part of this boundary.
 */
export const dialog = CustomElement.define({
  tag: "ui-dialog",
  properties: {},
  events: {},
});

export const dialogPopup = CustomElement.define({
  tag: "ui-dialog-popup",
  properties: {},
  events: {},
});

/**
 * The rail's search box.
 *
 * Both events are bound, and each answers something the other cannot.
 *
 * `search` is the debounced one: the element emits it once the reader has
 * stopped typing, so one query goes to `/grep` rather than one per keystroke.
 * It is what decides *when* to search, and the clear button and Escape emit
 * it immediately rather than waiting out a debounce nobody is still typing
 * into.
 *
 * The inner input's `input` is bound too, and must be: the view binds `value`
 * to the Model, Foldkit re-asserts a controlled value on every patch, and a
 * Model that heard only the debounced query would rewrite the box back to it
 * mid-word. That binding keeps the Model level with the box. The clear paths
 * reach it as well — the element fires a native `input` on the control before
 * emitting `search` — so the two never disagree about what the box holds.
 */
export const searchField = CustomElement.define({
  tag: "ui-search-field",
  properties: { value: Schema.String },
  events: { search: Schema.Struct({ value: Schema.String }) },
});

/** A segmented control. `change` carries the value now selected. */
export const toggleGroup = CustomElement.define({
  tag: "ui-toggle-group",
  properties: { value: Schema.String },
  events: { change: Schema.Struct({ value: Schema.NullOr(Schema.String) }) },
});

export const toggle = CustomElement.define({
  tag: "ui-toggle",
  properties: { pressed: Schema.Boolean },
  events: { change: Schema.Struct({ pressed: Schema.Boolean }) },
});

/**
 * A switch wrapping a real checkbox.
 *
 * The checkbox is the view's own child and carries the checked state, so this
 * element declares no property: it is styling and keyboard behaviour around a
 * native control that already submits with its form.
 */
export const uiSwitch = CustomElement.define({
  tag: "ui-switch",
  properties: {},
  events: {},
});

/** Tabs. `value` names the open panel; `change` reports the reader's choice. */
export const tabs = CustomElement.define({
  tag: "ui-tabs",
  properties: { value: Schema.NullOr(Schema.String) },
  events: { change: Schema.Struct({ value: Schema.String }) },
});

export const tabList = CustomElement.define({
  tag: "ui-tab-list",
  properties: {},
  events: {},
});

/**
 * A popover menu. `menu-select` carries the chosen item's `value`, which is
 * why the items below need no events of their own — the menu reports for them.
 */
export const menu = CustomElement.define({
  tag: "ui-menu",
  properties: {},
  events: { "menu-select": Schema.Struct({ value: Schema.String }) },
});

export const menuPopup = CustomElement.define({
  tag: "ui-menu-popup",
  properties: {},
  events: {},
});

export const menuItem = CustomElement.define({
  tag: "ui-menu-item",
  properties: {},
  events: {},
});

/**
 * Every spec above, bound to one view's Message universe in one call.
 *
 * A view takes the builder Foldkit hands it and gets the whole library back
 * typed against its own Messages, so a screen never binds specs one at a time
 * and cannot bind one to the wrong universe by hand.
 */
export const elements = <Message>(h: Html.HtmlBuilder<Message>) => ({
  dialog: dialog.withMessage(h),
  dialogPopup: dialogPopup.withMessage(h),
  searchField: searchField.withMessage(h),
  toggleGroup: toggleGroup.withMessage(h),
  toggle: toggle.withMessage(h),
  uiSwitch: uiSwitch.withMessage(h),
  tabs: tabs.withMessage(h),
  tabList: tabList.withMessage(h),
  menu: menu.withMessage(h),
  menuPopup: menuPopup.withMessage(h),
  menuItem: menuItem.withMessage(h),
});

/** The library, as a view sees it. */
export type Elements<Message> = ReturnType<typeof elements<Message>>;

/** What an imperative call did, as a fact the Model can hold. */
export const Outcome = Schema.Literals(["Closed", "NotFound"]);
export type Outcome = typeof Outcome.Type;

/**
 * Open the `ui-dialog` matching `selector`.
 *
 * Most dialogs here open from a child carrying `data-dialog-trigger`, which
 * the element wires itself — this is for the one that opens from a menu item
 * instead, where there is no trigger to carry.
 */
export const showDialog = (selector: string): Effect.Effect<Outcome> =>
  Effect.sync(() => {
    const found = document.querySelector(selector);
    if (!(found instanceof HTMLElement) || !("show" in found)) return "NotFound";
    // SAFETY: `show` was just found on the element, and these selectors name
    // only `ui-dialog` — whose `show()` takes no arguments.
    (found as { show: () => void }).show();
    return "Closed";
  });

/**
 * Close the `ui-dialog` matching `selector`.
 *
 * An Effect rather than a Command, because the Message it should answer with
 * is the application's business and this module has no opinion about it:
 * `app.command.shell.ts` wraps this with the Message that belongs to the
 * dialog being closed. What lives here is the part that is base-wc's — that
 * closing is `hide()` on a custom element.
 *
 * Foldkit's own `Dom.closeDialog` is not this: it calls `.close()` and fails
 * unless the selector names an `HTMLDialogElement`. `ui-dialog` is a custom
 * element with its own `hide()`, so the two are not interchangeable.
 *
 * The element is found by selector rather than held anywhere. A live element
 * in the Model would be a handle in a value meant to hold facts.
 *
 * `NotFound` is an outcome, not a failure. Asking a dialog to close after the
 * render that removed it is a race the reader can cause — closing the last
 * task while the dialog is dismissing — and it is not an error.
 */
export const hideDialog = (selector: string): Effect.Effect<Outcome> =>
  Effect.sync(() => {
    const found = document.querySelector(selector);
    if (!(found instanceof HTMLElement) || !("hide" in found)) return "NotFound";
    // SAFETY: `hide` was just found on the element, and these selectors name
    // only `ui-dialog` — whose `hide()` takes no arguments.
    (found as { hide: () => void }).hide();
    return "Closed";
  });
