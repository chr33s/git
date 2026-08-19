/**
 * The `@chr33s/base-wc` elements this UI uses, registered in one place.
 *
 * A bare `import "@chr33s/base-wc/src/switch"` does not survive bundling: the
 * package marks only `./dist/elements.js` and its stylesheets as having side
 * effects, and this build consumes the *TypeScript sources* behind `./src/*`
 * — which that list does not cover — so a bundler is entitled to drop the
 * import as dead weight. It does, and the elements then never register: the
 * markup renders as inert unknown tags with none of the ARIA or keyboard
 * behaviour.
 *
 * Importing the classes and holding onto them keeps the modules alive, and
 * each one self-registers when it is evaluated. `REGISTERED` is exported and
 * read by `main.ts` so nothing here can be shaken out either.
 */
import { UIDialog, UIDialogBackdrop, UIDialogPopup } from "@chr33s/base-wc/src/dialog";
import { UIMenu, UIMenuItem, UIMenuPopup } from "@chr33s/base-wc/src/menu";
import { UISearchField } from "@chr33s/base-wc/src/search-field";
import { UISwitch } from "@chr33s/base-wc/src/switch";
import { UITabList, UITabs } from "@chr33s/base-wc/src/tabs";
import { UIToggle, UIToggleGroup } from "@chr33s/base-wc/src/toggle";

/** Every custom element constructor the templates rely on. */
export const REGISTERED: readonly CustomElementConstructor[] = [
  UIDialog,
  UIDialogBackdrop,
  UIDialogPopup,
  UIMenu,
  UIMenuItem,
  UIMenuPopup,
  UISearchField,
  UISwitch,
  UITabList,
  UITabs,
  UIToggle,
  UIToggleGroup,
];
