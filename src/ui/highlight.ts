/**
 * Lazy access to `@pierre/diffs`.
 *
 * The package brings Shiki with it, and Shiki's default entry carries every
 * bundled grammar and theme — several megabytes before anything is rendered.
 * Only two screens ever need it (Code, and a Change Request's Diff tab), so it
 * is loaded on first use and cached, which keeps it out of the initial bundle
 * and off the critical path for Tasks, Activity and Settings.
 *
 * The `web-components` import is the side effect that registers
 * `<diffs-container>`; without it the renderers draw into an unstyled element.
 * See `vite.config.ts` for why it is reached through an alias.
 */
import type { File as DiffsFile, FileDiff } from "@pierre/diffs";
import type { Editor } from "@pierre/diffs/edit";

export interface Diffs {
  readonly File: typeof DiffsFile;
  readonly FileDiff: typeof FileDiff;
  readonly Editor: typeof Editor;
}

let pending: Promise<Diffs> | null = null;

export const diffs = (): Promise<Diffs> => {
  pending ??= (async (): Promise<Diffs> => {
    const [module, edit] = await Promise.all([
      import("@pierre/diffs"),
      import("@pierre/diffs/edit"),
      import("@pierre/diffs/components/web-components"),
    ]);
    return { File: module.File, FileDiff: module.FileDiff, Editor: edit.Editor };
  })();
  return pending;
};
