import { AsyncData } from "foldkit";

import type { CodeView, Model } from "./app.model.ts";

/**
 * The Code screen's own vocabulary: its sample repository, and the pure
 * arithmetic over paths and refs that the screen used to do inside a component.
 *
 * `viewOf` lives here rather than in the view because `update` reads it too,
 * and `update` must not import a view: the views pull `@pierre/diffs` and
 * Shiki behind them, and a pure transition should be testable without either.
 *
 * The sample data is the design's, verbatim. It is what an unreachable server
 * leaves showing, which keeps the branch reviewable without a running worker
 * and keeps the fallback honest rather than passing stale data off as live.
 */

/** `refs/heads/main` → `main`; anything else unchanged. */
export const shortRef = (name: string): string =>
  name.startsWith("refs/heads/") ? name.slice("refs/heads/".length) : name;

/** Just the branch names out of a ref list. */
export const branchNames = (refs: readonly { readonly name: string }[]): readonly string[] =>
  refs.filter((ref) => ref.name.startsWith("refs/heads/")).map((ref) => shortRef(ref.name));

/**
 * Every directory a path list implies — the everything-open set.
 *
 * A first visit opens everything, by listing everything: the tree is always
 * built over a "closed" baseline, so construction, reset and an empty saved
 * set all agree on what an unlisted folder does.
 */
export const allDirectories = (paths: readonly string[]): readonly string[] => {
  const directories = new Set<string>();
  for (const path of paths) {
    let at = path.indexOf("/");
    while (at !== -1) {
      directories.add(path.slice(0, at));
      at = path.indexOf("/", at + 1);
    }
  }
  return [...directories].sort();
};

/** The file a fresh view opens: the README, when the tree has one. */
export const readmeOf = (paths: readonly string[]): string | null =>
  paths.find((path) => /^readme(\.md|\.txt)?$/i.test(path)) ?? null;

/**
 * A detached HEAD can be read or branched from, but commits need a branch.
 */
export const writableBranch = (
  ref: string,
  branches: readonly string[],
  fallback: string | null,
): boolean => ref === fallback || branches.includes(ref);

/** One file's badge in the explorer, as `@pierre/trees` wants it. */
export interface StatusEntry {
  readonly path: string;
  readonly status: "modified" | "added";
}

/** Where the explorer's open folders are remembered, per repository. */
export const expansionKey = (repo: string): string => `gp-explorer-open:${repo}`;

export const FALLBACK_PATHS: readonly string[] = [
  ".github/workflows/ci.yml",
  ".husky/pre-commit",
  "docs/architecture.md",
  "node_modules/.package-lock.json",
  "src/components/Avatar.tsx",
  "src/components/Badge.tsx",
  "src/components/Button.test.tsx",
  "src/components/Button.tsx",
  "src/components/Card.test.tsx",
  "src/components/Card.tsx",
  "src/components/Dialog.tsx",
  "src/components/Input.tsx",
  "src/components/Spinner.tsx",
  "src/components/Tabs.tsx",
  "src/components/Tooltip.tsx",
  "src/hooks/useDebounce.test.ts",
  "src/hooks/useDebounce.ts",
  "src/hooks/useLocalStorage.ts",
  "src/hooks/useMediaQuery.ts",
  "src/hooks/useOnClickOutside.ts",
  "src/styles/globals.css",
  "src/styles/tokens.css",
  "src/styles/typography.css",
  "README.md",
];

/** The M and A badges the design shows against changed files. */
export const FALLBACK_STATUS: readonly StatusEntry[] = [
  { path: "src/components/Button.tsx", status: "modified" },
  { path: "src/components/Tabs.tsx", status: "added" },
  { path: "src/components/Tooltip.tsx", status: "added" },
  { path: "src/hooks/useOnClickOutside.ts", status: "added" },
  { path: "src/styles/globals.css", status: "modified" },
  { path: "src/styles/tokens.css", status: "added" },
  { path: "src/styles/typography.css", status: "added" },
];

export const FALLBACK_README = `# git+ core

The reference server implementation for git+ — a source platform where every
unit of work is a **Task**, and code changes are **Change Requests**: Tasks
with a diff attached.

\`\`\`
$ gitp clone git-plus/core
\`\`\`

See docs/architecture.md for the service layout and CONTRIBUTING.md before
opening a Change Request.
`;

/** The sample repository, which is what an unreachable server leaves showing. */
const SAMPLE: CodeView = {
  ref: "main",
  defaultBranch: "main",
  branches: ["main"],
  paths: FALLBACK_PATHS,
  selected: "README.md",
  content: FALLBACK_README,
  head: {
    sha: "e4a91c2",
    message: "merge CR-18: update pipeline config",
    author: "rbaek",
    avatar: "RB",
    when: "2h ago",
  },
  tip: null,
  offline: true,
  reason: "",
};

/** What is on screen: the repository, or the sample standing in for it. */
export const viewOf = (model: Model): CodeView => {
  const held = AsyncData.getData(model.codeScreen.view);
  if (held._tag === "Some") return held.value;
  const failure = AsyncData.getError(model.codeScreen.view);
  return {
    ...SAMPLE,
    reason: failure._tag === "Some" ? failure.value : "no API client was provided",
  };
};
