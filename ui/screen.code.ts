/**
 * Code — the repository explorer and file view.
 *
 * This is the screen wired to the live server. `GET /:repo/files` answers every
 * blob at a ref as a flat list of tree paths, and `@pierre/trees` is path-first
 * — it takes `paths: string[]` and derives the folder structure itself — so the
 * response needs no reshaping. Selecting a row fetches that blob through
 * `GET /:repo/file` and renders it with `@pierre/diffs`, which brings Shiki
 * highlighting and a header that matches the design's file card.
 *
 * When the API cannot be reached the screen falls back to the design's own
 * fixture tree and README, and says so in a quiet inline note. That keeps the
 * branch reviewable without a running worker, and keeps the fallback honest
 * rather than passing stale data off as live.
 */
import { html, type TemplateResult } from "lit";
import { customElement, state } from "lit/decorators.js";

import { FileTree, type GitStatusEntry } from "@pierre/trees";
import type { File as DiffsFile } from "@pierre/diffs";

import { ApiError, type GitApi } from "./api.ts";
import { GitPlusElement } from "./base.ts";
import { diffs } from "./highlight.ts";
import * as icons from "./icons.ts";
import * as theme from "./theme.ts";

/** The design's explorer, used when the server is unreachable. */
const FALLBACK_PATHS: readonly string[] = [
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
const FALLBACK_STATUS: readonly GitStatusEntry[] = [
  { path: "src/components/Button.tsx", status: "modified" },
  { path: "src/components/Tabs.tsx", status: "added" },
  { path: "src/components/Tooltip.tsx", status: "added" },
  { path: "src/hooks/useOnClickOutside.ts", status: "added" },
  { path: "src/styles/globals.css", status: "modified" },
  { path: "src/styles/tokens.css", status: "added" },
  { path: "src/styles/typography.css", status: "added" },
];

const FALLBACK_README = `# git+ core

The reference server implementation for git+ — a source platform where every
unit of work is a **Task**, and code changes are **Change Requests**: Tasks
with a diff attached.

\`\`\`
$ gitp clone git-plus/core
\`\`\`

See docs/architecture.md for the service layout and CONTRIBUTING.md before
opening a Change Request.
`;

/**
 * The tip commit, as the commit bar shows it.
 *
 * `author` is optional because the JSON API does not carry one: `/log/:oid`
 * answers `{ oid, message }` and `/commit/:oid` adds only parents and tree. The
 * bar omits the name rather than printing a placeholder that looks like data.
 */
interface HeadCommit {
  readonly sha: string;
  readonly message: string;
  readonly author?: string;
  readonly avatar?: string;
}

@customElement("gp-code")
export class GpCode extends GitPlusElement {
  /** Injected by the shell so every screen shares one client. */
  api: GitApi | null = null;

  @state() private accessor ref = "main";
  @state() private accessor branches: readonly string[] = ["main"];
  @state() private accessor selected: string | null = null;
  @state() private accessor content: string | null = null;
  @state() private accessor head: HeadCommit | null = null;
  @state() private accessor offline = false;
  @state() private accessor loading = true;

  #tree: FileTree | null = null;
  #treeHost: HTMLElement | null = null;
  #viewer: DiffsFile | null = null;
  #paths: readonly string[] = [];

  override connectedCallback(): void {
    super.connectedCallback();
    void this.#load();
  }

  override disconnectedCallback(): void {
    super.disconnectedCallback();
    this.#tree?.cleanUp();
    this.#tree = null;
    this.#treeHost = null;
    this.#viewer = null;
  }

  /**
   * Fetch the ref list, the paths at the tip, and the tip commit.
   *
   * A failure at any step drops the whole screen to fixtures rather than
   * showing a half-live tree beside a fixture README, which would be the more
   * confusing outcome.
   */
  async #load(): Promise<void> {
    const api = this.api;
    if (api === null) {
      this.#useFallback();
      return;
    }
    try {
      const refs = await api.refs();
      const heads = refs
        .filter((ref) => ref.name.startsWith("refs/heads/"))
        .map((ref) => ref.name.slice("refs/heads/".length));
      if (heads.length > 0) {
        this.branches = heads;
        this.ref = heads.includes("main") ? "main" : (heads[0] ?? "main");
      }

      const files = await api.files(this.ref);
      this.#paths = files.map((file) => file.path);

      const tip = refs.find((ref) => ref.name === `refs/heads/${this.ref}`);
      if (tip !== undefined) {
        const commits = await api.log(tip.oid);
        const latest = commits[0];
        if (latest !== undefined) {
          const subject = latest.message.split("\n", 1)[0] ?? latest.message;
          this.head = { sha: latest.oid.slice(0, 7), message: subject };
        }
      }

      this.offline = false;
      await this.#openDefault(api);
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      this.#useFallback();
    } finally {
      this.loading = false;
      this.#mountTree();
    }
  }

  /** Open the README if there is one, so the screen is never blank. */
  async #openDefault(api: GitApi): Promise<void> {
    const readme = this.#paths.find((path) => /^readme(\.md|\.txt)?$/i.test(path));
    if (readme === undefined) return;
    this.selected = readme;
    this.content = await api.file(this.ref, readme);
  }

  #useFallback(): void {
    this.offline = true;
    this.#paths = FALLBACK_PATHS;
    this.selected = "README.md";
    this.content = FALLBACK_README;
    this.head = {
      sha: "e4a91c2",
      message: "merge CR-18: update pipeline config",
      author: "rbaek",
      avatar: "RB",
    };
  }

  /**
   * Build the tree once, then feed it new paths on later loads.
   *
   * `FileTree` owns its own shadow root and rendering, so it is mounted
   * imperatively into a host element Lit leaves alone — Lit must not re-render
   * the subtree the tree manages.
   */
  #mountTree(): void {
    const host = this.querySelector<HTMLElement>(".gp-explorer-tree");
    if (host === null) return;

    if (this.#tree !== null && this.#treeHost === host) {
      this.#tree.resetPaths(this.#paths);
      if (this.offline) this.#tree.setGitStatus(FALLBACK_STATUS);
      return;
    }

    this.#tree?.cleanUp();
    this.#tree = new FileTree({
      paths: this.#paths,
      initialExpansion: "open",
      flattenEmptyDirectories: true,
      // The design's explorer header carries only "+" and "…"; repository-wide
      // search lives in the sidebar's ⌘K field, so the tree does not add a
      // second search box of its own.
      search: false,
      gitStatus: this.offline ? FALLBACK_STATUS : [],
      onSelectionChange: (paths) => {
        const path = paths[0];
        if (path !== undefined) void this.#open(path);
      },
    });
    this.#tree.render({ containerWrapper: host });
    this.#treeHost = host;
  }

  /** Selecting a row loads that blob and swaps the right-hand pane. */
  async #open(path: string): Promise<void> {
    // Directories arrive here too; they have no blob to read.
    if (!this.#paths.includes(path)) return;
    this.selected = path;
    const api = this.api;
    if (api === null || this.offline) {
      this.content = path === "README.md" ? FALLBACK_README : `// ${path}`;
      return;
    }
    try {
      this.content = await api.file(this.ref, path);
    } catch (error) {
      if (!(error instanceof ApiError)) throw error;
      this.content = `// ${path} — ${error.message}`;
    }
  }

  protected override updated(): void {
    this.#mountTree();
    void this.#renderSource();
  }

  /**
   * Hand the file to `@pierre/diffs`, which owns its own shadow root.
   *
   * The module is fetched on first use — see `highlight.ts` — so the path and
   * content are re-read after the await in case the selection moved on while
   * the chunk was in flight.
   */
  async #renderSource(): Promise<void> {
    const host = this.querySelector<HTMLElement>(".gp-source-host");
    if (host === null || this.selected === null || this.content === null) return;
    const { File } = await diffs();
    const path = this.selected;
    const contents = this.content;
    if (path === null || contents === null) return;
    this.#viewer ??= new File({
      themeType: theme.current(),
      disableFileHeader: true,
      overflow: "scroll",
      stickyHeader: false,
    });
    this.#viewer.setThemeType(theme.current());
    this.#viewer.render({ file: { name: path, contents }, containerWrapper: host });
  }

  protected override render(): TemplateResult {
    return html`
      <div class="gp-explorer">
        <div class="gp-explorer-head">
          <div class="gp-explorer-title">Explorer</div>
          <div class="gp-explorer-actions">${icons.plus()} ${icons.ellipsis()}</div>
        </div>
        <div class="gp-explorer-tree"></div>
      </div>

      <div class="gp-main">
        <header class="gp-repo-header">
          <div class="gp-breadcrumb">
            <span class="gp-breadcrumb-owner">git-plus</span>
            <span class="gp-breadcrumb-sep">/</span>
            <span class="gp-breadcrumb-name">${this.api?.repo ?? "core"}</span>
            <span class="gp-pill-outline">Public</span>
          </div>
          <div class="gp-repo-actions">
            <button class="gp-branch-trigger" type="button">
              ${icons.branch()} ${this.ref} ${icons.chevronDown()}
            </button>
            <button class="gp-btn-primary" type="button">Clone</button>
          </div>
        </header>

        <div class="gp-screen gp-screen--code">
          ${
            this.offline
              ? html`<p class="gp-notice">
                  Showing the design's sample repository — the git+ API did not answer.
                </p>`
              : ""
          }
          ${this.head === null ? "" : this.#commitBar(this.head)}

          <div class="gp-card gp-file-card">
            <div class="gp-card-head">
              ${icons.document_()} ${this.selected ?? "—"}
              <button
                class="gp-icon-btn"
                type="button"
                title="View source"
                aria-label="View source"
              >
                ${icons.code(14)}
              </button>
            </div>
            ${
              this.loading
                ? html`<div class="gp-empty">Loading…</div>`
                : html`<div class="gp-source-host gp-diff-host"></div>`
            }
          </div>
        </div>
      </div>
    `;
  }

  #commitBar(head: HeadCommit): TemplateResult {
    return html`
      <div class="gp-commit-bar">
        ${head.avatar === undefined ? "" : html`<span class="gp-avatar">${head.avatar}</span>`}
        ${
          head.author === undefined
            ? ""
            : html`<span class="gp-commit-author">${head.author}</span>`
        }
        <span>${head.message}</span>
        <span class="gp-commit-sha">${head.sha}</span>
      </div>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "gp-code": GpCode;
  }
}
