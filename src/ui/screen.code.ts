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
 * The pane edits as well as views. The pencil attaches `@pierre/diffs`'s edit
 * mode to the rendered file — the same highlighted surface, made writable,
 * with its own undo stack — and the explorer's "+" opens the same editor over
 * a new path; committing either sends `POST /:repo/commit` with the file layered
 * onto the branch's current tree and `expected` pinned to the tip the editor
 * opened at — so a commit that lands mid-edit is a visible conflict, not a
 * silent overwrite. Deleting is the same request with `content: null`.
 *
 * When the API cannot be reached the screen falls back to the design's own
 * fixture tree and README, and says so in a quiet inline note. That keeps the
 * branch reviewable without a running worker, and keeps the fallback honest
 * rather than passing stale data off as live — and read-only, since there is
 * nothing to write to.
 */
import { Schema } from "effect";
import { html, nothing, type TemplateResult } from "lit";
import { customElement, property, state } from "lit/decorators.js";

import { UIDialog } from "@chr33s/base-wc/src/dialog";
import type { MenuSelectDetail } from "@chr33s/base-wc/src/menu";
import { FileTree, type GitStatusEntry } from "@pierre/trees";
import type { File as DiffsFile, FileDiff as DiffsFileDiff } from "@pierre/diffs";
import type { Editor } from "@pierre/diffs/edit";

import {
  ApiError,
  type CommitDetail,
  type CommitFilesRequest,
  type CommitSummary,
  describe,
  type FileWrite,
  type CodeApi,
  syncCapable,
  type SyncState,
  type Unavailable,
  type Whoami,
} from "./api.ts";
import { GitPlusElement, navigate } from "./base.ts";
import { store } from "./store.ts";
import { diffs } from "./highlight.ts";
import * as icons from "./icons.ts";
import { current as currentTheme, type Theme } from "./theme.ts";
import { ago, initials } from "./time.ts";

/**
 * Which explorer folders are open, remembered per repository.
 *
 * Saved from the visible rows on every tree change and restored through
 * `initialExpandedPaths`. Visible rows are exactly the restorable state: a
 * folder hidden under a collapsed ancestor cannot be reopened without its
 * ancestor, so recording only what shows also guarantees the saved set never
 * names a folder whose ancestors are closed. No entry at all is a first
 * visit, which opens everything — by listing everything, because the tree is
 * always built over a "closed" baseline: `resetPaths` rebuilds its store
 * from construction options, and one baseline means construction, reset and
 * an empty saved set all agree on what an unlisted folder does. The fixtures
 * (`api === null`) stay out of storage entirely so a sample layout never
 * shapes a real one.
 */
const expansionKey = (repo: string): string => `gp-explorer-open:${repo}`;

/** Every directory a path list implies — the everything-open set. */
const allDirectories = (paths: readonly string[]): readonly string[] => {
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

/** The open directories as the tree shows them right now. */
const openDirectories = (tree: FileTree): readonly string[] =>
  tree
    .getVisibleRows(0, tree.getVisibleCount())
    .filter((row) => row.kind === "directory" && row.isExpanded)
    .map((row) => row.path);

const StoredExpansion = Schema.Array(Schema.String);

const storedExpansion = (repo: string): readonly string[] | null => {
  const raw = localStorage.getItem(expansionKey(repo));
  if (raw === null) return null;
  try {
    return Schema.decodeUnknownSync(StoredExpansion)(JSON.parse(raw));
  } catch {
    // Whatever is stored is not this shape — a first visit's default beats
    // trusting it.
    return null;
  }
};

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
 * Author and age arrive on `/commit/:oid` itself — the enriched view means no
 * raw-object round trip.
 */
interface HeadCommit {
  readonly sha: string;
  readonly message: string;
  readonly author: string;
  readonly avatar: string;
  readonly when: string;
}

/** One coherent repository view, committed only after every request succeeds. */
interface CodeSnapshot {
  readonly ref: string;
  readonly branches: readonly string[];
  readonly paths: readonly string[];
  readonly selected: string | null;
  readonly content: string | null;
  readonly head: HeadCommit | null;
  /** The tip's full oid — what `expected` pins when an edit commits. */
  readonly tip: string | null;
  readonly offline: boolean;
  readonly reason: string;
}

@customElement("gp-code")
export class GpCode extends GitPlusElement {
  /**
   * Injected by the shell so every screen shares one client. Reactive,
   * because the shell swaps it once the OPFS clone is ready — from the HTTP
   * client to the local repository — and the screen reloads from whichever
   * it currently holds.
   */
  @property({ attribute: false }) accessor api: CodeApi | null = null;

  @property({ type: String }) accessor theme: Theme = currentTheme();

  /** The `/whoami` answer, resolved once by the shell. */
  @property({ attribute: false }) accessor who: Whoami | null = null;

  /** A path navigation asked for — a search hit, or `#/code/<path>`. */
  @property({ type: String }) accessor wanted: string | null = null;

  @state() private accessor ref = "main";
  @state() private accessor branches: readonly string[] = ["main"];
  @state() private accessor selected: string | null = null;
  @state() private accessor content: string | null = null;
  @state() private accessor head: HeadCommit | null = null;
  @state() private accessor offline = false;
  /** Why the fallback is showing, in the reader\'s terms. */
  @state() private accessor reason = "";
  @state() private accessor loading = true;

  /** `edit` attaches the diffs editor to the rendered blob, in place. */
  @state() private accessor mode: "view" | "edit" = "view";

  /** Which side panel is open under the commit bar, if any. */
  @state() private accessor panel: "none" | "commits" | "filelog" = "none";
  /** Recent commits at the tip, for the commit-bar panel. */
  @state() private accessor commitLog: readonly CommitDetail[] | null = null;
  /** Commits touching the open file, for the file-history panel. */
  @state() private accessor fileLog: readonly CommitSummary[] | null = null;
  /** Set while viewing the open file at an older commit — read-only. */
  @state() private accessor at: string | null = null;
  @state() private accessor copied = false;
  /** Editing a path that does not exist yet, from the explorer's "+". */
  @state() private accessor editingNew = false;
  @state() private accessor saving = false;
  /** Reviewing a diff — the draft while editing, the last change while viewing. */
  @state() private accessor diffing = false;
  /** Why the diff pane shows a note instead of hunks, when it does. */
  @state() private accessor diffNote: string | null = null;
  /** Why the last commit attempt failed, shown inside the editor. */
  @state() private accessor editError: string | null = null;

  /** Where the branch stands against origin — local mode only. */
  @state() private accessor syncState: SyncState | null = null;
  /** A push or fetch is in flight; the sync buttons disable meanwhile. */
  @state() private accessor syncing = false;
  /** What the last sync attempt said, cleared by the next one. */
  @state() private accessor syncNotice: string | null = null;

  /** Bisect marks, while the commits panel is being used to hunt a culprit. */
  @state() private accessor bisect: {
    readonly good: readonly string[];
    readonly bad: string | null;
    readonly answer: {
      readonly kind: "test" | "found";
      readonly commit: string;
      readonly steps: number;
    } | null;
  } | null = null;

  /** The tip's full oid at last load — the `expected` for the next commit. */
  #tip: string | null = null;

  #tree: FileTree | null = null;
  #treeHost: HTMLElement | null = null;
  #treeUnsubscribe: (() => void) | null = null;
  #viewer: DiffsFile | null = null;
  /** Renders the draft-against-blob review; one instance, reused per toggle. */
  #diffView: DiffsFileDiff | null = null;
  /** The fetched previous version behind a view-mode diff, keyed by anchor+path. */
  #viewDiffOld: { anchor: string; path: string; contents: string | null } | null = null;
  /** One editor for the screen's lifetime; attached and detached per edit. */
  #editSession: Editor<undefined> | null = null;
  /** The detach `Editor.edit` returned — non-null exactly while attached. */
  #detachEditor: (() => void) | null = null;
  #paths: readonly string[] = [];
  #loadGeneration = 0;
  #fileGeneration = 0;

  override connectedCallback(): void {
    super.connectedCallback();
    void this.#load();
  }

  override willUpdate(changed: Map<string, unknown>): void {
    // A later navigation naming a path — another search hit — opens it in
    // place; the initial one is honoured by `#load` before the tree exists.
    if (changed.has("wanted") && this.wanted !== null && this.wanted !== this.selected) {
      void this.#open(this.wanted);
    }
    // The shell swapped clients — the OPFS clone came ready. The old value is
    // `undefined` only on the first update, which `connectedCallback` loads.
    if (changed.has("api") && changed.get("api") !== undefined) {
      void this.#load();
    }
  }

  override disconnectedCallback(): void {
    super.disconnectedCallback();
    this.#loadGeneration += 1;
    this.#fileGeneration += 1;
    this.#treeUnsubscribe?.();
    this.#treeUnsubscribe = null;
    this.#tree?.cleanUp();
    this.#tree = null;
    this.#treeHost = null;
    this.#detachDraft();
    this.#editSession?.cleanUp();
    this.#editSession = null;
    this.#diffView?.cleanUp();
    this.#diffView = null;
    this.#viewer = null;
  }

  /** Detach the editor — the draft dies with it, the viewer owns the pane. */
  #detachDraft(): void {
    this.#detachEditor?.();
    this.#detachEditor = null;
  }

  /**
   * Fetch the ref list, the paths at the tip, and the tip commit.
   *
   * A failure at any step drops the whole screen to fixtures rather than
   * showing a half-live tree beside a fixture README, which would be the more
   * confusing outcome.
   */
  async #load(): Promise<void> {
    const generation = ++this.#loadGeneration;
    this.#fileGeneration += 1;
    const api = this.api;
    if (api === null) {
      this.#commit(this.#fallback());
      this.loading = false;
      return;
    }
    try {
      const refs = await api.refs();
      const branches = this.#branchNames(refs);
      const ref = branches.includes("main") ? "main" : (branches[0] ?? "main");
      const snapshot = await this.#snapshot(api, ref, refs, this.wanted ?? undefined);
      if (generation === this.#loadGeneration) this.#commit(snapshot);
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      if (generation === this.#loadGeneration) this.#commit(this.#fallback(error));
    } finally {
      if (generation === this.#loadGeneration) {
        this.loading = false;
        this.#mountTree();
      }
    }
  }

  #branchNames(refs: readonly { readonly name: string }[]): readonly string[] {
    return refs
      .filter((ref) => ref.name.startsWith("refs/heads/"))
      .map((ref) => ref.name.slice("refs/heads/".length));
  }

  /**
   * Load one ref without consulting or mutating the component's current ref.
   *
   * `keep` names a path to stay on if it still exists — a reload after a
   * commit should show the file that was just written, not jump to the README.
   */
  async #snapshot(
    api: CodeApi,
    ref: string,
    knownRefs?: Awaited<ReturnType<CodeApi["refs"]>>,
    keep?: string,
  ): Promise<CodeSnapshot> {
    const [refs, files] = await Promise.all([
      knownRefs === undefined ? api.refs() : Promise.resolve(knownRefs),
      api.files(ref),
    ]);
    const paths = files.map((file) => file.path);
    const readme = paths.find((path) => /^readme(\.md|\.txt)?$/i.test(path)) ?? null;
    const selected = keep !== undefined && paths.includes(keep) ? keep : readme;
    const tip = refs.find((candidate) => candidate.name === `refs/heads/${ref}`);
    const [commit, content] = await Promise.all([
      tip === undefined ? Promise.resolve(null) : api.commitDetail(tip.oid),
      selected === null ? Promise.resolve(null) : api.file(ref, selected),
    ]);
    const head =
      commit === null
        ? null
        : {
            sha: commit.oid.slice(0, 7),
            message: commit.subject,
            author: commit.author,
            avatar: initials(commit.author),
            when: ago(commit.at),
          };
    return {
      ref,
      branches: this.#branchNames(refs),
      paths,
      selected,
      content,
      head,
      tip: tip?.oid ?? null,
      offline: false,
      reason: "",
    };
  }

  /**
   * Switch the ref the whole screen is showing.
   *
   * The explorer, the file view and the commit bar all read one ref, so this
   * refetches the lot rather than trying to patch each in place.
   */
  async #switchTo(ref: string): Promise<void> {
    const api = this.api;
    if (api === null) return;
    const generation = ++this.#loadGeneration;
    this.#fileGeneration += 1;
    this.loading = true;
    try {
      const snapshot = await this.#snapshot(api, ref);
      if (generation === this.#loadGeneration) this.#commit(snapshot);
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      if (generation === this.#loadGeneration) this.#commit(this.#fallback(error));
    } finally {
      if (generation === this.#loadGeneration) {
        this.loading = false;
        this.#mountTree();
      }
    }
  }

  #fallback(error?: Unavailable): CodeSnapshot {
    return {
      ref: "main",
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
      reason: error === undefined ? "no API client was provided" : describe(error),
    };
  }

  #commit(snapshot: CodeSnapshot): void {
    this.ref = snapshot.ref;
    this.branches = snapshot.branches;
    this.#paths = snapshot.paths;
    this.selected = snapshot.selected;
    this.content = snapshot.content;
    this.head = snapshot.head;
    this.#tip = snapshot.tip;
    this.offline = snapshot.offline;
    this.reason = snapshot.reason;
    this.mode = "view";
    this.editingNew = false;
    this.editError = null;
    this.panel = "none";
    this.commitLog = null;
    this.fileLog = null;
    this.at = null;
    this.diffing = false;
    this.diffNote = null;
    this.#viewDiffOld = null;
    this.#detachDraft();
    this.#viewer = null;
    void this.#refreshSync();
  }

  /**
   * Where the current branch stands against origin — answered only by the
   * local client; against the HTTP client there is no "against", and the
   * controls stay hidden.
   */
  async #refreshSync(): Promise<void> {
    const api = syncCapable(this.api);
    if (api === null || this.offline) {
      this.syncState = null;
      return;
    }
    try {
      this.syncState = await api.sync(this.ref);
    } catch {
      this.syncState = null;
    }
  }

  async #push(): Promise<void> {
    const api = syncCapable(this.api);
    if (api === null || this.syncing) return;
    this.syncing = true;
    this.syncNotice = null;
    try {
      const results = await api.push(this.ref);
      const refused = results.filter((result) => !result.ok);
      this.syncNotice =
        refused.length === 0
          ? `Pushed ${this.ref} to origin.`
          : `origin refused ${refused[0]?.ref ?? this.ref}: ${refused[0]?.reason ?? "unknown"}`;
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      this.syncNotice = `Push failed — ${describe(error)}.`;
    } finally {
      this.syncing = false;
      void this.#refreshSync();
    }
  }

  async #fetchOrigin(): Promise<void> {
    const api = syncCapable(this.api);
    if (api === null || this.syncing) return;
    this.syncing = true;
    this.syncNotice = null;
    try {
      const fetched = await api.fetchOrigin();
      this.syncNotice =
        fetched.updated === 0
          ? "Already up to date with origin."
          : `Fetched origin — ${fetched.updated} ref${fetched.updated === 1 ? "" : "s"} moved.`;
      await this.#reload(this.selected ?? undefined);
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      this.syncNotice = `Fetch failed — ${describe(error)}.`;
    } finally {
      this.syncing = false;
      void this.#refreshSync();
    }
  }

  get #defaultBranch(): string {
    return this.branches.includes("main") ? "main" : (this.branches[0] ?? "main");
  }

  /**
   * Open a Change Request for the current branch.
   *
   * The order is the honest one: push first, so the revision the event names
   * exists on the server, then sign and append `pr.opened`, then go look at
   * it. A failure at any step says which step.
   */
  #propose = async (event: SubmitEvent): Promise<void> => {
    event.preventDefault();
    const form = event.currentTarget;
    if (!(form instanceof HTMLFormElement)) return;
    const titleField = form.elements.namedItem("title");
    const descField = form.elements.namedItem("desc");
    if (!(titleField instanceof HTMLInputElement)) return;
    if (!(descField instanceof HTMLTextAreaElement)) return;
    const title = titleField.value.trim();
    if (title === "" || this.#tip === null) return;
    const api = syncCapable(this.api);
    this.syncing = true;
    this.syncNotice = null;
    try {
      if (api !== null) {
        const state = await api.sync(this.ref);
        if (state.ahead > 0) {
          const results = await api.push(this.ref);
          const refused = results.find((result) => !result.ok);
          if (refused !== undefined) {
            this.syncNotice = `push refused: ${refused.reason ?? refused.ref}`;
            return;
          }
        }
      }
      const pr = await store.openPullRemote({
        title,
        description: descField.value.trim(),
        base: this.#defaultBranch,
        head: this.#tip,
      });
      form.reset();
      const dialog = this.querySelector("ui-dialog.gp-propose");
      if (dialog instanceof UIDialog) dialog.hide();
      if (pr === null) {
        this.syncNotice = "the hub refused the Change Request — is this key a member?";
        return;
      }
      navigate(this, { screen: "detail", id: pr });
    } finally {
      this.syncing = false;
      void this.#refreshSync();
    }
  };

  async #cherryPick(commit: string): Promise<void> {
    const api = this.api;
    if (api === null || this.syncing) return;
    this.syncing = true;
    this.syncNotice = null;
    try {
      const outcome = await api.cherryPick(commit, this.ref);
      this.syncNotice =
        outcome.kind === "conflicted"
          ? `cherry-pick conflicted — resolve on a branch`
          : outcome.kind === "up-to-date"
            ? `${this.ref} already has ${commit.slice(0, 7)}`
            : `picked ${commit.slice(0, 7)} onto ${this.ref}`;
      if (outcome.kind === "replayed") await this.#reload(this.selected ?? undefined);
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      this.syncNotice = `cherry-pick failed — ${describe(error)}`;
    } finally {
      this.syncing = false;
    }
  }

  async #rebaseOntoDefault(): Promise<void> {
    const api = this.api;
    const onto = this.#defaultBranch;
    if (api === null || this.syncing || this.ref === onto) return;
    this.syncing = true;
    this.syncNotice = null;
    try {
      const outcome = await api.rebase(this.ref, onto);
      this.syncNotice =
        outcome.kind === "conflicted"
          ? "rebase conflicted — resolve by hand"
          : outcome.kind === "up-to-date"
            ? `${this.ref} is already on ${onto}`
            : `rebased ${this.ref} onto ${onto}`;
      if (outcome.kind === "replayed") await this.#reload(this.selected ?? undefined);
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      this.syncNotice = `rebase failed — ${describe(error)}`;
    } finally {
      this.syncing = false;
    }
  }

  #mark(commit: string, as: "good" | "bad"): void {
    const current = this.bisect ?? { good: [], bad: null, answer: null };
    const next =
      as === "bad"
        ? { ...current, bad: commit }
        : { ...current, good: [...current.good.filter((oid) => oid !== commit), commit] };
    this.bisect = next;
    const bad = next.bad;
    if (bad !== null && next.good.length > 0) void this.#step({ good: next.good, bad });
  }

  async #step(marks: { readonly good: readonly string[]; readonly bad: string }): Promise<void> {
    const api = this.api;
    if (api === null) return;
    try {
      const answer = await api.bisect(marks.good, marks.bad);
      this.bisect = { good: marks.good, bad: marks.bad, answer };
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      this.syncNotice = `bisect failed — ${describe(error)}`;
    }
  }

  /**
   * The sync controls: what there is to push, what there is to fetch, and
   * the buttons that do either. Rendered only in local mode — the header is
   * otherwise exactly what it always was.
   */
  #syncControls(): TemplateResult | typeof nothing {
    const state = this.syncState;
    if (state === null || syncCapable(this.api) === null) return nothing;
    return html`
      <span class="gp-sync" title="This branch lives in this browser (OPFS); origin is the server.">
        <button
          class="gp-btn-quiet"
          type="button"
          title=${
            state.ahead === 0
              ? "Nothing to push"
              : `Push ${state.ahead} commit${state.ahead === 1 ? "" : "s"} to origin`
          }
          ?disabled=${this.syncing || state.ahead === 0}
          @click=${() => void this.#push()}
        >
          Push ↑${state.ahead}
        </button>
        <button
          class="gp-btn-quiet"
          type="button"
          title="Fetch origin"
          ?disabled=${this.syncing}
          @click=${() => void this.#fetchOrigin()}
        >
          Fetch${state.behind > 0 ? ` ↓${state.behind}` : ""}
        </button>
        ${this.ref === this.#defaultBranch ? nothing : this.#proposeDialog()}
      </span>
    `;
  }

  /**
   * Open a Change Request for this branch: pushed first, signed with the
   * browser's key, and read back from the projection. Only offered off the
   * default branch — a Change Request proposing a branch onto itself is not
   * a proposal.
   */
  #proposeDialog(): TemplateResult {
    return html`
      <ui-dialog class="gp-propose">
        <button
          class="gp-btn-primary"
          data-dialog-trigger
          type="button"
          ?disabled=${this.syncing}
          title="Push ${this.ref} and open a Change Request against ${this.#defaultBranch}"
        >
          Propose
        </button>
        <ui-dialog-popup class="gp-dialog">
          <h2 class="gp-dialog-title" data-dialog-title>Propose ${this.ref}</h2>
          <p class="gp-dialog-hint" data-dialog-description>
            Pushes the branch, then opens a Change Request against ${this.#defaultBranch} — signed
            with this browser's key.
          </p>
          <form @submit=${this.#propose}>
            <label class="gp-field-label" for="gp-propose-title">Title</label>
            <input
              id="gp-propose-title"
              class="gp-input"
              name="title"
              required
              autocomplete="off"
              placeholder="What this changes"
            />
            <label class="gp-field-label" for="gp-propose-desc">Description</label>
            <textarea
              id="gp-propose-desc"
              class="gp-textarea"
              name="desc"
              rows="3"
              placeholder="Why, and anything a reviewer should know…"
            ></textarea>
            <div class="gp-dialog-actions">
              <button class="gp-btn-primary" type="submit" ?disabled=${this.syncing}>
                Open Change Request
              </button>
            </div>
          </form>
        </ui-dialog-popup>
      </ui-dialog>
    `;
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
    const repo = this.offline ? null : (this.api?.repo ?? null);

    if (this.#tree !== null && this.#treeHost === host) {
      // Every render lands here, and the reset rebuilds the tree's store
      // from its construction options — so pass what is open right *now*,
      // or each render would snap folders back to how the tree was born.
      this.#tree.resetPaths(this.#paths, {
        initialExpandedPaths: [
          ...new Set([
            ...openDirectories(this.#tree),
            ...allDirectories(this.selected === null ? [] : [this.selected]),
          ]),
        ],
      });
      if (this.offline) this.#tree.setGitStatus(FALLBACK_STATUS);
      return;
    }

    // The first update runs before the load commits any paths. A tree
    // mounted over none would immediately record "nothing is open" over a
    // real record — so no tree until there is something to show.
    if (this.#paths.length === 0) return;

    this.#treeUnsubscribe?.();
    this.#treeUnsubscribe = null;
    this.#tree?.cleanUp();
    const stored = repo === null ? null : storedExpansion(repo);
    this.#tree = new FileTree({
      paths: this.#paths,
      initialExpansion: "closed",
      initialExpandedPaths: stored ?? allDirectories(this.#paths),
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
    if (repo !== null) {
      const tree = this.#tree;
      this.#treeUnsubscribe = tree.subscribe(() => {
        localStorage.setItem(expansionKey(repo), JSON.stringify(openDirectories(tree)));
      });
    }
    this.#tree.render({ containerWrapper: host });
    this.#treeHost = host;
  }

  /** Selecting a row loads that blob and swaps the right-hand pane. */
  async #open(path: string): Promise<void> {
    // Directories arrive here too; they have no blob to read.
    if (!this.#paths.includes(path)) return;
    const generation = ++this.#fileGeneration;
    const ref = this.ref;
    this.selected = path;
    this.content = null;
    // Walking the tree abandons an open editor rather than carrying a draft
    // of one file over to another, and a history view belongs to the file it
    // was opened from.
    this.mode = "view";
    this.editingNew = false;
    this.editError = null;
    this.at = null;
    this.diffing = false;
    this.diffNote = null;
    if (this.panel === "filelog") {
      this.panel = "none";
      this.fileLog = null;
    }
    const api = this.api;
    if (api === null || this.offline) {
      this.content = path === "README.md" ? FALLBACK_README : `// ${path}`;
      return;
    }
    try {
      const content = await api.file(ref, path);
      if (generation === this.#fileGeneration && ref === this.ref && path === this.selected) {
        this.content = content;
      }
    } catch (error) {
      if (!(error instanceof ApiError)) throw error;
      if (generation === this.#fileGeneration && ref === this.ref && path === this.selected) {
        this.content = `// ${path} — ${error.message}`;
      }
    }
  }

  /** Open the current blob in the editor. */
  #edit(): void {
    if (this.offline || this.loading || this.selected === null || this.content === null) return;
    this.diffing = false;
    this.editingNew = false;
    this.editError = null;
    this.mode = "edit";
  }

  /** The explorer's "+": the same editor, over a path that does not exist. */
  #newFile(): void {
    if (this.offline || this.loading || this.api === null) return;
    this.diffing = false;
    this.editingNew = true;
    this.editError = null;
    this.mode = "edit";
  }

  #cancelEdit(): void {
    this.diffing = false;
    this.mode = "view";
    this.editingNew = false;
    this.editError = null;
  }

  /** Re-read the current ref after a write, staying on `keep` if it survived. */
  async #reload(keep?: string): Promise<void> {
    const api = this.api;
    if (api === null) return;
    const generation = ++this.#loadGeneration;
    this.#fileGeneration += 1;
    try {
      const snapshot = await this.#snapshot(api, this.ref, undefined, keep);
      if (generation === this.#loadGeneration) this.#commit(snapshot);
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      if (generation === this.#loadGeneration) this.#commit(this.#fallback(error));
    } finally {
      if (generation === this.#loadGeneration) this.#mountTree();
    }
  }

  /** Commit what the editor holds: the draft content at the chosen path. */
  async #save(): Promise<void> {
    if (this.saving || this.#detachEditor === null) return;
    const path = this.editingNew
      ? (this.querySelector<HTMLInputElement>(".gp-editor-path")?.value ?? "").trim()
      : this.selected;
    if (path === null || path === "") {
      this.editError = "name the file to create";
      return;
    }
    if (this.editingNew && this.#paths.includes(path)) {
      this.editError = `${path} already exists — select it in the explorer and edit it instead`;
      return;
    }
    const message = (
      this.querySelector<HTMLInputElement>(".gp-editor-message")?.value ?? ""
    ).trim();
    const fallback = `${this.editingNew ? "add" : "update"} ${path}`;
    const content = this.#editSession?.getText() ?? this.content ?? "";
    await this.#write({ path, content }, message === "" ? fallback : message, path);
  }

  /** Remove the open file — the same commit request, with `content: null`. */
  async #delete(): Promise<void> {
    const path = this.selected;
    if (path === null || this.editingNew || this.saving) return;
    await this.#write({ path, content: null }, `delete ${path}`, undefined);
  }

  /**
   * One commit against the tip the editor opened at, then a reload.
   *
   * `expected` pins that tip, so a commit that landed mid-edit answers
   * `RefConflict` — surfaced as an error the writer can act on — instead of
   * being silently parented over.
   */
  async #write(file: FileWrite, message: string, keep: string | undefined): Promise<void> {
    const api = this.api;
    if (api === null) return;
    this.saving = true;
    this.editError = null;
    const options: CommitFilesRequest = {
      branch: this.ref,
      message,
      files: [file],
    };
    if (this.#tip !== null) options.expected = this.#tip;
    try {
      await api.commitFiles(options);
      await this.#reload(keep);
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      this.editError =
        error instanceof ApiError && error.tag === "RefConflict"
          ? `someone else committed to ${this.ref} while you were editing — copy your draft, reload, and reapply it`
          : describe(error);
    } finally {
      this.saving = false;
    }
  }

  /** Show the open file as it was at `oid` — a read-only look back. */
  async #openAt(oid: string): Promise<void> {
    const api = this.api;
    const path = this.selected;
    if (api === null || path === null || this.offline) return;
    const generation = ++this.#fileGeneration;
    this.mode = "view";
    this.content = null;
    this.at = oid;
    try {
      const content = await api.file(oid, path);
      if (generation === this.#fileGeneration && path === this.selected) this.content = content;
    } catch (error) {
      if (!(error instanceof ApiError)) throw error;
      if (generation === this.#fileGeneration && path === this.selected) {
        this.content = `// ${path} — ${error.message}`;
      }
    }
  }

  /** The commit bar toggles the recent-history panel it summarises. */
  async #toggleCommits(): Promise<void> {
    if (this.panel === "commits") {
      this.panel = "none";
      return;
    }
    this.panel = "commits";
    const api = this.api;
    const tip = this.#tip;
    if (api === null || tip === null || this.commitLog !== null) return;
    try {
      const commits = await api.recentCommits(tip, 20);
      if (this.#tip === tip) this.commitLog = commits;
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      this.commitLog = [];
    }
  }

  /** The clock on the file card: commits that touched the open file. */
  async #toggleFileLog(): Promise<void> {
    if (this.panel === "filelog") {
      this.panel = "none";
      return;
    }
    this.panel = "filelog";
    const api = this.api;
    const tip = this.#tip;
    const path = this.selected;
    if (api === null || tip === null || path === null) return;
    this.fileLog = null;
    try {
      const entries = await api.history(tip, path);
      if (this.#tip === tip && this.selected === path) this.fileLog = entries;
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      this.fileLog = [];
    }
  }

  /** The branch menu's "New branch…": create at the current tip, switch to it. */
  #createBranch = async (event: SubmitEvent): Promise<void> => {
    event.preventDefault();
    const api = this.api;
    const form = event.currentTarget;
    if (api === null || !(form instanceof HTMLFormElement)) return;
    const field = form.elements.namedItem("branch");
    if (!(field instanceof HTMLInputElement)) return;
    const name = field.value.trim();
    if (name === "") return;
    try {
      await api.branchCreate(name, this.ref);
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      this.editError = null;
      this.reason = "";
      // The dialog stays open with the field intact; the title carries why.
      field.setCustomValidity(error instanceof ApiError ? error.message : "not reachable");
      field.reportValidity();
      return;
    }
    form.reset();
    this.#branchDialog()?.hide();
    await this.#switchTo(name);
  };

  #branchDialog(): UIDialog | null {
    const dialog = this.querySelector(".gp-new-branch");
    return dialog instanceof UIDialog ? dialog : null;
  }

  /** The Clone dialog's copy: the same URL `git clone` would be handed. */
  #copyClone = (): void => {
    const url = this.api?.cloneUrl;
    if (url === undefined) return;
    void navigator.clipboard.writeText(url).then(() => {
      this.copied = true;
      setTimeout(() => {
        this.copied = false;
      }, 1500);
    });
  };

  protected override updated(): void {
    this.#mountTree();
    void this.#renderSource();
    void this.#renderDiffReview();
  }

  /**
   * Put the caret in the editable pane, once it exists.
   *
   * The surface materializes through the package's own render queue, some
   * frames after attach, and `focus()` before that is a silent no-op — so
   * keep asking briefly. The loop ends once the caret is somewhere
   * deliberate: in the pane means it worked, in a field means the user has
   * already moved on, and yanking the caret back would be worse than typing
   * one click later. (What has focus right after a toolbar click is the
   * button itself, which is neither.) With a line, not bare: bare `focus()`
   * moves element focus without placing a caret, and a contenteditable with
   * no selection swallows keystrokes.
   */
  #focusEditor(): void {
    const session = this.#editSession;
    if (session === null) return;
    const deepActive = (): Element | null => {
      let active: Element | null = document.activeElement;
      while (active?.shadowRoot?.activeElement) active = active.shadowRoot.activeElement;
      return active;
    };
    let attempts = 0;
    const tryFocus = (): void => {
      if (this.#detachEditor === null || this.#editSession !== session || this.diffing) return;
      const active = deepActive();
      if (
        active instanceof HTMLElement &&
        (active.isContentEditable || active.tagName === "INPUT")
      ) {
        return;
      }
      session.focus({ lineNumber: "first-visible" });
      if (++attempts < 30) requestAnimationFrame(tryFocus);
    };
    requestAnimationFrame(tryFocus);
  }

  /**
   * The diff behind the ± toggle, in either mode.
   *
   * Editing, it is the draft against the blob it opened from, read straight
   * off the attached editor — which stays alive under the hidden source
   * pane, so toggling back loses nothing; a new file diffs against no old
   * side at all, which renders as pure addition. Viewing, it is the change
   * that produced the version on screen: the file at `at` (or the tip)
   * against the previous commit that touched it, fetched once per
   * anchor+path and cached. An old side equal to the new one renders as a
   * note rather than a silently empty pane.
   */
  async #renderDiffReview(): Promise<void> {
    if (!this.diffing) return;
    const editing = this.mode === "edit";
    let name: string;
    let oldContents: string | null;
    let newContents: string;
    if (editing) {
      name =
        (this.editingNew
          ? (this.querySelector<HTMLInputElement>(".gp-editor-path")?.value ?? "").trim() ||
            "untitled"
          : this.selected) ?? "untitled";
      oldContents = this.editingNew ? null : (this.content ?? "");
      newContents = this.#editSession?.getText() ?? "";
    } else {
      const api = this.api;
      const path = this.selected;
      const anchor = this.at ?? this.#tip;
      if (api === null || path === null || anchor === null || this.content === null) return;
      name = path;
      newContents = this.content;
      const cached = this.#viewDiffOld;
      if (cached !== null && cached.anchor === anchor && cached.path === path) {
        oldContents = cached.contents;
      } else {
        try {
          const entries = await api.history(anchor, path, "2");
          const previous = entries[1]?.oid;
          oldContents = previous === undefined ? null : await api.file(previous, path);
        } catch (error) {
          if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
          this.diffNote = `cannot load the previous version — ${describe(error)}`;
          return;
        }
        this.#viewDiffOld = { anchor, path, contents: oldContents };
        // The screen may have moved on while the fetch was in flight.
        if (!this.diffing || this.mode === "edit" || path !== this.selected) return;
      }
    }
    const note =
      oldContents !== null && oldContents === newContents
        ? editing
          ? "No changes yet — the draft matches the file."
          : "No change to show."
        : null;
    if (this.diffNote !== note) this.diffNote = note;
    if (note !== null) return;
    const host = this.querySelector<HTMLElement>(".gp-diff-review-host");
    if (host === null) return;
    const { FileDiff } = await diffs();
    if (!this.diffing) return;
    const oldFile = oldContents === null ? null : { name, contents: oldContents };
    const newFile = { name, contents: newContents };
    // A fresh instance per diff session: a reused one does not repaint when
    // only the file contents moved between sessions — even `forceRender`
    // leaves the previous hunks on screen. An empty host is what a new
    // session looks like, since the toggle recreates the element.
    if (host.childElementCount === 0 && this.#diffView !== null) {
      this.#diffView.cleanUp();
      this.#diffView = null;
    }
    this.#diffView ??= new FileDiff({
      themeType: this.theme,
      diffStyle: "unified",
      disableFileHeader: true,
      overflow: "scroll",
    });
    this.#diffView.setThemeType(this.theme);
    this.#diffView.render({ oldFile, newFile, containerWrapper: host });
  }

  /**
   * Hand the file to `@pierre/diffs`, which owns its own shadow root — and,
   * in edit mode, attach the package's editor to that same rendered surface.
   *
   * The module is fetched on first use — see `highlight.ts` — so the path and
   * content are re-read after the await in case the selection moved on while
   * the chunk was in flight.
   *
   * This runs on every update, which makes it the one place attach and
   * detach both happen: while the editor holds the pane a re-render from
   * `content` would drop the draft, so an attached session only accepts a
   * theme push; and any path that leaves edit mode — cancel, tree walk, a
   * history look-back — lands here with `mode` back at `view`, where the
   * detach and the fresh render below restore the pristine blob.
   */
  async #renderSource(): Promise<void> {
    const host = this.querySelector<HTMLElement>(".gp-source-host");
    if (host === null) return;
    const { File, Editor } = await diffs();
    let discarded = false;
    if (this.#detachEditor !== null) {
      if (this.mode === "edit") {
        this.#syncEditorTheme(host);
        return;
      }
      this.#detachDraft();
      discarded = true;
    }
    const editing = this.mode === "edit";
    const file =
      editing && this.editingNew
        ? // No blob yet, and no name until the path field is committed —
          // plain text until then, which the render after `#save` corrects.
          { name: "untitled", contents: "" }
        : this.selected !== null && this.content !== null
          ? { name: this.selected, contents: this.content }
          : null;
    if (file === null) return;
    // A fresh `File` appends its own `<diffs-container>` and never removes a
    // predecessor's — without this, every discarded viewer (the OPFS client
    // swap, a branch switch) left its pane stacked above the live one.
    if (this.#viewer === null) host.replaceChildren();
    this.#viewer ??= new File({
      themeType: this.theme,
      disableFileHeader: true,
      overflow: "scroll",
      stickyHeader: false,
    });
    this.#viewer.setThemeType(this.theme);
    // `forceRender` after a discarded draft: the pristine `content` string
    // is unchanged, so a plain render would compare equal and skip — leaving
    // the abandoned draft on screen.
    this.#viewer.render({ file, containerWrapper: host, forceRender: discarded });
    if (editing) {
      this.#editSession ??= new Editor<undefined>();
      const session = this.#editSession;
      this.#detachEditor = session.edit(this.#viewer);
      // A new file needs its name before its content — the caret belongs in
      // the path field. Otherwise it belongs in the pane; see #focusEditor.
      if (this.editingNew) {
        this.querySelector<HTMLInputElement>(".gp-editor-path")?.focus();
        return;
      }
      this.#focusEditor();
    }
  }

  /**
   * Push the current palette into an attached editor without rebuilding
   * the document from `content` — that would drop the draft.
   *
   * `setThemeType` swaps the host `color-scheme`, which is enough for the
   * read-only renderer. The editor tokenizer paints tokens from a color
   * map, so a real theme change also has to run its render-view sync; the
   * draft is what we hand it, so the File's stored contents stay what the
   * session holds.
   */
  #syncEditorTheme(host: HTMLElement): void {
    const viewer = this.#viewer;
    const session = this.#editSession;
    if (viewer === null || session === null) return;
    if ((viewer.options.themeType ?? "system") === this.theme) return;
    viewer.setThemeType(this.theme);
    const name =
      (this.editingNew
        ? (this.querySelector<HTMLInputElement>(".gp-editor-path")?.value ?? "").trim() ||
          "untitled"
        : this.selected) ?? "untitled";
    viewer.render({
      file: { name, contents: session.getText() },
      containerWrapper: host,
      forceRender: true,
    });
  }

  protected override render(): TemplateResult {
    return html`
      <div class="gp-explorer">
        <div class="gp-explorer-head">
          <div class="gp-explorer-title">Explorer</div>
          <div class="gp-explorer-actions">
            <button
              class="gp-icon-btn"
              type="button"
              title=${this.offline ? "Read-only — the git+ API is not reachable" : "New file"}
              aria-label="New file"
              ?disabled=${this.offline || this.loading}
              @click=${() => this.#newFile()}
            >
              ${icons.plus()}
            </button>
            <ui-menu class="gp-explorer-menu" @menu-select=${this.#onExplorerAction}>
              <button
                class="gp-icon-btn"
                type="button"
                data-menu-trigger
                title="Explorer actions"
                aria-label="Explorer actions"
              >
                ${icons.ellipsis()}
              </button>
              <ui-menu-popup class="gp-menu-popup">
                <ui-menu-item class="gp-menu-item" value="refresh">Refresh</ui-menu-item>
              </ui-menu-popup>
            </ui-menu>
          </div>
        </div>
        <div class="gp-explorer-tree"></div>
      </div>

      <div class="gp-main">
        <header class="gp-repo-header">
          <div class="gp-breadcrumb">
            <span class="gp-breadcrumb-owner">git-plus</span>
            <span class="gp-breadcrumb-sep">/</span>
            <span class="gp-breadcrumb-name">${this.api?.repo ?? "core"}</span>
            <span class="gp-pill-outline" title=${this.who?.why ?? ""}
              >${this.who?.member === true ? "Member" : "Public"}</span
            >
          </div>
          <div class="gp-repo-actions">
            ${this.#syncControls()} ${this.#branchMenu()} ${this.#clone()}
          </div>
        </header>

        <div class="gp-screen gp-screen--code">
          ${
            this.offline
              ? html`<p class="gp-notice">
                  Showing the design's sample repository — ${this.reason}.
                </p>`
              : ""
          }
          ${this.syncNotice === null ? "" : html`<p class="gp-notice">${this.syncNotice}</p>`}
          ${this.head === null ? "" : this.#commitBar(this.head)} ${this.#panel()}

          <div class="gp-card gp-file-card">
            <div class="gp-card-head">
              ${icons.document_()}
              ${
                this.mode === "edit" && this.editingNew
                  ? html`<input
                      class="gp-input gp-editor-path"
                      placeholder="path/to/file.md"
                      aria-label="New file path"
                      autocomplete="off"
                      spellcheck="false"
                    />`
                  : html`${this.selected ?? "—"}`
              }
              <button
                class="gp-icon-btn"
                type="button"
                title="File history"
                aria-label="File history"
                ?data-active=${this.panel === "filelog"}
                ?disabled=${
                  this.offline || this.loading || this.selected === null || this.editingNew
                }
                @click=${() => void this.#toggleFileLog()}
              >
                ${icons.clock(14)}
              </button>
              <button
                class="gp-icon-btn"
                type="button"
                data-tight
                title=${
                  this.diffing
                    ? "Back to the file"
                    : this.mode === "edit"
                      ? "Review changes as a diff"
                      : "Diff against the previous version"
                }
                aria-label="Review changes"
                ?data-active=${this.diffing}
                ?disabled=${
                  this.offline ||
                  this.loading ||
                  this.saving ||
                  (this.mode !== "edit" && this.selected === null)
                }
                @click=${() => {
                  this.diffing = !this.diffing;
                  this.diffNote = null;
                  if (!this.diffing && this.mode === "edit") this.#focusEditor();
                }}
              >
                ${icons.diff(14)}
              </button>
              ${
                this.mode === "view"
                  ? html`<button
                      class="gp-icon-btn"
                      type="button"
                      data-tight
                      title=${
                        this.offline
                          ? "Read-only — the git+ API is not reachable"
                          : this.at !== null
                            ? "Read-only — viewing an old commit"
                            : "Edit file"
                      }
                      aria-label="Edit file"
                      ?disabled=${
                        this.offline || this.loading || this.selected === null || this.at !== null
                      }
                      @click=${() => this.#edit()}
                    >
                      ${icons.pencil(14)}
                    </button>`
                  : html`${
                        this.editingNew
                          ? nothing
                          : html`<button
                              class="gp-icon-btn"
                              type="button"
                              data-tight
                              title="Remove this file in a new commit"
                              aria-label="Delete file"
                              ?disabled=${this.saving}
                              @click=${() => void this.#delete()}
                            >
                              ${icons.trash(14)}
                            </button>`
                      }
                      <button
                        class="gp-icon-btn"
                        type="button"
                        data-tight
                        title="Cancel editing"
                        aria-label="Cancel editing"
                        ?disabled=${this.saving}
                        @click=${() => this.#cancelEdit()}
                      >
                        ${icons.close(14)}
                      </button>`
              }
            </div>
            ${this.#atBanner()}
            ${
              this.loading
                ? html`<div class="gp-empty">Loading…</div>`
                : html`<div class="gp-source-host gp-diff-host" ?hidden=${this.diffing}></div>
                    ${
                      this.diffing
                        ? this.diffNote !== null
                          ? html`<div class="gp-empty">${this.diffNote}</div>`
                          : html`<div class="gp-diff-review-host gp-diff-host"></div>`
                        : nothing
                    }
                    ${this.mode === "edit" ? this.#editorBar() : nothing}`
            }
          </div>
        </div>
      </div>
    `;
  }

  /**
   * The commit bar under the editable pane: message and Commit.
   *
   * The content itself lives in the source host above: `#renderSource`
   * attaches `@pierre/diffs`'s editor to the rendered file, and `#save`
   * reads the draft back from it at commit time. Cancel and delete sit in
   * the card head beside the filename, where the pencil that opened the
   * session was.
   */
  #editorBar(): TemplateResult {
    return html`
      ${
        this.editError === null
          ? nothing
          : html`<p class="gp-notice" data-error>${this.editError}</p>`
      }
      <div class="gp-editor-bar">
        <input
          class="gp-input gp-editor-message"
          aria-label="Commit message"
          placeholder=${
            this.editingNew ? "commit message" : `update ${this.selected ?? "this file"}`
          }
          autocomplete="off"
        />
        <button
          class="gp-btn-primary"
          type="button"
          ?disabled=${this.saving}
          @click=${() => void this.#save()}
        >
          ${this.saving ? "Committing…" : "Commit"}
        </button>
      </div>
    `;
  }

  /** base-wc types its own `menu-select` detail, so nothing here is asserted. */
  #onBranchSelect = (event: CustomEvent<MenuSelectDetail>): void => {
    if (event.detail.value === "__new-branch") {
      this.#branchDialog()?.show();
      return;
    }
    if (event.detail.value === "__rebase") {
      void this.#rebaseOntoDefault();
      return;
    }
    void this.#switchTo(event.detail.value);
  };

  #onExplorerAction = (event: CustomEvent<MenuSelectDetail>): void => {
    if (event.detail.value === "refresh") void this.#reload(this.selected ?? undefined);
  };

  /**
   * The branch picker, over the real ref list.
   *
   * `ui-menu` from base-wc owns the popover, keyboard handling and dismissal;
   * `menu-select` carries the chosen value. The last item creates a branch at
   * the current tip through `POST /branches/create`. Offline there is nothing
   * to pick or create, so the design's plain button renders disabled.
   */
  #branchMenu(): TemplateResult {
    const trigger = html`${icons.branch()} ${this.ref} ${icons.chevronDown()}`;
    if (this.offline || this.api === null) {
      return html`<button class="gp-branch-trigger" type="button" disabled>${trigger}</button>`;
    }
    return html`
      <ui-menu class="gp-branch-menu" @menu-select=${this.#onBranchSelect}>
        <button class="gp-branch-trigger" type="button" data-menu-trigger>${trigger}</button>
        <ui-menu-popup class="gp-menu-popup">
          ${this.branches.map(
            (branch) => html`
              <ui-menu-item
                class="gp-menu-item"
                value=${branch}
                ?data-current=${branch === this.ref}
              >
                ${icons.branch(12)} ${branch}
              </ui-menu-item>
            `,
          )}
          <ui-menu-item class="gp-menu-item" data-action value="__new-branch">
            ${icons.plus(12)} New branch…
          </ui-menu-item>
          ${
            this.ref === this.#defaultBranch
              ? nothing
              : html`
                  <ui-menu-item class="gp-menu-item" data-action value="__rebase">
                    ${icons.branch(12)} Rebase onto ${this.#defaultBranch}
                  </ui-menu-item>
                `
          }
        </ui-menu-popup>
      </ui-menu>
      <ui-dialog class="gp-new-branch">
        <ui-dialog-popup class="gp-dialog">
          <h2 class="gp-dialog-title" data-dialog-title>New branch</h2>
          <p class="gp-dialog-hint" data-dialog-description>
            Created at the tip of ${this.ref} and switched to.
          </p>
          <form @submit=${this.#createBranch}>
            <label class="gp-field-label" for="gp-new-branch-name">Name</label>
            <input
              id="gp-new-branch-name"
              class="gp-input"
              name="branch"
              required
              autocomplete="off"
              spellcheck="false"
              placeholder="topic/branch-name"
              @input=${(event: Event) => {
                if (event.target instanceof HTMLInputElement) event.target.setCustomValidity("");
              }}
            />
            <div class="gp-dialog-actions">
              <button
                class="gp-btn-quiet"
                type="button"
                @click=${() => this.#branchDialog()?.hide()}
              >
                Cancel
              </button>
              <button class="gp-btn-primary" type="submit">Create branch</button>
            </div>
          </form>
        </ui-dialog-popup>
      </ui-dialog>
    `;
  }

  /** The Clone dialog: the smart-HTTP URL, and a copy that says it copied. */
  #clone(): TemplateResult {
    return html`
      <ui-dialog class="gp-clone">
        <button class="gp-btn-primary" data-dialog-trigger type="button">Clone</button>
        <ui-dialog-popup class="gp-dialog">
          <h2 class="gp-dialog-title" data-dialog-title>Clone</h2>
          <p class="gp-dialog-hint" data-dialog-description>
            Smart HTTP, served from the same place as this page.
          </p>
          <div class="gp-clone-row">
            <input
              class="gp-input gp-clone-url"
              readonly
              .value=${this.api?.cloneUrl ?? ""}
              aria-label="Clone URL"
              @focus=${(event: Event) => {
                if (event.target instanceof HTMLInputElement) event.target.select();
              }}
            />
            <button class="gp-btn-quiet" type="button" @click=${this.#copyClone}>
              ${this.copied ? "Copied" : "Copy"}
            </button>
          </div>
          <p class="gp-dialog-hint">git clone ${this.api?.cloneUrl ?? ""}</p>
        </ui-dialog-popup>
      </ui-dialog>
    `;
  }

  /** Whichever history panel is open: the branch's commits, or one file's. */
  #panel(): TemplateResult | typeof nothing {
    if (this.panel === "none") return nothing;
    if (this.panel === "commits") {
      const log = this.commitLog;
      return html`
        <div class="gp-panel-card gp-history-panel">
          ${
            log === null
              ? html`<div class="gp-empty">Loading history…</div>`
              : log.length === 0
                ? html`<div class="gp-empty">No history to show.</div>`
                : log.map(
                    (commit) => html`
                      <div class="gp-list-row">
                        <span class="gp-sha">${commit.oid.slice(0, 7)}</span>
                        <span>${commit.subject}</span>
                        <span class="gp-when">${initials(commit.author)} · ${ago(commit.at)}</span>
                        <span class="gp-row-actions">
                          <button
                            class="gp-link-btn"
                            type="button"
                            title="Replay this commit onto ${this.ref}"
                            ?disabled=${this.syncing || this.offline}
                            @click=${() => void this.#cherryPick(commit.oid)}
                          >
                            pick
                          </button>
                          <button
                            class="gp-link-btn"
                            type="button"
                            title="Mark good for bisect"
                            ?disabled=${this.offline}
                            @click=${() => this.#mark(commit.oid, "good")}
                          >
                            good
                          </button>
                          <button
                            class="gp-link-btn"
                            type="button"
                            title="Mark bad for bisect"
                            ?disabled=${this.offline}
                            @click=${() => this.#mark(commit.oid, "bad")}
                          >
                            bad
                          </button>
                        </span>
                      </div>
                    `,
                  )
          }
          ${
            this.bisect === null || this.bisect.answer === null
              ? nothing
              : html`
                  <p class="gp-notice">
                    ${
                      this.bisect.answer.kind === "found"
                        ? `bisect: first bad commit is ${this.bisect.answer.commit.slice(0, 7)}`
                        : `bisect: test ${this.bisect.answer.commit.slice(0, 7)} — about ${String(this.bisect.answer.steps)} step(s) left`
                    }
                    <button
                      class="gp-link-btn"
                      type="button"
                      @click=${() => {
                        this.bisect = null;
                      }}
                    >
                      reset
                    </button>
                  </p>
                `
          }
        </div>
      `;
    }
    const entries = this.fileLog;
    return html`
      <div class="gp-panel-card gp-history-panel">
        ${
          entries === null
            ? html`<div class="gp-empty">Loading file history…</div>`
            : entries.length === 0
              ? html`<div class="gp-empty">No commits touch this file.</div>`
              : entries.map(
                  (entry) => html`
                    <button
                      class="gp-list-row gp-filelog-row"
                      type="button"
                      ?data-current=${this.at === entry.oid}
                      @click=${() => void this.#openAt(entry.oid)}
                    >
                      <span class="gp-sha">${entry.oid.slice(0, 7)}</span>
                      <span>${(entry.message.split("\n", 1)[0] ?? "").trim()}</span>
                    </button>
                  `,
                )
        }
      </div>
    `;
  }

  /** The read-only banner while the pane shows an old revision. */
  #atBanner(): TemplateResult | typeof nothing {
    const path = this.selected;
    const at = this.at;
    if (path === null || at === null) return nothing;
    return html`
      <p class="gp-notice" data-history>
        Viewing ${path} at ${at.slice(0, 7)} — read-only.
        <button class="gp-link-btn" type="button" @click=${() => void this.#open(path)}>
          Back to tip
        </button>
      </p>
    `;
  }

  #commitBar(head: HeadCommit): TemplateResult {
    return html`
      <button
        class="gp-commit-bar"
        type="button"
        title="Recent commits"
        aria-expanded=${this.panel === "commits" ? "true" : "false"}
        ?disabled=${this.offline}
        @click=${() => void this.#toggleCommits()}
      >
        ${head.avatar === undefined ? "" : html`<span class="gp-avatar">${head.avatar}</span>`}
        ${
          head.author === undefined
            ? ""
            : html`<span class="gp-commit-author">${head.author}</span>`
        }
        <span class="gp-commit-subject">${head.message}</span>
        <span class="gp-commit-sha">${head.sha}</span>
        <span>${head.when}</span>
      </button>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "gp-code": GpCode;
  }
}
