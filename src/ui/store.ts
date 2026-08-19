/**
 * The Task store: the fixtures, made mutable.
 *
 * The design's Task data (`fixtures.ts`) used to be read directly by every
 * screen, which made the UI a rendering of frozen data — the "New task"
 * button, the comment box and the merge button could not do anything. This
 * module seeds itself from those fixtures and owns every change to them:
 * creating a Task, appending a comment, merging a Change Request.
 *
 * Changes live in this browser tab and vanish on refresh, deliberately. The
 * git-native hub (`src/hub/PullRequest.ts`, `src/hub/Projection.ts`) is what
 * will hold them for real, and it has no HTTP surface yet — persisting to
 * `localStorage` in the meantime would fake a durability the system does not
 * have. When the hub API lands, this module is the one place that changes:
 * the screens already speak only to it.
 *
 * Tasks are immutable values (`model.ts` declares every member `readonly`), so
 * a mutation replaces the entry rather than editing it in place, and notifies
 * subscribers — each screen re-renders from the store rather than patching its
 * own copy.
 */
import { tasks as seed } from "./fixtures.ts";
import { type Comment, isChangeRequest, type Person, type SessionRow, type Task } from "./model.ts";

/** The Tasks-screen segments: everything, pure Tasks, or Change Requests. */
export type Filter = "all" | "tasks" | "crs";

/** One line of the Tasks list: a task at its depth in the hierarchy. */
export interface Row {
  readonly task: Task;
  readonly depth: number;
}

/**
 * A release and the work filed under it.
 *
 * `milestone` is `null` for the trailing group of work that belongs to no
 * release — and for every narrowed list, where there is no hierarchy left to
 * group by.
 */
export interface Group {
  readonly milestone: Task | null;
  readonly rows: readonly Row[];
}

/** The children list, with `id` taken out of it / put into it. */
const detach = (task: Task, id: string): Task => ({
  ...task,
  children: (task.children ?? []).filter((child) => child !== id),
});

const attach = (task: Task, id: string): Task =>
  (task.children ?? []).includes(id) ? task : { ...task, children: [...(task.children ?? []), id] };

class TaskStore extends EventTarget {
  #tasks: Task[] = [...seed];
  #sessions: readonly SessionRow[] = [];
  #liveNotice: string | null = null;

  /** Why live state is withheld — an authentication refusal, not offline. */
  get liveNotice(): string | null {
    return this.#liveNotice;
  }

  /**
   * The repository refused to be read. Refusal is not absence: the fixtures
   * are the documented *offline sample*, and presenting them over a private
   * repository that turned this browser away would dress a denial up as
   * data. The store empties and says why instead.
   */
  denyLive(reason: string): void {
    this.#liveNotice = reason;
    this.#tasks = [];
    this.#sessions = [];
    this.#notify();
  }

  /** The hub's sessions, for the Activity screen; empty until it answers. */
  get sessions(): readonly SessionRow[] {
    return this.#sessions;
  }

  /** Adopt the hub's session listing; see `adopt` for the tasks half. */
  adoptSessions(sessions: readonly SessionRow[]): void {
    this.#sessions = [...sessions];
    this.#liveNotice = null;
    this.#notify();
  }

  /** Every task, in creation order — roots and children interleaved. */
  list(): readonly Task[] {
    return this.#tasks;
  }

  get(id: string): Task | undefined {
    return this.#tasks.find((task) => task.id === id);
  }

  /** How many Tasks and Change Requests are still open, for the nav badge. */
  openCount(): number {
    return this.#tasks.filter((task) => task.status !== "Done" && task.status !== "Merged").length;
  }

  /**
   * The Tasks list, in display order.
   *
   * Unfiltered, the hierarchy: every root, each followed by its children —
   * mixing Tasks and Change Requests in one list is the point of a Change
   * Request being a specialization rather than a sibling type. Narrowed by
   * kind or by a search query, the result is a flat list instead: a filter
   * that hides a parent has nothing to hang its children under, so pretending
   * the hierarchy survived would misdraw it.
   */
  /**
   * Where a task sits, outermost first: its release, then whatever is between.
   *
   * One chain rather than a "milestone" and a "parent" read separately — the
   * hub records a single edge, and two rows claiming otherwise said the same
   * thing twice for anything filed straight under a release. The first entry
   * is the root the task hangs from; the last is its own parent. Empty for a
   * task that belongs to nothing.
   *
   * Guarded against a cycle even so. `GET /hub/tasks` severs one before it
   * reaches here, but this store also holds fixtures and tab-local moves that
   * never went near it, and a walk that trusted the data would hang the tab
   * rather than misdraw one row.
   */
  ancestorsOf(task: Task): readonly Task[] {
    const seen = new Set<string>([task.id]);
    const chain: Task[] = [];
    let at = task;
    for (;;) {
      const parent = at.parent === undefined ? undefined : this.get(at.parent);
      if (parent === undefined || seen.has(parent.id)) break;
      seen.add(parent.id);
      chain.unshift(parent);
      at = parent;
    }
    return chain;
  }

  /**
   * The Tasks list, grouped by release.
   *
   * A release is a task like any other, so it heads its group rather than
   * taking a row of its own: the design's two levels — an epic, and the work
   * under it — are the two the list draws, and nesting a third would push
   * every row across at an indent the design set deliberately.
   *
   * A root with no children is not a release, only work nobody filed, and it
   * lands in the trailing group. A narrowed list is one flat unlabelled group,
   * for the reason `rows` gives.
   */
  groups(filter: Filter = "all"): readonly Group[] {
    if (filter !== "all") return [{ milestone: null, rows: this.rows(filter) }];

    const under = (task: Task): Row[] => {
      const out: Row[] = [];
      for (const childId of task.children ?? []) {
        const child = this.get(childId);
        if (child === undefined) continue;
        out.push({ task: child, depth: 0 });
        for (const grandchildId of child.children ?? []) {
          const grandchild = this.get(grandchildId);
          if (grandchild !== undefined) out.push({ task: grandchild, depth: 1 });
        }
      }
      return out;
    };

    const groups: Group[] = [];
    const loose: Row[] = [];
    for (const task of this.#tasks) {
      if (task.parent !== undefined) continue;
      if ((task.children ?? []).length === 0) loose.push({ task, depth: 0 });
      else groups.push({ milestone: task, rows: under(task) });
    }
    if (loose.length > 0) groups.push({ milestone: null, rows: loose });
    return groups;
  }

  rows(filter: Filter = "all", query = ""): readonly Row[] {
    const needle = query.trim().toLowerCase();
    if (filter === "all" && needle === "") {
      return this.groups().flatMap((group) =>
        group.milestone === null
          ? group.rows
          : [
              { task: group.milestone, depth: 0 },
              ...group.rows.map((row) => ({ ...row, depth: 1 })),
            ],
      );
    }
    return this.#tasks
      .filter((task) => (filter === "tasks" ? task.kind === "Task" : true))
      .filter((task) => (filter === "crs" ? task.kind === "CR" : true))
      .filter(
        (task) =>
          needle === "" ||
          task.title.toLowerCase().includes(needle) ||
          task.id.toLowerCase().includes(needle),
      )
      .map((task) => ({ task, depth: 0 }));
  }

  /**
   * Create a Task.
   *
   * Only a plain Task: a Change Request is a Task with a diff attached, and a
   * diff needs a source ref to exist — which means pushing a branch, not
   * filling in a form. The id continues the fixtures' sequence so `T-21`
   * follows `T-20` rather than starting a second numbering.
   */
  create(input: { readonly title: string; readonly desc: string; readonly author: Person }): Task {
    const next =
      Math.max(...this.#tasks.map((task) => Number(task.id.split("-")[1] ?? "0") || 0)) + 1;
    const task: Task = {
      id: `T-${String(next)}`,
      kind: "Task",
      title: input.title,
      status: "Todo",
      avatar: input.author.avatar,
      updated: "just now",
      desc: input.desc,
      assignees: [input.author],
      labels: [],
      comments: [],
    };
    this.#tasks = [...this.#tasks, task];
    this.#notify();
    return task;
  }

  /** Append a comment to a task's discussion. */
  comment(id: string, comment: Comment): void {
    this.#replace(id, (task) => ({
      ...task,
      comments: [...task.comments, comment],
      updated: "just now",
    }));
  }

  /**
   * Merge a Change Request.
   *
   * Only when its review state allows it — the button is disabled otherwise,
   * but the rule belongs here, not in the button. The merge is a projection
   * change only: the fixture Change Requests name refs that exist in the
   * design, not in the repository, so there is nothing for the server's
   * `/merge` endpoint to act on.
   */
  merge(id: string): void {
    this.#replace(id, (task) => {
      if (!isChangeRequest(task) || !task.review.ok || task.review.merged === true) return task;
      return {
        ...task,
        status: "Merged",
        updated: "just now",
        review: {
          headline: "Merged",
          detail: `Squashed into ${task.targetRef} · just now`,
          ok: true,
          action: "Merged",
          merged: true,
        },
      };
    });
  }

  /** Re-render on change; returns the unsubscribe for `disconnectedCallback`. */
  subscribe(listener: () => void): () => void {
    this.addEventListener("change", listener);
    return () => this.removeEventListener("change", listener);
  }

  /**
   * Replace the contents with what the hub projected.
   *
   * Called by `hub.ts` when `GET /hub/tasks` / `GET /hub/pulls` answer with
   * anything: from then on the store is a view of the repository's own state
   * rather than the design's sample, and every screen follows without
   * knowing which it is showing.
   */
  adopt(tasks: readonly Task[]): void {
    this.#tasks = [...tasks];
    this.#liveNotice = null;
    this.#notify();
  }

  /** Apply one update to one task — the seam `hub.ts` hydrates details through. */
  patch(id: string, update: (task: Task) => Task): void {
    this.#replace(id, update);
  }

  /**
   * Fill a hub-sourced task's detail (discussion, checks) on demand.
   *
   * Routed through the lazy hub module so the detail screen needs no import
   * of the derived client; for a fixture id this resolves to a no-op there.
   */
  hydrate(id: string): void {
    void import("./hub.ts").then((hub) => hub.hydrate(id)).catch(() => {});
  }

  /**
   * Open a task in the hub, signed by the browser's key; the id once the
   * server holds it, `null` when it could not land — the caller then falls
   * back to `create` and the tab-local story above.
   */
  async createRemote(input: {
    readonly title: string;
    readonly description: string;
    readonly parent?: string;
  }): Promise<string | null> {
    return await import("./hub.ts").then((hub) => hub.createTask(input)).catch(() => null);
  }

  /**
   * Move a task under another, or out from under one.
   *
   * Tried in the hub first; a fixture task has no ref to append to, so it
   * moves in this tab only — the same split every other write here makes.
   */
  async move(id: string, parent: string): Promise<void> {
    const landed = await import("./hub.ts")
      .then((hub) => hub.moveTask(id, parent))
      .catch(() => false);
    if (landed) return;
    const was = this.get(id)?.parent;
    if (was !== undefined) this.#replace(was, (task) => detach(task, id));
    if (parent !== "") this.#replace(parent, (task) => attach(task, id));
    this.#replace(id, (task) => {
      const { parent: _, ...rest } = task;
      return parent === "" ? rest : { ...rest, parent };
    });
  }

  /** Comment on a hub pull request; `false` falls back to `comment`. */
  async commentRemote(id: string, body: string): Promise<boolean> {
    return await import("./hub.ts").then((hub) => hub.commentOn(id, body)).catch(() => false);
  }

  /** Open a Change Request in the hub for a pushed revision. */
  async openPullRemote(input: {
    readonly title: string;
    readonly description: string;
    readonly base: string;
    readonly head: string;
  }): Promise<string | null> {
    return await import("./hub.ts").then((hub) => hub.openPull(input)).catch(() => null);
  }

  /** Approve or reject a hub Change Request's current revision. */
  async reviewRemote(id: string, decision: "approve" | "reject"): Promise<boolean> {
    return await import("./hub.ts").then((hub) => hub.review(id, decision)).catch(() => false);
  }

  /** Reply in one of a hub Change Request's threads. */
  async replyRemote(id: string, thread: string, body: string): Promise<boolean> {
    return await import("./hub.ts").then((hub) => hub.reply(id, thread, body)).catch(() => false);
  }

  /** Resolve or reopen one of a hub Change Request's threads. */
  async resolveRemote(id: string, thread: string, resolved: boolean): Promise<boolean> {
    return await import("./hub.ts")
      .then((hub) => hub.resolveThread(id, thread, resolved))
      .catch(() => false);
  }

  /**
   * Settle a hub Change Request through the hub's merge endpoint — `null`
   * on success (the projection re-read shows the merge), otherwise the
   * reason it stays open. Never falls back to a tab-local "merged".
   */
  async mergeRemote(id: string): Promise<string | null> {
    return await import("./hub.ts")
      .then((hub) => hub.merge(id))
      .catch(() => "the hub could not be reached — the Change Request stays open");
  }

  /** Claim, release, or close a hub task — its advisory lease and its end. */
  async taskActionRemote(
    id: string,
    action: "claim" | "release" | "complete" | "abandon",
  ): Promise<boolean> {
    return await import("./hub.ts").then((hub) => hub.taskAction(id, action)).catch(() => false);
  }

  #replace(id: string, update: (task: Task) => Task): void {
    const at = this.#tasks.findIndex((task) => task.id === id);
    const before = this.#tasks[at];
    if (before === undefined) return;
    const after = update(before);
    if (after === before) return;
    this.#tasks = this.#tasks.with(at, after);
    this.#notify();
  }

  #notify(): void {
    this.dispatchEvent(new Event("change"));
  }
}

/** The one store every screen shares. */
export const store = new TaskStore();

/**
 * Ask the hub what this repository actually holds, off the boot path.
 *
 * The import is dynamic for the same reason Shiki's is: the derived client
 * carries the `HttpApi` declaration and the Effect runtime, and first paint
 * should not wait on either. When the hub is unreachable or empty the
 * promise resolves into nothing and the fixtures above remain — which is the
 * documented offline behaviour, not a failure.
 */
void import("./hub.ts").then((hub) => hub.seed()).catch(() => {});
