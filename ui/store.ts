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
import { type Comment, isChangeRequest, type Person, type Task } from "./model.ts";

/** The Tasks-screen segments: everything, pure Tasks, or Change Requests. */
export type Filter = "all" | "tasks" | "crs";

/** One line of the Tasks list: a task at its depth in the hierarchy. */
export interface Row {
  readonly task: Task;
  readonly depth: number;
}

class TaskStore extends EventTarget {
  #tasks: Task[] = [...seed];

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
  rows(filter: Filter = "all", query = ""): readonly Row[] {
    const needle = query.trim().toLowerCase();
    if (filter === "all" && needle === "") {
      const out: Row[] = [];
      for (const task of this.#tasks) {
        if (task.parent !== undefined) continue;
        out.push({ task, depth: 0 });
        for (const childId of task.children ?? []) {
          const child = this.get(childId);
          if (child !== undefined) out.push({ task: child, depth: 1 });
        }
      }
      return out;
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
      milestone: "—",
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
