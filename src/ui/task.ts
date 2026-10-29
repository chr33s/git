/**
 * The Task domain, as pure functions over a list.
 *
 * These are the queries and edits `store.ts` used to own as methods on a
 * mutable `EventTarget`. Nothing here holds state: every function takes the
 * tasks it reads and returns a value, so the same call on the same list
 * always answers the same way — which is what lets the Foldkit Model own the
 * list and `update` stay a pure transition.
 *
 * The rules are unchanged from the store, including the ones that look like
 * details: the ancestor walk still guards against a cycle, a narrowed list is
 * still flat rather than pretending a hierarchy survived a filter, and a
 * release still heads its group rather than taking a row of its own.
 */
import { type ChangeRequest, isChangeRequest, type Person, type Task } from "./model.ts";

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

export const byId = (tasks: readonly Task[], id: string): Task | undefined =>
  tasks.find((task) => task.id === id);

/** How many Tasks and Change Requests are still open, for the nav badge. */
export const openCount = (tasks: readonly Task[]): number =>
  tasks.filter((task) => task.status !== "Done" && task.status !== "Merged").length;

/**
 * Where a task sits, outermost first: its release, then whatever is between.
 *
 * One chain rather than a "milestone" and a "parent" read separately — the hub
 * records a single edge, and two rows claiming otherwise said the same thing
 * twice for anything filed straight under a release. The first entry is the
 * root the task hangs from; the last is its own parent. Empty for a task that
 * belongs to nothing.
 *
 * Guarded against a cycle even so. `GET /hub/tasks` severs one before it
 * reaches here, but this list also holds fixtures and tab-local moves that
 * never went near it, and a walk that trusted the data would hang the tab
 * rather than misdraw one row.
 */
export const ancestorsOf = (tasks: readonly Task[], task: Task): readonly Task[] => {
  const seen = new Set<string>([task.id]);
  const chain: Task[] = [];
  let at = task;
  for (;;) {
    const parent = at.parent === undefined ? undefined : byId(tasks, at.parent);
    if (parent === undefined || seen.has(parent.id)) break;
    seen.add(parent.id);
    chain.unshift(parent);
    at = parent;
  }
  return chain;
};

/** Every descendant of `task`, flattened, capped at one visible indent. */
const under = (tasks: readonly Task[], task: Task): Row[] => {
  const out: Row[] = [];
  const seen = new Set([task.id]);
  const pending = (task.children ?? []).map((id) => ({ id, depth: 0 })).reverse();
  while (pending.length > 0) {
    const next = pending.pop();
    if (next === undefined || seen.has(next.id)) continue;
    seen.add(next.id);
    const child = byId(tasks, next.id);
    if (child === undefined) continue;
    out.push({ task: child, depth: Math.min(next.depth, 1) });
    for (const id of [...(child.children ?? [])].reverse())
      pending.push({ id, depth: next.depth + 1 });
  }
  return out;
};

/**
 * The Tasks list, grouped by release.
 *
 * A release is a task like any other, so it heads its group rather than taking
 * a row of its own: the design's two levels — an epic, and the work under it —
 * are the two the list draws, and nesting a third would push every row across
 * at an indent the design set deliberately.
 *
 * A root with no children is not a release, only work nobody filed, and it
 * lands in the trailing group. A narrowed list is one flat unlabelled group,
 * for the reason `rows` gives.
 */
export const groups = (tasks: readonly Task[], filter: Filter = "all"): readonly Group[] => {
  if (filter !== "all") return [{ milestone: null, rows: rows(tasks, filter) }];

  const out: Group[] = [];
  const loose: Row[] = [];
  for (const task of tasks) {
    if (task.parent !== undefined) continue;
    if ((task.children ?? []).length === 0) loose.push({ task, depth: 0 });
    else out.push({ milestone: task, rows: under(tasks, task) });
  }
  if (loose.length > 0) out.push({ milestone: null, rows: loose });
  return out;
};

/**
 * The Tasks list, in display order.
 *
 * Unfiltered, the hierarchy: every root, each followed by its children —
 * mixing Tasks and Change Requests in one list is the point of a Change
 * Request being a specialization rather than a sibling type. Narrowed by kind
 * or by a search query, the result is a flat list instead: a filter that hides
 * a parent has nothing to hang its children under, so pretending the hierarchy
 * survived would misdraw it.
 */
export const rows = (
  tasks: readonly Task[],
  filter: Filter = "all",
  query = "",
): readonly Row[] => {
  const needle = query.trim().toLowerCase();
  if (filter === "all" && needle === "") {
    return groups(tasks).flatMap((group) =>
      group.milestone === null
        ? group.rows
        : [{ task: group.milestone, depth: 0 }, ...group.rows.map((row) => ({ ...row, depth: 1 }))],
    );
  }
  return tasks
    .filter((task) => (filter === "tasks" ? task.kind === "Task" : true))
    .filter((task) => (filter === "crs" ? task.kind === "CR" : true))
    .filter(
      (task) =>
        needle === "" ||
        task.title.toLowerCase().includes(needle) ||
        task.id.toLowerCase().includes(needle),
    )
    .map((task) => ({ task, depth: 0 }));
};

/**
 * Replace one task, or answer with the same list.
 *
 * Returning the identical array when nothing changed is not an optimisation
 * here, it is what keeps a Foldkit re-render from redrawing a list whose
 * contents are unchanged.
 */
export const replace = (
  tasks: readonly Task[],
  id: string,
  update: (task: Task) => Task,
): readonly Task[] => {
  const at = tasks.findIndex((task) => task.id === id);
  const before = tasks[at];
  if (before === undefined) return tasks;
  const after = update(before);
  return after === before ? tasks : tasks.with(at, after);
};

/** The children list, with `id` taken out of it / put into it. */
const detach = (task: Task, id: string): Task => ({
  ...task,
  children: (task.children ?? []).filter((child) => child !== id),
});

const attach = (task: Task, id: string): Task =>
  (task.children ?? []).includes(id) ? task : { ...task, children: [...(task.children ?? []), id] };

/** Move a task under another, or — with an empty `parent` — out from under one. */
export const moved = (tasks: readonly Task[], id: string, parent: string): readonly Task[] => {
  const task = byId(tasks, id);
  if (task === undefined) return tasks;
  let next = tasks;
  if (task.parent !== undefined) next = replace(next, task.parent, (held) => detach(held, id));
  if (parent !== "") next = replace(next, parent, (held) => attach(held, id));
  return replace(next, id, (held) => {
    const { parent: _was, ...rest } = held;
    return parent === "" ? rest : { ...rest, parent };
  });
};

/**
 * The id a new Task takes.
 *
 * Continues the fixtures' sequence so `T-21` follows `T-20` rather than
 * starting a second numbering, and skips any id already taken.
 */
export const nextId = (tasks: readonly Task[]): string => {
  const ids = new Set(tasks.map((task) => task.id));
  let next = 1;
  for (const id of ids) {
    const number = /^T-(\d+)$/.exec(id)?.[1];
    const value = number === undefined ? 0 : Number(number);
    if (Number.isSafeInteger(value) && value < Number.MAX_SAFE_INTEGER)
      next = Math.max(next, value + 1);
  }
  while (ids.has(`T-${String(next)}`)) next++;
  return `T-${String(next)}`;
};

/**
 * A newly opened Task.
 *
 * Only a plain Task: a Change Request is a Task with a diff attached, and a
 * diff needs a source ref to exist — which means pushing a branch, not filling
 * in a form.
 */
export const opened = (input: {
  readonly id: string;
  readonly title: string;
  readonly desc: string;
  readonly author: Person;
}): Task => ({
  id: input.id,
  kind: "Task",
  title: input.title,
  status: "Todo",
  avatar: input.author.avatar,
  updated: "just now",
  desc: input.desc,
  assignees: [input.author],
  labels: [],
  comments: [],
});

/**
 * A Change Request, settled.
 *
 * Only when its review state allows it — the button is disabled otherwise, but
 * the rule belongs here, not in the button. Anything else comes back unchanged.
 */
export const merged = (task: Task): Task => {
  if (!isChangeRequest(task) || !task.review.ok || task.review.merged === true) return task;
  const settled: ChangeRequest = {
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
  return settled;
};
