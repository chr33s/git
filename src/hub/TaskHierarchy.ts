/** Derive task edges across the whole namespace before selecting a response page. */
export const hierarchy = (edges: ReadonlyMap<string, string | null>) => {
  const parents = new Map(edges);
  const done = new Set<string>();
  for (const task of parents.keys()) {
    const path: string[] = [];
    const positions = new Map<string, number>();
    let at: string | null = task;
    while (at !== null && !done.has(at) && !positions.has(at)) {
      positions.set(at, path.length);
      path.push(at);
      at = parents.get(at) ?? null;
    }
    const cycle = at === null ? undefined : positions.get(at);
    // Detach every cycle member, preserving tasks hanging beneath the cycle.
    if (cycle !== undefined) {
      for (const member of path.slice(cycle)) parents.set(member, null);
    }
    for (const visited of path) done.add(visited);
  }
  const children = new Map<string, string[]>();
  for (const [task, parent] of parents) {
    if (parent === null) continue;
    const held = children.get(parent);
    if (held === undefined) children.set(parent, [task]);
    else held.push(task);
  }
  return { parents, children };
};
