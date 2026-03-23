/**
 * Server-side hook system for Git push events.
 *
 * Hooks run during receive-pack to allow custom logic:
 * - pre-receive:  called before any refs are updated; can reject entire push
 * - update:       called per-ref; can reject individual ref updates
 * - post-receive: called after all refs are updated; for notifications / CI triggers
 */

/** A single ref update passed through the hook pipeline. */
export interface HookRefUpdate {
  ref: string;
  oldOid: string;
  newOid: string;
}

/** Context provided to every hook invocation. */
export interface HookContext {
  /** Repository name. */
  repository: string;
  /** The ref updates in this push. */
  updates: HookRefUpdate[];
  /** Capabilities advertised by the client. */
  capabilities: Set<string>;
}

/** Result returned by a hook handler. */
export interface HookResult {
  /** Whether the hook accepted the operation. */
  ok: boolean;
  /** Human-readable message (sent back to the client on rejection). */
  message?: string;
}

/** Result for per-ref `update` hooks. */
export interface UpdateHookResult extends HookResult {
  /** The ref this result applies to. */
  ref: string;
}

/** Hook handler signature. */
export type Hook<T extends HookResult = HookResult> = (context: HookContext) => Promise<T>;

/** Per-ref update hook that receives context plus the specific ref being updated. */
export type UpdateHook = (context: HookContext, update: HookRefUpdate) => Promise<HookResult>;

export type HookName = "pre-receive" | "update" | "post-receive";

/**
 * Registry and executor for server-side hooks.
 *
 * Usage:
 * ```ts
 * const hooks = new HookRunner();
 * hooks.register("pre-receive", async (ctx) => {
 *   if (ctx.updates.some(u => u.ref === "refs/heads/protected")) {
 *     return { ok: false, message: "Cannot push to protected branch" };
 *   }
 *   return { ok: true };
 * });
 * ```
 */
export class HookRunner {
  #preReceive: Hook[] = [];
  #update: UpdateHook[] = [];
  #postReceive: Hook[] = [];

  /** Register a hook handler. */
  register(name: "pre-receive", handler: Hook): void;
  register(name: "update", handler: UpdateHook): void;
  register(name: "post-receive", handler: Hook): void;
  register(name: HookName, handler: Hook | UpdateHook): void {
    switch (name) {
      case "pre-receive":
        this.#preReceive.push(handler as Hook);
        break;
      case "update":
        this.#update.push(handler as UpdateHook);
        break;
      case "post-receive":
        this.#postReceive.push(handler as Hook);
        break;
    }
  }

  /** Unregister all hooks of a given name, or all hooks if no name given. */
  clear(name?: HookName): void {
    if (!name || name === "pre-receive") this.#preReceive = [];
    if (!name || name === "update") this.#update = [];
    if (!name || name === "post-receive") this.#postReceive = [];
  }

  /** Returns true if any hooks are registered for the given name. */
  has(name: HookName): boolean {
    switch (name) {
      case "pre-receive":
        return this.#preReceive.length > 0;
      case "update":
        return this.#update.length > 0;
      case "post-receive":
        return this.#postReceive.length > 0;
    }
  }

  /**
   * Run pre-receive hooks. All handlers must return `{ ok: true }` for the
   * push to proceed. The first rejection short-circuits.
   */
  async runPreReceive(context: HookContext): Promise<HookResult> {
    for (const hook of this.#preReceive) {
      const result = await hook(context);
      if (!result.ok) {
        return result;
      }
    }
    return { ok: true };
  }

  /**
   * Run per-ref update hooks. Returns a map of ref → result.
   * Each ref is independently accepted or rejected.
   */
  async runUpdate(context: HookContext): Promise<Map<string, HookResult>> {
    const results = new Map<string, HookResult>();

    for (const update of context.updates) {
      let refResult: HookResult = { ok: true };

      for (const hook of this.#update) {
        const result = await hook(context, update);
        if (!result.ok) {
          refResult = result;
          break;
        }
      }

      results.set(update.ref, refResult);
    }

    return results;
  }

  /**
   * Run post-receive hooks. These run after refs are updated and cannot
   * reject the push. Errors are caught and returned but do not affect the push.
   */
  async runPostReceive(context: HookContext): Promise<HookResult[]> {
    const results: HookResult[] = [];

    for (const hook of this.#postReceive) {
      try {
        results.push(await hook(context));
      } catch (error) {
        results.push({
          ok: false,
          message: error instanceof Error ? error.message : "Hook error",
        });
      }
    }

    return results;
  }
}
