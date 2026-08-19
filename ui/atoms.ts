/**
 * Atoms, subscribed the Lit way.
 *
 * One shared `AtomRegistry` holds every atom's state for the page, and
 * `AtomController` is the bridge to Lit's reactive-controller lifecycle: it
 * subscribes on `hostConnected`, re-renders the host on every change, and
 * unsubscribes on `hostDisconnected` — the same discipline the screens
 * already apply to the task store by hand.
 */
import type { ReactiveController, ReactiveControllerHost } from "lit";

import { type Atom, AtomRegistry } from "effect/unstable/reactivity";

/** The one registry every screen shares — atom state is page state. */
export const registry = AtomRegistry.make();

export class AtomController<A> implements ReactiveController {
  readonly #host: ReactiveControllerHost;
  readonly #atom: Atom.Atom<A>;
  #unsubscribe: (() => void) | null = null;

  /** The atom's current value; reading it never triggers a fetch by itself. */
  value: A;

  constructor(host: ReactiveControllerHost, atom: Atom.Atom<A>) {
    this.#host = host;
    this.#atom = atom;
    this.value = registry.get(atom);
    host.addController(this);
  }

  hostConnected(): void {
    this.#unsubscribe = registry.subscribe(
      this.#atom,
      (value) => {
        this.value = value;
        this.#host.requestUpdate();
      },
      { immediate: true },
    );
  }

  hostDisconnected(): void {
    this.#unsubscribe?.();
    this.#unsubscribe = null;
  }
}
