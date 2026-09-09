/**
 * One shared `AtomRegistry` for the page.
 *
 * The hub's listings are queried as atoms — memoized and result-tracked — so
 * asking twice costs one fetch. The registry holds their state, and it is
 * page state: a second registry would be a second set of answers.
 *
 * Nothing subscribes to it from a view. Foldkit's Commands read through it and
 * turn what it answers into Messages, which is what keeps the atoms an
 * implementation detail of the hub rather than a second store beside the Model.
 */
import { AtomRegistry } from "effect/unstable/reactivity";

export const registry = AtomRegistry.make();
