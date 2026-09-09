/**
 * One shared `AtomRegistry` for the page.
 *
 * The hub's listings are queried as atoms — result-tracked, so a caller reads
 * one of three settled outcomes rather than a promise it has to classify. The
 * registry holds their state, and it is page state: a second registry would be
 * a second set of answers.
 *
 * It deliberately carries no idle TTL, so a node is released as soon as its
 * last subscriber leaves and the next read runs the query again. Every read
 * here subscribes and unsubscribes around a single answer, which makes each
 * one a fresh read of the repository — and that is what the projection polling
 * in `hub.ts` depends on: a cached answer there would be a task or a merge the
 * repository has already recorded and the page would never see.
 *
 * Nothing subscribes to it from a view. Foldkit's Commands read through it and
 * turn what it answers into Messages, which is what keeps the atoms an
 * implementation detail of the hub rather than a second store beside the Model.
 */
import { AtomRegistry } from "effect/unstable/reactivity";

export const registry = AtomRegistry.make();
