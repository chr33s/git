/** The host creates repositories on first write; reading an unknown name is inert. */
import { Context, Effect, Layer, Semaphore } from "effect";

import { initializeBare, stores as filesystem } from "../git/Node.ts";
import { PackStore } from "../git/Packed.ts";
import { ObjectStore, RefStore } from "../git/Store.ts";

export const stores = (directory: string) =>
  Layer.effectContext(
    Effect.gen(function* () {
      const objects = yield* ObjectStore;
      const refs = yield* RefStore;
      const packs = yield* PackStore;
      const starting = yield* Semaphore.make(1);
      let initialized = false;
      const initialize = Effect.fn("NodeHost.initialize")(() =>
        Effect.suspend(() =>
          initialized
            ? Effect.void
            : Effect.gen(function* () {
                if (initialized) return;
                // The default branch is the constant, valid "main". Filesystem
                // failures remain typed and a later write can retry initialization.
                yield* initializeBare(directory).pipe(Effect.catchTag("Invalid", Effect.die));
                initialized = true;
              }).pipe(Semaphore.withPermit(starting)),
        ),
      );

      return Context.make(
        ObjectStore,
        ObjectStore.of({
          ...objects,
          write: Effect.fn("NodeHost.objects.write")((object) =>
            initialize().pipe(Effect.andThen(objects.write(object))),
          ),
        }),
      ).pipe(
        Context.add(
          RefStore,
          RefStore.of({
            ...refs,
            apply: Effect.fn("NodeHost.refs.apply")((updates, options) =>
              // Empty batches (including a read-only negotiation) create nothing.
              (updates.length === 0 ? Effect.void : initialize()).pipe(
                Effect.andThen(refs.apply(updates, options)),
              ),
            ),
            setHead: Effect.fn("NodeHost.refs.setHead")((target) =>
              initialize().pipe(Effect.andThen(refs.setHead(target))),
            ),
            updateShallow: Effect.fn("NodeHost.refs.updateShallow")((update) =>
              (update.add.length === 0 ? Effect.void : initialize()).pipe(
                Effect.andThen(refs.updateShallow(update)),
              ),
            ),
          }),
        ),
        Context.add(
          PackStore,
          PackStore.of({
            ...packs,
            write: Effect.fn("NodeHost.packs.write")((pack) =>
              initialize().pipe(Effect.andThen(packs.write(pack))),
            ),
          }),
        ),
      );
    }),
  ).pipe(Layer.provideMerge(filesystem(directory)));
