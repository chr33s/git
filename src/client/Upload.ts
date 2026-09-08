/** Replayable upload bodies without retaining a complete pack in memory. */
import { Cause, Effect, Stream } from "effect";
import { StorageFailure } from "../git/Error.ts";

const failed = (cause: unknown) =>
  new StorageFailure({ operation: "upload", path: "temporary request body", cause });

const ownFile = Effect.fn("Upload.ownFile")(function* (directory: FileSystemDirectoryHandle) {
  const name = `.git-upload-${crypto.randomUUID()}`;
  // Take ownership before creating the file. Tab termination releases the Web
  // Lock even when Effect finalizers cannot run, allowing a later upload to
  // reclaim the file without deleting another tab's in-flight request body.
  yield* Effect.acquireRelease(
    Effect.tryPromise({
      try: async () => {
        const acquired = Promise.withResolvers<void>();
        const release = Promise.withResolvers<void>();
        const finished = navigator.locks.request(name, () => {
          acquired.resolve();
          return release.promise;
        });
        void finished.catch(acquired.reject);
        await acquired.promise;
        return async () => {
          release.resolve();
          await finished;
        };
      },
      catch: failed,
    }),
    (release) => Effect.promise(release),
  );
  yield* Effect.tryPromise({
    try: async () => {
      for await (const [entry] of directory.entries()) {
        // Match only our canonical names, never a live writer's swap file.
        if (!/^\.git-upload-[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/.test(entry))
          continue;
        await navigator.locks.request(entry, { ifAvailable: true }, async (lock) => {
          if (lock !== null) await directory.removeEntry(entry).catch(() => undefined);
        });
      }
    },
    catch: failed,
  });
  return yield* Effect.acquireRelease(
    Effect.tryPromise({
      try: () => directory.getFileHandle(name, { create: true }),
      catch: failed,
    }),
    () => Effect.promise(() => directory.removeEntry(name).catch(() => undefined)),
  );
});

export const prepare = Effect.fn("Upload.prepare")(function* <E>(
  source: Stream.Stream<Uint8Array, E>,
) {
  let failedSource: Cause.Cause<E> | undefined;
  const monitored = source.pipe(
    Stream.catchCause((cause) => {
      if (!Cause.hasInterruptsOnly(cause)) failedSource = cause;
      return Stream.failCause(cause);
    }),
  );

  // Browser fetch refuses request streams on HTTP/1.x. A temporary OPFS File
  // keeps that transport and replay working without accumulating pack buffers.
  if (globalThis.navigator?.storage?.getDirectory !== undefined) {
    const directory = yield* Effect.tryPromise({
      try: () => navigator.storage.getDirectory(),
      catch: failed,
    });
    const handle = yield* ownFile(directory);
    const writer = yield* Effect.acquireRelease(
      Effect.tryPromise({ try: () => handle.createWritable(), catch: failed }),
      (writer) => Effect.promise(() => writer.abort().catch(() => undefined)),
    );
    yield* Stream.runForEach(monitored, (bytes) =>
      Effect.tryPromise({
        // SAFETY: pack and pkt-line encoders allocate ordinary ArrayBuffers.
        try: () => writer.write(bytes as Uint8Array<ArrayBuffer>),
        catch: failed,
      }),
    );
    yield* Effect.tryPromise({ try: () => writer.close(), catch: failed });
    const file = yield* Effect.tryPromise({ try: () => handle.getFile(), catch: failed });
    return {
      options: async (): Promise<RequestInit> => ({ body: file }),
      failure: () => failedSource,
    };
  }

  // Own the inner reader even while fetch locks the outer stream. That lets
  // scope cleanup interrupt pack generation after an early refusal or abort.
  const readers = new Set<ReadableStreamDefaultReader<Uint8Array>>();
  const close = async () => {
    for (const reader of readers) {
      await reader.cancel().catch(() => undefined);
      reader.releaseLock();
    }
    readers.clear();
  };
  yield* Effect.addFinalizer(() => Effect.promise(close));
  return {
    options: async (): Promise<RequestInit & { readonly duplex: "half" }> => {
      await close();
      failedSource = undefined;
      const reader = Stream.toReadableStream(monitored, {
        strategy: { highWaterMark: 0 },
      }).getReader();
      readers.add(reader);
      const body = new ReadableStream<Uint8Array>(
        {
          async pull(controller) {
            const next = await reader.read();
            if (next.done) {
              controller.close();
              readers.delete(reader);
              reader.releaseLock();
            } else controller.enqueue(next.value);
          },
          async cancel() {
            await reader.cancel().catch(() => undefined);
            readers.delete(reader);
            reader.releaseLock();
          },
        },
        { highWaterMark: 0 },
      );
      return { body, duplex: "half" };
    },
    failure: () => failedSource,
  };
});
