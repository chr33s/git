/**
 * Browser (OPFS) and Node (fs) stores.
 *
 * Today these are `OpfsStorage` (`src/client.storage.ts`) and `NodeStorage`
 * (`src/cli.storage.ts`), two more implementations of the same 16-method
 * filesystem interface — including `applyRefChanges`, which neither provides,
 * so the client hand-rolls read-then-write ref updates
 * (`client.ts#writeRefIfUnchanged`) and races itself across tabs.
 *
 * Sketch: same three ports as the server. OPFS gets real atomicity from
 * `createSyncAccessHandle` locks; Node gets it from `rename(2)`. The layers are
 * the only thing that changes between environments — nothing above them knows
 * which one is loaded.
 */
import { Effect, Layer } from "effect";
import { FileSystem, Path } from "effect";
import { IndexStore, ObjectStore, RefStore } from "../git/Store.ts";

/** Browser: OPFS directories under `<repo>/objects`, refs in one file per ref. */
export declare const opfs: (repo: string) => Layer.Layer<ObjectStore | RefStore | IndexStore>;

/**
 * Node: built on the platform `FileSystem`/`Path` services rather than direct
 * `node:fs` imports, so CLI tests run against an in-memory filesystem layer and
 * stop needing a temp directory per test (`test.helpers.ts` today).
 */
export const node = (root: string): Layer.Layer<ObjectStore, never, FileSystem.FileSystem | Path.Path> =>
  Layer.effect(
    ObjectStore,
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      return makeNodeObjectStore(fs, path, root);
    }),
  );

declare const makeNodeObjectStore: (
  fs: FileSystem.FileSystem,
  path: Path.Path,
  root: string,
) => ObjectStore["Service"];

/**
 * In-memory, for tests. Today the equivalent is `MemoryStorage` in
 * `git.storage.ts`, reachable only by passing it into a constructor by hand;
 * here it is a layer swap at the edge of a test, and everything under it —
 * including `Repository` and the HTTP handlers — is unchanged.
 */
export declare const memory: Layer.Layer<ObjectStore | RefStore | IndexStore>;
