/**
 * Local stores: browser (OPFS), node (fs), and in-memory.
 *
 * The same three ports as the server. OPFS gets real atomicity from
 * `createSyncAccessHandle` locks; Node gets it from `rename(2)`. The layers are
 * the only thing that changes between environments — nothing above them knows
 * which one is loaded.
 */
import { Effect, Layer } from "effect";
import { FileSystem, Path } from "effect";
import {
  IndexStore,
  ObjectStore,
  RefStore,
  type ServerStores,
  type Stores,
} from "../git/Store.sketch.ts";

/** Browser: OPFS directories under `<repo>/objects`, refs in one file per ref. */
export declare const opfs: (repo: string) => Layer.Layer<Stores>;

/**
 * Node: built on the platform `FileSystem`/`Path` services rather than direct
 * `node:fs` imports, so CLI tests run against an in-memory filesystem layer and
 * never need a temp directory per test.
 */
export const node = (
  root: string,
): Layer.Layer<ServerStores, never, FileSystem.FileSystem | Path.Path> =>
  Layer.mergeAll(
    Layer.effect(
      ObjectStore,
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        return makeNodeObjectStore(fs, path, root);
      }),
    ),
    Layer.effect(
      RefStore,
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        // Atomic ref update is `write temp + rename(2)` per ref, under the
        // host's per-repo lock for the batch. The port makes this mandatory.
        return makeNodeRefStore(fs, path, root);
      }),
    ),
  );

declare const makeNodeObjectStore: (
  fs: FileSystem.FileSystem,
  path: Path.Path,
  root: string,
) => ObjectStore["Service"];
declare const makeNodeRefStore: (
  fs: FileSystem.FileSystem,
  path: Path.Path,
  root: string,
) => RefStore["Service"];

/**
 * In-memory, for tests: a layer swap at the edge of a test, and everything
 * under it — including `Repository` and the HTTP handlers — is unchanged.
 */
export declare const memory: Layer.Layer<Stores>;
