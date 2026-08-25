import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

import { fileLayer } from "./Bundles.node.ts";
import { bundleStoreContract } from "./BundleStore.contract.ts";
import { memoryLayer } from "./BundleStore.ts";

const runner = { describe, it };

bundleStoreContract(
  "Memory",
  {
    run: (effect) => Effect.runPromise(effect.pipe(Effect.provide(memoryLayer))),
  },
  runner,
);

bundleStoreContract(
  "Node",
  {
    run: async (effect) => {
      const directory = await mkdtemp(join(tmpdir(), "bundles-node-case-"));
      try {
        return await Effect.runPromise(effect.pipe(Effect.provide(fileLayer(directory))));
      } finally {
        await rm(directory, { recursive: true, force: true });
      }
    },
  },
  runner,
);
