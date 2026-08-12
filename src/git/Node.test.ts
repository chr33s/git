/**
 * The filesystem backend against the same contract as the in-memory one.
 *
 * Two backends, one suite — and with `Cloudflare.integration.test.ts`, three.
 */
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

import { stores } from "./Node.ts";
import { storeContract } from "./Store.contract.ts";

storeContract(
  "Node",
  {
    run: async (effect) => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-store-"));
      try {
        return await Effect.runPromise(
          effect.pipe(Effect.provide(stores(root))) as Effect.Effect<never>,
        );
      } finally {
        await fs.rm(root, { force: true, recursive: true });
      }
    },
  },
  { describe, it },
);
