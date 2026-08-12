import { describe, it } from "node:test";

import { Effect } from "effect";

import { stores } from "./Memory.ts";
import { storeContract } from "./Store.contract.ts";

storeContract(
  "Memory",
  {
    // A fresh layer per call: `Layer.effect` builds new maps each time it is
    // provided, so nothing leaks between tests.
    run: (effect) => Effect.runPromise(effect.pipe(Effect.provide(stores)) as Effect.Effect<never>),
  },
  { describe, it },
);
