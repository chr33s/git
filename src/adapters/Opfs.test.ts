/**
 * The OPFS backend against the same storage contract as every other backend.
 *
 * Node has no OPFS, so the tests run over an in-memory fake of the
 * `FileSystemDirectoryHandle` surface the adapter actually uses — handle
 * traversal, `createWritable`, `removeEntry`, entry iteration. What the fake
 * does not fake is the contract: the suite is the same one DO SQLite and the
 * real filesystem pass.
 */
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

import { storeContract } from "../git/Store.contract.ts";
import { fakeRoot } from "./Opfs.fake.ts";
import { stores } from "./Opfs.ts";

storeContract(
  "OPFS",
  {
    // A fresh in-memory origin per test, like a fresh browser profile.
    run: (effect) => Effect.runPromise(effect.pipe(Effect.provide(stores(fakeRoot())))),
  },
  { describe, it },
);
