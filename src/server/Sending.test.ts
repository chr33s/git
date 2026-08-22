/**
 * The standing instruction: a remote configured to be sent to gets sent to.
 *
 * Two halves, and the second is the one that matters. What gets forwarded is a
 * question about patterns and can be answered without a network. Whether a
 * failure to forward can hurt the push that caused it is a question about
 * ordering, and §25 answers it: it cannot.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer, Logger } from "effect";

import { concatBytes, EMPTY_TREE_OID, type Signature } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import { FLUSH, pkt } from "../git/Pkt.ts";
import * as GitRepository from "../git/Repository.ts";
import { Hooks, Repository } from "../git/Repository.ts";
import { of as fixedRemotes, type Remote } from "./Remotes.ts";
import { covered, hooks } from "./Sending.ts";

const author: Signature = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const remote = (sync: Remote["sync"]): Remote => ({
  name: "mirror",
  url: "http://127.0.0.1:1/nothing",
  credential: null,
  key: null,
  sync,
  createdAt: new Date(0),
});

const results = [
  { ref: "refs/heads/main", from: null, to: EMPTY_TREE_OID, ok: true },
  { ref: "refs/heads/topic", from: null, to: EMPTY_TREE_OID, ok: true },
  { ref: "refs/tags/v1", from: null, to: EMPTY_TREE_OID, ok: true },
  { ref: "refs/heads/refused", from: null, to: EMPTY_TREE_OID, ok: false },
] as const;

const named = (carried: ReadonlyArray<{ readonly ref: string }>) =>
  carried.map((result) => result.ref);

describe("what a standing instruction carries", () => {
  it.effect("takes everything the mode covers when no patterns are named", () =>
    Effect.sync(() => {
      // `{mode: "push"}` has to mean what it looks like it means. Read as "no
      // patterns, so nothing matches", a remote configured the obvious way would
      // sit there silently forwarding nothing.
      assert.deepEqual(named(covered(remote({ mode: "push", refs: [] }), results)), [
        "refs/heads/main",
        "refs/heads/topic",
        "refs/tags/v1",
      ]);
    }),
  );

  it.effect("never carries a session ref by default, and carries it when named", () =>
    Effect.sync(() => {
      // Sessions hold the prompts an agent was given, which is the most
      // leak-prone thing this repository stores. "Everything" configured once —
      // a mirror, a backup, a fork — would put them somewhere nobody chose to
      // put them, and a forge that knows nothing of hub refs would then serve
      // them to whoever can read that repository.
      const withSession = [
        ...results,
        {
          ref: "refs/hub/session/0198f2aa-71c4-7d2e-9a3b-4c5d6e7f8a9b",
          from: null,
          to: EMPTY_TREE_OID,
          ok: true,
        },
      ] as const;

      assert.deepEqual(
        named(covered(remote({ mode: "mirror", refs: [] }), withSession)),
        ["refs/heads/main", "refs/heads/topic", "refs/tags/v1"],
        "a mirror of everything is still not a mirror of the prompts",
      );

      // Named, it goes: that is what configuring a provenance remote *is*.
      assert.deepEqual(
        named(covered(remote({ mode: "push", refs: ["refs/hub/session/*"] }), withSession)),
        ["refs/hub/session/0198f2aa-71c4-7d2e-9a3b-4c5d6e7f8a9b"],
      );
    }),
  );

  it.effect("takes only what the patterns name, and only what the push applied", () =>
    Effect.sync(() => {
      assert.deepEqual(
        named(covered(remote({ mode: "mirror", refs: ["refs/heads/*"] }), results)),
        ["refs/heads/main", "refs/heads/topic"],
      );
      assert.deepEqual(
        named(covered(remote({ mode: "push", refs: ["refs/heads/main"] }), results)),
        ["refs/heads/main"],
        "an exact name is exact",
      );
      // A command the receive refused never happened; forwarding it would tell
      // the other side something this repository does not hold.
      assert.equal(
        named(covered(remote({ mode: "push", refs: [] }), results)).includes("refs/heads/refused"),
        false,
      );
    }),
  );

  it.effect("says which refs a remote refused, rather than reporting nothing", () =>
    Effect.promise(async () => {
      // A receive-pack that answers at all answers `200`, and the refusals ride
      // inside the response — so a mirror rejecting every ref looked exactly
      // like one keeping up, and the only symptom was a downstream that was
      // quietly months behind. The forward still cannot fail the push that
      // caused it; what it can do is name the ref and the reason.
      const logged: Array<string> = [];
      const capture = Logger.make<unknown, void>(({ message }) => {
        logged.push(String(message));
      });

      // The remote advertises a `main` this repository has never heard of, which
      // is a divergence `push` refuses on its own — no second request, and no
      // pack to build. The refusal is the point, not how it was reached.
      const elsewhere = "b".repeat(40);
      const advertisement = concatBytes([
        pkt("# service=git-receive-pack\n"),
        FLUSH,
        pkt(`${elsewhere} refs/heads/main\0report-status\n`),
        FLUSH,
      ]);
      const original = globalThis.fetch;
      globalThis.fetch = () =>
        Promise.resolve(
          new Response(advertisement, {
            headers: { "content-type": "application/x-git-upload-pack-advertisement" },
          }),
        );

      try {
        await Effect.runPromise(
          Effect.gen(function* () {
            const repository = yield* Repository;
            const hook = yield* Hooks;
            const commit = yield* repository.commitTree({
              tree: EMPTY_TREE_OID,
              parents: [],
              message: "mine\n",
              author,
            });
            yield* repository.setRef({ name: "refs/heads/main", to: commit, expected: null });
            yield* hook.postReceive([{ ref: "refs/heads/main", from: null, to: commit, ok: true }]);
          }).pipe(
            Effect.provide(
              hooks({
                background: (effect) => effect.pipe(Effect.asVoid, Effect.ignoreCause),
              }).pipe(
                Layer.provide(
                  fixedRemotes([
                    {
                      name: "mirror",
                      url: "http://127.0.0.1:1/nothing",
                      sync: { mode: "mirror", refs: [] },
                    },
                  ]),
                ),
                Layer.provideMerge(
                  GitRepository.layer.pipe(
                    Layer.provide(GitRepository.hooksNoop),
                    Layer.provideMerge(stores),
                  ),
                ),
                Layer.merge(Logger.layer([capture])),
              ),
            ),
          ),
        );
      } finally {
        globalThis.fetch = original;
      }

      const warned = logged.find((line) => line.includes("mirror"));
      assert.notEqual(
        warned,
        undefined,
        `nothing said the mirror refused anything: ${logged.join(" | ")}`,
      );
      assert.match(warned ?? "", /refs\/heads\/main/);
      assert.match(warned ?? "", /non-fast-forward/);
    }),
  );

  it.effect("does not roll back the push when the remote is unreachable", () =>
    Effect.promise(async () => {
      // §25 is explicit: replication failure MUST NOT roll back the originating
      // write. The URL here refuses connections, so this is the real failure and
      // not a simulated one — and it has to end with the ref moved and the hook
      // having said nothing to the caller.
      const applied = await Effect.runPromise(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const hook = yield* Hooks;
          const commit = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [],
            message: "first\n",
            author,
          });
          yield* repository.setRef({ name: "refs/heads/main", to: commit, expected: null });

          // Awaited rather than forked, so the failure has actually happened by
          // the time the assertion runs.
          yield* hook.postReceive([{ ref: "refs/heads/main", from: null, to: commit, ok: true }]);

          return yield* repository.resolve("refs/heads/main");
        }).pipe(
          Effect.provide(
            hooks({
              background: (effect) => effect.pipe(Effect.asVoid, Effect.ignoreCause),
            }).pipe(
              Layer.provide(
                fixedRemotes([
                  {
                    name: "mirror",
                    url: "http://127.0.0.1:1/nothing",
                    sync: { mode: "mirror", refs: [] },
                  },
                ]),
              ),
              Layer.provideMerge(
                GitRepository.layer.pipe(
                  Layer.provide(GitRepository.hooksNoop),
                  Layer.provideMerge(stores),
                ),
              ),
            ),
          ),
        ),
      );

      assert.notEqual(applied, null, "the ref this push moved is still moved");
    }),
  );
});
