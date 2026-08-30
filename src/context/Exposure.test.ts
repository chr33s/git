/**
 * What a recorded exposure proves, and what it stops proving when something
 * moves underneath it.
 *
 * The two claims worth testing are the ones that are quiet failures otherwise.
 * A record whose `context/view` edge is missing verifies perfectly until the
 * first `gc`, and then never again — so the edge is asserted as a real tree
 * entry rather than as a string in JSON (docs/context-pack.md §10). And a
 * retained render that has since expired is not a failed audit; it is an
 * absent one, which §11 asks to be reported as its own state.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { generate, type PrivateKey } from "../crypto/SshSignature.ts";
import { type Signature } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { indexMemory, IndexStore, workTreeMemory, WorkTree } from "../git/Work.ts";
import * as Checkout from "../git/Checkout.ts";
import * as Event from "../hub/Event.ts";
import * as Trace from "../hub/Trace.ts";
import * as Exposure from "./Exposure.ts";
import * as Pack from "./Pack.ts";
import * as Render from "./Render.ts";
import * as Select from "./Select.ts";

const REPO = "SHA256:test";
const SESSION = "0192f000-0000-7000-8000-000000000000";

const author: Signature = {
  name: "Runner",
  email: "runner@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const encode = (text: string) => new TextEncoder().encode(text);

const world = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provideMerge(stores),
  Layer.provideMerge(indexMemory),
  Layer.provideMerge(workTreeMemory),
);

const scenario = <A, E>(
  effect: Effect.Effect<A, E, Repository | WorkTree | IndexStore>,
): Promise<A> => Effect.runPromise(effect.pipe(Effect.provide(world)));

interface World {
  readonly key: PrivateKey;
  readonly view: Pack.View;
  readonly pack: Pack.Pack;
  readonly segments: ReadonlyArray<Render.Segment>;
}

/** A dirty checkout, so the view under test is an overlay nothing else holds. */
const opened = Effect.fn("test.opened")(function* () {
  const key = yield* generate("runner@example.com");
  const work = yield* WorkTree;

  yield* work.write("src/auth.ts", encode("export const authorize = () => true\n"), 0o100644);
  yield* work.write("AGENTS.md", encode("Do the authorize work carefully.\n"), 0o100644);
  yield* Checkout.add(["."]);
  const made = yield* Checkout.commit({ message: "first\n", author });

  // Modified after the commit: `view.tree` is now a tree no commit reaches.
  yield* work.write("src/auth.ts", encode("export const authorize = () => false\n"), 0o100644);
  const view = yield* Pack.capture(made.oid);
  const pack = yield* Select.select({ task: "authorize policy", view });
  const segments = yield* Select.render(pack, "authorize policy");
  return { key, view, pack, segments } satisfies World;
});

describe("Context Exposure", () => {
  it.effect("keeps view.tree reachable through a real Git edge", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const world = yield* opened();
          const exposed = yield* Exposure.expose({
            repo: REPO,
            session: SESSION,
            key: world.key,
            pack: world.pack,
            segments: world.segments,
          });

          const info = yield* repository.readCommit(exposed.commit);
          const entry = yield* repository.findPath(info.tree, Exposure.VIEW);
          assert.notEqual(entry, null);
          // The edge, not the string: the tree entry's own oid is view.tree,
          // which is what makes the object reachable and so survivable.
          assert.equal(Pack.qualify(entry!.oid), world.view.tree);
          assert.equal(entry!.mode, "040000");

          // Reachability is a property of the graph, so `gc` is the check that
          // actually settles it — the overlay tree is referenced by no commit.
          yield* repository.gc();
          const after = yield* repository.readObject(entry!.oid);
          assert.equal(after.type, "tree");
        }),
      ),
    ),
  );

  it.effect("verifies every dimension of a record it just wrote", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const world = yield* opened();
          const exposed = yield* Exposure.expose({
            repo: REPO,
            session: SESSION,
            key: world.key,
            pack: world.pack,
            segments: world.segments,
            capture: { transport: "otel", traceId: "4bf92f3577b34da6a3ce929d0e0e4736" },
          });

          const audit = yield* Exposure.audit({
            commit: exposed.commit,
            repo: REPO,
            session: SESSION,
          });
          assert.equal(audit.signature.ok, true);
          assert.equal(audit.signers.length, 1);
          assert.equal(audit.binding.ok, true);
          assert.equal(audit.pack.ok, true);
          assert.equal(audit.retained.ok, true);
          assert.equal(audit.evidence?.ok, true);
          assert.equal(audit.render.state, "verified");
          assert.equal(audit.capture?.traceId, "4bf92f3577b34da6a3ce929d0e0e4736");
          assert.equal(audit.ok, true);
          assert.equal(audit.exposure, Exposure.identify(exposed.commit));
        }),
      ),
    ),
  );

  it.effect("reports an unretained render as absent, not as a failure", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const world = yield* opened();
          const exposed = yield* Exposure.expose({
            repo: REPO,
            session: SESSION,
            key: world.key,
            pack: world.pack,
            segments: world.segments,
            retain: false,
          });

          const audit = yield* Exposure.audit({
            commit: exposed.commit,
            repo: REPO,
            session: SESSION,
          });
          // The commitment stands; the body is gone. Those are two different
          // facts, and the audit still passes.
          assert.equal(audit.render.state, "absent");
          assert.equal(audit.payload?.renderDigest, exposed.digest);
          assert.equal(audit.ok, true);
        }),
      ),
    ),
  );

  it.effect("refuses a record whose repository or session is not the one asked about", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const world = yield* opened();
          const exposed = yield* Exposure.expose({
            repo: REPO,
            session: SESSION,
            key: world.key,
            pack: world.pack,
            segments: world.segments,
          });

          const elsewhere = yield* Exposure.audit({
            commit: exposed.commit,
            repo: "SHA256:somebody-else",
            session: SESSION,
          });
          assert.equal(elsewhere.binding.ok, false);
          assert.equal(elsewhere.ok, false);
        }),
      ),
    ),
  );

  it.effect("notices when the retained render is not the bytes committed to", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const world = yield* opened();
          const exposed = yield* Exposure.expose({
            repo: REPO,
            session: SESSION,
            key: world.key,
            pack: world.pack,
            segments: world.segments,
          });

          // A second record whose payload claims the first one's digest while
          // retaining a different render: the digest is what catches it.
          const other = yield* Render.commit([
            { placement: "user", mediaType: "text/plain", body: encode("something else") },
          ]);
          const info = yield* repository.readCommit(exposed.commit);
          const packEntry = yield* repository.findPath(info.tree, Exposure.PACK);
          const viewEntry = yield* repository.findPath(info.tree, Exposure.VIEW);
          const swapped = yield* repository.writeFiles({
            base: info.tree,
            changes: [{ path: Exposure.RENDER, content: other.bytes }],
          });
          assert.notEqual(packEntry, null);
          assert.notEqual(viewEntry, null);
          const tampered = yield* repository.commitTree({
            tree: swapped,
            parents: [],
            message: "context-exposure tampered\n",
            author,
          });

          const audit = yield* Exposure.audit({
            commit: tampered,
            repo: REPO,
            session: SESSION,
          });
          assert.equal(audit.render.state, "unreadable");
          assert.match(audit.render.state === "unreadable" ? audit.render.reason : "", /hashes to/);
          assert.equal(audit.ok, false);
        }),
      ),
    ),
  );

  it.effect("lands on the trace ref, not on the session ref", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const world = yield* opened();
          const exposed = yield* Exposure.expose({
            repo: REPO,
            session: SESSION,
            key: world.key,
            pack: world.pack,
            segments: world.segments,
          });

          // §3: exposures live under the policy-invisible trace namespace. A
          // session ref that grew one would put audit volume into the fold
          // every protected-branch push runs.
          assert.equal(yield* repository.resolve(Trace.refOf(SESSION)), exposed.commit);
          assert.equal(yield* repository.resolve(`refs/hub/session/${SESSION}`), null);
          assert.deepEqual(yield* Trace.traces(), [SESSION]);

          // And it is an ordinary hub commit, so the append-only machinery
          // reads it the way it reads every other one.
          assert.equal(yield* Event.isHubCommit(exposed.commit), true);

          const walked = yield* Exposure.entries(SESSION);
          assert.equal(walked.exposures.length, 1);
          assert.equal(walked.exposures[0]?.payload.type, "context-exposure");
        }),
      ),
    ),
  );

  it.effect("chains a second exposure onto the first", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const world = yield* opened();
          const first = yield* Exposure.expose({
            repo: REPO,
            session: SESSION,
            key: world.key,
            pack: world.pack,
            segments: world.segments,
          });
          const second = yield* Exposure.expose({
            repo: REPO,
            session: SESSION,
            key: world.key,
            pack: world.pack,
            segments: [
              ...world.segments,
              { placement: "user", mediaType: "text/plain", body: encode("and again") },
            ],
          });

          // Different renders, so different commitments — §15's fifth
          // criterion, seen from the record rather than from the framing.
          assert.notEqual(first.digest, second.digest);
          const walked = yield* Exposure.entries(SESSION);
          assert.deepEqual(
            walked.exposures.map((exposure) => exposure.commit),
            [first.commit, second.commit],
          );
        }),
      ),
    ),
  );
});
