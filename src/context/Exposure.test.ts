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
import { hashObject, type Signature } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { indexMemory, IndexStore, workTreeMemory, WorkTree } from "../git/Work.ts";
import * as Checkout from "../git/Checkout.ts";
import * as Event from "../hub/Event.ts";
import * as Trace from "../hub/Trace.ts";
import * as Records from "../telemetry/Records.ts";
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
          // The mode git itself writes; the zero-padded spelling is one its
          // own `fsck` reports as `zeroPaddedFilemode`.
          assert.equal(entry!.mode, "40000");

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

  it.effect("scans every string a caller hands in, not a list of two", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const world = yield* opened();
          const token = `ghp_${"A".repeat(36)}`;

          // `telemetry/Records.prose` was rewritten to walk for this reason —
          // its list was wrong four times over — and this side stayed a list
          // of two, so the identical `capture.traceId` was refused on an
          // `invocation-telemetry` record and signed on a `context-exposure`.
          for (const capture of [
            { transport: "otlp", traceId: token },
            { transport: "otlp", spanId: token },
            { transport: "otlp", semconv: { profile: token, revision: "1.37.0" } },
          ]) {
            const refused = yield* Exposure.expose({
              repo: REPO,
              session: SESSION,
              key: world.key,
              pack: world.pack,
              segments: world.segments,
              capture,
            }).pipe(Effect.flip);
            assert.equal(refused._tag, "Invalid", JSON.stringify(capture));
          }

          // And the pack's own descriptive strings, which go into
          // `context/pack.json` on the same ref — named as such, because one
          // message reading "this task looks like…" for a finding somewhere
          // else is an operator rewording a prompt that was never the problem.
          // The omission reasons and the selector's own name, which were
          // simply missing from the enumerated half.
          for (const pack of [
            { ...world.pack, omissions: [{ reason: `filtered: ${token}` }] },
            { ...world.pack, selector: { name: token, version: "1" } },
          ]) {
            const refused = yield* Exposure.expose({
              repo: REPO,
              session: SESSION,
              key: world.key,
              pack,
              segments: world.segments,
            }).pipe(Effect.flip);
            assert.equal(refused._tag, "Invalid", JSON.stringify(pack.omissions ?? pack.selector));
          }

          const described = yield* Exposure.expose({
            repo: REPO,
            session: SESSION,
            key: world.key,
            pack: {
              ...world.pack,
              items: world.pack.items.map((item) => ({ ...item, symbol: token })),
            },
            segments: world.segments,
          }).pipe(Effect.flip);
          assert.equal(described._tag, "Invalid");
          assert.equal(described.field, "pack.symbol");

          // But not the repository's own values. A `path` is what the selector
          // chose, not what anybody typed, and the dense-string rule matches
          // across a slash — so a content-hashed asset path refused the
          // exposure and blamed the operator's prompt for a file name they
          // cannot reword.
          const hashed = `assets/${"a3F9kL2mQ8xR7bN4vC1zY6wE5tU0iO9p"}.js`;
          yield* Exposure.expose({
            repo: REPO,
            session: SESSION,
            key: world.key,
            pack: {
              ...world.pack,
              items: world.pack.items.map((item) => ({ ...item, path: hashed })),
            },
            segments: world.segments,
          });
        }),
      ),
    ),
  );

  it.effect("scans the capture's transport as well as the task", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const world = yield* opened();
          // Both are strings a caller hands in, both land verbatim on an
          // append-only ref that replicates, and only the task was looked at
          // — while `telemetry/Records` scans the identical field on its own
          // records.
          const refused = yield* Exposure.expose({
            repo: REPO,
            session: SESSION,
            key: world.key,
            pack: world.pack,
            segments: world.segments,
            capture: { transport: `otlp ghp_${"A".repeat(36)}` },
          }).pipe(Effect.flip);
          assert.equal(refused._tag, "Invalid");
        }),
      ),
    ),
  );

  it.effect("refuses a hand-built pack its own reader would reject", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const world = yield* opened();
          // `expose` is a library entry point, and the comment above the pack
          // gate claimed the pack was "held to exactly what `Pack.decode` will
          // accept" while running two of its four checks. `decode` also
          // refuses a cross-kind raw field, and `encode`'s key list emits one
          // faithfully — so this got signed onto the append-only trace ref,
          // after which every audit of it reported no readable pack, forever.
          const refused = yield* Exposure.expose({
            repo: REPO,
            session: SESSION,
            key: world.key,
            pack: {
              ...world.pack,
              // SAFETY: deliberately not an `Item` — the point is that a
              // library caller can hand `expose` one of these, and that
              // `Pack.encode` writes the extra field faithfully.
              items: [
                {
                  kind: "blob",
                  path: "src/auth.ts",
                  blob: `sha1:${"0".repeat(40)}`,
                  commit: `sha1:${"1".repeat(40)}`,
                } as never,
              ],
            },
            segments: world.segments,
          }).pipe(Effect.flip);
          assert.equal(refused._tag, "Invalid");
        }),
      ),
    ),
  );

  it.effect("refuses an oversized payload before it writes anything", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const world = yield* opened();
          const before = yield* repository.resolve(Trace.refOf(SESSION));

          // `capture.transport` is an unbounded string, and `Trace.append`
          // asks about payload size *last* — so this used to write the pack
          // blob, the render blob and the `context/` tree and then refuse,
          // leaving exactly the orphaned objects every other hoisted check in
          // `expose` exists to prevent.
          const refused = yield* Exposure.expose({
            repo: REPO,
            session: SESSION,
            key: world.key,
            pack: world.pack,
            segments: world.segments,
            capture: { transport: "x".repeat(600 * 1024) },
          }).pipe(Effect.flip);
          assert.equal(refused._tag, "Invalid");
          assert.equal(yield* repository.resolve(Trace.refOf(SESSION)), before);

          // And the objects, which is where the defect actually shows: the ref
          // never moves either way, because `Trace.append` is what refused.
          const packOid = yield* hashObject({ type: "blob", data: Pack.encode(world.pack) });
          assert.equal(
            yield* repository.readBlob(packOid).pipe(
              Effect.as(true),
              Effect.catchTag("ObjectNotFound", () => Effect.succeed(false)),
            ),
            false,
          );
        }),
      ),
    ),
  );

  it.effect("does not audit an exposure that names another session", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const world = yield* opened();
          const mine = yield* Exposure.expose({
            repo: REPO,
            session: SESSION,
            key: world.key,
            pack: world.pack,
            segments: world.segments,
          });

          // An exposure for a different session, moved onto this ref — which
          // replication does not gate. Audited as this session's it failed
          // `binding` and drove the non-zero exit of `context audit`, so a
          // peer could plant one and fail this repository's deploy gate for
          // good.
          const other = "0192f000-0000-7000-8000-0000000000ee";
          const theirs = yield* Exposure.expose({
            repo: REPO,
            session: other,
            key: world.key,
            pack: world.pack,
            segments: [
              ...world.segments,
              { placement: "user", mediaType: "text/plain", body: encode("elsewhere") },
            ],
          });
          const info = yield* repository.readCommit(theirs.commit);
          const grafted = yield* repository.commitTree({
            tree: info.tree,
            parents: [mine.commit],
            message: info.message,
            author: info.author,
          });
          yield* repository.setRef({ name: Trace.refOf(SESSION), to: grafted });

          const walked = yield* Exposure.entries(SESSION);
          assert.deepEqual(
            walked.exposures.map((exposure) => exposure.commit),
            [mine.commit],
          );
          // Reported, not dropped: it is on the ref, and an audit surface that
          // silently discards records is the other half of the same problem.
          assert.deepEqual(walked.foreign, [grafted]);

          // And the repository is the other half of the binding. Bound to the
          // session alone, an exposure naming another repo was audited as this
          // one's, failed `binding`, and drove the non-zero exit — so a peer
          // could plant one, and a `hub init` that mints a new `repoId` did the
          // same thing benignly.
          const elsewhere = yield* Exposure.entries(SESSION, undefined, "SHA256:somewhere-else");
          assert.deepEqual(elsewhere.exposures, []);
          assert.equal(elsewhere.foreign.includes(mine.commit), true);
        }),
      ),
    ),
  );

  it.effect("leaves no record invisible to both readers", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const world = yield* opened();
          const made = yield* Exposure.expose({
            repo: REPO,
            session: SESSION,
            key: world.key,
            pack: world.pack,
            segments: world.segments,
          });

          // A payload declaring a telemetry kind and malformed for it, under a
          // commit message naming this namespace. `Exposure.entries` skips it
          // on the payload's claim; `Records.entries` decided on the *unsigned*
          // message and skipped it too — so it was in neither `audits` nor
          // `unreadable`, `context audit S` exited 0, and `session show
          // --audit` reported it in no section at all.
          const broken = new TextEncoder().encode(
            JSON.stringify({ type: "tool-operation", version: 1, repo: REPO, session: SESSION }),
          );
          const orphan = yield* Trace.append({
            session: SESSION,
            type: Exposure.TYPE,
            id: "0192f000-0000-7000-8000-00000000dddd",
            payload: broken,
            key: world.key,
          });

          const seen = yield* Exposure.entries(SESSION);
          assert.deepEqual(
            seen.exposures.map((exposure) => exposure.commit),
            [made.commit],
          );
          const read = yield* Records.entries(SESSION);
          assert.equal(read.unreadable.includes(orphan), true);

          // And a payload that names no kind at all, under this namespace's
          // own label. `declaredType` answers `null` for anything that is not
          // JSON, is not an object, or has no `type` — so the guard above took
          // neither branch: not damage here, not this namespace's over in
          // `Records.entries`, and readable enough that `Trace.walk` left it
          // out of `unreadable`. It was in no section of any audit, and
          // `context audit S && deploy` deployed past it.
          const nonsense = yield* Trace.append({
            session: SESSION,
            type: Exposure.TYPE,
            id: "0192f000-0000-7000-8000-00000000cccc",
            payload: new TextEncoder().encode(JSON.stringify({ foo: 1 })),
            key: world.key,
          });
          const after = yield* Exposure.entries(SESSION);
          assert.equal(after.unreadable.includes(nonsense), true);

          // And one that names no kind anywhere — nothing in the payload, and
          // an empty commit message. The fallback closed the gap only when one
          // of the two named something; with neither, this was claimed by no
          // reader, left out of `unreadable` because its payload reads fine,
          // and appeared in no section of any audit.
          const mute = yield* Trace.append({
            session: SESSION,
            type: "",
            id: "",
            payload: new TextEncoder().encode(JSON.stringify({ foo: 1 })),
            key: world.key,
          });
          const claimed = yield* Records.entries(SESSION);
          assert.equal(claimed.unreadable.includes(mute), true);
        }),
      ),
    ),
  );

  it.effect("does not call another namespace's record a damaged exposure", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const world = yield* opened();
          const made = yield* Exposure.expose({
            repo: REPO,
            session: SESSION,
            key: world.key,
            pack: world.pack,
            segments: world.segments,
          });

          // A valid telemetry payload under a `context-exposure` message. The
          // mirrored case is handled in `Records.entries`; without the
          // symmetry here the same commit rendered as a full Runtime row *and*
          // failed `context audit` with "could not be read", so
          // `git+ context audit S && deploy` stopped deploying permanently
          // over a record that reads perfectly well one namespace over.
          const telemetry = new TextEncoder().encode(
            JSON.stringify({
              type: "invocation-telemetry",
              version: 1,
              repo: REPO,
              session: SESSION,
              id: "0192f000-0000-7000-8000-00000000eeee",
              issuedAt: "2026-01-01T00:00:00.000Z",
              trustHead: null,
              exposure: null,
              capture: null,
              operation: { name: "chat" },
            }),
          );
          yield* Trace.append({
            session: SESSION,
            type: Exposure.TYPE,
            id: "0192f000-0000-7000-8000-00000000eeee",
            payload: telemetry,
            key: world.key,
          });

          const walked = yield* Exposure.entries(SESSION);
          assert.deepEqual(
            walked.exposures.map((exposure) => exposure.commit),
            [made.commit],
          );
          assert.deepEqual(walked.unreadable, []);
        }),
      ),
    ),
  );

  it.effect("audits a record whose commit message calls it something else", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const world = yield* opened();
          const made = yield* Exposure.expose({
            repo: REPO,
            session: SESSION,
            key: world.key,
            pack: world.pack,
            segments: world.segments,
          });

          // The same signed payload, appended again under a message that calls
          // it a tool operation. The message is unsigned — it is a hint that
          // survives redaction, nothing more — so a `hub.trace` holder chooses
          // it freely.
          const info = yield* repository.readCommit(made.commit);
          const record = yield* repository.findPath(info.tree, "event.json");
          const bytes = yield* repository.readBlob(record!.oid);
          const mislabelled = yield* Trace.append({
            session: SESSION,
            type: "tool-operation",
            id: "0192f000-0000-7000-8000-00000000ffff",
            payload: bytes,
            key: world.key,
          });

          // Selected on the message alone, this record was in neither
          // `exposures` nor `unreadable`: `context audit` enumerated nothing
          // for it and exited 0, so a deploy gated on that command walked past
          // an exposure nobody had looked at.
          const walked = yield* Exposure.entries(SESSION);
          assert.deepEqual(
            walked.exposures.map((exposure) => exposure.commit),
            [made.commit, mislabelled],
          );
        }),
      ),
    ),
  );
});
