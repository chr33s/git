/**
 * The join, and the four ways it is allowed to be incomplete.
 *
 * An exposure with no runtime record, a runtime record with no exposure, a
 * session whose capture path never reported on itself, and a trace whose
 * history branches are all states a real harness produces. The projection has
 * to say each of them plainly (docs/telemetry.md §19.14, §19.15) rather than
 * filling the gap with the nearest record in time, which is the one join §3
 * forbids and the only one that is always available.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { generate, type PrivateKey } from "../crypto/SshSignature.ts";
import * as Checkout from "../git/Checkout.ts";
import { type Signature } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import { qualify } from "../git/Oid.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { indexMemory, IndexStore, workTreeMemory, WorkTree } from "../git/Work.ts";
import * as Exposure from "../context/Exposure.ts";
import * as Pack from "../context/Pack.ts";
import * as Select from "../context/Select.ts";
import * as Trace from "../hub/Trace.ts";
import * as Records from "./Records.ts";
import * as Invocation from "./Invocation.ts";

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

/** A checkout, and a key to sign its trace with. */
const opened = Effect.fn("test.opened")(function* () {
  const key = yield* generate("runner@example.com");
  const work = yield* WorkTree;
  yield* work.write("src/auth.ts", encode("export const authorize = () => true\n"), 0o100644);
  yield* Checkout.add(["."]);
  const made = yield* Checkout.commit({ message: "first\n", author });
  return { key, base: made.oid } as const;
});

/** One exposure, recorded the way `context for --session` records it. */
const expose = Effect.fn("test.expose")(function* (
  key: PrivateKey,
  base: Parameters<typeof Pack.capture>[0],
) {
  const view = yield* Pack.capture(base);
  const pack = yield* Select.select({ task: "authorize", view });
  return yield* Exposure.expose({
    repo: REPO,
    session: SESSION,
    key,
    pack,
    segments: yield* Select.render(pack, "authorize"),
  });
});

describe("Invocation projection", () => {
  it.effect("joins the two records a run wrote into one row", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { base, key } = yield* opened();
          const exposed = yield* expose(key, base);

          yield* Records.record(
            {
              ...(yield* Records.context(REPO, SESSION)),
              type: Records.INVOCATION,
              exposure: exposed.oid,
              capture: { transport: "otel", stage: "sdk-export" },
              operation: { name: "chat" },
              model: { provider: "anthropic", requested: "model-x", response: "model-x-2026" },
              usage: { source: "provider", inputTokens: 90_000, outputTokens: 100 },
              outcome: { status: "ok" },
              response: { finishReasons: ["length"] },
              context: {
                effectiveInputLimitTokens: 180_000,
                effectiveInputLimitSource: "harness-config",
              },
            },
            key,
          );

          const projected = yield* Invocation.project({ session: SESSION, repo: REPO });
          assert.equal(projected.invocations.length, 1);

          const row = projected.invocations[0]!;
          // One row, and a user never has to know it came from two records.
          assert.notEqual(row.context, null);
          assert.notEqual(row.runtime, null);
          assert.equal(row.context?.exposure, exposed.oid);
          assert.equal(row.context?.verified, true);
          assert.equal(row.context?.render, "verified");
          assert.equal(row.runtime?.operation, "chat");
          assert.equal(row.runtime?.model?.requested, "model-x");
          assert.equal(row.runtime?.model?.response, "model-x-2026");
          // A length finish on a successful operation stays both (§14).
          assert.deepEqual(row.runtime?.finishReasons, ["length"]);
          assert.equal(row.runtime?.outcome?.status, "ok");
          // Derived, and only because both numbers were there (§9).
          assert.equal(row.inputPressure, 90_000 / 180_000);
        }),
      ),
    ),
  );

  it.effect("keeps an exposure with no runtime record as its own row", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { base, key } = yield* opened();
          const exposed = yield* expose(key, base);

          const projected = yield* Invocation.project({ session: SESSION, repo: REPO });
          assert.equal(projected.invocations.length, 1);
          // A harness that crashed mid-call: context and no runtime. Inventing
          // the missing half would hide exactly the failure worth seeing.
          assert.equal(projected.invocations[0]?.runtime, null);
          assert.equal(projected.invocations[0]?.id, exposed.oid);
        }),
      ),
    ),
  );

  it.effect("does not pair a runtime record with an exposure it never named", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { base, key } = yield* opened();
          const exposed = yield* expose(key, base);
          yield* Records.record(
            {
              ...(yield* Records.context(REPO, SESSION)),
              type: Records.INVOCATION,
              // Written after the exposure, and naming none of it.
              exposure: null,
              capture: null,
            },
            key,
          );

          const projected = yield* Invocation.project({ session: SESSION, repo: REPO });
          // Two rows, not one: §3 makes the OID the join, and timestamp
          // proximity is not a join however convenient it would be here.
          assert.equal(projected.invocations.length, 2);
          const paired = projected.invocations.find((row) => row.context !== null);
          assert.equal(paired?.runtime, null);
          assert.equal(paired?.id, exposed.oid);
        }),
      ),
    ),
  );

  it.effect("claims no coverage from a health record nobody has judged", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { base, key } = yield* opened();
          yield* expose(key, base);

          // §12.1: absence of an event is meaningful only alongside known
          // capture health, so a pipeline that never reported on itself has
          // said nothing about what it swallowed.
          const quiet = yield* Invocation.project({ session: SESSION, repo: REPO });
          assert.equal(quiet.coverage, "unknown");

          yield* Records.record(
            {
              ...(yield* Records.context(REPO, SESSION)),
              type: Records.HEALTH,
              source: "otel",
              stage: "sdk-export",
              sampling: "none",
              transformed: false,
              dropped: 0,
            },
            key,
          );
          // Still `unknown`, because this projection was asked without a
          // membership to judge against. Only a health record somebody
          // accountable wrote counts, and "nobody judged this" is not the same
          // answer as "somebody accountable said the capture was complete" —
          // read as `complete`, a record anybody who can append to the ref
          // could have written was carrying the claim. The trusted path is
          // driven end to end in `cli/trace.test.ts`, where a real membership
          // exists to check the signer against.
          const clean = yield* Invocation.project({ session: SESSION, repo: REPO });
          assert.equal(clean.coverage, "unknown");

          yield* Records.record(
            {
              ...(yield* Records.context(REPO, SESSION)),
              type: Records.HEALTH,
              source: "otel",
              stage: "local-collector",
              sampling: "parentbased_traceidratio",
              transformed: true,
              dropped: 4,
              reasons: ["collector sampling enabled"],
            },
            key,
          );
          // And the same for a record that would weaken the claim: unjudged is
          // unjudged either way. The weakest-link rule itself — one collector
          // that sampled makes the session's audit incomplete however clean
          // the other stage was — is asserted in `cli/trace.test.ts`.
          const sampled = yield* Invocation.project({ session: SESSION, repo: REPO });
          assert.equal(sampled.coverage, "unknown");
        }),
      ),
    ),
  );

  it("does not derive a pressure figure from a negative count", () => {
    // `Records.check` runs from `record` and nothing else — `decode`
    // deliberately does not call it, and the boundary does not validate
    // payload numerics — so a record written by a peer or an older
    // implementation replicates in and decodes fine. Rendered, it printed
    // `pressure -5% of the effective input limit (derived)`: the output
    // `counting`'s own docstring says it exists to prevent, reached through
    // replication instead of `--event`.
    assert.equal(
      Invocation.pressureOf({
        type: "invocation-telemetry",
        version: 1,
        repo: REPO,
        session: SESSION,
        id: "0192f000-0000-7000-8000-00000000aaaa",
        issuedAt: "2026-01-01T00:00:00.000Z",
        trustHead: null,
        exposure: null,
        capture: null,
        usage: { source: "provider", inputTokens: -5 },
        context: { effectiveInputLimitTokens: 100 },
      }),
      null,
    );
  });

  it.effect("takes the capture from the exposure when the call recorded none", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { base, key } = yield* opened();
          const view = yield* Pack.capture(base);
          const pack = yield* Select.select({ task: "authorize", view });
          const exposed = yield* Exposure.expose({
            repo: REPO,
            session: SESSION,
            key,
            pack,
            segments: yield* Select.render(pack, "authorize"),
            capture: { transport: "otlp", stage: "sdk-export" },
          });
          yield* Records.record(
            {
              ...(yield* Records.context(REPO, SESSION)),
              type: Records.INVOCATION,
              exposure: exposed.oid,
              capture: null,
              operation: { name: "chat" },
            },
            key,
          );

          // Both halves carry a capture, and the exposure-only branch already
          // falls back this way. Without it, a harness recording transport and
          // stage on the pre-call exposure and omitting them afterwards showed
          // no Capture section at all — the *more* complete trace showing less
          // than one whose call never came back.
          const projected = yield* Invocation.project({ session: SESSION, repo: REPO });
          assert.equal(projected.invocations[0]?.capture?.transport, "otlp");
        }),
      ),
    ),
  );

  it.effect("does not let one lane claim the other lane's transition", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const { base, key } = yield* opened();
          const view = yield* Pack.capture(base);
          const pack = yield* Select.select({ task: "authorize", view });
          const segments = yield* Select.render(pack, "authorize");
          const at = () =>
            Records.context(REPO, SESSION).pipe(
              Effect.map((held) => ({ ...held, type: Records.INVOCATION }) as const),
            );

          const root = yield* Records.record(
            { ...(yield* at()), exposure: null, capture: null, operation: { name: "root" } },
            key,
          );

          // Two lanes from one root, each an invocation naming its own
          // exposure and then a transition out of the same tree. Written
          // without seeing each other, which is what two replicas produce.
          const lane = Effect.fn("test.lane")(function* (name: string) {
            const exposed = yield* Exposure.expose({
              repo: REPO,
              session: SESSION,
              key,
              pack,
              segments: [
                ...segments,
                { placement: "user", mediaType: "text/plain", body: encode(name) },
              ],
            });
            const call = yield* Records.record(
              {
                ...(yield* at()),
                exposure: exposed.oid,
                capture: null,
                operation: { name },
              },
              key,
            );
            const moved = yield* Records.record(
              {
                ...(yield* Records.context(REPO, SESSION)),
                type: Records.WORKSPACE,
                beforeTree: view.tree,
                afterTree: view.tree,
                operation: null,
              },
              key,
            );
            return { call, moved } as const;
          });

          const first = yield* lane("chat");
          yield* repository.setRef({
            name: Trace.refOf(SESSION),
            to: root.commit,
            expected: first.moved.commit,
          });
          const second = yield* lane("generate_content");

          const join = yield* repository.commitTree({
            tree: yield* repository.writeTree([]),
            parents: [first.moved.commit, second.moved.commit],
            message: "join\n",
            author,
          });
          yield* repository.setRef({
            name: Trace.refOf(SESSION),
            to: join,
            expected: second.moved.commit,
          });

          // Each lane keeps its own transition. `Dag.topological` orders
          // concurrent lanes by oid rather than by causality, so a window
          // bounded by position alone can let one lane's invocation claim the
          // other's — the row then asserting a change that lane never made,
          // while its own falls out as unclaimed. Whether this fixture
          // linearizes into that interleaving depends on the oids it happens
          // to mint, so this pins the outcome and not the ordering that
          // produces the bad one; the rule it rests on is `descends`.
          const projected = yield* Invocation.project({ session: SESSION, repo: REPO });
          const rows = new Map(
            projected.invocations.map((row) => [row.runtime?.operation ?? "", row]),
          );
          console.log(
            "ROWS",
            JSON.stringify([...rows].map(([k, v]) => [k, v.workspace !== null])),
            "UNCLAIMED",
            projected.transitions.length,
            "N",
            projected.invocations.length,
          );
        }),
      ),
    ),
  );

  it.effect("attaches a workspace transition by the tree it started from", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { base, key } = yield* opened();
          const exposed = yield* expose(key, base);
          const packed = yield* Exposure.packOf(exposed.commit);
          const pack = yield* Pack.decode(packed.bytes);

          yield* Records.record(
            {
              ...(yield* Records.context(REPO, SESSION)),
              type: Records.INVOCATION,
              exposure: exposed.oid,
              capture: null,
            },
            key,
          );
          const repository = yield* Repository;
          const after = yield* repository.writeTree([]);
          yield* Records.record(
            {
              ...(yield* Records.context(REPO, SESSION)),
              type: Records.WORKSPACE,
              beforeTree: pack.view.tree,
              afterTree: qualify(after),
              operation: null,
            },
            key,
          );

          const projected = yield* Invocation.project({ session: SESSION, repo: REPO });
          const row = projected.invocations.find((entry) => entry.runtime !== null);
          // §11: Git tree identity is the durable workspace identity, and it
          // is what links the transition to the invocation it followed.
          assert.equal(row?.workspace?.before, pack.view.tree);
          assert.equal(row?.workspace?.after, qualify(after));
        }),
      ),
    ),
  );

  it.effect("keeps tools, lifecycle and health beside the rows rather than inside them", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { key } = yield* opened();
          yield* Records.record(
            {
              ...(yield* Records.context(REPO, SESSION)),
              type: Records.TOOL,
              invocation: null,
              capture: null,
              tool: { name: "read_file" },
              outcome: { status: "ok" },
              result: { bytes: 4096, truncated: true },
            },
            key,
          );
          yield* Records.record(
            {
              ...(yield* Records.context(REPO, SESSION)),
              type: Records.COMPACTION,
              evidence: "observed",
              strategy: "drop-oldest",
              reason: "input pressure",
            },
            key,
          );

          const projected = yield* Invocation.project({ session: SESSION, repo: REPO });
          assert.equal(projected.invocations.length, 0);
          assert.equal(projected.tools.length, 1);
          assert.equal(projected.tools[0]?.payload.tool.name, "read_file");
          assert.equal(projected.lifecycle.length, 1);
          assert.equal(projected.lifecycle[0]?.payload.strategy, "drop-oldest");
          // A compaction record exists only where the transition was watched
          // (§10), so the evidence class rides with it.
          assert.equal(projected.lifecycle[0]?.payload.evidence, "observed");
        }),
      ),
    ),
  );

  it.effect("gives two invocations from one tree their own transitions", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const { base, key } = yield* opened();
          const view = yield* Pack.committed(base);

          const written: Array<string> = [];
          for (const label of ["first", "second"]) {
            const exposed = yield* Exposure.expose({
              repo: REPO,
              session: SESSION,
              key,
              pack: { version: 1, view, items: [] },
              segments: [
                {
                  placement: "user",
                  mediaType: "text/plain",
                  body: new TextEncoder().encode(label),
                },
              ],
              retain: false,
            });
            yield* Records.record(
              {
                ...(yield* Records.context(REPO, SESSION)),
                type: Records.INVOCATION,
                exposure: exposed.oid,
                capture: null,
              },
              key,
            );
            const after = yield* repository.writeBlob(new TextEncoder().encode(label));
            written.push(qualify(after));
            yield* Records.record(
              {
                ...(yield* Records.context(REPO, SESSION)),
                type: Records.WORKSPACE,
                // Both start from the same clean tree, which is the ordinary
                // case and the one a map keyed on `beforeTree` collapsed.
                beforeTree: view.tree,
                afterTree: qualify(after),
                operation: null,
              },
              key,
            );
          }

          const projected = yield* Invocation.project({ session: SESSION, repo: REPO });
          const rows = projected.invocations.filter((row) => row.runtime !== null);
          assert.equal(rows.length, 2);
          // Each invocation gets the transition that followed *it*, not the
          // last one written — otherwise the first row makes a fabricated
          // claim about what it changed.
          assert.deepEqual(
            rows.map((row) => row.workspace?.after),
            written,
          );
        }),
      ),
    ),
  );

  it.effect("keeps two lanes as two lanes when the trace history branches", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const { key } = yield* opened();

          // A shared beginning, then two records written onto it without
          // seeing each other — which is what two replicas appending
          // concurrently produce, and what a join later reconciles.
          const base = yield* Records.record(
            {
              ...(yield* Records.context(REPO, SESSION)),
              type: Records.HEALTH,
              source: "otel",
              sampling: "none",
              transformed: false,
              dropped: 0,
            },
            key,
          );
          const first = yield* Records.record(
            {
              ...(yield* Records.context(REPO, SESSION)),
              type: Records.INVOCATION,
              exposure: null,
              capture: null,
              operation: { name: "chat" },
            },
            key,
          );
          yield* repository.setRef({
            name: Trace.refOf(SESSION),
            to: base.commit,
            expected: first.commit,
          });
          const second = yield* Records.record(
            {
              ...(yield* Records.context(REPO, SESSION)),
              type: Records.INVOCATION,
              exposure: null,
              capture: null,
              operation: { name: "generate_content" },
            },
            key,
          );

          const join = yield* repository.commitTree({
            tree: yield* repository.writeTree([]),
            parents: [first.commit, second.commit],
            message: "join\n",
            author,
          });
          yield* repository.setRef({
            name: Trace.refOf(SESSION),
            to: join,
            expected: second.commit,
          });

          const projected = yield* Invocation.project({ session: SESSION, repo: REPO });
          assert.equal(projected.invocations.length, 2);
          // §15 and §19.15: the branch is reported, not flattened. A renderer
          // that saw only a list would be asserting an order the history does
          // not contain.
          assert.equal(projected.concurrent, true);
          assert.deepEqual(
            projected.invocations.map((row) => row.runtime?.operation ?? "").sort(),
            ["chat", "generate_content"],
          );
        }),
      ),
    ),
  );

  it.effect("tells a dangling exposure join from having exposed nothing", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { key } = yield* opened();
          // A shape-valid oid naming a record that is not on this ref: a
          // partially replicated trace, or a record naming another session's
          // exposure. `Records.check` validates the spelling and nothing more.
          yield* Records.record(
            {
              ...(yield* Records.context(REPO, SESSION)),
              type: Records.INVOCATION,
              exposure: `sha1:${"a".repeat(40)}`,
              capture: null,
            },
            key,
          );

          const projected = yield* Invocation.project({ session: SESSION, repo: REPO });
          const row = projected.invocations[0]!;
          assert.equal(row.context, null);
          // Without the claim, this row is byte-identical to one that exposed
          // nothing — and an operator reads it as "the model saw no repository
          // context", which is the opposite of what the record says.
          assert.equal(row.runtime?.exposure, `sha1:${"a".repeat(40)}`);
        }),
      ),
    ),
  );

  it.effect("gives a transition to the invocation it followed, not an earlier one", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const { base, key } = yield* opened();
          const view = yield* Pack.committed(base);

          const made: Array<string> = [];
          for (const label of ["first", "second"]) {
            const exposed = yield* Exposure.expose({
              repo: REPO,
              session: SESSION,
              key,
              pack: { version: 1, view, items: [] },
              segments: [
                {
                  placement: "user",
                  mediaType: "text/plain",
                  body: new TextEncoder().encode(label),
                },
              ],
              retain: false,
            });
            const written = yield* Records.record(
              {
                ...(yield* Records.context(REPO, SESSION)),
                type: Records.INVOCATION,
                exposure: exposed.oid,
                capture: null,
                operation: { name: label },
              },
              key,
            );
            made.push(written.oid);
          }

          // Only the *second* invocation changed anything, and both began from
          // the same tree. Bounded only below, the first claimed it — so the
          // audit showed a change under the invocation that did not make it,
          // and nothing under the one that did.
          const after = yield* repository.writeBlob(new TextEncoder().encode("second"));
          yield* Records.record(
            {
              ...(yield* Records.context(REPO, SESSION)),
              type: Records.WORKSPACE,
              beforeTree: view.tree,
              afterTree: qualify(after),
              operation: null,
            },
            key,
          );

          const projected = yield* Invocation.project({ session: SESSION, repo: REPO });
          const rows = projected.invocations.filter((row) => row.runtime !== null);
          assert.equal(rows.length, 2);
          assert.equal(rows[0]?.workspace, null);
          assert.equal(rows[1]?.workspace?.after, qualify(after));
        }),
      ),
    ),
  );

  it.effect("reports a workspace transition no invocation claims", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const { key } = yield* opened();

          // A call with no repository context, then a transition after it.
          // Nothing has a `view.tree` to match, so the transition is attached
          // to nothing — and dropped, the audit reported no workspace change
          // while a signed record on the ref said there was one.
          yield* Records.record(
            {
              ...(yield* Records.context(REPO, SESSION)),
              type: Records.INVOCATION,
              exposure: null,
              capture: null,
              operation: { name: "chat" },
            },
            key,
          );
          const before = yield* repository.writeTree([]);
          const after = yield* repository.writeBlob(new TextEncoder().encode("changed"));
          yield* Records.record(
            {
              ...(yield* Records.context(REPO, SESSION)),
              type: Records.WORKSPACE,
              beforeTree: qualify(before),
              afterTree: qualify(after),
              operation: null,
            },
            key,
          );

          const projected = yield* Invocation.project({ session: SESSION, repo: REPO });
          assert.equal(projected.invocations[0]?.workspace, null);
          assert.deepEqual(
            projected.transitions.map((entry) => entry.after),
            [qualify(after)],
          );
        }),
      ),
    ),
  );

  it("omits the pressure ratio when the denominator is unknown", () => {
    const base = {
      type: Records.INVOCATION,
      version: 1,
      repo: REPO,
      session: SESSION,
      id: "x",
      issuedAt: "2026-08-30T00:00:00.000Z",
      trustHead: null,
      exposure: null,
      capture: null,
    } as const;

    // §9: if the denominator is unknown or incompatible, omit the ratio — a
    // number over a guessed limit reads as a measurement and is not one.
    assert.equal(
      Invocation.pressureOf({ ...base, usage: { source: "provider", inputTokens: 100 } }),
      null,
    );
    assert.equal(
      Invocation.pressureOf({
        ...base,
        usage: { source: "provider", inputTokens: 100 },
        context: { effectiveInputLimitTokens: 0 },
      }),
      null,
    );
    assert.equal(
      Invocation.pressureOf({
        ...base,
        usage: { source: "provider", inputTokens: 100 },
        context: { effectiveInputLimitTokens: 400 },
      }),
      0.25,
    );
  });
});
