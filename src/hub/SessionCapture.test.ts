import assert from "node:assert/strict";
import { it } from "@effect/vitest";
import { Effect, Layer, Schema } from "effect";
import { formatPublicKey, generate } from "../crypto/SshSignature.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import * as Certificate from "../trust/Certificate.ts";
import * as Genesis from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import * as Capture from "./SessionCapture.ts";
import * as Session from "./Session.ts";
import * as Redaction from "./Redaction.ts";
import * as Event from "./Event.ts";

const world = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provideMerge(stores),
);
const fixture = Effect.fn("test.captureFixture")(function* () {
  const root = yield* generate("root@example.com");
  const agent = yield* generate("agent@example.com");
  const genesis = yield* Genesis.create([formatPublicKey(root.publicKey)], 1);
  yield* Genesis.writeGenesis(genesis, [yield* Genesis.signGenesis(genesis, root)]);
  yield* Log.issue(
    yield* Certificate.grant({
      repo: genesis.repoId,
      publicKey: formatPublicKey(agent.publicKey),
      capabilities: ["repo.read", "hub.session", "hub.redact"],
      id: Log.newId(),
    }),
    [root],
  );
  const { session } = yield* Session.open({
    repo: genesis.repoId,
    agent: { kind: "test", model: "", harness: "" },
    prompt: "work",
    key: agent,
  });
  const output: Capture.Output = {
    repo: genesis.repoId,
    session,
    key: agent,
    note: "gotcha: durable output",
    commits: [],
    refs: [],
    pulls: [],
    usage: null,
  };
  return { output, binding: { repo: genesis.repoId, session } };
});

it.effect("reconciles a retained handoff after append without duplicating the observation", () =>
  Effect.gen(function* () {
    const { output, binding } = yield* fixture();
    const prepared = yield* Capture.prepare(output);
    // The journal crosses JSON storage before an interrupted caller retries.
    const retained = yield* Schema.decodeEffect(Schema.fromJsonString(Capture.Prepared))(
      JSON.stringify(prepared),
    );
    const first = yield* Capture.append(prepared, binding);
    const second = yield* Capture.append(retained, binding);
    assert.equal(first.recovered, false);
    assert.equal(second.recovered, true);
    assert.equal(first.commit, second.commit);
    const records = (yield* Session.entries(binding.session)).events.filter(
      (event) => event.payload.type === "session.produced",
    );
    assert.equal(records.length, 1);
  }).pipe(Effect.provide(world)),
);

it.effect("keeps a prepared record's identity when another output arrives before publication", () =>
  Effect.gen(function* () {
    const { output, binding } = yield* fixture();
    const prepared = yield* Capture.prepare(output);
    yield* Session.produced({ ...output, note: "gotcha: intervening output" });
    const first = yield* Capture.append(prepared, binding);
    assert.equal(first.recovered, false);
    assert.equal((yield* Capture.append(prepared, binding)).recovered, true);
    const records = (yield* Session.entries(binding.session)).events.filter(
      (event) => event.payload.type === "session.produced",
    );
    assert.equal(records.length, 2);
    assert.ok(records.some((record) => record.commit === first.commit));
  }).pipe(Effect.provide(world)),
);

it.effect("rejects a handoff consumed by another session before changing refs", () =>
  Effect.gen(function* () {
    const { output, binding } = yield* fixture();
    const prepared = yield* Capture.prepare(output);
    const repository = yield* Repository;
    const before = yield* repository.refs;
    const error = yield* Capture.append(prepared, { ...binding, session: Session.newId() }).pipe(
      Effect.flip,
    );
    assert.equal(error._tag, "Invalid");
    assert.deepEqual(yield* repository.refs, before);
  }).pipe(Effect.provide(world)),
);

it.effect("rejects tampered retained bytes and leaves the session unchanged", () =>
  Effect.gen(function* () {
    const { output, binding } = yield* fixture();
    const prepared = yield* Capture.prepare(output);
    const before = (yield* Session.entries(binding.session)).head;
    const error = yield* Capture.append(
      { ...prepared, payload: prepared.payload.replace("durable output", "invented output") },
      binding,
    ).pipe(Effect.flip);
    assert.equal(error._tag, "Invalid");
    assert.equal((yield* Session.entries(binding.session)).head, before);
  }).pipe(Effect.provide(world)),
);

it.effect("recovering an accepted output writes no source objects", () =>
  Effect.gen(function* () {
    const { output, binding } = yield* fixture();
    const prepared = yield* Capture.prepare(output);
    yield* Capture.append(prepared, binding);
    const repository = yield* Repository;
    let writes = 0;
    const observed = Repository.of({
      ...repository,
      writeBlob: Effect.fn("test.observeRestoration")(function* (bytes: Uint8Array) {
        writes += 1;
        return yield* repository.writeBlob(bytes);
      }),
    });
    const retry = yield* Capture.append(prepared, binding).pipe(
      Effect.provideService(Repository, observed),
    );
    assert.equal(retry.recovered, true);
    assert.equal(
      writes,
      0,
      "recovery must not restore a redacted payload from local handoff bytes",
    );
  }).pipe(Effect.provide(world)),
);

it.effect("a retained handoff cannot restore a payload that redaction and collection removed", () =>
  Effect.gen(function* () {
    const { output, binding } = yield* fixture();
    const prepared = yield* Capture.prepare(output);
    const accepted = yield* Capture.append(prepared, binding);
    const payload = yield* Session.decode(new TextEncoder().encode(prepared.payload));
    const repository = yield* Repository;
    const source = yield* repository.readCommit(accepted.commit);
    const blob = yield* repository.findPath(source.tree, `${Event.RECORD}.json`);
    assert.ok(blob);
    yield* Session.redact({
      repo: binding.repo,
      session: binding.session,
      target: payload.id,
      reason: "remove private learning",
      key: output.key,
    });
    yield* repository.gc({ exclude: yield* Redaction.excluded() });
    assert.equal(yield* repository.contains(blob.oid), false);
    assert.equal((yield* Capture.append(prepared, binding)).recovered, true);
    assert.equal(
      yield* repository.contains(blob.oid),
      false,
      "recovery must not undo physical purge",
    );
  }).pipe(Effect.provide(world)),
);
