/** Capture and replay explicit repository inputs for deterministic maintenance. */
import { Effect, Schema } from "effect";
import { compareUtf8 } from "../text.ts";
import { Invalid } from "../git/Error.ts";
import { isOid, qualify, unqualify } from "../git/Oid.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import { checkRefName } from "../git/Store.ts";
import * as Pack from "../context/Pack.ts";
import * as Memory from "../hub/Memory.ts";
import * as Input from "../hub/MemoryInput.ts";
import * as Session from "../hub/Session.ts";
import * as Genesis from "../trust/Genesis.ts";
import * as Check from "./Check.ts";
import * as Concept from "./Concept.ts";

/** Reconstruct a reader over captured refs. Object operations still address immutable OIDs. */
export const reader = Effect.fn("knowledge.Inputs.reader")(function* (
  manifest: Input.InputManifest,
) {
  const repository = yield* Repository;
  const refs = new Map<string, Oid>();
  const pairs = [
    ...manifest.sessionHeads,
    ...manifest.coverage.continuation,
    ...manifest.noteHeads,
    ...manifest.outcomeHeads,
    ...manifest.proposalHeads,
    { ref: Genesis.GENESIS_REF, head: manifest.genesis },
    { ref: "refs/meta/trust/log", head: manifest.trustHead },
    { ref: "refs/meta/policy", head: manifest.policyHead },
    { ref: Memory.MEMORY_REF, head: manifest.expectedMemoryHead },
    { ref: manifest.sourceRef ?? "HEAD", head: manifest.sourceHead },
  ];
  for (const { ref, head } of pairs) {
    if (head === null) continue;
    const oid = unqualify(head);
    if (oid === null)
      return yield* new Invalid({
        field: "manifest",
        reason: `invalid captured object identity for ${ref}`,
      });
    refs.set(ref, oid);
  }
  const sourceRef = manifest.sourceRef ?? "HEAD";
  return Repository.of({
    ...repository,
    refs: Effect.succeed([...refs]),
    head: Effect.succeed(sourceRef),
    readRef: (ref) => Effect.succeed(refs.get(ref) ?? null),
    resolve: (ref) =>
      Effect.succeed(isOid(ref) ? ref : (refs.get(ref === "HEAD" ? sourceRef : ref) ?? null)),
  });
});

export interface Capture {
  readonly sourceRef: string;
  readonly evaluatedAt: string;
  readonly limits: { readonly concepts: number; readonly sessions: number; readonly bytes: number };
  readonly configuration: Schema.Json;
}

export const capture = Effect.fn("knowledge.Inputs.capture")(function* (input: Capture) {
  const repository = yield* Repository;
  if (!input.sourceRef.startsWith("refs/heads/") || checkRefName(input.sourceRef) !== null)
    return yield* new Invalid({
      field: "ref",
      reason: "maintenance requires a full committed source branch ref",
    });
  const instant = Concept.instantOf(input.evaluatedAt);
  if (instant.state !== "instant")
    return yield* new Invalid({ field: "at", reason: instant.reason });
  // Destination expectation precedes source discovery, even though most runs only report.
  const expected = yield* repository.readRef(Memory.MEMORY_REF);
  const refs = [...(yield* repository.refs)].sort(([left], [right]) => compareUtf8(left, right));
  const heads = new Map(refs);
  const source = heads.get(input.sourceRef);
  if (source === undefined)
    return yield* new Invalid({ field: "ref", reason: `no committed ${input.sourceRef}` });
  const headOf = (ref: string) => {
    const oid = heads.get(ref);
    return oid === undefined ? null : qualify(oid);
  };
  const pairs = (prefix: string) =>
    refs
      .filter(([ref]) => ref.startsWith(prefix))
      .map(([ref, head]) => ({ ref, head: qualify(head) }));
  const manifest = {
    version: 1,
    repository: null,
    view: yield* Pack.committed(source),
    sourceRef: input.sourceRef,
    sourceHead: qualify(source),
    sessionHeads: refs
      .filter(([ref]) => Session.sessionOf(ref) !== null)
      .map(([ref, head]) => ({ ref, head: qualify(head) })),
    noteHeads: pairs("refs/hub/note/"),
    outcomeHeads: pairs("refs/hub/pr/"),
    proposalHeads: pairs("refs/heads/agent-memory/"),
    genesis: headOf(Genesis.GENESIS_REF),
    trustHead: headOf("refs/meta/trust/log"),
    policyHead: headOf("refs/meta/policy"),
    evaluatedAt: new Date(instant.at).toISOString(),
    versions: {
      parser: Concept.PARSER_VERSION,
      checker: Check.CHECKER_VERSION,
      projection: Memory.PROJECTION_VERSION,
      selection: "knowledge-maintenance/1",
    },
    configurationDigest: yield* Input.digest(input.configuration),
    limits: input.limits,
    expectedMemoryHead: expected === null ? null : qualify(expected),
    coverage: { complete: true, omittedSessions: 0, continuation: [] },
  } satisfies Input.InputManifest;
  const pinned = yield* reader(manifest);
  const genesis = yield* Genesis.readGenesis().pipe(Effect.provideService(Repository, pinned));
  return { ...manifest, repository: genesis?.genesis.repoId ?? null } satisfies Input.InputManifest;
});

/** Observed changes are supersession, never a relabeling of previously checked bytes. */
export const current = Effect.fn("knowledge.Inputs.current")(function* (
  manifest: Input.InputManifest,
  ownSessionRef?: string,
) {
  const repository = yield* Repository;
  const refs = new Map(yield* repository.refs);
  const expected = new Map<string, string | null>([
    ...manifest.sessionHeads.map(({ ref, head }) => [ref, head] as const),
    ...manifest.coverage.continuation.map(({ ref, head }) => [ref, head] as const),
    ...manifest.noteHeads.map(({ ref, head }) => [ref, head] as const),
    ...manifest.outcomeHeads.map(({ ref, head }) => [ref, head] as const),
    [Genesis.GENESIS_REF, manifest.genesis],
    ["refs/meta/trust/log", manifest.trustHead],
    ["refs/meta/policy", manifest.policyHead],
    [manifest.sourceRef ?? "HEAD", manifest.sourceHead],
  ]);
  for (const [ref, head] of expected) {
    if (ref === ownSessionRef) continue;
    const actual = refs.get(ref);
    if ((actual === undefined ? null : qualify(actual)) !== head) return false;
  }
  for (const ref of refs.keys()) {
    if (ref === ownSessionRef) continue;
    if (
      (Session.sessionOf(ref) !== null ||
        ref.startsWith("refs/hub/note/") ||
        ref.startsWith("refs/hub/pr/")) &&
      !expected.has(ref)
    )
      return false;
  }
  return true;
});
