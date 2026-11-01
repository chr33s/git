/** Bounded, verified identity material for operator-controlled provisioning. */
import { Effect, Layer, Schema, Stream } from "effect";
import { Invalid } from "../git/Error.ts";
import { concatBytes } from "../git/Format.ts";
import * as Memory from "../git/Memory.ts";
import { ImportBudget } from "../git/Pack.ts";
import * as Repository from "../git/Repository.ts";
import { isOid, RefStore } from "../git/Store.ts";
import * as TraversalBudget from "../git/TraversalBudget.ts";
import { permits } from "../trust/Certificate.ts";
import { GENESIS_REF, readGenesis } from "../trust/Genesis.ts";
import { LOG_REF } from "../trust/Log.ts";
import { project } from "../trust/Projection.ts";
import { routeOf } from "./Route.ts";

export const MAX_BYTES = 2 * 1024 * 1024;
export const MAX_PACK_BYTES = 1024 * 1024;
export const importLimits = { objects: 4096, expandedBytes: 8 * 1024 * 1024 };

/** Reserved outside all HTTP-addressable repository namespaces. */
export const keyOf = (repo: string): string => `_operator/provisioning/v1/${repo}.json`;

export const Artifact = Schema.Struct({
  version: Schema.Literal(1),
  repo: Schema.String,
  repoId: Schema.String,
  genesis: Schema.String,
  trust: Schema.String,
  pack: Schema.Uint8ArrayFromBase64,
});
export interface Artifact extends Schema.Schema.Type<typeof Artifact> {}

const invalid = (reason: string) => new Invalid({ field: "provisioning", reason });
const canonicalName = (name: string) => routeOf(`/${name}/refs`)?.repo === name;
const isolated = Repository.layer.pipe(
  Layer.provide(Repository.hooksNoop),
  Layer.provideMerge(Memory.stores),
);

/** Verification never writes the destination repository. */
export const validate = Effect.fn("Provisioning.validate")(function* (artifact: Artifact) {
  if (!canonicalName(artifact.repo)) return yield* invalid("expected a canonical repository name");
  const { genesis, trust } = artifact;
  if (!isOid(genesis) || !isOid(trust)) return yield* invalid("invalid identity ref");
  if (artifact.pack.byteLength > MAX_PACK_BYTES)
    return yield* invalid("identity pack is too large");
  return yield* Effect.gen(function* () {
    const repository = yield* Repository.Repository;
    const refs = yield* RefStore;
    yield* repository.unpack(Stream.make(artifact.pack));
    const applied = yield* refs.apply(
      [
        { name: GENESIS_REF, expected: null, value: genesis },
        { name: LOG_REF, expected: null, value: trust },
      ],
      { atomic: true },
    );
    if (!applied.every((result) => result.applied))
      return yield* invalid("identity refs were not installed");
    // Check the complete closure even when genesis/signature caches are warm.
    const closure = yield* repository.fetch({ wants: [genesis, trust], haves: [] });
    // Fetch planning does not open leaf blobs. Reading each planned object also
    // catches omitted record blobs when signature caches already know the tips.
    yield* Effect.forEach(closure.oids, (oid) => repository.readObject(oid), { discard: true });
    const stored = yield* readGenesis();
    if (stored === null || stored.genesis.repoId !== artifact.repoId)
      return yield* invalid("repository identity does not match the artifact");
    const projection = yield* project(stored.genesis);
    if (projection.rejected.length > 0)
      return yield* invalid("trust log contains rejected records");
    const now = new Date();
    const administrator = [...projection.members.values()].some(
      (member) =>
        permits(member.capabilities, "repo.admin") &&
        member.grantedAt <= now &&
        (member.expiresAt === null || member.expiresAt > now),
    );
    if (!administrator) return yield* invalid("identity needs a currently valid administrator key");
    return artifact;
  }).pipe(
    Effect.provide(Layer.fresh(isolated)),
    Effect.provideService(ImportBudget, importLimits),
    Effect.provideService(TraversalBudget.TraversalBudget, TraversalBudget.make(20_000)),
  );
});

export const decode = Effect.fn("Provisioning.decode")(function* (bytes: Uint8Array, repo: string) {
  if (bytes.byteLength > MAX_BYTES) return yield* invalid("artifact is too large");
  const artifact = yield* Schema.decodeEffect(Schema.fromJsonString(Artifact))(
    new TextDecoder().decode(bytes),
  ).pipe(Effect.mapError(() => invalid("malformed artifact")));
  if (artifact.repo !== repo)
    return yield* invalid("artifact belongs to a different repository name");
  return yield* validate(artifact);
});

/** Export the identity/trust closure, without unrelated refs or filesystem key files. */
export const prepare = Effect.fn("Provisioning.prepare")(function* (repo: string) {
  if (!canonicalName(repo)) return yield* invalid("expected a canonical repository name");
  const repository = yield* Repository.Repository;
  const stored = yield* readGenesis();
  const trust = yield* repository.resolve(LOG_REF);
  if (stored === null || trust === null)
    return yield* invalid("initialize identity and membership first");
  const chunks: Uint8Array[] = [];
  let size = 0;
  yield* repository.packOf([stored.commit, trust], []).pipe(
    Stream.runForEach((chunk) => {
      size += chunk.byteLength;
      if (size > MAX_PACK_BYTES) return Effect.fail(invalid("identity pack is too large"));
      chunks.push(chunk);
      return Effect.void;
    }),
    Effect.provideService(TraversalBudget.TraversalBudget, TraversalBudget.make(20_000)),
  );
  const artifact = yield* validate({
    version: 1,
    repo,
    repoId: stored.genesis.repoId,
    genesis: stored.commit,
    trust,
    pack: concatBytes(chunks),
  });
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Artifact))(artifact);
  const bytes = new TextEncoder().encode(encoded);
  if (bytes.byteLength > MAX_BYTES) return yield* invalid("artifact is too large");
  return bytes;
});
