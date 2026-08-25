/**
 * Bundle list, publication, and HTTP / protocol integration.
 *
 * Capture refs, generate, verify, store, then publish the manifest — in that
 * order, so a client never observes an entry pointing at incomplete bytes.
 */
import { Context, Effect, Layer, Option, Stream } from "effect";

import { anonymousReadAllowed } from "./Auth.ts";
import { project as projectTrust } from "../trust/Projection.ts";
import { readGenesis } from "../trust/Genesis.ts";
import { BundleCorrupt, Invalid, type ObjectNotFound, StorageFailure } from "../git/Error.ts";
import { hiddenFromAdvertisement } from "../git/Refspec.ts";
import { Repository } from "../git/Repository.ts";
import { isOid, ObjectStore, type Oid, storageOf } from "../git/Store.ts";
import * as Artifact from "./Artifact.ts";
import * as BundleBuilder from "./BundleBuilder.ts";
import {
  advertisedIds,
  artifactPath,
  type BundleArtifact,
  type BundleFilter,
  type BundleKind,
  type BundleManifest,
  type BundleSnapshot,
  catchupArtifacts,
  cloneArtifacts,
  decodeManifest,
  encodeConfigList,
  encodeProtocolList,
  familyState,
  artifactId,
  latestToken,
  listEntries,
  nextToken,
  objectIdFromPath,
  objectIdOf,
  publishArtifact,
} from "./BundleFormat.ts";
import { BundleStore } from "./BundleStore.ts";
import { defaults, ServerFeatures } from "./Features.ts";
import type { OperationHandle } from "./Operations.ts";

export interface BundleFamilySummary {
  readonly filter: BundleFilter;
  readonly full: string | null;
  readonly incrementals: number;
  readonly latestCreationToken: string | null;
}

export interface BundlesSummary {
  readonly enabled: boolean;
  readonly families: ReadonlyArray<BundleFamilySummary>;
}

export class Bundles extends Context.Service<
  Bundles,
  {
    readonly enabled: Effect.Effect<boolean>;
    readonly snapshot: (
      filter: BundleFilter,
    ) => Effect.Effect<BundleSnapshot, StorageFailure | Invalid>;
    readonly manifest: Effect.Effect<BundleManifest | null, StorageFailure>;
    readonly summary: Effect.Effect<BundlesSummary, StorageFailure>;
    readonly build: (input: {
      readonly kind: BundleKind;
      readonly filter: BundleFilter;
      readonly operation?: OperationHandle;
    }) => Effect.Effect<BundleArtifact, StorageFailure | Invalid | BundleCorrupt | ObjectNotFound>;
    readonly protocolLines: (
      kind: "clone" | "catchup",
      baseUrl: string,
    ) => Effect.Effect<ReadonlyArray<string>, StorageFailure>;
    readonly configText: (
      kind: "clone" | "catchup",
      baseUrl: string,
    ) => Effect.Effect<string, StorageFailure>;
    readonly handle: (request: Request) => Effect.Effect<Response | null, StorageFailure>;
  }
>()("server/Bundles") {}

const featuresOf = Effect.fnUntraced(function* () {
  return Option.getOrElse(yield* Effect.serviceOption(ServerFeatures), () => defaults);
});

const repoName = Effect.fnUntraced(function* () {
  const named = yield* storageOf();
  return named ?? "repository";
});

const advertisedRefs = Effect.fn("Bundles.advertisedRefs")(function* () {
  const repository = yield* Repository;
  const listed = yield* repository.refs;
  const refs: Record<string, Oid> = {};
  for (const [name, oid] of listed) {
    if (hiddenFromAdvertisement(name) || !isOid(oid)) continue;
    refs[name] = oid;
  }
  return refs;
});

const cacheMode = (): Effect.Effect<"public" | "private", never, Repository> =>
  readGenesis().pipe(
    Effect.orElseSucceed(() => null),
    Effect.flatMap((stored) => {
      if (stored === null) return Effect.succeed<"public">("public");
      return projectTrust(stored.genesis).pipe(
        Effect.orElseSucceed(() => null),
        Effect.map((projection) =>
          projection !== null && anonymousReadAllowed(projection) ? "public" : "private",
        ),
      );
    }),
  );

const uriOf = (baseUrl: string, artifact: BundleArtifact): string => {
  const root = baseUrl.replace(/\/+$/, "");
  return `${root}/bundles/${artifact.objectId}`;
};

const entriesFor = (
  manifest: BundleManifest | null,
  kind: "clone" | "catchup",
  baseUrl: string,
) => {
  const artifacts = [];
  if (manifest !== null) {
    for (const family of manifest.families) {
      const listed = kind === "clone" ? cloneArtifacts(family) : catchupArtifacts(family);
      for (const artifact of listed) artifacts.push(artifact);
    }
  }
  return listEntries(artifacts, (artifact) => uriOf(baseUrl, artifact));
};

const originOf = (request: Request) => {
  const url = new URL(request.url);
  const segments = url.pathname.split("/").filter((segment) => segment !== "");
  const first = segments[0];
  const repo = first === undefined || first === "bundles" ? "repository" : first;
  return { repo, base: `${url.origin}/${repo}` };
};

export const layer = Layer.effect(
  Bundles,
  Effect.gen(function* () {
    const store = yield* BundleStore;
    const context = yield* Effect.context<ObjectStore | Repository>();
    const withRepo = <A, E>(effect: Effect.Effect<A, E, ObjectStore | Repository>) =>
      effect.pipe(Effect.provide(context));

    const snapshot = Effect.fn("Bundles.snapshot")(function* (
      filter: BundleFilter,
    ): Effect.fn.Return<BundleSnapshot, Invalid | StorageFailure> {
      return {
        createdAt: new Date(),
        refs: yield* withRepo(advertisedRefs()),
        filter,
      } satisfies BundleSnapshot;
    });

    const manifestOf = Effect.fn("Bundles.manifest")(function* () {
      return yield* store.list(yield* repoName());
    });

    const summary = Effect.fn("Bundles.summary")(function* () {
      const features = yield* featuresOf();
      const manifest = yield* manifestOf();
      const families = (manifest?.families ?? []).map((family) => {
        const latest =
          family.incrementals.at(-1)?.creationToken ?? family.full?.creationToken ?? null;
        return {
          filter: family.filter,
          full: family.full?.id ?? null,
          incrementals: family.incrementals.length,
          latestCreationToken: latest === null ? null : latest.toString(),
        } satisfies BundleFamilySummary;
      });
      return { enabled: features.bundles.enabled, families } satisfies BundlesSummary;
    });

    const build = Effect.fn("Bundles.build")(function* (input: {
      readonly kind: BundleKind;
      readonly filter: BundleFilter;
      readonly operation?: OperationHandle;
    }): Effect.fn.Return<
      BundleArtifact,
      BundleCorrupt | Invalid | ObjectNotFound | StorageFailure
    > {
      const repo = yield* repoName();
      const captured = yield* snapshot(input.filter);
      if (Object.keys(captured.refs).length === 0) {
        return yield* new Invalid({ field: "bundle", reason: "repository has no advertised refs" });
      }
      const current = yield* manifestOf();
      const family = familyState(current, input.filter);
      const previous =
        input.kind === "incremental" ? (family.incrementals.at(-1) ?? family.full) : undefined;
      if (input.kind === "incremental" && previous === null) {
        return yield* new Invalid({
          field: "bundle",
          reason: "cannot build an incremental bundle before a full base exists",
        });
      }
      yield* input.operation?.info("walking objects") ?? Effect.void;
      const request =
        previous === undefined || previous === null
          ? { snapshot: captured, kind: input.kind }
          : { snapshot: captured, kind: input.kind, prerequisiteRefs: previous.refs };
      const built = yield* withRepo(BundleBuilder.build(request));
      yield* (
        input.operation?.progress(
          { current: 0, total: built.oids.length, unit: "objects" },
          "writing bundle",
        ) ?? Effect.void
      );

      const token = nextToken(latestToken(current), captured.createdAt);
      const tempId = `tmp/${token.toString()}-${crypto.randomUUID()}`;
      const written = yield* store.write(
        tempId,
        built.stream.pipe(
          Stream.provide(context),
          Stream.mapError((error) =>
            error._tag === "StorageFailure"
              ? error
              : new StorageFailure({
                  operation: "bundle.write",
                  path: tempId,
                  cause: error,
                }),
          ),
        ),
      );
      const finalId = objectIdOf(input.filter, token, written.checksum);
      yield* store.move(tempId, finalId);

      yield* input.operation?.info("verifying bundle") ?? Effect.void;
      const bytes = yield* BundleBuilder.collect(yield* store.read(finalId));
      yield* BundleBuilder.verifyBundle(bytes, {
        refs: captured.refs,
        prerequisites: built.header.prerequisites,
        filter: input.filter,
      });
      yield* input.operation?.commit ?? Effect.void;

      const artifact: BundleArtifact = {
        id: artifactId(input.kind, input.filter, token),
        kind: input.kind,
        filter: input.filter,
        creationToken: token,
        refs: captured.refs,
        prerequisites: built.header.prerequisites,
        objectId: finalId,
        bytes: written.bytes,
        checksum: written.checksum,
        createdAt: captured.createdAt.toISOString(),
      };
      yield* store.publish(repo, publishArtifact(current, artifact));
      yield* input.operation?.info("published") ?? Effect.void;
      return artifact;
    });

    const protocolLines = Effect.fn("Bundles.protocolLines")(function* (
      kind: "clone" | "catchup",
      baseUrl: string,
    ) {
      return encodeProtocolList(entriesFor(yield* manifestOf(), kind, baseUrl));
    });

    const configText = Effect.fn("Bundles.configText")(function* (
      kind: "clone" | "catchup",
      baseUrl: string,
    ) {
      return encodeConfigList(entriesFor(yield* manifestOf(), kind, baseUrl));
    });

    const handle = Effect.fn("Bundles.handle")(function* (
      request: Request,
    ): Effect.fn.Return<Response | null, StorageFailure> {
      if (request.method !== "GET" && request.method !== "HEAD") return null;
      const url = new URL(request.url);
      const segments = url.pathname.split("/").filter((segment) => segment !== "");
      const at = segments.lastIndexOf("bundles");
      if (at === -1) return null;
      const rest = segments.slice(at + 1);

      if (rest.length === 1 && (rest[0] === "clone" || rest[0] === "catchup")) {
        const { base } = originOf(request);
        const body = yield* configText(rest[0], base);
        const bytes = new TextEncoder().encode(body);
        return new Response(request.method === "HEAD" ? null : body, {
          headers: {
            "content-type": "text/plain; charset=utf-8",
            "content-length": String(bytes.byteLength),
            "cache-control": "no-cache",
          },
        });
      }

      const pointer = artifactPath(url.pathname);
      if (pointer === null) return null;
      const id = objectIdFromPath(pointer);
      const stat = yield* store.stat(id);
      if (stat === null) return new Response("not found\n", { status: 404 });
      const cache = yield* withRepo(cacheMode());
      return yield* Artifact.respond(request, {
        bytes: stat.bytes,
        etag: stat.checksum,
        contentType: "application/x-git-bundle",
        cache,
        read: (range) => store.read(id, range),
      });
    });

    return Bundles.of({
      enabled: featuresOf().pipe(Effect.map((features) => features.bundles.enabled)),
      snapshot,
      manifest: Effect.suspend(() => manifestOf()),
      summary: Effect.suspend(() => summary()),
      build,
      protocolLines,
      configText,
      handle,
    });
  }),
);

/** Advertise `bundle-uri` when a published full bundle exists. */
export const advertise = Effect.fn("Bundles.advertise")(function* () {
  const service = yield* Effect.serviceOption(Bundles);
  if (Option.isNone(service)) return false;
  const features = yield* featuresOf();
  if (!features.bundles.enabled) return false;
  const summary = yield* service.value.summary;
  return summary.families.some((family) => family.full !== null);
});

export const tryHandle = Effect.fn("Bundles.tryHandle")(function* (request: Request) {
  const service = yield* Effect.serviceOption(Bundles);
  if (Option.isNone(service)) return null;
  return yield* service.value.handle(request);
});

export const protocolList = Effect.fn("Bundles.protocolList")(function* (request: Request) {
  const service = yield* Effect.serviceOption(Bundles);
  if (Option.isNone(service)) return [];
  const url = new URL(request.url);
  const segments = url.pathname.split("/").filter((segment) => segment !== "");
  const repo = segments[0] ?? "repository";
  return yield* service.value.protocolLines("clone", `${url.origin}/${repo}`);
});

export { advertisedIds, decodeManifest };
export type { BundleArtifact, BundleManifest, BundleSnapshot };
