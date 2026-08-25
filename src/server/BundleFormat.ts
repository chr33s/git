/**
 * Bundle bytes and bundle-list text — the codecs, not the store.
 *
 * A bundle is a small header plus a pack. The header is the only part this
 * module invents; the pack is `Pack.pack`. The list formats are what protocol
 * v2 `bundle-uri` and the HTTP clone/catch-up endpoints speak.
 */
import { Result, Schema } from "effect";

import { BundleCorrupt } from "../git/Error.ts";
import { isOid, type Oid } from "../git/Oid.ts";

export type BundleFilter = null | "blob:none";
export type BundleKind = "full" | "incremental";
export type BundleFamily = "full" | "blobnone";

export interface BundleHeader {
  readonly version: 2 | 3;
  readonly filter: BundleFilter;
  readonly refs: Readonly<Record<string, Oid>>;
  readonly prerequisites: ReadonlyArray<Oid>;
}

export interface BundleArtifact {
  readonly id: string;
  readonly kind: BundleKind;
  readonly filter: BundleFilter;
  readonly creationToken: bigint;
  readonly refs: Readonly<Record<string, Oid>>;
  readonly prerequisites: ReadonlyArray<Oid>;
  readonly objectId: string;
  readonly bytes: number;
  readonly checksum: string;
  readonly createdAt: string;
}

export interface BundleFamilyState {
  readonly filter: BundleFilter;
  readonly full: BundleArtifact | null;
  readonly incrementals: ReadonlyArray<BundleArtifact>;
}

export interface BundleManifest {
  readonly version: 1;
  readonly families: ReadonlyArray<BundleFamilyState>;
}

export interface BundleSnapshot {
  readonly createdAt: Date;
  readonly refs: Readonly<Record<string, Oid>>;
  readonly filter: BundleFilter;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Git's bundle-list identifier: a letter, then letters, digits or hyphens. */
export const isBundleId = (value: string): boolean => /^[a-zA-Z][a-zA-Z0-9-]*$/.test(value);

export const familyOf = (filter: BundleFilter): BundleFamily =>
  filter === "blob:none" ? "blobnone" : "full";

export const filterOf = (family: BundleFamily): BundleFilter =>
  family === "blobnone" ? "blob:none" : null;

/**
 * Stable list identifier. Token is decimal so ordering is visible in the
 * name; the prefix keeps full and incremental, and the two families, apart.
 */
export const artifactId = (kind: BundleKind, filter: BundleFilter, token: bigint): string => {
  const family = filter === "blob:none" ? (kind === "full" ? "nfull" : "ninc") : kind;
  return `${family}-${token.toString()}`;
};

/** Object key suffix: `<family>/<creationToken>-<checksum>.bundle`. */
export const objectIdOf = (filter: BundleFilter, token: bigint, checksum: string): string =>
  `${familyOf(filter)}/${token.toString()}-${checksum}.bundle`;

/**
 * The next creation token: strictly greater than any already published, and
 * at least the current millisecond so a clock-based heuristic stays honest.
 */
export const nextToken = (previous: bigint | null, now: Date): bigint => {
  const millis = BigInt(now.getTime());
  if (previous === null || previous < millis) return millis;
  return previous + 1n;
};

export const latestToken = (manifest: BundleManifest | null): bigint | null => {
  if (manifest === null) return null;
  let latest: bigint | null = null;
  for (const family of manifest.families) {
    const artifacts =
      family.full === null ? family.incrementals : [family.full, ...family.incrementals];
    for (const artifact of artifacts) {
      if (latest === null || artifact.creationToken > latest) latest = artifact.creationToken;
    }
  }
  return latest;
};

export const familyState = (
  manifest: BundleManifest | null,
  filter: BundleFilter,
): BundleFamilyState => {
  const found = manifest?.families.find((family) => family.filter === filter);
  return found ?? { filter, full: null, incrementals: [] };
};

/** Clone list: current full base plus the incrementals chained from it. */
export const cloneArtifacts = (state: BundleFamilyState): ReadonlyArray<BundleArtifact> =>
  state.full === null ? [] : [state.full, ...state.incrementals];

/** Catch-up list: incrementals only — never a newly-created full base. */
export const catchupArtifacts = (state: BundleFamilyState): ReadonlyArray<BundleArtifact> =>
  state.incrementals;

const headerLine = (line: string): Uint8Array => encoder.encode(`${line}\n`);

/** Bundle header bytes, including the terminating blank line. */
export const encodeHeader = (header: BundleHeader): Uint8Array => {
  const lines: Uint8Array[] = [];
  if (header.version === 3 || header.filter !== null) {
    lines.push(headerLine("# v3 git bundle"));
    lines.push(headerLine("@object-format=sha1"));
    if (header.filter !== null) lines.push(headerLine(`@filter=${header.filter}`));
  } else {
    lines.push(headerLine("# v2 git bundle"));
  }
  for (const oid of header.prerequisites) {
    lines.push(headerLine(`- ${oid} prerequisite`));
  }
  const names = Object.keys(header.refs).sort();
  for (const name of names) {
    const oid = header.refs[name];
    if (oid === undefined) continue;
    lines.push(headerLine(`${oid} ${name}`));
  }
  lines.push(encoder.encode("\n"));
  let total = 0;
  for (const line of lines) total += line.length;
  const out = new Uint8Array(total);
  let offset = 0;
  for (const line of lines) {
    out.set(line, offset);
    offset += line.length;
  }
  return out;
};

const parseOid = (text: string): Oid | null => (isOid(text) ? text : null);

/**
 * Split a bundle's leading header from the pack that follows.
 *
 * The header ends at the first blank line. Anything after that is pack bytes
 * and is not interpreted here.
 */
export const parseHeader = (
  bytes: Uint8Array,
): Result.Result<{ readonly header: BundleHeader; readonly packOffset: number }, BundleCorrupt> => {
  const text = decoder.decode(bytes);
  const blank = text.indexOf("\n\n");
  if (blank === -1) {
    return Result.fail(new BundleCorrupt({ reason: "bundle header is missing a blank line" }));
  }
  const head = text.slice(0, blank);
  const lines = head.split("\n");
  const first = lines[0];
  if (first !== "# v2 git bundle" && first !== "# v3 git bundle") {
    return Result.fail(
      new BundleCorrupt({ reason: `unrecognised bundle signature '${first ?? ""}'` }),
    );
  }
  const version: 2 | 3 = first === "# v3 git bundle" ? 3 : 2;
  let filter: BundleFilter = null;
  const refs: Record<string, Oid> = {};
  const prerequisites: Oid[] = [];

  for (const line of lines.slice(1)) {
    if (line.startsWith("@")) {
      if (line === "@filter=blob:none") filter = "blob:none";
      continue;
    }
    if (line.startsWith("- ")) {
      const oid = parseOid(line.slice(2, 42));
      if (oid === null) {
        return Result.fail(new BundleCorrupt({ reason: `bad prerequisite line '${line}'` }));
      }
      prerequisites.push(oid);
      continue;
    }
    if (line.length < 42 || line[40] !== " ") {
      return Result.fail(new BundleCorrupt({ reason: `bad ref line '${line}'` }));
    }
    const oid = parseOid(line.slice(0, 40));
    if (oid === null) {
      return Result.fail(new BundleCorrupt({ reason: `bad ref oid in '${line}'` }));
    }
    refs[line.slice(41)] = oid;
  }

  return Result.succeed({
    header: { version, filter, refs, prerequisites },
    packOffset: blank + 2,
  });
};

export interface BundleListEntry {
  readonly id: string;
  readonly uri: string;
  readonly creationToken: bigint;
  readonly filter: BundleFilter;
}

const byToken = (left: BundleListEntry, right: BundleListEntry): number =>
  left.creationToken < right.creationToken ? -1 : left.creationToken > right.creationToken ? 1 : 0;

export const listEntries = (
  artifacts: ReadonlyArray<BundleArtifact>,
  uriOf: (artifact: BundleArtifact) => string,
): ReadonlyArray<BundleListEntry> =>
  artifacts
    .map((artifact) => ({
      id: artifact.id,
      uri: uriOf(artifact),
      creationToken: artifact.creationToken,
      filter: artifact.filter,
    }))
    .sort(byToken);

/** Protocol v2 `bundle-uri` body: flattened `bundle.<key>=<value>` lines. */
export const encodeProtocolList = (
  entries: ReadonlyArray<BundleListEntry>,
): ReadonlyArray<string> => {
  const lines = ["bundle.version=1", "bundle.mode=all", "bundle.heuristic=creationToken"];
  for (const entry of entries) {
    lines.push(`bundle.${entry.id}.uri=${entry.uri}`);
    lines.push(`bundle.${entry.id}.creationToken=${entry.creationToken.toString()}`);
    if (entry.filter !== null) lines.push(`bundle.${entry.id}.filter=${entry.filter}`);
  }
  return lines;
};

/** HTTP clone/catch-up body: gitconfig INI. */
export const encodeConfigList = (entries: ReadonlyArray<BundleListEntry>): string => {
  const lines = ["[bundle]", "\tversion = 1", "\tmode = all", "\theuristic = creationToken"];
  for (const entry of entries) {
    lines.push("", `[bundle "${entry.id}"]`, `\turi = ${entry.uri}`);
    lines.push(`\tcreationToken = ${entry.creationToken.toString()}`);
    if (entry.filter !== null) lines.push(`\tfilter = ${entry.filter}`);
  }
  return `${lines.join("\n")}\n`;
};

export interface ArtifactPointer {
  readonly family: BundleFamily;
  readonly file: string;
}

/** Parse `/bundles/<family>/<file>.bundle` out of a request path. */
export const artifactPath = (pathname: string): ArtifactPointer | null => {
  const segments = pathname.split("/").filter((segment) => segment !== "");
  const at = segments.lastIndexOf("bundles");
  if (at === -1) return null;
  const family = segments[at + 1];
  const file = segments[at + 2];
  if ((family !== "full" && family !== "blobnone") || file === undefined) return null;
  if (!file.endsWith(".bundle") || segments.length !== at + 3) return null;
  if (file.includes("..") || file.includes("/")) return null;
  return { family, file };
};

export const objectIdFromPath = (pointer: ArtifactPointer): string =>
  `${pointer.family}/${pointer.file}`;

const artifactJson = (artifact: BundleArtifact) => ({
  id: artifact.id,
  kind: artifact.kind,
  filter: artifact.filter,
  creationToken: artifact.creationToken.toString(),
  refs: artifact.refs,
  prerequisites: artifact.prerequisites,
  objectId: artifact.objectId,
  bytes: artifact.bytes,
  checksum: artifact.checksum,
  createdAt: artifact.createdAt,
});

const OidString = Schema.String.pipe(Schema.refine(isOid));
const BundleId = Schema.String.check(Schema.isPattern(/^[a-zA-Z][a-zA-Z0-9-]*$/));
const TokenString = Schema.String.check(Schema.isPattern(/^[0-9]+$/));

const ArtifactWire = Schema.Struct({
  id: BundleId,
  kind: Schema.Literals(["full", "incremental"]),
  filter: Schema.NullOr(Schema.Literals(["blob:none"])),
  creationToken: TokenString,
  refs: Schema.Record(Schema.String, OidString),
  prerequisites: Schema.Array(OidString),
  objectId: Schema.String,
  bytes: Schema.Finite,
  checksum: Schema.String,
  createdAt: Schema.String,
});

const FamilyWire = Schema.Struct({
  filter: Schema.NullOr(Schema.Literals(["blob:none"])),
  full: Schema.NullOr(ArtifactWire),
  incrementals: Schema.Array(ArtifactWire),
});

const ManifestWire = Schema.Struct({
  version: Schema.Literals([1]),
  families: Schema.Array(FamilyWire),
});

const decodeManifestWire = Schema.decodeUnknownResult(ManifestWire);

const fromWire = (wire: typeof ArtifactWire.Type): BundleArtifact => {
  const refs: Record<string, Oid> = {};
  for (const [name, value] of Object.entries(wire.refs)) {
    if (isOid(value)) refs[name] = value;
  }
  const prerequisites: Oid[] = [];
  for (const value of wire.prerequisites) {
    if (isOid(value)) prerequisites.push(value);
  }
  return {
    id: wire.id,
    kind: wire.kind,
    filter: wire.filter,
    creationToken: BigInt(wire.creationToken),
    refs,
    prerequisites,
    objectId: wire.objectId,
    bytes: wire.bytes,
    checksum: wire.checksum,
    createdAt: wire.createdAt,
  };
};

export const encodeManifest = (manifest: BundleManifest): string =>
  JSON.stringify({
    version: 1,
    families: manifest.families.map((family) => ({
      filter: family.filter,
      full: family.full === null ? null : artifactJson(family.full),
      incrementals: family.incrementals.map(artifactJson),
    })),
  });

export const decodeManifest = (text: string): BundleManifest | null => {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return null;
  }
  const decoded = decodeManifestWire(parsed);
  if (Result.isFailure(decoded)) return null;
  return {
    version: 1,
    families: decoded.success.families.map((family) => ({
      filter: family.filter,
      full: family.full === null ? null : fromWire(family.full),
      incrementals: family.incrementals.map(fromWire),
    })),
  };
};

export const emptyManifest = (): BundleManifest => ({
  version: 1,
  families: [
    { filter: null, full: null, incrementals: [] },
    { filter: "blob:none", full: null, incrementals: [] },
  ],
});

/**
 * Advertise `artifact` in `filter`'s family. A new full retires the previous
 * full and its incrementals from the advertised lists; they become prune
 * candidates. An incremental appends to the current chain.
 */
export const publishArtifact = (
  manifest: BundleManifest | null,
  artifact: BundleArtifact,
): BundleManifest => {
  const current = manifest ?? emptyManifest();
  const families = current.families.map((family) => {
    if (family.filter !== artifact.filter) return family;
    if (artifact.kind === "full") {
      return { filter: family.filter, full: artifact, incrementals: [] };
    }
    return {
      filter: family.filter,
      full: family.full,
      incrementals: [...family.incrementals, artifact],
    };
  });
  if (!families.some((family) => family.filter === artifact.filter)) {
    families.push(
      artifact.kind === "full"
        ? { filter: artifact.filter, full: artifact, incrementals: [] }
        : { filter: artifact.filter, full: null, incrementals: [artifact] },
    );
  }
  return { version: 1, families };
};

/** Every object id the manifest still names. */
export const advertisedIds = (manifest: BundleManifest | null): ReadonlySet<string> => {
  const ids = new Set<string>();
  if (manifest === null) return ids;
  for (const family of manifest.families) {
    if (family.full !== null) ids.add(family.full.objectId);
    for (const incremental of family.incrementals) ids.add(incremental.objectId);
  }
  return ids;
};
