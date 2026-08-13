/**
 * Wire-format codecs — the seam between pure and effectful.
 *
 * Everything below this line is synchronous byte work with no I/O, so it is
 * plain functions returning `Result`; Effect adds nothing to a function that
 * turns a `Uint8Array` into a commit. Everything above it (`Repository`, the
 * stores) is effectful. Hashing is the exception: Web Crypto is async on every
 * runtime this targets.
 *
 * Parsing splits the signature line into its parts and returns failures
 * instead of silently defaulting to `""`.
 */
import { Effect, Result } from "effect";
import { Invalid } from "./Error.ts";
import { isOid, type ObjectType, type Oid, type RawObject } from "./Store.ts";

export interface Signature {
  readonly name: string;
  readonly email: string;
  readonly at: Date;
  /** Minutes east of UTC, as git records it. */
  readonly offset: number;
}

export interface CommitInfo {
  readonly tree: Oid;
  readonly parents: ReadonlyArray<Oid>;
  readonly author: Signature;
  readonly committer: Signature;
  readonly message: string;
}

export interface TreeEntry {
  readonly mode: string;
  readonly name: string;
  readonly oid: Oid;
  /**
   * The name as git stores it, when its bytes are not valid UTF-8.
   *
   * git names are bytes, not text: a repository can hold `café.txt` written
   * in Latin-1, and decoding it for display replaces those bytes with U+FFFD.
   * Re-encoding the replacement would rename the file — silently, on the next
   * commit that rewrites the tree — so the original bytes ride along and
   * `encodeTree` writes them back unchanged.
   */
  readonly raw?: Uint8Array;
}

/**
 * What a tree entry's mode means, however it happens to be spelled.
 *
 * git writes `40000` for a subtree, but trees in the wild carry `040000` —
 * its own `fsck` has a name for it, `zeroPaddedFilemode`, and reads them
 * anyway. Comparing the string treats such an entry as a file, and then the
 * directory is rebuilt empty and everything under it is dropped from the next
 * commit. Every decision about what an entry *is* goes through these, in this
 * one place, because the last time the comparison was written out by hand it
 * was written out inconsistently: six call sites still said `!== "40000"`
 * after the rest of the codebase had moved on.
 */
export const isTree = (mode: string): boolean => Number.parseInt(mode, 8) === 0o40000;

/** A gitlink names a commit in another repository — content this one lacks. */
export const isGitlink = (mode: string): boolean => Number.parseInt(mode, 8) === 0o160000;

/** The modes a non-directory entry may carry, spelled as git spells them. */
const FILE_MODES = new Set<number>([0o100644, 0o100755, 0o120000, 0o160000]);

/** Whether a mode may name a leaf entry, zero padding and all. */
export const isFileMode = (mode: string): boolean => FILE_MODES.has(Number.parseInt(mode, 8));

/**
 * Whether two entries carry the same mode, however each is spelled — and an
 * absent entry, which has no mode, matches only another absent one.
 *
 * The comparison a three-way merge makes about every path it touches. Done as
 * string equality, one side's `0100644` against the other's `100644` misses
 * the "identical on both sides" shortcut, elects a mode by falling through to
 * ours, and reports a `content` conflict on a file nobody changed.
 */
export const sameMode = (left: string | undefined, right: string | undefined): boolean =>
  left === undefined || right === undefined
    ? left === right
    : Number.parseInt(left, 8) === Number.parseInt(right, 8);

/** An annotated tag: a real object, unlike a lightweight tag's bare ref. */
export interface TagInfo {
  readonly object: Oid;
  readonly type: ObjectType;
  /** The short name, as `refs/tags/<tag>` spells it. */
  readonly tag: string;
  /** git tolerates a tag with no tagger, and old repositories have them. */
  readonly tagger?: Signature;
  readonly message: string;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const invalid = (field: string, reason: string) => Result.fail(new Invalid({ field, reason }));

/**
 * One buffer from several, which every writer here needs.
 *
 * `CommitPack.ts` keeps its own: it already knows the total and takes it as
 * an argument, which is a different function wearing the same name.
 */
export const concatBytes = (parts: ReadonlyArray<Uint8Array>): Uint8Array<ArrayBuffer> => {
  const total = parts.reduce((sum, part) => sum + part.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
};

export const bytesToHex = (bytes: Uint8Array): string => {
  let hex = "";
  for (const byte of bytes) hex += byte.toString(16).padStart(2, "0");
  return hex;
};

export const hexToBytes = (hex: string): Uint8Array => {
  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index++) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
};

/** `<type> <length>\0<payload>` — git's loose object framing. */
export const encodeObject = (object: RawObject): Uint8Array => {
  const header = encoder.encode(`${object.type} ${object.data.length}\0`);
  const out = new Uint8Array(header.length + object.data.length);
  out.set(header);
  out.set(object.data, header.length);
  return out;
};

export const decodeObject = (bytes: Uint8Array): Result.Result<RawObject, Invalid> => {
  const nul = bytes.indexOf(0);
  if (nul === -1) return invalid("object", "missing header terminator");

  const header = decoder.decode(bytes.subarray(0, nul));
  const [type, length] = header.split(" ");
  if (type !== "blob" && type !== "tree" && type !== "commit" && type !== "tag") {
    return invalid("object", `unknown object type '${type}'`);
  }

  const data = bytes.subarray(nul + 1);
  if (Number(length) !== data.length) {
    return invalid("object", `header says ${length} bytes, body has ${data.length}`);
  }

  return Result.succeed({ type, data });
};

/**
 * The object id: SHA-1 over the framed object.
 *
 * `Effect.promise` rather than `tryPromise` — a digest of bytes already in
 * memory has no failure a caller could act on.
 */
export const hashObject = (object: RawObject): Effect.Effect<Oid> =>
  Effect.promise(async () => {
    const framed = encodeObject(object);
    const digest = await crypto.subtle.digest("SHA-1", framed.slice().buffer);
    // SAFETY: a SHA-1 digest is twenty bytes, so its hex form is exactly the
    // forty lowercase hex characters the Oid brand names.
    return bytesToHex(new Uint8Array(digest)) as Oid;
  });

const parseSignature = (raw: string, field: string): Result.Result<Signature, Invalid> => {
  const match = raw.match(/^(.*?) <(.*?)> (\d+) ([+-]\d{4})$/);
  if (match === null) return invalid(field, `malformed signature: '${raw}'`);

  const [, name = "", email = "", seconds = "0", zone = "+0000"] = match;
  const sign = zone.startsWith("-") ? -1 : 1;
  const offset = sign * (Number(zone.slice(1, 3)) * 60 + Number(zone.slice(3, 5)));

  return Result.succeed({
    name,
    email,
    at: new Date(Number(seconds) * 1000),
    offset,
  });
};

const formatSignature = (signature: Signature): string => {
  const sign = signature.offset < 0 ? "-" : "+";
  const absolute = Math.abs(signature.offset);
  const zone = `${sign}${String(Math.floor(absolute / 60)).padStart(2, "0")}${String(absolute % 60).padStart(2, "0")}`;
  return `${signature.name} <${signature.email}> ${Math.floor(signature.at.getTime() / 1000)} ${zone}`;
};

export const parseCommit = (data: Uint8Array): Result.Result<CommitInfo, Invalid> => {
  const text = decoder.decode(data);
  const split = text.indexOf("\n\n");
  const headerText = split === -1 ? text : text.slice(0, split);
  const message = split === -1 ? "" : text.slice(split + 2);
  const lines = headerText.split("\n");

  const tree = lines.find((line) => line.startsWith("tree "))?.slice(5);
  if (tree === undefined || !isOid(tree)) return invalid("tree", "missing or malformed tree");

  const parents: Oid[] = [];
  for (const line of lines.filter((line) => line.startsWith("parent "))) {
    const parent = line.slice(7);
    if (!isOid(parent)) return invalid("parent", `malformed parent '${parent}'`);
    parents.push(parent);
  }

  const authorLine = lines.find((line) => line.startsWith("author "))?.slice(7);
  if (authorLine === undefined) return invalid("author", "missing author");
  const author = parseSignature(authorLine, "author");
  if (Result.isFailure(author)) return Result.fail(author.failure);

  const committerLine = lines.find((line) => line.startsWith("committer "))?.slice(10);
  const committer =
    committerLine === undefined ? author : parseSignature(committerLine, "committer");
  if (Result.isFailure(committer)) return Result.fail(committer.failure);

  return Result.succeed({
    tree,
    parents,
    author: author.success,
    committer: committer.success,
    message,
  });
};

export const encodeCommit = (commit: CommitInfo): Uint8Array => {
  const lines = [
    `tree ${commit.tree}`,
    ...commit.parents.map((parent) => `parent ${parent}`),
    `author ${formatSignature(commit.author)}`,
    `committer ${formatSignature(commit.committer)}`,
  ];
  return encoder.encode(`${lines.join("\n")}\n\n${commit.message}`);
};

export const parseTag = (data: Uint8Array): Result.Result<TagInfo, Invalid> => {
  const text = decoder.decode(data);
  const split = text.indexOf("\n\n");
  const headerText = split === -1 ? text : text.slice(0, split);
  const message = split === -1 ? "" : text.slice(split + 2);
  const lines = headerText.split("\n");

  const object = lines.find((line) => line.startsWith("object "))?.slice(7);
  if (object === undefined || !isOid(object))
    return invalid("object", "missing or malformed object");

  const type = lines.find((line) => line.startsWith("type "))?.slice(5);
  if (type !== "blob" && type !== "tree" && type !== "commit" && type !== "tag") {
    return invalid("type", `unknown tagged type '${type}'`);
  }

  const name = lines.find((line) => line.startsWith("tag "))?.slice(4);
  if (name === undefined || name === "") return invalid("tag", "missing tag name");

  // git allows a tag without a tagger — older repositories have them.
  const taggerLine = lines.find((line) => line.startsWith("tagger "))?.slice(7);
  if (taggerLine === undefined) {
    return Result.succeed({ object, type, tag: name, message });
  }

  const tagger = parseSignature(taggerLine, "tagger");
  if (Result.isFailure(tagger)) return Result.fail(tagger.failure);

  return Result.succeed({ object, type, tag: name, tagger: tagger.success, message });
};

export const encodeTag = (tag: TagInfo): Uint8Array => {
  const lines = [`object ${tag.object}`, `type ${tag.type}`, `tag ${tag.tag}`];
  if (tag.tagger !== undefined) lines.push(`tagger ${formatSignature(tag.tagger)}`);
  return encoder.encode(`${lines.join("\n")}\n\n${tag.message}`);
};

/** Byte-wise, the way `memcmp` orders: a prefix sorts before what extends it. */
const compareBytes = (left: Uint8Array, right: Uint8Array): number => {
  const shared = Math.min(left.length, right.length);
  for (let at = 0; at < shared; at++) {
    if (left[at] !== right[at]) return left[at]! - right[at]!;
  }
  return left.length - right.length;
};

export const parseTree = (data: Uint8Array): Result.Result<ReadonlyArray<TreeEntry>, Invalid> => {
  const entries: TreeEntry[] = [];
  let offset = 0;

  while (offset < data.length) {
    const space = data.indexOf(0x20, offset);
    if (space === -1) return invalid("tree", "entry missing mode terminator");

    const nul = data.indexOf(0, space);
    if (nul === -1 || nul + 21 > data.length) return invalid("tree", "entry truncated");

    const bytes = data.subarray(space + 1, nul);
    const name = decoder.decode(bytes);
    // Only when the decode is not reversible, so an ordinary tree entry
    // carries nothing extra.
    const reversible = compareBytes(encoder.encode(name), bytes) === 0;

    // SAFETY: the bound check above guarantees twenty bytes after the NUL,
    // and twenty bytes hex-encode to the forty characters the Oid brand names.
    const entry = {
      mode: decoder.decode(data.subarray(offset, space)),
      name,
      oid: bytesToHex(data.subarray(nul + 1, nul + 21)) as Oid,
    };
    entries.push(reversible ? entry : { ...entry, raw: new Uint8Array(bytes) });
    offset = nul + 21;
  }

  return Result.succeed(entries);
};

export const encodeTree = (entries: ReadonlyArray<TreeEntry>): Uint8Array => {
  // git requires entries sorted by name, with directories sorted as `name/`,
  // and it sorts the UTF-8 bytes. Comparing the strings would sort by UTF-16
  // code unit instead, which puts a surrogate pair before U+E000..U+FFFF
  // rather than after — a tree whose oid git disagrees with.
  const nameOf = (entry: TreeEntry) => entry.raw ?? encoder.encode(entry.name);
  const sorted = [...entries]
    .map((entry) => {
      const name = nameOf(entry);
      // A directory sorts as `name/`, which is one byte on the end.
      // `040000` is the same mode: sorting it as a file puts the entry in
      // the wrong place and the tree hashes differently from git's.
      const key = isTree(entry.mode) ? Uint8Array.from([...name, 0x2f]) : name;
      return { entry, key, name };
    })
    .sort((left, right) => compareBytes(left.key, right.key));

  const parts: Uint8Array[] = [];
  for (const { entry, name } of sorted) {
    const prefix = encoder.encode(`${entry.mode} `);
    const out = new Uint8Array(prefix.length + name.length + 1 + 20);
    out.set(prefix);
    out.set(name, prefix.length);
    out[prefix.length + name.length] = 0;
    out.set(hexToBytes(entry.oid), prefix.length + name.length + 1);
    parts.push(out);
  }

  const total = parts.reduce((sum, part) => sum + part.length, 0);
  const tree = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    tree.set(part, offset);
    offset += part.length;
  }
  return tree;
};

/** The empty tree, which git special-cases and every first commit needs. */
export const EMPTY_TREE_OID =
  // SAFETY: the literal is git's well-known empty-tree id, forty lowercase
  // hex characters as the Oid brand requires.
  "4b825dc642cb6eb9a060e54bf8d69288fbee4904" as Oid;

const ZERO_OID = "0".repeat(40);

/** One line of a reflog, as the file-shaped backends store it. */
export interface ReflogLine {
  readonly from: Oid | null;
  readonly to: Oid | null;
  readonly at: Date;
  readonly message: string;
}

/**
 * A ref update carries a reason, not a person, so the identity in the line is
 * this server. git only requires the field to be there and parse.
 */
const REFLOG_IDENTITY = "chr33s-git <git@localhost>";

/**
 * git's own reflog line — `<old> <new> <who> <unixtime> <tz>\t<message>`.
 *
 * The format is shared by every backend that keeps reflogs as text, and it is
 * git's rather than one of our own because `logs/refs/heads/main` in a `Node`
 * repository is a file `git reflog` reads.
 */
export const encodeReflogLine = (entry: ReflogLine): string =>
  `${entry.from ?? ZERO_OID} ${entry.to ?? ZERO_OID} ${REFLOG_IDENTITY} ${Math.floor(
    entry.at.getTime() / 1000,
  )} +0000\t${entry.message}\n`;

/**
 * The inverse, tolerant of what else may be in the file.
 *
 * A repository this server writes is also written by `git` itself, and older
 * builds here put an ISO timestamp where git puts the committer — so both are
 * read. A line whose timestamp parses as neither yields an invalid `Date`,
 * which callers must treat as "unknown", never as "now".
 */
export const parseReflogLine = (line: string): ReflogLine | null => {
  if (line.length === 0) return null;
  const [values = "", message = ""] = line.split("\t");
  const fields = values.split(" ");
  const [from = ZERO_OID, to = ZERO_OID] = fields;

  const zone = fields.at(-1) ?? "";
  const seconds = fields.at(-2) ?? "";
  const at =
    /^[-+]\d{4}$/.test(zone) && /^\d+$/.test(seconds)
      ? new Date(Number(seconds) * 1000)
      : new Date(fields[2] ?? "");

  return {
    // The all-zero oid is this format's "no ref on this side"; anything else
    // that fails to parse as an oid is a mangled line and reads the same way.
    from: from === ZERO_OID || !isOid(from) ? null : from,
    to: to === ZERO_OID || !isOid(to) ? null : to,
    at,
    message,
  };
};
