/**
 * Export a tree as a tar, tar.gz or zip archive.
 *
 *   GET …/archive/<name>?ref=<ref>&path=<dir>
 *
 * The extension on `<name>` picks the format, the way `git archive --format`
 * is inferred from the output filename and the way every forge spells it.
 *
 * The result is a `Stream`, not a buffer: a repository's worth of blobs is
 * exactly the thing that must not be materialised at once, which is the same
 * reason `Repository.packOf` streams. Only the file *list* is held — paths and
 * oids, no content — and each blob is read as the consumer pulls it.
 *
 * The writers are hand-rolled because this module has to load in a Worker: no
 * `node:zlib`, no archiver dependency. Gzip is `CompressionStream`, which both
 * workerd and node ≥18 implement.
 */
import { Effect, Stream } from "effect";

import { ObjectNotFound, type StorageFailure, statusOf } from "../git/Error.ts";
import { crc32 } from "../git/PackIndex.ts";
import { Repository, type TreeFile } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export type Format = "tar" | "tar.gz" | "zip";

/** What reading a tree can go wrong with; the archive inherits it verbatim. */
export type ArchiveError = ObjectNotFound | StorageFailure;

const CONTENT_TYPES = {
  tar: "application/x-tar",
  "tar.gz": "application/gzip",
  zip: "application/zip",
} as const satisfies Record<Format, string>;

/** `null` for an extension this module cannot write, so a router can fall through. */
export const formatOf = (name: string): Format | null => {
  const lower = name.toLowerCase();
  if (lower.endsWith(".tar.gz") || lower.endsWith(".tgz")) return "tar.gz";
  if (lower.endsWith(".zip")) return "zip";
  if (lower.endsWith(".tar")) return "tar";
  return null;
};

/** The name without the extension `formatOf` matched — the natural prefix. */
const stemOf = (name: string): string => {
  const lower = name.toLowerCase();
  for (const suffix of [".tar.gz", ".tgz", ".zip", ".tar"]) {
    if (lower.endsWith(suffix)) return name.slice(0, -suffix.length);
  }
  return name;
};

/** One entry to write, resolved down to metadata; content stays behind `oid`. */
type Entry =
  | { readonly kind: "directory"; readonly path: string }
  | {
      readonly kind: "file";
      readonly path: string;
      /** The git mode, so the writers decide the archive's own mode. */
      readonly mode: string;
      readonly oid: Oid;
    };

const normalizePrefix = (prefix: string | undefined): string => {
  const trimmed = (prefix ?? "").replace(/^\/+|\/+$/g, "");
  return trimmed === "" ? "" : `${trimmed}/`;
};

/**
 * Files plus the directories that contain them, parents first.
 *
 * git has no directory objects to list, but both formats want an entry per
 * directory — without one, an empty `prefix/` would not survive extraction and
 * `unzip -l` would show no structure at all.
 */
const planOf = (files: ReadonlyArray<TreeFile>, prefix: string): ReadonlyArray<Entry> => {
  const entries: Entry[] = [];
  const seen = new Set<string>();

  for (const file of files) {
    // A gitlink names a commit in another repository; there is nothing here to
    // write for it, and git archive skips it too.
    if (file.mode === "160000") continue;

    const path = `${prefix}${file.path}`;
    const segments = path.split("/");
    let directory = "";
    for (const segment of segments.slice(0, -1)) {
      directory = `${directory}${segment}/`;
      if (seen.has(directory)) continue;
      seen.add(directory);
      entries.push({ kind: "directory", path: directory });
    }
    entries.push({ kind: "file", path, mode: file.mode, oid: file.oid });
  }

  return entries;
};

const BLOCK = 512;

/** Zero bytes to round `size` up to a whole 512-byte block, or nothing. */
const padding = (size: number): ReadonlyArray<Uint8Array> => {
  const remainder = size % BLOCK;
  return remainder === 0 ? [] : [new Uint8Array(BLOCK - remainder)];
};

const putAscii = (block: Uint8Array, offset: number, width: number, text: string): void => {
  block.set(encoder.encode(text).subarray(0, width), offset);
};

/** ustar numeric fields are octal, zero-padded, NUL-terminated. */
const putOctal = (block: Uint8Array, offset: number, width: number, value: number): void => {
  putAscii(block, offset, width, value.toString(8).padStart(width - 1, "0"));
};

interface Header {
  readonly name: string;
  readonly mode: number;
  readonly size: number;
  readonly typeflag: "0" | "2" | "5" | "x";
  readonly link: string;
  readonly mtime: number;
  readonly prefix: string;
}

const ustarHeader = (header: Header): Uint8Array => {
  const block = new Uint8Array(BLOCK);

  putAscii(block, 0, 100, header.name);
  putOctal(block, 100, 8, header.mode);
  putOctal(block, 108, 8, 0);
  putOctal(block, 116, 8, 0);
  putOctal(block, 124, 12, header.size);
  putOctal(block, 136, 12, header.mtime);
  block.fill(0x20, 148, 156); // checksum field counts as spaces while summing
  putAscii(block, 156, 1, header.typeflag);
  putAscii(block, 157, 100, header.link);
  putAscii(block, 257, 6, "ustar\0");
  putAscii(block, 263, 2, "00");
  putAscii(block, 265, 32, "root");
  putAscii(block, 297, 32, "root");
  putAscii(block, 345, 155, header.prefix);

  let sum = 0;
  for (const byte of block) sum += byte;
  // Six octal digits, NUL, space — the one field whose padding is not uniform.
  putAscii(block, 148, 8, `${sum.toString(8).padStart(6, "0")}\0 `);

  return block;
};

/**
 * A path split across ustar's `prefix` and `name` fields, or `null` when no
 * split fits — the split has to fall on a `/`, and neither half may overflow.
 */
const splitPath = (path: string): { readonly name: string; readonly prefix: string } | null => {
  const bytes = encoder.encode(path);
  if (bytes.length <= 100) return { name: path, prefix: "" };

  // Ascending: the first slash that leaves a short enough tail keeps `prefix`
  // as small as it can be, which is the split most likely to fit both fields.
  for (let index = 0; index < bytes.length; index++) {
    if (bytes[index] !== 0x2f) continue;
    const tail = bytes.length - index - 1;
    if (tail > 100 || tail === 0) continue;
    if (index > 155 || index === 0) continue;
    return {
      name: decoder.decode(bytes.subarray(index + 1)),
      prefix: decoder.decode(bytes.subarray(0, index)),
    };
  }

  return null;
};

/**
 * The header blocks for one entry.
 *
 * A path that ustar's two fields cannot hold gets a pax extended header (`x`)
 * carrying `path=…` — pax over GNU's `L` because pax is POSIX.1-2001 and GNU
 * tar, bsdtar and macOS tar all read it, while `L` is GNU's alone.
 */
const tarHeaders = (header: Omit<Header, "prefix">): ReadonlyArray<Uint8Array> => {
  const split = splitPath(header.name);
  if (split !== null) return [ustarHeader({ ...header, ...split })];

  // "<len> path=<value>\n", where <len> counts itself — solved by growing the
  // guess until the printed length agrees with it.
  const value = `path=${header.name}\n`;
  let length = encoder.encode(value).length + 2;
  while (encoder.encode(`${length}`).length + 1 + encoder.encode(value).length !== length) {
    length = encoder.encode(`${length}`).length + 1 + encoder.encode(value).length;
  }
  const record = encoder.encode(`${length} ${value}`);

  // The truncated name is what a tar without pax support would see; readers
  // that do support it replace the name from the record.
  const truncated = decoder.decode(encoder.encode(header.name).subarray(0, 100));

  return [
    ustarHeader({
      name: "PaxHeaders/pax",
      mode: 0o644,
      size: record.length,
      typeflag: "x",
      link: "",
      mtime: header.mtime,
      prefix: "",
    }),
    record,
    ...padding(record.length),
    ustarHeader({ ...header, name: truncated, prefix: "" }),
  ];
};

type Reader = Pick<typeof Repository.Service, "readBlob">;

const tarChunks = (
  repository: Reader,
  entry: Entry,
  mtime: number,
): Effect.Effect<ReadonlyArray<Uint8Array>, ArchiveError> =>
  Effect.gen(function* () {
    if (entry.kind === "directory") {
      return tarHeaders({
        name: entry.path,
        mode: 0o755,
        size: 0,
        typeflag: "5",
        link: "",
        mtime,
      });
    }

    // A symlink's blob *is* the target, so it travels in the header rather
    // than as content — a symlink entry has no content at all.
    if (entry.mode === "120000") {
      const target = decoder.decode(yield* repository.readBlob(entry.oid));
      return tarHeaders({
        name: entry.path,
        mode: 0o777,
        size: 0,
        typeflag: "2",
        link: target,
        mtime,
      });
    }

    const data = yield* repository.readBlob(entry.oid);
    return [
      ...tarHeaders({
        name: entry.path,
        mode: entry.mode === "100755" ? 0o755 : 0o644,
        size: data.length,
        typeflag: "0",
        link: "",
        mtime,
      }),
      data,
      ...padding(data.length),
    ];
  });

const tarStream = (
  repository: Reader,
  plan: ReadonlyArray<Entry>,
  mtime: number,
): Stream.Stream<Uint8Array, ArchiveError> =>
  Stream.fromIterable(plan).pipe(
    Stream.flatMap((entry) =>
      Stream.unwrap(tarChunks(repository, entry, mtime).pipe(Effect.map(Stream.fromIterable))),
    ),
    // Two zero blocks are what says "end of archive"; without them GNU tar
    // reports an unexpected EOF.
    Stream.concat(Stream.make(new Uint8Array(BLOCK * 2))),
  );

const gzip = (
  stream: Stream.Stream<Uint8Array, ArchiveError>,
): Stream.Stream<Uint8Array, ArchiveError> =>
  Stream.fromReadableStream({
    evaluate: () => {
      // SAFETY: `CompressionStream` accepts any `BufferSource`, which its
      // declared pair type does not narrow to what the source emits.
      const source = Stream.toReadableStream(stream) as ReadableStream<BufferSource>;
      return source.pipeThrough(new CompressionStream("gzip"));
    },
    onError: (cause) => {
      // SAFETY: the web stream squashes the cause to its failure value, so
      // the typed error survives the round trip; anything else is not ours
      // to classify.
      return cause as ArchiveError;
    },
  });

/** Zip's mtime encoding: packed DOS date and time fields, whose epoch is 1980. */
interface DosStamp {
  readonly time: number;
  readonly date: number;
}

const dosStamp = (date: Date): DosStamp => {
  const year = Math.max(date.getUTCFullYear(), 1980);
  return {
    time: (date.getUTCHours() << 11) | (date.getUTCMinutes() << 5) | (date.getUTCSeconds() >>> 1),
    date: ((year - 1980) << 9) | ((date.getUTCMonth() + 1) << 5) | date.getUTCDate(),
  };
};

interface Central {
  readonly name: Uint8Array;
  readonly crc: number;
  readonly size: number;
  /** Unix mode, which zip carries in the high half of the external attributes. */
  readonly mode: number;
  readonly offset: number;
}

/** UTF-8 names, stored (method 0), sizes known up front — no data descriptor. */
const FLAGS = 0x0800;

const localHeader = (record: Central, stamp: DosStamp): Uint8Array => {
  const bytes = new Uint8Array(30 + record.name.length);
  const view = new DataView(bytes.buffer);
  view.setUint32(0, 0x0403_4b50, true);
  view.setUint16(4, 20, true);
  view.setUint16(6, FLAGS, true);
  view.setUint16(8, 0, true);
  view.setUint16(10, stamp.time, true);
  view.setUint16(12, stamp.date, true);
  view.setUint32(14, record.crc, true);
  view.setUint32(18, record.size, true);
  view.setUint32(22, record.size, true);
  view.setUint16(26, record.name.length, true);
  view.setUint16(28, 0, true);
  bytes.set(record.name, 30);
  return bytes;
};

const centralDirectory = (
  records: ReadonlyArray<Central>,
  offset: number,
  stamp: DosStamp,
): ReadonlyArray<Uint8Array> => {
  const chunks: Uint8Array[] = [];
  let size = 0;

  for (const record of records) {
    const bytes = new Uint8Array(46 + record.name.length);
    const view = new DataView(bytes.buffer);
    view.setUint32(0, 0x0201_4b50, true);
    // "made by" 3.0 on Unix: what tells an extractor the external attributes
    // hold a unix mode, and therefore an executable bit worth restoring.
    view.setUint16(4, 0x031e, true);
    view.setUint16(6, 20, true);
    view.setUint16(8, FLAGS, true);
    view.setUint16(10, 0, true);
    view.setUint16(12, stamp.time, true);
    view.setUint16(14, stamp.date, true);
    view.setUint32(16, record.crc, true);
    view.setUint32(20, record.size, true);
    view.setUint32(24, record.size, true);
    view.setUint16(28, record.name.length, true);
    view.setUint32(
      38,
      ((record.mode << 16) >>> 0) | ((record.mode & 0o40000) !== 0 ? 0x10 : 0),
      true,
    );
    view.setUint32(42, record.offset, true);
    bytes.set(record.name, 46);
    chunks.push(bytes);
    size += bytes.length;
  }

  const end = new Uint8Array(22);
  const view = new DataView(end.buffer);
  view.setUint32(0, 0x0605_4b50, true);
  view.setUint16(8, records.length, true);
  view.setUint16(10, records.length, true);
  view.setUint32(12, size, true);
  view.setUint32(16, offset, true);
  chunks.push(end);

  return chunks;
};

/**
 * Store-only (method 0).
 *
 * Deflate would be the smaller archive, but it would also be a second
 * compressor written by hand next to the gzip path, and `CompressionStream`
 * cannot produce a raw deflate member per entry without buffering it — the one
 * thing this module exists to avoid.
 */
const zipStream = (
  repository: Reader,
  plan: ReadonlyArray<Entry>,
  mtime: Date,
): Stream.Stream<Uint8Array, ArchiveError> =>
  Stream.suspend(() => {
    const stamp = dosStamp(mtime);
    const records: Central[] = [];
    let offset = 0;

    const entryChunks = (entry: Entry) =>
      Effect.gen(function* () {
        const data =
          entry.kind === "directory" ? new Uint8Array(0) : yield* repository.readBlob(entry.oid);
        const mode =
          entry.kind === "directory"
            ? 0o40755
            : entry.mode === "120000"
              ? 0o120777
              : entry.mode === "100755"
                ? 0o100755
                : 0o100644;

        const record: Central = {
          name: encoder.encode(entry.path),
          crc: crc32(data),
          size: data.length,
          mode,
          offset,
        };
        records.push(record);

        const header = localHeader(record, stamp);
        offset += header.length + data.length;
        return data.length === 0 ? [header] : [header, data];
      });

    return Stream.fromIterable(plan).pipe(
      Stream.flatMap((entry) =>
        Stream.unwrap(entryChunks(entry).pipe(Effect.map(Stream.fromIterable))),
      ),
      // Suspended so the records exist by the time it is pulled: the central
      // directory is the one part that cannot be written before the entries.
      Stream.concat(
        Stream.suspend(() => Stream.fromIterable(centralDirectory(records, offset, stamp))),
      ),
    );
  });

/**
 * An archive of `tree`.
 *
 * `mtime` defaults to the epoch so the same tree produces byte-identical
 * archives — a checksum published next to a download has to keep matching.
 */
export const archive = Effect.fn("Archive.archive")(function* (input: {
  readonly tree: Oid;
  readonly format: Format;
  readonly prefix?: string;
  readonly mtime?: Date;
}) {
  const repository = yield* Repository;
  // Paths and oids only: the list is bounded by the tree's shape, not by its
  // content, and every blob is still read lazily below.
  const files = yield* repository.listFiles(input.tree);
  const plan = planOf(files, normalizePrefix(input.prefix));
  const mtime = input.mtime ?? new Date(0);

  const stream: Stream.Stream<Uint8Array, ArchiveError> =
    input.format === "zip"
      ? zipStream(repository, plan, mtime)
      : tarStream(repository, plan, Math.floor(mtime.getTime() / 1000));

  return input.format === "tar.gz" ? gzip(stream) : stream;
});

const failure = (status: number, message: string): Response =>
  new Response(`${message}\n`, {
    status,
    headers: { "content-type": "text/plain; charset=utf-8" },
  });

/** The tree an archive request names: a ref, tag or commit, peeled to a tree. */
const treeOf = (
  repository: typeof Repository.Service,
  start: Oid,
): Effect.Effect<Oid | null, ArchiveError> =>
  Effect.gen(function* () {
    let current = start;
    // Bounded by the tag chain's length; a tag of a tag of a commit is legal.
    for (let step = 0; step < 16; step++) {
      const object = yield* repository.readObject(current);
      if (object.type === "tree") return current;
      if (object.type === "commit") {
        current = (yield* repository.readCommit(current)).tree;
        continue;
      }
      if (object.type === "tag") {
        current = (yield* repository.readTag(current)).object;
        continue;
      }
      return null;
    }
    return null;
  });

/**
 * Route `GET …/archive/<name>`; `null` means "not an archive request", so a
 * host can try the next handler.
 *
 * `prefix` overrides the directory every entry sits under inside the archive.
 * The default is `<name>` without its extension, which is what makes an
 * unpacked `myrepo-v1.tar.gz` leave one `myrepo-v1/` behind rather than
 * scatter its contents over the current directory.
 */
export const handle = Effect.fn("Archive.handle")(
  function* (request: Request, options?: { readonly prefix?: string }) {
    const url = new URL(request.url);
    const segments = url.pathname.split("/").filter((segment) => segment !== "");

    const at = segments.lastIndexOf("archive");
    const name = at === -1 ? undefined : segments[at + 1];
    if (name === undefined || at + 2 !== segments.length) return null;

    const format = formatOf(name);
    if (format === null) return null;
    if (request.method !== "GET") return failure(405, `unsupported method ${request.method}`);

    const repository = yield* Repository;
    const ref = url.searchParams.get("ref") ?? "HEAD";

    const resolved = yield* repository.resolve(ref);
    if (resolved === null) return failure(404, `unknown ref '${ref}'`);

    const root = yield* treeOf(repository, resolved);
    if (root === null) return failure(404, `'${ref}' does not name a tree`);

    const path = url.searchParams.get("path") ?? "";
    const tree = yield* Effect.gen(function* () {
      if (path === "") return root;
      const entry = yield* repository.findPath(root, path);
      // Only a directory can become an archive; a blob would have no entries.
      return entry === null || entry.mode !== "40000" ? null : entry.oid;
    });
    if (tree === null) return failure(404, `no such directory '${path}'`);

    const stream = yield* archive({
      tree,
      format,
      prefix: options?.prefix ?? stemOf(name),
    });

    return new Response(Stream.toReadableStream(stream), {
      headers: {
        "content-type": CONTENT_TYPES[format],
        "content-disposition": `attachment; filename="${name.replace(/"/g, "")}"`,
      },
    });
  },
  Effect.catchTag("ObjectNotFound", (error) =>
    Effect.succeed(failure(statusOf(error), `unknown object ${error.oid}`)),
  ),
);
