/**
 * Bulk commit over NDJSON.
 *
 *   POST …/commit-pack     one JSON object per line, in order
 *
 * The JSON commit endpoint wants the whole commit in one payload — every file,
 * base64-encoded, inside a single string. That is the shape that cannot scale:
 * a hundred-megabyte commit becomes a hundred-megabyte request body, decoded
 * into a hundred-megabyte JavaScript string, before a single blob is written.
 * NDJSON moves the boundary: the client sends bounded chunks and the server
 * writes each file as it finishes, so what is held is one file's bytes rather
 * than the commit's.
 *
 * The line protocol, in the order the lines must arrive:
 *
 *   {"type":"commit","branch":"main","message":"…","author":{…},"expected":"<oid>|null"}
 *   {"type":"file","path":"a/b.txt","mode":"100644"}   begins a file
 *   {"type":"chunk","data":"<base64>"}                 zero or more, in order
 *   {"type":"end"}                                     ends the current file
 *   {"type":"delete","path":"old.txt"}                 removes a path
 *   {"type":"done"}                                    commit now
 *
 * `commit` comes exactly once and first: it names the branch whose tree the
 * commit builds on, so nothing can be applied before it is read. `branch`
 * defaults to `main`, `expected` is the compare-and-swap — absent appends to
 * the branch, `null` demands it not exist, an oid demands that exact tip.
 * Every `chunk` is decoded on its own, so a client may cut a file at any byte
 * boundary it likes; the base64 of each slice is independently padded.
 *
 * The ref is moved only after the body has been drained, which is what makes
 * every rejection below leave the branch exactly where it was. Blobs and trees
 * written before a rejection stay in the object store, unreachable, for `gc` —
 * git's own push does the same, and the alternative is buffering the commit.
 */
import { Data, Effect, Predicate, Stream } from "effect";

import type { Invalid, ObjectNotFound, RefConflict, StorageFailure } from "../git/Error.ts";
import { EMPTY_TREE_OID, type Signature } from "../git/Format.ts";
import { Repository } from "../git/Repository.ts";
import { isOid, type Oid } from "../git/Store.ts";
import * as Policy from "./Policy.ts";

/**
 * What one record, and one file, may hold.
 *
 * Both are bounds on memory rather than on usefulness: a body streams, but a
 * record that never ends and a file whose chunks are never released are each
 * a way to hold all of it at once — which is what streaming was for.
 */
const MAX_LINE = 8 * 1024 * 1024;
const MAX_FILE = 64 * 1024 * 1024;

/** The only failure this module raises, carrying the status it becomes. */
class Rejected extends Data.TaggedError("Rejected")<{
  readonly status: number;
  readonly message: string;
}> {}

/** Every body this endpoint answers: the commit it made, or why it would not. */
type Reply =
  | { readonly oid: Oid; readonly tree: Oid; readonly files: number }
  | { readonly error: string };

const json = (value: Reply, status = 200): Response =>
  new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json", "cache-control": "no-cache" },
  });

const failure = (status: number, message: string): Response => json({ error: message }, status);

/**
 * A repository failure as a status the client can act on. `Invalid` is 422
 * rather than 400 because the line parsed fine — it named a path or a mode the
 * repository will not take.
 */
const rejected = <A, R>(
  effect: Effect.Effect<A, ObjectNotFound | RefConflict | StorageFailure | Invalid, R>,
): Effect.Effect<A, Rejected, R> =>
  effect.pipe(
    Effect.catchTags({
      Invalid: (error) =>
        Effect.fail(new Rejected({ status: 422, message: `${error.field}: ${error.reason}` })),
      RefConflict: (error) =>
        Effect.fail(
          new Rejected({
            status: 409,
            message: `${error.ref} is at ${error.actual ?? "nothing"}, not ${
              error.expected ?? "nothing"
            }`,
          }),
        ),
      ObjectNotFound: (error) =>
        Effect.fail(new Rejected({ status: 404, message: `unknown object ${error.oid}` })),
      StorageFailure: () =>
        Effect.fail(new Rejected({ status: 500, message: "could not write to the repository" })),
    }),
  );

/**
 * What `JSON.parse` can hand back. Naming the domain keeps every field read
 * below a real value, never an unexamined `unknown`.
 */
type Json = string | number | boolean | null | ReadonlyArray<Json> | JsonRecord;

/** One protocol line, parsed but not yet interpreted: fields are read one by one. */
interface JsonRecord {
  readonly [field: string]: Json;
}

/** Among JSON values, the non-null non-array objects are exactly the records. */
const isJsonRecord = (value: Json | undefined): value is JsonRecord => Predicate.isObject(value);

/** `null` rather than a throw: a bad line is an answer, not an exception. */
const parseLine = (line: string): JsonRecord | null => {
  try {
    const value: Json = JSON.parse(line);
    return isJsonRecord(value) ? value : null;
  } catch {
    return null;
  }
};

const decodeBase64 = (data: string): Uint8Array | null => {
  try {
    const binary = atob(data);
    const bytes = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index++) bytes[index] = binary.charCodeAt(index);
    return bytes;
  } catch {
    return null;
  }
};

const concat = (chunks: ReadonlyArray<Uint8Array>, size: number): Uint8Array => {
  const bytes = new Uint8Array(size);
  let at = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, at);
    at += chunk.length;
  }
  return bytes;
};

const signatureOf = (value: Json | undefined): Signature => {
  const wire: JsonRecord = isJsonRecord(value) ? value : {};
  const at = Predicate.isString(wire.at) ? new Date(wire.at) : new Date();
  return {
    name: Predicate.isString(wire.name) ? wire.name : "Anonymous",
    email: Predicate.isString(wire.email) ? wire.email : "anonymous@example.com",
    at: Number.isNaN(at.getTime()) ? new Date() : at,
    offset: Predicate.isNumber(wire.offset) ? wire.offset : 0,
  };
};

interface Header {
  readonly branch: string;
  readonly message: string;
  readonly author: Signature;
  /** Absent appends; `null` demands the branch not exist. */
  readonly expected?: Oid | null;
}

interface OpenFile {
  readonly path: string;
  readonly mode: string;
  readonly chunks: Uint8Array[];
  size: number;
}

/**
 * Everything the parser carries between lines, in one value rather than a
 * handful of closed-over bindings — the loop writes it from inside a callback,
 * and a record's fields survive that where a narrowed `let` would not.
 */
interface State {
  header: Header | null;
  open: OpenFile | null;
  /** Rebuilt after every file, which is what lets the finished bytes go. */
  tree: Oid;
  files: number;
  /** `done` seen. The commit happens after the body, never on this line. */
  closed: boolean;
}

const bad = (message: string) => Effect.fail(new Rejected({ status: 400, message }));

const unprocessable = (message: string) => Effect.fail(new Rejected({ status: 422, message }));

const pack = Effect.fn("CommitPack.pack")(function* (request: Request) {
  const repository = yield* Repository;

  const body = request.body;
  const bytes: Stream.Stream<Uint8Array, Rejected> =
    body === null
      ? Stream.empty
      : Stream.fromReadableStream({
          evaluate: () => body,
          onError: () => new Rejected({ status: 400, message: "the request body ended abruptly" }),
        });

  const state: State = {
    header: null,
    open: null,
    tree: EMPTY_TREE_OID,
    files: 0,
    closed: false,
  };

  const onLine = (raw: string) =>
    Effect.gen(function* () {
      // Whitespace between records is framing, not a record.
      const line = raw.trim();
      if (line === "") return;
      if (state.closed) return yield* bad("a record followed 'done'");

      const record = parseLine(line);
      if (record === null) return yield* bad("a line is not a JSON object");

      const type = record.type;
      // A non-string `type` shows as its JSON in the rejections below, rather
      // than as an object's default stringification.
      const label = Predicate.isString(type) ? type : JSON.stringify(type);
      if (type !== "commit" && state.header === null) {
        return yield* bad(`'${label}' arrived before the commit header`);
      }

      switch (type) {
        case "commit": {
          if (state.header !== null) return yield* bad("a second commit header");

          const branch =
            Predicate.isString(record.branch) && record.branch !== "" ? record.branch : "main";
          const expected = record.expected;
          if (
            expected !== undefined &&
            expected !== null &&
            !(Predicate.isString(expected) && isOid(expected))
          ) {
            return yield* bad("'expected' is neither an oid nor null");
          }

          // The base tree is read now rather than at `done`: every file is
          // applied on top of it as it arrives, so it has to exist first.
          const ref = branch.startsWith("refs/") ? branch : `refs/heads/${branch}`;

          // And the boundary is crossed *here*, before a byte of the body is
          // written. This endpoint takes a ref name from its caller and moves
          // it, which is every rule the boundary exists for — branch
          // protection, genesis immutability, hub append-only, `policy.write`
          // — and asked at `done` it was asked too late: the blobs and trees
          // had already been written by `writeFiles` as they streamed, so a
          // caller the boundary refused had still put arbitrary content into
          // the object store. The branch is known from the first record, so
          // there is nothing to wait for.
          const refusal = yield* Policy.gateWrite(ref).pipe(
            Effect.orElseSucceed(() => "the repository's policy could not be evaluated"),
          );
          if (refusal !== null) return yield* bad(refusal);

          const tip = yield* rejected(repository.resolve(ref));
          state.tree =
            tip === null ? EMPTY_TREE_OID : (yield* rejected(repository.readCommit(tip))).tree;

          // The rejection above leaves `expected` an oid, `null`, or absent —
          // exactly the three states the header's compare-and-swap field has.
          state.header = {
            branch,
            message: Predicate.isString(record.message) ? record.message : "",
            author: signatureOf(record.author),
            // The tip the tree was snapshotted from. Committing without it
            // would parent this tree on whatever arrived while the body was
            // still streaming, silently reverting that commit's files — so a
            // caller who named no expectation still gets the one implied by
            // the tree they are sending.
            //
            // SAFETY: the rejection above leaves `expected` an oid, `null`, or
            // absent, so what is left here is exactly `Oid | null`.
            expected: expected === undefined ? tip : (expected as Oid | null),
          };
          return;
        }

        case "file": {
          const open = state.open;
          if (open !== null) return yield* bad(`'file' while '${open.path}' is still open`);
          if (!Predicate.isString(record.path) || record.path === "") {
            return yield* unprocessable("'file' needs a non-empty path");
          }
          state.open = {
            path: record.path,
            mode: Predicate.isString(record.mode) ? record.mode : "100644",
            chunks: [],
            size: 0,
          };
          return;
        }

        case "chunk": {
          const open = state.open;
          if (open === null) return yield* bad("'chunk' outside a file");
          if (!Predicate.isString(record.data)) return yield* bad("'chunk' needs base64 'data'");
          // Each chunk is padded base64 in its own right, so the client may
          // cut a file at any byte it likes and the pieces still concatenate.
          const decoded = decodeBase64(record.data);
          if (decoded === null) return yield* bad("'chunk' data is not base64");
          open.chunks.push(decoded);
          open.size += decoded.length;
          // Per record *and* per file: bounding the record alone lets a
          // hundred well-formed chunks hold a gigabyte before `end` releases
          // them, which is the buffering this endpoint exists to avoid.
          if (open.size > MAX_FILE) {
            return yield* bad(`'${open.path}' exceeds the ${MAX_FILE}-byte limit for one file`);
          }
          return;
        }

        case "end": {
          const open = state.open;
          if (open === null) return yield* bad("'end' without a file");
          // One `writeFiles` per file rather than one for the whole commit:
          // the trees on this path are rewritten each time, and in exchange
          // every finished file's bytes are released instead of held to `done`.
          state.tree = yield* rejected(
            repository.writeFiles({
              base: state.tree,
              changes: [
                { path: open.path, content: concat(open.chunks, open.size), mode: open.mode },
              ],
            }),
          );
          state.files++;
          state.open = null;
          return;
        }

        case "delete": {
          const open = state.open;
          if (open !== null) return yield* bad(`'delete' while '${open.path}' is still open`);
          if (!Predicate.isString(record.path) || record.path === "") {
            return yield* unprocessable("'delete' needs a non-empty path");
          }
          state.tree = yield* rejected(
            repository.writeFiles({
              base: state.tree,
              changes: [{ path: record.path, content: null }],
            }),
          );
          return;
        }

        case "done": {
          const open = state.open;
          if (open !== null) return yield* bad(`'done' while '${open.path}' is still open`);
          state.closed = true;
          return;
        }

        default:
          return yield* bad(`unknown record type '${label}'`);
      }
    });

  /**
   * The bound has to be applied to the bytes, not to the lines.
   *
   * `splitLines` accumulates until it finds a newline, so checking the line it
   * eventually emits cannot prevent the buffering it was meant to prevent — a
   * body containing no newline at all is held whole first. Counting bytes
   * since the last newline is the only place the limit actually bites.
   */
  let sinceNewline = 0;
  const bounded = bytes.pipe(
    Stream.mapEffect((chunk: Uint8Array) => {
      const newline = chunk.lastIndexOf(0x0a);
      sinceNewline = newline === -1 ? sinceNewline + chunk.length : chunk.length - newline - 1;
      return sinceNewline > MAX_LINE
        ? bad(`a record may not exceed ${MAX_LINE} bytes; send the content in chunks`)
        : Effect.succeed(chunk);
    }),
  );

  yield* Stream.runForEach(Stream.splitLines(Stream.decodeText(bounded)), onLine);

  const header = state.header;
  const open = state.open;
  if (header === null) return yield* bad("no commit header");
  if (open !== null) return yield* bad(`'${open.path}' was never ended`);
  // A truncated upload is otherwise indistinguishable from a complete one, and
  // committing half a push is the one outcome worth refusing outright.
  if (!state.closed) return yield* bad("the body ended before 'done'");

  const oid = yield* rejected(
    repository.commit({
      branch: header.branch,
      tree: state.tree,
      message: header.message,
      author: header.author,
      // `commit` reads a missing `expected` and an undefined one the same
      // way: both say "wherever the branch happens to be".
      expected: header.expected,
    }),
  );

  return json({ oid, tree: state.tree, files: state.files });
});

/**
 * Route a commit-pack request whose repository the caller has already
 * resolved. `null` means "not a commit-pack request", so a host can try the
 * next handler — the same contract as `Lfs.handle`.
 */
export const handle = (request: Request): Effect.Effect<Response | null, never, Repository> =>
  Effect.suspend(() => {
    const segments = new URL(request.url).pathname.split("/").filter((part) => part !== "");
    if (segments.at(-1) !== "commit-pack") return Effect.succeed(null);
    if (request.method !== "POST") {
      return Effect.succeed(failure(405, "commit-pack requires POST"));
    }

    return pack(request).pipe(
      Effect.catch((error) => Effect.succeed(failure(error.status, error.message))),
    );
  });
