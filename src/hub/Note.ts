/** Signed append-only lifecycle events for code-anchored repository notes. */
import { DateTime, Effect, Schema } from "effect";

import { NAMESPACE, type PrivateKey, sign } from "../crypto/SshSignature.ts";
import { Invalid } from "../git/Error.ts";
import { TRUST_LOG } from "../git/Refspec.ts";
import { Repository } from "../git/Repository.ts";
import { checkRefName, type Oid } from "../git/Store.ts";
import { validatePath } from "../git/Work.ts";
import * as Event from "./Event.ts";
import { Fingerprint } from "./Anchor.ts";
import * as Secrets from "./Secrets.ts";
import * as Tombstone from "./Tombstone.ts";

export const Baseline = Fingerprint;
export interface Baseline extends Schema.Schema.Type<typeof Baseline> {}

export const refOf = (note: string): string => `refs/hub/note/${note}`;

export const noteOf = (ref: string): string | null => {
  const prefix = "refs/hub/note/";
  if (!ref.startsWith(prefix)) return null;
  const id = ref.slice(prefix.length);
  return id.length === 0 || id.includes("/") ? null : id;
};

export const isNoteId = (id: string): boolean =>
  id.length > 0 && id.length <= 128 && !id.includes("/") && checkRefName(refOf(id)) === null;

export const newId = Event.newId;

/**
 * Whether a path query names this path.
 *
 * `src/auth.ts` matches itself; `src/auth/` matches everything beneath it; no
 * query matches everything. Here rather than in `NoteAudit`, which is where it
 * started: a listing needs the predicate and nothing else from that module,
 * and importing it there pulled `WorkTree`, `History` and `Diff` into every
 * host that only lists notes.
 */
export const covers = (query: string | undefined, path: string): boolean => {
  const prefix = query?.replace(/\/+$/, "") ?? "";
  return prefix === "" || path === prefix || path.startsWith(`${prefix}/`);
};

export const notes = Effect.fn("hub.Note.notes")(function* () {
  const repository = yield* Repository;
  const found: string[] = [];
  for (const [name] of yield* repository.refs) {
    const id = noteOf(name);
    if (id !== null) found.push(id);
  }
  return found.sort();
});

const envelope = {
  version: Schema.Literal(1),
  repo: Schema.String,
  note: Schema.String,
  id: Schema.String,
  issuedAt: Schema.String,
  trustHead: Schema.NullOr(Schema.String),
};

export const NoteCreated = Schema.Struct({
  type: Schema.tag("note.created"),
  ...envelope,
  path: Schema.String,
  anchor: Schema.String,
  text: Schema.String,
  baseline: Schema.NullOr(Baseline),
  pinned: Schema.Boolean,
});
export const NoteConfirmed = Schema.Struct({
  type: Schema.tag("note.confirmed"),
  ...envelope,
  baseline: Baseline,
});
export const NoteReplaced = Schema.Struct({
  type: Schema.tag("note.replaced"),
  ...envelope,
  text: Schema.String,
  baseline: Baseline,
});
export const NoteRetired = Schema.Struct({
  type: Schema.tag("note.retired"),
  ...envelope,
  reason: Schema.NullOr(Schema.String),
});
export const NoteRestored = Schema.Struct({
  type: Schema.tag("note.restored"),
  ...envelope,
  baseline: Schema.NullOr(Baseline),
});
export const NotePinned = Schema.Struct({ type: Schema.tag("note.pinned"), ...envelope });
export const NoteUnpinned = Schema.Struct({ type: Schema.tag("note.unpinned"), ...envelope });

/** A record removed from this note; see `Task`'s own and `Tombstone.ts`. */
export const RecordRedacted = Schema.Struct({
  type: Schema.tag("event.redacted"),
  ...envelope,
  ...Tombstone.fields,
});

export const NotePayload = Schema.Union([
  NoteCreated,
  NoteConfirmed,
  NoteReplaced,
  NoteRetired,
  NoteRestored,
  NotePinned,
  NoteUnpinned,
  RecordRedacted,
]).pipe(Schema.toTaggedUnion("type"));
export type NotePayload = typeof NotePayload.Type;
export type NoteCreated = typeof NoteCreated.Type;

const decodePayload = Schema.decodeUnknownEffect(NotePayload);
const encoder = new TextEncoder();

export const encode = (payload: NotePayload): Uint8Array => {
  const { id, issuedAt, note, repo, trustHead, type, version, ...rest } = payload;
  return encoder.encode(
    `${JSON.stringify({ version, type, repo, note, id, issuedAt, trustHead, ...rest }, null, 2)}\n`,
  );
};

export const decode = Effect.fn("hub.Note.decode")(function* (bytes: Uint8Array) {
  const json = yield* Effect.try({
    try: () => JSON.parse(new TextDecoder().decode(bytes)),
    catch: () => new Invalid({ field: "note", reason: "note event is not valid JSON" }),
  });
  return yield* decodePayload(json).pipe(
    Effect.mapError(
      (issue) => new Invalid({ field: "note", reason: `malformed note event: ${issue.message}` }),
    ),
  );
});

export const context = Effect.fn("hub.Note.context")(function* (repo: string, note: string) {
  const repository = yield* Repository;
  return {
    version: 1,
    repo,
    note,
    id: newId(),
    issuedAt: DateTime.formatIso(yield* DateTime.now),
    trustHead: yield* repository.resolve(TRUST_LOG),
  } as const;
});

const prose = (payload: NotePayload): string => {
  switch (payload.type) {
    case "note.created":
    case "note.replaced":
      return payload.text;
    case "note.retired":
      return payload.reason ?? "";
    case "event.redacted":
      return payload.reason;
    case "note.confirmed":
    case "note.restored":
    case "note.pinned":
    case "note.unpinned":
      return "";
  }
};

export const MAX_PAYLOAD = 256 * 1024;

export const validate = Effect.fn("hub.Note.validate")(function* (payload: NotePayload) {
  if (!isNoteId(payload.note)) {
    return yield* new Invalid({
      field: "note",
      reason: `'${payload.note}' cannot name a note; it must be one ref path component`,
    });
  }
  if (!Number.isFinite(Date.parse(payload.issuedAt))) {
    return yield* new Invalid({ field: "issuedAt", reason: `'${payload.issuedAt}' is not a time` });
  }
  if (payload.type === "note.created") {
    yield* validatePath(payload.path);
    if (payload.anchor.trim() === "") {
      return yield* new Invalid({ field: "anchor", reason: "an anchor may not be empty" });
    }
  }
  if (
    (payload.type === "note.created" || payload.type === "note.replaced") &&
    payload.text.trim() === ""
  ) {
    return yield* new Invalid({ field: "text", reason: "a note may not be empty" });
  }
});

export const issue = Effect.fn("hub.Note.issue")(function* (payload: NotePayload, key: PrivateKey) {
  yield* validate(payload);
  const bytes = encode(payload);
  if (bytes.length > MAX_PAYLOAD) {
    return yield* new Invalid({
      field: "note",
      reason: `a note record may not exceed ${MAX_PAYLOAD} bytes; this one is ${bytes.length}`,
    });
  }
  const leaked = Secrets.scan(prose(payload));
  if (leaked.length > 0) {
    return yield* new Invalid({
      field: "note",
      reason: `this note looks like it carries ${leaked.map((item) => `a ${item.kind} (${item.hint})`).join(", ")}`,
    });
  }
  const signature = yield* sign(key, bytes, NAMESPACE);
  return yield* Event.appendTo({
    ref: refOf(payload.note),
    message: `${payload.type} ${payload.id}\n`,
    payload: bytes,
    signatures: [signature],
  });
});

export const create = Effect.fn("hub.Note.create")(function* (input: {
  readonly repo: string;
  readonly path: string;
  readonly anchor: string;
  readonly text: string;
  readonly baseline: Baseline | null;
  readonly key: PrivateKey;
  readonly pinned?: boolean;
  readonly note?: string;
}) {
  const note = input.note ?? newId();
  const base = yield* context(input.repo, note);
  const commit = yield* issue(
    {
      ...base,
      type: "note.created",
      path: input.path,
      anchor: input.anchor,
      text: input.text,
      baseline: input.baseline,
      pinned: input.pinned ?? false,
    },
    input.key,
  );
  return { note, commit };
});

export const confirm = Effect.fn("hub.Note.confirm")(function* (input: {
  readonly repo: string;
  readonly note: string;
  readonly baseline: Baseline;
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.note);
  return yield* issue({ ...base, type: "note.confirmed", baseline: input.baseline }, input.key);
});

export const replace = Effect.fn("hub.Note.replace")(function* (input: {
  readonly repo: string;
  readonly note: string;
  readonly text: string;
  readonly baseline: Baseline;
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.note);
  return yield* issue(
    { ...base, type: "note.replaced", text: input.text, baseline: input.baseline },
    input.key,
  );
});

export const retire = Effect.fn("hub.Note.retire")(function* (input: {
  readonly repo: string;
  readonly note: string;
  readonly reason?: string | null;
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.note);
  return yield* issue({ ...base, type: "note.retired", reason: input.reason ?? null }, input.key);
});

export const restore = Effect.fn("hub.Note.restore")(function* (input: {
  readonly repo: string;
  readonly note: string;
  readonly baseline: Baseline | null;
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.note);
  return yield* issue({ ...base, type: "note.restored", baseline: input.baseline }, input.key);
});

export const setPinned = Effect.fn("hub.Note.setPinned")(function* (input: {
  readonly repo: string;
  readonly note: string;
  readonly pinned: boolean;
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.note);
  return yield* issue({ ...base, type: input.pinned ? "note.pinned" : "note.unpinned" }, input.key);
});

/**
 * Remove one record's content from this note; see `Task.redact`.
 *
 * §28 makes note text untrusted data that a repository may need to take back
 * — a constraint written with a credential quoted into it is exactly the case
 * — and the way back is the same tombstone every other hub namespace uses:
 * the payload blob goes, the commit and its place in the DAG stay, and the
 * projection reads the gap rather than a rewritten history.
 */
export const redact = Effect.fn("hub.Note.redact")(function* (input: {
  readonly repo: string;
  readonly note: string;
  readonly target: string;
  readonly reason: string;
  readonly key: PrivateKey;
}) {
  yield* Tombstone.permitted(input.key);

  const walked = yield* entries(input.note);
  const claimants = walked.events.filter(({ payload }) => payload.id === input.target);
  const target = claimants[0];
  if (target === undefined) {
    return yield* new Invalid({
      field: "target",
      reason: `${input.note} has no record ${input.target}`,
    });
  }
  if (claimants.length > 1) {
    return yield* new Invalid({
      field: "target",
      reason: `${input.note} has ${claimants.length} records claiming ${input.target}`,
    });
  }
  if (target.payload.type === "event.redacted") {
    return yield* new Invalid({
      field: "target",
      reason: "a tombstone is the record of a removal and is not itself removable",
    });
  }

  const base = yield* context(input.repo, input.note);
  return yield* issue(
    {
      ...base,
      type: "event.redacted",
      target: input.target,
      targetCommit: Event.qualify(target.commit),
      reason: input.reason,
    },
    input.key,
  );
});

export const entries = Effect.fn("hub.Note.entries")(function* (note: string) {
  const walked = yield* Event.walk(refOf(note), decode);
  return {
    events: walked.records.map((record) => ({
      commit: record.commit,
      payload: record.payload,
      bytes: record.bytes,
      signatures: record.signatures,
    })),
    parents: walked.parents,
    ordered: walked.ordered,
    unreadable: walked.unreadable,
  } as const;
});

export interface Entry {
  readonly commit: Oid;
  readonly payload: NotePayload;
  readonly bytes: Uint8Array;
  readonly signatures: ReadonlyArray<string>;
}
