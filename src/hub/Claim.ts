/**
 * Who owns a record on a trace ref, and whether it belongs to this repository.
 *
 * Two questions, asked in five places between them, and each place used to
 * answer them for itself. That is where this module comes from: over fifty
 * review rounds the same two defects came back six and four times, always in
 * the shape of one call site knowing something the others did not.
 *
 * **Ownership.** A trace ref carries context exposures and runtime telemetry,
 * read by two functions that must partition the ref between them. Every
 * variant of the same bug was a record neither claimed, or one both did:
 *
 * - a valid `context-exposure` payload under a `tool-operation` message;
 * - a valid `invocation-telemetry` payload under a `context-exposure` message;
 * - a payload naming a kind of one namespace and malformed for it;
 * - a payload naming no `type` at all;
 * - a commit message naming no type at all;
 * - a commit message naming a type neither namespace knows.
 *
 * Each was closed by adding a case to one reader. The fix is not a sixth case:
 * it is that the partition has to be *total by construction*. `ownerOf` is a
 * function into two values with no third, so a record it is asked about always
 * lands somewhere, and the two readers cannot disagree because they ask the
 * same function.
 *
 * The signed payload decides and the commit message is the fallback. The
 * message is unsigned — whoever may append chooses it freely — but it survives
 * redaction, which is the one state where the payload cannot answer.
 *
 * **Binding.** A record's envelope names the repository and the session it was
 * written for. Replication is not policy-gated, so a ref can carry a record
 * written elsewhere: a peer's, another session's, another repository's. Every
 * reader has to refuse those, and each one grew its own version of the test —
 * `Records.entries` on the session, `Invocation.project` on the repository,
 * `cli/context.removalsOn` on the repository again, `Redaction.tombstonesOn`
 * on both but only in one of its three namespace branches.
 *
 * What a reader does with an unbound record is *not* this module's business,
 * and deliberately so: `gc` must ignore it, an audit must report it rather
 * than drop it in silence, and a fetch must still explain its absence. Those
 * are three different right answers to one fact, which is why the fact is
 * computed here and the policy stays at each call site.
 */
import { Predicate, Result } from "effect";

/** The two namespaces a trace ref carries. */
export type Namespace = "context" | "telemetry";

/** What a Context Exposure's commit message and payload both name it. */
export const EXPOSURE = "context-exposure";

const decoder = new TextDecoder();

/**
 * The `type` a payload gives itself, without running any schema.
 *
 * Parsed rather than searched: JSON permits `\uXXXX` inside a string, so a
 * byte search for the literal misses `"context-exposure"` — which this
 * codebase's own writers never emit and another implementation may.
 */
export const declaredType = (bytes: Uint8Array): string | null => {
  const parsed = Result.try({
    try: (): string | null => {
      const json: unknown = JSON.parse(decoder.decode(bytes));
      if (!Predicate.isObject(json)) return null;
      if (!Predicate.hasProperty(json, "type")) return null;
      const type: unknown = json.type;
      return Predicate.isString(type) ? type : null;
    },
    catch: () => null,
  });
  return Result.isSuccess(parsed) ? parsed.success : null;
};

/**
 * Which namespace owns this record.
 *
 * Total: every record lands in one of the two. Telemetry is the default
 * because a trace ref is its ref — a record nothing else claims is still
 * something the ref holds, and a reader that says so is the difference between
 * an audit somebody can walk around and one they cannot.
 */
export const ownerOf = (record: {
  /** The signed payload, where this replica still holds it. */
  readonly payload?: Uint8Array | undefined;
  /** The commit message's own word for it, which survives redaction. */
  readonly type?: string | null | undefined;
}): Namespace => {
  const said = (record.payload === undefined ? null : declaredType(record.payload)) ?? record.type;
  return said === EXPOSURE ? "context" : "telemetry";
};

/** Whether a record's own envelope names this repository and this ref. */
export const bound = (
  payload: { readonly repo: string; readonly session: string },
  to: { readonly repo?: string | undefined; readonly session?: string | undefined },
): boolean =>
  (to.session === undefined || payload.session === to.session) &&
  (to.repo === undefined || payload.repo === to.repo);
