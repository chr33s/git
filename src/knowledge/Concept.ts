/**
 * Knowledge Concepts: OKF-compatible Markdown, read without being rewritten.
 *
 * ```text
 * .gitplus/knowledge/**.md      the bundle, ordinary source files
 *       ↓
 *   bounded parse                ← this module
 *       ↓
 *   portable metadata + Git+ provenance extension
 *       ↓
 *   Check.ts                     what still holds, dimension by dimension
 * ```
 *
 * Two rules run through everything here. **The file is the artifact**: this
 * module reads and never writes, so a Concept's blob oid stays the identity of
 * its exact bytes and a historical audit can always go back to them rather
 * than to a reserialized parse result (docs/context-pack.knowledge.md §5.2).
 * **Nothing in a Concept is authority**: `verified`, `status`, an actor
 * string, a URL, an `executor` — all of it is data. Nothing here fetches,
 * executes, or follows anything (§5.2, §13).
 *
 * The YAML accepted is a deliberate subset: block mappings, block sequences,
 * plain and quoted scalars, and flow collections of scalars. Anchors, aliases
 * and tags are refused rather than resolved, because the constructs that make
 * a YAML parser dangerous are exactly the ones a Concept has no need for — and
 * a duplicate mapping key is refused too, since a later key silently changing
 * the meaning of a signed reference is the failure §5.2 names.
 *
 * Timestamps are kept as authored strings. `stale_after: 2026-12-31` is a
 * date, not an instant, and coercing it to midnight somewhere would invent a
 * deadline the author did not write (§5.3).
 */
import { Predicate, Result } from "effect";

/** The default bundle location; a caller may name another under the view. */
export const BUNDLE = ".gitplus/knowledge";

/**
 * The upstream format this parser profiles, pinned to a commit.
 *
 * A moving `main` is not a format (§5.1). This is the revision the conformance
 * fixtures were read against, and it is recorded in every report so a reader
 * can tell which interpretation produced it.
 */
export const OKF_REVISION = "62432a095456147ee71e70ac6e4dc0d2dea3ac30";
export const OKF_SPEC_BLOB = "c06e3eede0c910d0ecf12524c34204156f8795ac";
export const OKF_VERSION = "0.2";

/** This parser's own version, which keys any cache of its results. */
export const PARSER_VERSION = "okf-0.2/1";

/**
 * How much of a Concept this reads at all (§14).
 *
 * A proposed initial bound rather than a measured capacity claim: oversized
 * content is reported and excluded from automatic processing, not truncated
 * into a shorter document that reads as complete.
 */
export const MAX_CONCEPT_BYTES = 1024 * 1024;

/** Bounds on the metadata itself, so one hostile document costs one document. */
export const MAX_DEPTH = 16;
export const MAX_SCALAR = 8 * 1024;
export const MAX_NODES = 4096;
export const MAX_FRONTMATTER_BYTES = 128 * 1024;

/** Navigation and history documents, which are not Concepts (§5.1). */
export const NAVIGATION = new Set(["index.md", "log.md"]);

const decoder = new TextDecoder("utf-8", { fatal: false });

// -- diagnostics ----------------------------------------------------------------

export type Severity = "info" | "warning" | "error";

export interface Diagnostic {
  readonly code: string;
  readonly severity: Severity;
  readonly message: string;
  /**
   * Where the finding is, when it is somewhere a restricted reader may see.
   *
   * Spelled `| undefined` rather than merely optional so the absent case is a
   * value this module can pass along: `JSON.stringify` drops it, so an opaque
   * diagnostic and a located one serialize to what each of them means (§6.1).
   */
  readonly path?: string | undefined;
  readonly field?: string | undefined;
}

export const diagnostic = (
  code: string,
  severity: Severity,
  message: string,
  where?: { readonly path?: string; readonly field?: string },
): Diagnostic => ({ code, severity, message, path: where?.path, field: where?.field });

// -- YAML subset ----------------------------------------------------------------

export type Node = string | ReadonlyArray<Node> | { readonly [key: string]: Node };

export interface YamlFailure {
  readonly reason: string;
  readonly line: number;
}

interface Line {
  readonly indent: number;
  readonly text: string;
  readonly number: number;
}

/**
 * Whether a line's content starts a construct this subset refuses outright.
 *
 * Anchors and aliases are how a small document becomes a large one; a tag is
 * how a value becomes a constructor call. None of them appear in an OKF
 * Concept, and none of them are resolved here — refused with a reason is the
 * honest answer, and it keeps `parse` from ever being an evaluator (§5.2).
 */
const refused = (value: string): string | null => {
  const first = value.charAt(0);
  if (first === "&") return "anchors are not read";
  if (first === "*") return "aliases are not read";
  if (first === "!") return "tags are not read";
  return null;
};

/** A scalar as authored: quotes removed, nothing coerced, nothing evaluated. */
const scalar = (raw: string, line: number): Result.Result<string, YamlFailure> => {
  const value = raw.trim();
  if (value.length > MAX_SCALAR) {
    return Result.fail({ reason: `a scalar may not exceed ${MAX_SCALAR} characters`, line });
  }
  if (value === "" || value === "~" || value === "null") return Result.succeed("");

  const refusal = refused(value);
  if (refusal !== null) return Result.fail({ reason: refusal, line });

  if (value.startsWith("'") && value.endsWith("'") && value.length >= 2) {
    return Result.succeed(value.slice(1, -1).replaceAll("''", "'"));
  }
  if (value.startsWith('"') && value.endsWith('"') && value.length >= 2) {
    // The escapes YAML's double-quoted style actually uses in a Concept. An
    // unknown escape keeps its backslash rather than becoming a different
    // character: this is a reader, and inventing bytes the author did not
    // write is the one thing it must not do.
    return Result.succeed(
      value
        .slice(1, -1)
        .replaceAll("\\n", "\n")
        .replaceAll("\\t", "\t")
        .replaceAll('\\"', '"')
        .replaceAll("\\\\", "\\"),
    );
  }

  // A trailing comment on a plain scalar, which YAML separates with
  // whitespace. `sha1:abc # the discovery` is a value and a note about it.
  const comment = value.search(/\s#/u);
  return Result.succeed(comment === -1 ? value : value.slice(0, comment).trim());
};

/** A flow collection of scalars — `[a, b]` or `{a: b}` — and nothing nested. */
const flow = (value: string, line: number): Result.Result<Node, YamlFailure> | null => {
  const inner = value.slice(1, -1).trim();
  if (value.startsWith("[") && value.endsWith("]")) {
    if (inner === "") return Result.succeed([]);
    const members: Array<Node> = [];
    for (const member of split(inner)) {
      const parsed = scalar(member, line);
      if (Result.isFailure(parsed)) return parsed;
      members.push(parsed.success);
    }
    return Result.succeed(members);
  }
  if (value.startsWith("{") && value.endsWith("}")) {
    if (inner === "") return Result.succeed({});
    const mapping: Record<string, Node> = {};
    for (const member of split(inner)) {
      const at = member.indexOf(":");
      if (at === -1) return Result.fail({ reason: "a flow mapping needs 'key: value'", line });
      const key = member.slice(0, at).trim();
      if (Object.hasOwn(mapping, key)) {
        return Result.fail({ reason: `duplicate key '${key}'`, line });
      }
      const parsed = scalar(member.slice(at + 1), line);
      if (Result.isFailure(parsed)) return parsed;
      mapping[key] = parsed.success;
    }
    return Result.succeed(mapping);
  }
  return null;
};

/** Commas outside quotes, which is all the flow forms above need. */
const split = (value: string): ReadonlyArray<string> => {
  const parts: Array<string> = [];
  let quote: string | null = null;
  let start = 0;
  for (let at = 0; at < value.length; at += 1) {
    const character = value.charAt(at);
    if (quote !== null) {
      if (character === quote) quote = null;
      continue;
    }
    if (character === "'" || character === '"') quote = character;
    else if (character === ",") {
      parts.push(value.slice(start, at));
      start = at + 1;
    }
  }
  parts.push(value.slice(start));
  return parts.map((part) => part.trim()).filter((part) => part !== "");
};

/**
 * The frontmatter, as a tree of authored strings.
 *
 * Bounded on every axis a document controls — depth, node count, scalar
 * length — because this runs over content somebody else wrote and the whole
 * point of a bounded parser is that a hostile Concept costs one Concept.
 */
export const parseYaml = (text: string): Result.Result<Node, YamlFailure> => {
  if (text.length > MAX_FRONTMATTER_BYTES) {
    return Result.fail({ reason: "the frontmatter is too large to parse", line: 1 });
  }

  const lines: Array<Line> = [];
  for (const [at, raw] of text.split("\n").entries()) {
    const content = raw.replace(/\t/gu, "  ");
    const trimmed = content.trim();
    if (trimmed === "" || trimmed.startsWith("#")) continue;
    lines.push({
      indent: content.length - content.trimStart().length,
      text: trimmed,
      number: at + 1,
    });
  }
  if (lines.length === 0) return Result.succeed({});

  let nodes = 0;
  let at = 0;

  const block = (indent: number, depth: number): Result.Result<Node, YamlFailure> => {
    if (depth > MAX_DEPTH) {
      return Result.fail({ reason: "the metadata nests too deeply", line: lines[at]?.number ?? 0 });
    }
    if ((nodes += 1) > MAX_NODES) {
      return Result.fail({ reason: "the metadata holds too many nodes", line: 0 });
    }

    const first = lines[at];
    if (first === undefined) return Result.succeed("");
    return first.text.startsWith("- ") || first.text === "-"
      ? sequence(indent, depth)
      : mapping(indent, depth);
  };

  const sequence = (indent: number, depth: number): Result.Result<Node, YamlFailure> => {
    const members: Array<Node> = [];
    while (at < lines.length) {
      const line = lines[at]!;
      if (line.indent < indent || !(line.text === "-" || line.text.startsWith("- "))) break;
      if (line.indent > indent && members.length > 0) {
        return Result.fail({ reason: "inconsistent sequence indentation", line: line.number });
      }
      const rest = line.text === "-" ? "" : line.text.slice(2).trim();

      // `- key: value`, which continues as a mapping at the column the key
      // starts in: the members of a sequence of mappings are the common case
      // in `sources` and `gitplus.evidence`.
      const colon = keyEnd(rest);
      if (rest !== "" && colon !== null) {
        const column = line.indent + 2;
        lines[at] = { indent: column, text: rest, number: line.number };
        const member = mapping(column, depth + 1);
        if (Result.isFailure(member)) return member;
        members.push(member.success);
        continue;
      }

      at += 1;
      if (rest === "") {
        const nested = lines[at];
        if (nested !== undefined && nested.indent > line.indent) {
          const member = block(nested.indent, depth + 1);
          if (Result.isFailure(member)) return member;
          members.push(member.success);
          continue;
        }
        members.push("");
        continue;
      }
      const value = valueOf(rest, line.number);
      if (Result.isFailure(value)) return value;
      members.push(value.success);
    }
    return Result.succeed(members);
  };

  const mapping = (indent: number, depth: number): Result.Result<Node, YamlFailure> => {
    const entries: Record<string, Node> = {};
    while (at < lines.length) {
      const line = lines[at]!;
      if (line.indent < indent) break;
      if (line.indent > indent) {
        return Result.fail({ reason: "unexpected indentation", line: line.number });
      }
      if (line.text === "-" || line.text.startsWith("- ")) break;

      const colon = keyEnd(line.text);
      if (colon === null) {
        return Result.fail({ reason: `'${line.text}' is not 'key: value'`, line: line.number });
      }
      const key = line.text
        .slice(0, colon)
        .trim()
        .replace(/^["']|["']$/gu, "");
      if (key === "") return Result.fail({ reason: "an empty key", line: line.number });
      // Refused, never resolved last-wins: a later key quietly changing what a
      // signed reference points at is the ambiguity §5.2 makes a safety
      // diagnostic.
      if (Object.hasOwn(entries, key)) {
        return Result.fail({ reason: `duplicate key '${key}'`, line: line.number });
      }
      const rest = line.text.slice(colon + 1).trim();
      at += 1;

      if (rest === "" || rest.startsWith("#")) {
        const nested = lines[at];
        if (nested !== undefined && (nested.indent > line.indent || sibling(nested, line))) {
          const value = block(nested.indent, depth + 1);
          if (Result.isFailure(value)) return value;
          entries[key] = value.success;
          continue;
        }
        entries[key] = "";
        continue;
      }

      const value = valueOf(rest, line.number);
      if (Result.isFailure(value)) return value;
      entries[key] = value.success;
    }
    return Result.succeed(entries);
  };

  /** A sequence written at its parent key's own column, which YAML allows. */
  const sibling = (nested: Line, parent: Line): boolean =>
    nested.indent === parent.indent && (nested.text === "-" || nested.text.startsWith("- "));

  const valueOf = (rest: string, line: number): Result.Result<Node, YamlFailure> => {
    const collection = flow(rest, line);
    if (collection !== null) return collection;
    return scalar(rest, line);
  };

  const root = block(lines[0]!.indent, 0);
  if (Result.isFailure(root)) return root;
  if (at < lines.length) {
    return Result.fail({ reason: "trailing content", line: lines[at]!.number });
  }
  return root;
};

/** The colon that ends a key, ignoring one inside quotes or a `sha1:` value. */
const keyEnd = (text: string): number | null => {
  let quote: string | null = null;
  for (let at = 0; at < text.length; at += 1) {
    const character = text.charAt(at);
    if (quote !== null) {
      if (character === quote) quote = null;
      continue;
    }
    if (character === "'" || character === '"') quote = character;
    else if (character === ":") {
      const next = text.charAt(at + 1);
      if (next === "" || next === " ") return at;
    }
  }
  return null;
};

// -- timestamps -----------------------------------------------------------------

export type Instant =
  | { readonly state: "instant"; readonly at: number; readonly text: string }
  | { readonly state: "invalid"; readonly reason: string };

const TIMESTAMP =
  /^(\d{4})-(\d{2})-(\d{2})[Tt ](\d{2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?(Z|z|[+-]\d{2}:\d{2})$/u;

/**
 * An offset-aware instant, or the reason there is not one (§5.3).
 *
 * A date-only value is *invalid here*, not midnight somewhere: converting it
 * would pick a timezone the author never wrote and then report a Concept fresh
 * or stale on the strength of that guess. Calendar validity is checked rather
 * than assumed, so `2026-02-30` cannot become the first of March.
 */
export const instantOf = (value: string): Instant => {
  const match = TIMESTAMP.exec(value.trim());
  if (match === null) {
    return {
      state: "invalid",
      reason: `'${value}' is not an ISO 8601 datetime with an explicit UTC offset`,
    };
  }
  const [, year, month, day, hour, minute, second, zone] = match;
  // Checked on the authored fields, before any offset is applied. Comparing a
  // rolled `Date` back against them instead judged the *UTC* calendar day, so
  // an ordinary offset that crosses midnight — `2026-03-01T00:00:00+05:00` is
  // the last day of February in UTC — was refused as impossible.
  const [y, mo, d, h, mi, sec] = [year, month, day, hour, minute, second ?? "00"].map(Number);
  const days = new Date(Date.UTC(y!, mo!, 0)).getUTCDate();
  if (mo! < 1 || mo! > 12 || d! < 1 || d! > days || h! > 23 || mi! > 59 || sec! > 59) {
    return { state: "invalid", reason: `'${value}' is not a real calendar date` };
  }

  const at = Date.parse(
    `${year}-${month}-${day}T${hour}:${minute}:${second ?? "00"}${zone === "z" ? "Z" : zone}`,
  );
  if (Number.isNaN(at)) return { state: "invalid", reason: `'${value}' is not a real instant` };
  return { state: "instant", at, text: value.trim() };
};

// -- Concept --------------------------------------------------------------------

export interface Source {
  readonly id: string;
  readonly resource: string;
}

export type Evidence =
  | {
      readonly kind: "blob";
      readonly path: string;
      readonly blob: string;
      readonly range?: readonly [number, number];
    }
  | { readonly kind: "gitlink"; readonly path: string; readonly commit: string };

export interface External {
  readonly id: string;
  readonly retrievedAt?: string | undefined;
  readonly contentDigest?: string | undefined;
  readonly snapshot?: string | undefined;
}

/** Lifecycle, as OKF v0.2 has it: absent `status` means `stable`. */
export const LIFECYCLES = ["draft", "stable", "deprecated"] as const;
export type Lifecycle = (typeof LIFECYCLES)[number] | "other";

export interface Concept {
  /** Bundle-relative path with `.md` removed: the portable name (§4.1). */
  readonly id: string;
  /** Repository-relative path under the selected view. */
  readonly path: string;
  readonly type: string;
  readonly title: string | null;
  readonly description: string | null;
  readonly lifecycle: Lifecycle;
  /** The authored status string, whatever it was. */
  readonly status: string | null;
  /** As authored; interpretation is `instantOf`'s, and may fail (§5.3). */
  readonly staleAfter: string | null;
  readonly sources: ReadonlyArray<Source>;
  readonly cites: ReadonlyArray<string>;
  readonly evidence: ReadonlyArray<Evidence>;
  readonly external: ReadonlyArray<External>;
  readonly verificationRecords: ReadonlyArray<string>;
  /** Portable editorial metadata, kept whole and interpreted by nobody. */
  readonly metadata: Node;
  readonly body: string;
}

export type Parsed =
  | {
      readonly ok: true;
      readonly concept: Concept;
      readonly diagnostics: ReadonlyArray<Diagnostic>;
    }
  | { readonly ok: false; readonly diagnostics: ReadonlyArray<Diagnostic> };

const isMapping = (node: Node | undefined): node is { readonly [key: string]: Node } =>
  Predicate.isObject(node) && !Array.isArray(node);

const text = (node: Node | undefined): string | null =>
  Predicate.isString(node) && node !== "" ? node : null;

const members = (node: Node | undefined): ReadonlyArray<Node> =>
  Array.isArray(node) ? node : node === undefined || node === "" ? [] : [node];

/** The portable Concept id for a repository path under a bundle root (§4.1). */
export const idOf = (bundle: string, path: string): string | null => {
  const root = bundle.endsWith("/") ? bundle : `${bundle}/`;
  if (!path.startsWith(root) || !path.endsWith(".md")) return null;
  return path.slice(root.length, -3);
};

/** Whether a bundle path is a Concept rather than navigation or history (§5.1). */
export const isConceptPath = (bundle: string, path: string): boolean => {
  if (idOf(bundle, path) === null) return false;
  const name = path.slice(path.lastIndexOf("/") + 1);
  return !NAVIGATION.has(name);
};

/** The frontmatter block and the body, split on the document's own fences. */
export const frontmatter = (source: string): { yaml: string; body: string } | null => {
  const normalized = source.startsWith("\ufeff") ? source.slice(1) : source;
  if (!normalized.startsWith("---")) return null;
  const opening = normalized.indexOf("\n");
  if (opening === -1 || normalized.slice(3, opening).trim() !== "") return null;

  // Every candidate, not the first: a `----` rule or a `--- x` line inside the
  // frontmatter is not a close fence, and giving up on it reported a document
  // with a perfectly good fence below as having none.
  for (
    let at = normalized.indexOf("\n---", opening);
    at !== -1;
    at = normalized.indexOf("\n---", at + 1)
  ) {
    const after = normalized.indexOf("\n", at + 1);
    const line = after === -1 ? normalized.slice(at + 1) : normalized.slice(at + 1, after);
    if (line.trim() !== "---") continue;
    return {
      yaml: normalized.slice(opening + 1, at),
      body: after === -1 ? "" : normalized.slice(after + 1),
    };
  }
  return null;
};

/**
 * One Concept from its exact bytes.
 *
 * Every failure is a diagnostic with a code rather than an exception: a bundle
 * check reports what is wrong with each document and goes on to the next one,
 * and §13 makes a parse failure "omit from automatic recall; preserve the
 * original file" rather than a reason to fail the whole read.
 */
export const parse = (input: {
  readonly bundle: string;
  readonly path: string;
  readonly bytes: Uint8Array;
}): Parsed => {
  const where = { path: input.path };
  const id = idOf(input.bundle, input.path);
  if (id === null) {
    return {
      ok: false,
      diagnostics: [
        diagnostic("knowledge.path", "error", `${input.path} is not under ${input.bundle}`, where),
      ],
    };
  }
  if (input.bytes.length > MAX_CONCEPT_BYTES) {
    return {
      ok: false,
      diagnostics: [
        diagnostic(
          "knowledge.oversized",
          "error",
          `${input.path} is ${input.bytes.length} bytes; this checker reads at most ${MAX_CONCEPT_BYTES}`,
          where,
        ),
      ],
    };
  }

  const source = decoder.decode(input.bytes);
  const split = frontmatter(source);
  if (split === null) {
    return {
      ok: false,
      diagnostics: [
        diagnostic(
          "knowledge.frontmatter",
          "error",
          "the document has no closed YAML frontmatter block",
          where,
        ),
      ],
    };
  }

  const parsed = parseYaml(split.yaml);
  if (Result.isFailure(parsed)) {
    return {
      ok: false,
      diagnostics: [
        diagnostic(
          "knowledge.yaml",
          "error",
          `line ${parsed.failure.line}: ${parsed.failure.reason}`,
          where,
        ),
      ],
    };
  }
  const metadata = parsed.success;
  if (!isMapping(metadata)) {
    return {
      ok: false,
      diagnostics: [
        diagnostic("knowledge.yaml", "error", "the frontmatter is not a mapping", where),
      ],
    };
  }

  const type = text(metadata["type"]);
  if (type === null) {
    return {
      ok: false,
      diagnostics: [
        diagnostic("knowledge.type", "error", "a Concept needs a `type`", {
          ...where,
          field: "type",
        }),
      ],
    };
  }

  const diagnostics: Array<Diagnostic> = [];
  const status = text(metadata["status"]);
  // Under OKF v0.2 an absent `status` means `stable`; a status this version
  // does not interpret is `other` and is preserved rather than corrected.
  const lifecycle: Lifecycle =
    status === null
      ? "stable"
      : (LIFECYCLES.find((known): known is (typeof LIFECYCLES)[number] => known === status) ??
        "other");
  if (lifecycle === "other") {
    diagnostics.push(
      diagnostic(
        "knowledge.lifecycle.unknown",
        "info",
        `'${status}' is not a lifecycle this version interprets; it is preserved and not acted on`,
        { ...where, field: "status" },
      ),
    );
  }

  const sources: Array<Source> = [];
  for (const member of members(metadata["sources"])) {
    if (!isMapping(member)) continue;
    const sourceId = text(member["id"]);
    const resource = text(member["resource"]);
    if (sourceId === null || resource === null) {
      diagnostics.push(
        diagnostic("knowledge.source", "warning", "a source needs `id` and `resource`", {
          ...where,
          field: "sources",
        }),
      );
      continue;
    }
    sources.push({ id: sourceId, resource });
  }

  const extension = isMapping(metadata["gitplus"]) ? metadata["gitplus"] : {};

  const cites: Array<string> = [];
  for (const member of members(extension["cites"])) {
    const record = isMapping(member) ? text(member["record"]) : text(member);
    if (record === null) {
      diagnostics.push(
        diagnostic("knowledge.cite", "error", "a citation needs a `record` oid", {
          ...where,
          field: "gitplus.cites",
        }),
      );
      continue;
    }
    cites.push(record);
  }

  const evidence: Array<Evidence> = [];
  for (const member of members(extension["evidence"])) {
    if (!isMapping(member)) {
      diagnostics.push(
        diagnostic("knowledge.evidence", "error", "an evidence entry must be a mapping", {
          ...where,
          field: "gitplus.evidence",
        }),
      );
      continue;
    }
    const kind = text(member["kind"]) ?? "blob";
    const path = text(member["path"]);
    if (path === null) {
      diagnostics.push(
        diagnostic("knowledge.evidence", "error", "an evidence entry needs a `path`", {
          ...where,
          field: "gitplus.evidence",
        }),
      );
      continue;
    }
    if (kind === "gitlink") {
      const commit = text(member["commit"]);
      if (commit === null) {
        diagnostics.push(
          diagnostic("knowledge.evidence", "error", `${path} is a gitlink with no \`commit\``, {
            ...where,
            field: "gitplus.evidence",
          }),
        );
        continue;
      }
      evidence.push({ kind: "gitlink", path, commit });
      continue;
    }
    if (kind !== "blob") {
      diagnostics.push(
        diagnostic("knowledge.evidence", "error", `'${kind}' is not an evidence kind`, {
          ...where,
          field: "gitplus.evidence",
        }),
      );
      continue;
    }
    const blob = text(member["blob"]);
    if (blob === null) {
      diagnostics.push(
        diagnostic("knowledge.evidence", "error", `${path} is a blob with no \`blob\` oid`, {
          ...where,
          field: "gitplus.evidence",
        }),
      );
      continue;
    }
    const range = rangeOf(member["range"]);
    if (range === "invalid") {
      diagnostics.push(
        diagnostic(
          "knowledge.evidence.range",
          "error",
          `${path} declares a range that is not a non-empty half-open [start, end)`,
          { ...where, field: "gitplus.evidence" },
        ),
      );
      continue;
    }
    evidence.push(
      range === null ? { kind: "blob", path, blob } : { kind: "blob", path, blob, range },
    );
  }

  const external: Array<External> = [];
  const declared = extension["external"];
  if (isMapping(declared)) {
    for (const [sourceId, value] of Object.entries(declared)) {
      const capture = isMapping(value) ? value : {};
      external.push({
        id: sourceId,
        retrievedAt: text(capture["retrieved_at"]) ?? undefined,
        contentDigest: text(capture["content_digest"]) ?? undefined,
        snapshot: text(capture["snapshot"]) ?? undefined,
      });
    }
  }

  const verificationRecords: Array<string> = [];
  for (const member of members(extension["verification_records"])) {
    const record = isMapping(member) ? text(member["record"]) : text(member);
    if (record !== null) verificationRecords.push(record);
  }

  return {
    ok: true,
    diagnostics,
    concept: {
      id,
      path: input.path,
      type,
      title: text(metadata["title"]),
      description: text(metadata["description"]),
      lifecycle,
      status,
      staleAfter: text(metadata["stale_after"]),
      sources,
      cites,
      evidence,
      external,
      verificationRecords,
      metadata,
      body: split.body,
    },
  };
};

/** A declared byte range, refused rather than clamped where it is not one. */
const rangeOf = (node: Node | undefined): readonly [number, number] | null | "invalid" => {
  if (node === undefined || node === "") return null;
  if (!Array.isArray(node) || node.length !== 2) return "invalid";
  // Decimal digits only: `Number` reads "", "1e1" and "0x20" as 0, 10 and 32,
  // and a missing bound clamped to zero is exactly what this refuses.
  const [start, end] = node.map((member) =>
    Predicate.isString(member) && /^(?:0|[1-9]\d*)$/u.test(member) ? Number(member) : Number.NaN,
  );
  if (
    start === undefined ||
    end === undefined ||
    !Number.isSafeInteger(start) ||
    !Number.isSafeInteger(end) ||
    start < 0 ||
    start >= end
  ) {
    return "invalid";
  }
  return [start, end];
};

/**
 * The deterministic summary a Memory entry carries for a Concept (§8.2).
 *
 * The authored `description` when there is one, otherwise a bounded extract of
 * the authored body — never a model call to re-render the same source, and
 * never a rewrite that drops the author's hedging.
 */
export const summarize = (concept: Concept, limit = 320): string => {
  if (concept.description !== null) return clip(concept.description, limit);
  const paragraph = concept.body
    .split(/\n\s*\n/u)
    .map((block) => block.trim())
    .find((block) => block !== "" && !block.startsWith("#") && !block.startsWith("["));
  return paragraph === undefined ? "" : clip(paragraph.replaceAll(/\s+/gu, " "), limit);
};

const clip = (value: string, limit: number): string => {
  if (value.length <= limit) return value;
  let end = limit - 1;
  // Never inside a surrogate pair: half an astral character is a lone
  // surrogate, which the encoder turns into U+FFFD in the persisted note.
  const last = value.charCodeAt(end - 1);
  if (last >= 0xd800 && last <= 0xdbff) end -= 1;
  return `${value.slice(0, end).trimEnd()}…`;
};
