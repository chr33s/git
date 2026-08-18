/**
 * The JSON wire contract consumed by the browser UI.
 *
 * These schemas are imported by both the HTTP declaration and the browser
 * client. A response therefore earns its TypeScript type only after runtime
 * decoding against the same schema the server uses to encode it.
 */
import { Schema } from "effect";

import { isOid } from "../git/Oid.ts";

export const OidString = Schema.String.pipe(Schema.refine(isOid));

export const Ref = Schema.Struct({ name: Schema.String, oid: OidString });
export type Ref = (typeof Ref)["Type"];

export const Page = <A extends Schema.Top>(item: A) =>
  Schema.Struct({
    items: Schema.Array(item),
    next_cursor: Schema.NullOr(Schema.String),
    has_more: Schema.Boolean,
  });

export const RefsResponse = Schema.Struct({ refs: Schema.Array(Ref) });
export const RefPage = Page(Ref);

export const FileEntry = Schema.Struct({
  path: Schema.String,
  mode: Schema.String,
  oid: OidString,
});
export type FileEntry = (typeof FileEntry)["Type"];

export const FilesResponse = Schema.Struct({ files: Schema.Array(FileEntry) });

export const FileContent = Schema.Struct({
  path: Schema.String,
  mode: Schema.String,
  oid: OidString,
  content: Schema.String,
  encoding: Schema.Literals(["base64"]),
  size: Schema.Finite,
});
export type FileContent = (typeof FileContent)["Type"];

export const DiffRequest = Schema.Struct({
  from: Schema.String,
  to: Schema.String,
  path: Schema.optional(Schema.String),
  context: Schema.optional(Schema.Finite),
});
export type DiffRequest = (typeof DiffRequest)["Type"];

export const DiffFile = Schema.Struct({
  path: Schema.String,
  status: Schema.Literals(["added", "removed", "modified"]),
  binary: Schema.Boolean,
  patch: Schema.String,
});
export type DiffFile = (typeof DiffFile)["Type"];

export const DiffResponse = Schema.Struct({ files: Schema.Array(DiffFile) });

export const CommitSummary = Schema.Struct({
  oid: OidString,
  message: Schema.String,
});
export type CommitSummary = (typeof CommitSummary)["Type"];

export const LogResponse = Schema.Struct({ commits: Schema.Array(CommitSummary) });
export const CommitPage = Page(CommitSummary);

export const Commit = Schema.Struct({
  message: Schema.String,
  parents: Schema.Array(OidString),
  tree: OidString,
});
export type Commit = (typeof Commit)["Type"];

export const HistoryEntry = Schema.Struct({
  oid: OidString,
  message: Schema.String,
  blob: Schema.NullOr(OidString),
});
export const HistoryPage = Page(HistoryEntry);

export const RawObject = Schema.Struct({
  oid: OidString,
  type: Schema.Literals(["blob", "tree", "commit", "tag"]),
  size: Schema.Finite,
  content: Schema.String,
  encoding: Schema.Literals(["base64"]),
});
export type RawObject = (typeof RawObject)["Type"];

export const WhoamiVerdict = Schema.Struct({
  push: Schema.Literals(["allowed", "refused"]),
  why: Schema.Array(Schema.String),
});

export const WhoamiAnswer = Schema.Struct({
  repo: Schema.NullOr(Schema.String),
  subject: Schema.NullOr(Schema.String),
  member: Schema.Boolean,
  why: Schema.NullOr(Schema.String),
  capabilities: Schema.Array(Schema.String),
  expiresAt: Schema.NullOr(Schema.String),
  trust: Schema.NullOr(
    Schema.Struct({
      maxTrustAgeSeconds: Schema.Int,
      fresh: Schema.Boolean,
      reason: Schema.NullOr(Schema.String),
    }),
  ),
  /** Only where the repository bounds what it accepts being told it cost. */
  budget: Schema.NullOr(
    Schema.Struct({
      maxUsageTokens: Schema.Int,
      windowSeconds: Schema.Int,
      usedTokens: Schema.Int,
      remainingTokens: Schema.Int,
    }),
  ),
  branches: Schema.Record(Schema.String, WhoamiVerdict),
});
export type WhoamiAnswer = (typeof WhoamiAnswer)["Type"];
