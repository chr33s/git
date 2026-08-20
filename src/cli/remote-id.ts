/** Pure resolution for `git+id://grepo1…`; the Node delegate lives beside it. */
import { Result } from "effect";

import { Invalid } from "../git/Error.ts";
import { decodeIdentifier } from "../social/Encode.ts";
import type { KnownRepo } from "../trust/KnownRepos.ts";

export const identifierFromUrl = (url: string): string | null => {
  const prefix = "git+id://";
  if (!url.toLowerCase().startsWith(prefix)) return null;
  const body = url.slice(prefix.length).replace(/^\/+/, "");
  const end = body.search(/[/?#]/);
  const encoded = end === -1 ? body : body.slice(0, end);
  if (encoded === "") return null;
  try {
    return decodeURIComponent(encoded);
  } catch {
    return null;
  }
};

/** A previously pinned URL wins; embedded hints are bootstrap locations only. */
export const resolveLocation = (
  encoded: string,
  known: ReadonlyArray<KnownRepo>,
): Result.Result<string, Invalid> => {
  const decoded = decodeIdentifier(encoded);
  if (Result.isFailure(decoded)) return Result.fail(decoded.failure);
  if (decoded.success.kind !== "repository") {
    return Result.fail(
      new Invalid({
        field: "identifier",
        reason: "a PrincipalID cannot be used as a repository clone URL",
      }),
    );
  }

  const pinned = known.find((entry) => entry.repoId === decoded.success.id);
  if (pinned !== undefined) return Result.succeed(pinned.url);
  const hint = decoded.success.hints[0];
  return hint === undefined
    ? Result.fail(
        new Invalid({
          field: "identifier",
          reason: `${decoded.success.id} has no known location or embedded hint`,
        }),
      )
    : Result.succeed(hint);
};
