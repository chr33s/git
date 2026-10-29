/** Accepted agent-output links; histories never guess which work a session authored. */
import { Effect, Schema } from "effect";
import { isOid, qualify, unqualify } from "../git/Oid.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as Session from "../hub/Session.ts";
import type { InputManifest } from "../hub/MemoryInput.ts";
import type { Projection } from "../trust/Projection.ts";
import * as Provenance from "./Provenance.ts";

export const Link = Schema.Struct({
  session: Schema.String,
  record: Schema.String,
  originalHead: Schema.String,
  commits: Schema.Array(Schema.String),
  refs: Schema.Array(Schema.String),
  pulls: Schema.Array(Schema.String),
});
export type Link = typeof Link.Type;

export const collect = Effect.fn("knowledge.OutcomeSources.collect")(function* (input: {
  readonly manifest: InputManifest;
  readonly pr: string;
  readonly revisions: ReadonlySet<Oid>;
  readonly trust: Projection;
}) {
  const repository = yield* Repository;
  const heads = new Map(
    input.manifest.sessionHeads.flatMap(({ ref, head }) => {
      const oid = unqualify(head);
      return oid === null ? [] : [[ref, oid] as const];
    }),
  );
  const judge = Provenance.judge(
    input.manifest.repository,
    input.trust,
    heads,
    new Date(input.manifest.evaluatedAt),
  );
  const links: Array<Link> = [];
  const exclusions: Array<{ record: string; reason: string }> = [];
  const selected = input.manifest.sessionHeads.slice(0, input.manifest.limits.sessions);
  let complete = selected.length === input.manifest.sessionHeads.length;
  for (const { ref, head } of selected) {
    const session = Session.sessionOf(ref);
    const oid = unqualify(head);
    if (session === null || oid === null) {
      complete = false;
      continue;
    }
    const walked = yield* Session.entries(session, oid);
    if (walked.unreadable.length > 0) complete = false;
    for (const { commit, payload } of walked.events) {
      if (payload.type !== "session.produced") continue;
      // Publication outputs cannot become fresh independent learning examples.
      if (payload.refs.some((name) => name.startsWith("refs/heads/agent-memory/"))) continue;
      const commits = payload.commits.flatMap((named) => {
        const found = isOid(named) ? named : unqualify(named);
        return found === null ? [] : [found];
      });
      for (const original of commits.filter((named) => input.revisions.has(named))) {
        const source = yield* repository.readCommit(original);
        const trailer = Session.trailerOf(source.message);
        if (
          !payload.pulls.includes(input.pr) &&
          !("session" in trailer && trailer.session === session)
        )
          continue;
        const accepted = yield* Provenance.evaluate(qualify(commit), judge);
        if (accepted.state !== "accepted" || accepted.session !== session) {
          exclusions.push({ record: qualify(commit), reason: `session-${accepted.state}` });
          continue;
        }
        links.push({
          session,
          record: qualify(commit),
          originalHead: qualify(original),
          commits: commits.map(qualify),
          refs: payload.refs,
          pulls: payload.pulls,
        });
      }
    }
  }
  return {
    links,
    exclusions,
    complete,
    omittedSessions: input.manifest.sessionHeads.length - selected.length,
  };
});
