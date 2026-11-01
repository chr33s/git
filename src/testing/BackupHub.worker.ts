/** Signed application records for native namespace recovery tests. */
import { Effect } from "effect";
import { generate, formatPublicKey } from "../crypto/SshSignature.ts";
import type { Oid } from "../git/Store.ts";
import * as Genesis from "../trust/Genesis.ts";
import * as Certificate from "../trust/Certificate.ts";
import * as Log from "../trust/Log.ts";
import * as Trust from "../trust/Projection.ts";
import * as PullRequest from "../hub/PullRequest.ts";
import * as Projection from "../hub/Projection.ts";
import * as Session from "../hub/Session.ts";

const pr = "backup-pull-request";
const session = "backup-session";

export const seed = Effect.fn("BackupFixture.seedHub")(function* (head: Oid) {
  const root = yield* generate("backup-root@example.invalid");
  const member = yield* generate("backup-member@example.invalid");
  const genesis = yield* Genesis.create([formatPublicKey(root.publicKey)], 1);
  yield* Genesis.writeGenesis(genesis, [yield* Genesis.signGenesis(genesis, root)]);
  yield* Log.issue(
    yield* Certificate.grant({
      repo: genesis.repoId,
      publicKey: formatPublicKey(member.publicKey),
      capabilities: ["repo.read", "hub.create-pr", "hub.comment", "hub.session"],
      id: Log.newId(),
    }),
    [root],
  );
  yield* PullRequest.open({
    repo: genesis.repoId,
    id: pr,
    title: "Recover signed hub history",
    base: "refs/heads/main",
    head,
    key: member,
  });
  yield* PullRequest.comment({
    repo: genesis.repoId,
    pr,
    body: "Retain this recovery discussion",
    key: member,
  });
  yield* Session.open({
    repo: genesis.repoId,
    session,
    agent: { kind: "test", model: "fixture", harness: "native" },
    prompt: "Recover the complete installation",
    key: member,
  });
  yield* Session.produced({
    repo: genesis.repoId,
    session,
    key: member,
    commits: [head],
    refs: ["refs/heads/main"],
    pulls: [pr],
    note: "Recovery evidence retained",
    usage: { inputTokens: 13, outputTokens: 7 },
  });
});

export const inspect = Effect.fn("BackupFixture.inspectHub")(function* () {
  const stored = yield* Genesis.readGenesis();
  if (stored === null) return yield* Effect.die("restored hub genesis missing");
  const { genesis } = stored;
  const trust = yield* Trust.project(genesis);
  const pullRequest = yield* Projection.project(genesis, trust, pr);
  const work = yield* Session.project(session);
  if (
    trust.rejected.length !== 0 ||
    pullRequest.rejected.length !== 0 ||
    pullRequest.title !== "Recover signed hub history" ||
    pullRequest.state !== "open" ||
    !pullRequest.threads.some((thread) =>
      thread.comments.some((comment) => comment.body === "Retain this recovery discussion"),
    ) ||
    !work.prompts.some((prompt) => prompt.prompt === "Recover the complete installation") ||
    work.unreadable.length !== 0
  )
    return yield* Effect.die("restored hub projection differs");
  return { repoId: genesis.repoId, trust, pullRequest, session: work };
});
