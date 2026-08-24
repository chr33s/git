/** Commands that publish and query one principal's signed social log. */
import { Console, Effect, Layer, Result } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";

import { fingerprint, parsePublicKey } from "../crypto/SshSignature.ts";
import { Invalid } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import * as Introduce from "../social/Introduce.ts";
import * as Inbox from "../social/Inbox.ts";
import * as Lineage from "../social/Lineage.ts";
import * as SocialLog from "../social/Log.ts";
import * as SocialProjection from "../social/Projection.ts";
import * as Statement from "../social/Statement.ts";
import * as Sync from "../social/Sync.node.ts";
import { localSocialWeb } from "../social/Web.node.ts";
import { isRepoId, type RepoId } from "../trust/Genesis.ts";
import { KnownRepos } from "../trust/KnownRepos.ts";
import { layer as knownRepos } from "../trust/KnownRepos.node.ts";
import { isPrincipalId, principalId, principalOf, type PrincipalId } from "../trust/Principal.ts";
import { project as projectTrust } from "../trust/Projection.ts";
import * as Verify from "../trust/Verify.ts";
import { pushInbox } from "./inbox.node.ts";
import {
  mustBeEnabled,
  mustResolve,
  readPrivateKey,
  readPublicKey,
  repoArgument,
  rootFlag,
  withRepo,
} from "./shared.ts";

const keyFlag = Flag.string("key").pipe(
  Flag.withDescription("Path to the identity device private key"),
);

type SyncTarget =
  | { readonly subject: PrincipalId; readonly location: null }
  | {
      readonly subject: PrincipalId;
      readonly location: string;
      readonly outcome: Sync.SyncOutcome;
    };

const principal = (value: string): Effect.Effect<PrincipalId, Invalid> =>
  isPrincipalId(value)
    ? Effect.succeed(value)
    : Effect.fail(new Invalid({ field: "principal", reason: `'${value}' is not a PrincipalID` }));

const repositoryId = (value: string): Effect.Effect<RepoId, Invalid> =>
  isRepoId(value)
    ? Effect.succeed(value)
    : Effect.fail(new Invalid({ field: "repo", reason: `'${value}' is not a RepoID` }));

const identity = Effect.fn("cli.social.identity")(function* (repo: string) {
  const repository = yield* Repository;
  const stored = yield* mustBeEnabled(repo);
  const trust = yield* projectTrust(stored.genesis);
  const author = principalId(stored.genesis.repoId);
  return {
    genesis: stored.genesis,
    trust,
    author,
    context: {
      author,
      id: SocialLog.newId(),
      socialHead: yield* repository.resolve(SocialLog.LOG_REF),
      trustHead: trust.head,
    },
  };
});

const publish = Effect.fn("cli.social.publish")(function* (
  repo: string,
  payload: Statement.SocialStatement,
  key: string,
) {
  const signer = yield* readPrivateKey(key);
  const current = yield* identity(repo);
  const print = yield* fingerprint(signer.publicKey);
  const authorization = yield* Verify.authorizeKey({
    projection: current.trust,
    signer: print,
    capability: "social.write",
  });
  if (!authorization.ok) {
    return yield* new Invalid({ field: "authorization", reason: authorization.reason });
  }
  const commit = yield* SocialLog.issue(payload, signer);
  const verified = yield* SocialLog.verified(current.genesis, current.trust);
  const refusal = verified.rejected.find((entry) => entry.commit === commit);
  if (refusal !== undefined) {
    return yield* new Invalid({ field: "social", reason: refusal.reason });
  }
  if (!verified.statements.some((entry) => entry.commit === commit)) {
    return yield* new Invalid({
      field: "social",
      reason: `${commit} did not enter the projection`,
    });
  }
  yield* Console.log(`${payload.type} ${payload.id}`);
  yield* Console.log(`  ${commit}`);
});

const follow = Command.make(
  "follow",
  {
    root: rootFlag,
    key: keyFlag,
    petname: Flag.string("name").pipe(Flag.withDescription("Your local name for this principal")),
    subject: Argument.string("principal"),
    repo: repoArgument,
  },
  ({ key, petname, repo, root, subject }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const current = yield* identity(repo);
        yield* publish(
          repo,
          Statement.follow({
            ...current.context,
            subject: yield* principal(subject),
            petname,
          }),
          key,
        );
      }),
    ),
);

const vouch = Command.make(
  "vouch",
  {
    root: rootFlag,
    key: keyFlag,
    scope: Flag.string("scope").pipe(
      Flag.withDefault("introduce.repo"),
      Flag.withDescription("Comma-separated introduce.repo, introduce.key, review, vouch"),
    ),
    depth: Flag.integer("depth").pipe(Flag.withDefault(0)),
    expires: Flag.integer("expires-in").pipe(Flag.withDefault(0)),
    subject: Argument.string("principal"),
    repo: repoArgument,
  },
  ({ depth, expires, key, repo, root, scope, subject }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const current = yield* identity(repo);
        const requested = scope.split(",").map((value) => value.trim());
        const accepted = requested.filter((value): value is Statement.SocialScope =>
          Statement.SOCIAL_SCOPES.some((candidate) => candidate === value),
        );
        if (accepted.length !== requested.length) {
          return yield* new Invalid({ field: "scope", reason: `'${scope}' has an unknown scope` });
        }
        yield* publish(
          repo,
          Statement.vouch({
            ...current.context,
            subject: yield* principal(subject),
            scope: accepted,
            depth,
            expiresAt: expires === 0 ? null : new Date(Date.now() + expires * 1000),
          }),
          key,
        );
      }),
    ),
);

const attest = Command.make(
  "attest",
  {
    root: rootFlag,
    key: keyFlag,
    url: Flag.string("url").pipe(
      Flag.withDescription("Repository URL (repeat for mirrors)"),
      Flag.atLeast(1),
    ),
    role: Flag.choice("role", ["origin", "mirror", "fork"]).pipe(Flag.withDefault("origin")),
    forkOf: Flag.string("fork-of").pipe(Flag.withDefault("")),
    lineage: Flag.string("lineage").pipe(Flag.withDefault("")),
    inbox: Flag.string("inbox").pipe(Flag.withDefault("")),
    target: Argument.string("RepoID"),
    repo: repoArgument,
  },
  ({ forkOf, inbox, key, lineage, repo, role, root, target, url }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const current = yield* identity(repo);
        yield* publish(
          repo,
          Statement.attestRepo({
            ...current.context,
            repo: yield* repositoryId(target),
            urls: url,
            role,
            forkOf: forkOf === "" ? null : yield* repositoryId(forkOf),
            lineage: lineage === "" ? null : lineage,
            inbox: inbox === "" ? null : inbox,
          }),
          key,
        );
      }),
    ),
);

const attestPrincipal = Command.make(
  "attest-principal",
  {
    root: rootFlag,
    key: keyFlag,
    publicKey: Flag.string("public-key").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Path to the subject's SSH public key"),
    ),
    externalIdentity: Flag.string("identity").pipe(
      Flag.withDefault(""),
      Flag.withDescription("External platform identity, for example github:alice"),
    ),
    proof: Flag.string("proof").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Bidirectional proof URL for --identity"),
    ),
    subject: Argument.string("PrincipalID"),
    repo: repoArgument,
  },
  ({ externalIdentity, key, proof, publicKey, repo, root, subject }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const current = yield* identity(repo);
        const target = yield* principal(subject);
        if (publicKey !== "" && externalIdentity === "" && proof === "") {
          yield* publish(
            repo,
            Statement.attestPrincipalKey({
              ...current.context,
              subject: target,
              publicKey: yield* readPublicKey(publicKey),
            }),
            key,
          );
          return;
        }
        if (publicKey === "" && externalIdentity !== "" && proof !== "") {
          yield* publish(
            repo,
            Statement.attestExternalIdentity({
              ...current.context,
              subject: target,
              identity: externalIdentity,
              proof,
            }),
            key,
          );
          return;
        }
        return yield* new Invalid({
          field: "attestation",
          reason: "provide either --public-key, or both --identity and --proof",
        });
      }),
    ),
);

const publishMirrors = Command.make(
  "publish-mirrors",
  {
    root: rootFlag,
    key: keyFlag,
    url: Flag.string("url").pipe(Flag.atLeast(1)),
    mode: Flag.choice("mode", ["read", "write"]).pipe(Flag.withDefault("read")),
    target: Argument.string("repo-or-self"),
    repo: repoArgument,
  },
  ({ key, mode, repo, root, target, url }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const current = yield* identity(repo);
        yield* publish(
          repo,
          Statement.mirrors({
            ...current.context,
            repo: target === "self" ? "self" : yield* repositoryId(target),
            urls: url.map((location) => ({ url: location, mode })),
          }),
          key,
        );
      }),
    ),
);

const label = Command.make(
  "label",
  {
    root: rootFlag,
    key: keyFlag,
    namespace: Flag.string("namespace"),
    value: Flag.string("label"),
    subject: Argument.string("subject"),
    repo: repoArgument,
  },
  ({ key, namespace, repo, root, subject, value }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const current = yield* identity(repo);
        yield* publish(
          repo,
          Statement.label({ ...current.context, subject, namespace, label: value }),
          key,
        );
      }),
    ),
);

const revoke = Command.make(
  "revoke",
  {
    root: rootFlag,
    key: keyFlag,
    reason: Flag.choice("reason", ["withdrawn", "superseded", "compromised"]).pipe(
      Flag.withDefault("withdrawn"),
    ),
    target: Argument.string("statement-id"),
    repo: repoArgument,
  },
  ({ key, reason, repo, root, target }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const current = yield* identity(repo);
        yield* publish(repo, Statement.revoke({ ...current.context, target, reason }), key);
      }),
    ),
);

const checkpoint = Command.make(
  "checkpoint",
  { root: rootFlag, key: keyFlag, repo: repoArgument },
  ({ key, repo, root }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const current = yield* identity(repo);
        yield* publish(
          repo,
          Statement.checkpoint({
            ...current.context,
            frontier: current.context.socialHead === null ? [] : [current.context.socialHead],
          }),
          key,
        );
      }),
    ),
);

const lineage = Command.make(
  "lineage",
  {
    root: rootFlag,
    revision: Flag.string("revision").pipe(Flag.withDefault("HEAD")),
    upstream: Flag.string("from").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Upstream revision; omit to compute an origin's root lineage"),
    ),
    repo: repoArgument,
  },
  ({ repo, revision, root, upstream }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;
        const head = yield* mustResolve(repository, revision);
        const from = upstream === "" ? undefined : yield* mustResolve(repository, upstream);
        const computed =
          from === undefined
            ? yield* Lineage.earliestUnique({ head })
            : yield* Lineage.earliestUnique({ head, upstream: from });
        yield* Console.log(computed);
      }),
    ),
);

const graphOf = Effect.fn("cli.social.graphOf")(function* (repo: string) {
  const current = yield* identity(repo);
  const log = yield* SocialLog.verified(current.genesis, current.trust);
  const web = yield* Effect.serviceOption(SocialProjection.SocialWeb);
  const held = web._tag === "Some" ? yield* web.value.logs : [log];
  const logs = new Map(
    [...held, log].map((candidate) => [`${candidate.principal}\u0000${candidate.head}`, candidate]),
  );
  return {
    current,
    log,
    graph: SocialProjection.project({ roots: [current.author], logs: [...logs.values()] }),
  };
});

const find = Command.make(
  "find",
  { root: rootFlag, query: Argument.string("name-or-RepoID"), repo: repoArgument },
  ({ query, repo, root }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const { current, graph } = yield* graphOf(repo).pipe(Effect.provide(localSocialWeb(root)));
        let wanted = isRepoId(query) ? query : null;
        if (wanted === null) {
          for (const [subject, name] of graph.petnames.get(current.author) ?? []) {
            if (name === query) {
              wanted = subject;
              break;
            }
          }
        }
        const found = Introduce.repositories(graph).filter(
          (entry) => wanted === null || entry.repo === wanted || entry.url.includes(query),
        );
        for (const entry of found) {
          yield* Console.log(
            `${entry.repo}\t${entry.role}\t${entry.url}\t${entry.paths.length} path(s)`,
          );
        }
      }),
    ),
);

const mirrors = Command.make(
  "mirrors",
  { root: rootFlag, target: Argument.string("repo-or-self"), repo: repoArgument },
  ({ repo, root, target }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const { graph } = yield* graphOf(repo).pipe(Effect.provide(localSocialWeb(root)));
        for (const entry of graph.active) {
          if (entry.payload.type !== "social.mirrors" || entry.payload.repo !== target) continue;
          for (const mirror of entry.payload.urls) {
            yield* Console.log(`${mirror.mode}\t${mirror.url}\t${entry.payload.author}`);
          }
        }
      }),
    ),
);

const who = Command.make(
  "who",
  { root: rootFlag, key: Argument.string("fingerprint"), repo: repoArgument },
  ({ key, repo, root }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const { graph } = yield* graphOf(repo).pipe(Effect.provide(localSocialWeb(root)));
        for (const entry of graph.active) {
          if (
            entry.payload.type !== "social.attest.principal" ||
            entry.payload.claim !== "key-of" ||
            entry.payload.publicKey === undefined
          ) {
            continue;
          }
          const parsed = parsePublicKey(entry.payload.publicKey);
          if (Result.isFailure(parsed) || (yield* fingerprint(parsed.success)) !== key) continue;
          const subject = principalOf(entry.payload.subject);
          if (subject !== null)
            yield* Console.log(`${subject}\tattested by ${entry.payload.author}`);
        }
      }),
    ),
);

const sync = Command.make(
  "sync",
  {
    root: rootFlag,
    depth: Flag.integer("depth").pipe(
      Flag.withDefault(1),
      Flag.withDescription("Follow depth to synchronize (1-16)"),
    ),
    token: Flag.string("token").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Credential shared by identity mirrors that require one"),
    ),
    repo: repoArgument,
  },
  ({ depth, repo, root, token }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        if (depth < 1 || depth > 16) {
          return yield* new Invalid({ field: "depth", reason: "sync depth must be from 1 to 16" });
        }
        const current = yield* identity(repo);
        const known = yield* KnownRepos;
        const pins = yield* known.list;
        const visited = new Set<PrincipalId>([current.author]);
        let frontier = new Set<PrincipalId>([current.author]);
        let synchronized = 0;

        for (let hop = 0; hop < depth && frontier.size > 0; hop++) {
          const { graph } = yield* graphOf(repo);
          const next = new Set<PrincipalId>();
          const targets: PrincipalId[] = [];
          for (const subject of Sync.followedBy(graph, frontier)) {
            if (visited.has(subject)) continue;
            visited.add(subject);
            next.add(subject);
            targets.push(subject);
          }
          const outcomes = yield* Effect.forEach(
            targets,
            (subject) => {
              const location = Sync.locationOf(graph, subject, pins);
              if (location === null) return Effect.succeed<SyncTarget>({ subject, location: null });
              return Sync.syncIdentity({
                root,
                principal: subject,
                url: location,
                token: token === "" ? undefined : token,
              }).pipe(Effect.map((outcome): SyncTarget => ({ subject, location, outcome })));
            },
            { concurrency: 8 },
          );
          for (const outcome of outcomes) {
            if (outcome.location === null) {
              yield* Console.error(
                `! ${outcome.subject}: no pinned, mirrored, or attested location`,
              );
              continue;
            }
            synchronized++;
            yield* Console.log(
              `${outcome.subject}\t${outcome.location}\t${outcome.outcome.fetched.length} fetched, ${outcome.outcome.joined.length} joined`,
            );
          }
          frontier = next;
        }
        yield* Console.log(`${synchronized} followed identity repo(s) synchronized`);
      }).pipe(Effect.provide(Layer.merge(localSocialWeb(root), knownRepos))),
    ),
);

const status = Command.make("status", { root: rootFlag, repo: repoArgument }, ({ repo, root }) =>
  withRepo(
    root,
    repo,
    Effect.gen(function* () {
      const { graph, log } = yield* graphOf(repo).pipe(Effect.provide(localSocialWeb(root)));
      yield* Console.log(`${log.principal}`);
      yield* Console.log(`  ${graph.active.length} active statement(s) at ${log.head ?? "empty"}`);
      for (const rejected of [...log.rejected, ...graph.rejected]) {
        yield* Console.error(`! ${rejected.commit}: ${rejected.reason}`);
      }
    }),
  ),
);

const inboxList = Command.make(
  "inbox-list",
  { root: rootFlag, repo: repoArgument },
  ({ repo, root }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        for (const proposal of yield* Inbox.pending()) {
          yield* Console.log(`${proposal.id}\t${proposal.head}\t${proposal.title}`);
        }
      }),
    ),
);

const inboxSubmit = Command.make(
  "inbox-submit",
  {
    url: Flag.string("url").pipe(Flag.withDescription("Announced repository inbox URL")),
    id: Flag.string("id").pipe(Flag.withDefault("")),
    head: Argument.string("revision"),
  },
  ({ head, id, url }) =>
    Effect.gen(function* () {
      const proposal = id === "" ? SocialLog.newId() : id;
      const destination = yield* pushInbox({ url, head, id: proposal });
      yield* Console.log(destination);
    }),
);

const inboxAdopt = Command.make(
  "inbox-adopt",
  {
    root: rootFlag,
    key: keyFlag,
    proposal: Argument.string("proposal-id"),
    repo: repoArgument,
  },
  ({ key, proposal, repo, root }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const current = yield* identity(repo);
        const opened = yield* Inbox.adopt({
          genesis: current.genesis,
          trust: current.trust,
          proposal,
          key: yield* readPrivateKey(key),
        });
        yield* Console.log(`${opened.pr}\t${opened.commit}`);
      }),
    ),
);

export const socialCommand = Command.make("social", {}, () =>
  Console.log(
    "git+ social <follow|vouch|attest|attest-principal|publish-mirrors|label|revoke|checkpoint|lineage|find|mirrors|who|sync|status|inbox-submit|inbox-list|inbox-adopt>",
  ),
).pipe(
  Command.withSubcommands([
    follow.pipe(Command.withDescription("Follow a principal under a local petname")),
    vouch.pipe(Command.withDescription("Publish attenuated trust in a principal")),
    attest.pipe(Command.withDescription("Attest a repository identity and its locations")),
    attestPrincipal.pipe(Command.withDescription("Attest a principal key or external identity")),
    publishMirrors.pipe(Command.withDescription("Publish your current mirror locations")),
    label.pipe(Command.withDescription("Attach a namespaced public label")),
    revoke.pipe(Command.withDescription("Withdraw an earlier social statement")),
    checkpoint.pipe(Command.withDescription("Publish a social-log freshness checkpoint")),
    lineage.pipe(Command.withDescription("Compute an earliest-unique-commit lineage key")),
    find.pipe(Command.withDescription("Find repositories attested by your rooted web")),
    mirrors.pipe(Command.withDescription("List attested mirror locations")),
    who.pipe(Command.withDescription("Resolve key attestations to PrincipalIDs")),
    sync.pipe(Command.withDescription("Fetch followed identity social logs, bounded by depth")),
    status.pipe(Command.withDescription("Show this identity's social-log projection")),
    inboxSubmit.pipe(Command.withDescription("Push a revision through an announced inbox")),
    inboxList.pipe(Command.withDescription("List quarantined contribution proposals")),
    inboxAdopt.pipe(Command.withDescription("Adopt a proposal as a signed pull request")),
  ]),
);
