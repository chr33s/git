/**
 * `chr33s-git hub …` — the commands that create and inspect a repository's
 * identity, its membership, and the trust a client places in it.
 *
 * Split into two halves that happen to share a prefix. `init`, `grant`,
 * `revoke` and `members` act on a repository this machine holds. `enable`,
 * `status` and `forget` act on the *client's* view of a remote one — what
 * `~/.config/chr33s-git/known_repos` pins — and touch no repository at all.
 *
 * Trust on first use is a decision, so it is asked rather than assumed: a
 * prompt by default, and `--yes` for the scripts that have already decided.
 * The one thing that never happens quietly is an identity that changed.
 */
import * as readline from "node:readline/promises";

import { Console, Effect, Layer } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";

import { formatPublicKey, isFingerprint } from "../crypto/SshSignature.ts";
import { fetchRepository, lsRemote } from "../client/Fetch.ts";
import { Invalid } from "../git/Error.ts";
import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import * as Refspec from "../git/Refspec.ts";
import { ObjectStore, RefStore } from "../git/Store.ts";
import * as Certificate from "../trust/Certificate.ts";
import {
  create,
  GENESIS_REF,
  load,
  readGenesis,
  RECORD as GENESIS_RECORD,
  signGenesis,
  writeGenesis,
} from "../trust/Genesis.ts";
import {
  canonicalUrl,
  decide,
  firstUseMessage,
  KnownRepos,
  mismatchMessage,
} from "../trust/KnownRepos.ts";
import { layer as knownRepos } from "../trust/KnownRepos.node.ts";
import * as Log from "../trust/Log.ts";
import * as Record from "../trust/Record.ts";
import { project } from "../trust/Projection.ts";
import { readPrivateKey, readPublicKey, repoArgument, rootFlag, withRepo } from "./shared.ts";

/**
 * One repository's stores *and* the domain service over them.
 *
 * `provideMerge` rather than `provide`: the fetch writes through the ports
 * directly and `readGenesis` reads through `Repository`, so both have to stay
 * visible to the same effect.
 */
const localRepository = (directory: string) =>
  GitRepository.layer.pipe(
    Layer.provide(GitRepository.hooksNoop),
    Layer.provideMerge(stores(directory)),
  );

const keyFlag = Flag.string("key").pipe(
  Flag.withDescription("Path to the SSH private key to sign with"),
);

const init = Command.make(
  "init",
  {
    root: rootFlag,
    key: keyFlag,
    threshold: Flag.integer("threshold").pipe(
      Flag.withDefault(1),
      Flag.withDescription("How many root keys an authority operation needs"),
    ),
    also: Flag.string("root-key").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Additional root public key files, comma-separated"),
    ),
    repo: repoArgument,
  },
  ({ also, key, repo, root, threshold }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      const extra = also === "" ? [] : also.split(",").map((value) => value.trim());
      // The private key carries its own public half, so there is no need to
      // go looking for a `.pub` beside it — and no failure when there is none.
      const lines = [
        formatPublicKey(signer.publicKey),
        ...(yield* Effect.forEach(extra, readPublicKey)),
      ];

      const genesis = yield* create(lines, threshold);
      yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          yield* writeGenesis(genesis, [yield* signGenesis(genesis, signer)]);
        }),
      );

      yield* Console.log(`Repository identity for ${repo}:`);
      yield* Console.log(`  ${genesis.repoId}`);
      yield* Console.log(`  ${lines.length} root key(s), threshold ${threshold}`);
    }),
);

const grant = Command.make(
  "grant",
  {
    root: rootFlag,
    key: keyFlag,
    subject: Flag.string("subject").pipe(
      Flag.withDescription("Path to the member's SSH public key"),
    ),
    capability: Flag.string("capability").pipe(
      Flag.withDescription("Capabilities to grant, comma-separated"),
      Flag.withAlias("c"),
    ),
    expires: Flag.integer("expires-in").pipe(
      Flag.withDefault(0),
      Flag.withDescription("Seconds until the grant expires; 0 never expires"),
    ),
    repo: repoArgument,
  },
  ({ capability, expires, key, repo, root, subject }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      const publicKey = yield* readPublicKey(subject);

      const printed = yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          const stored = yield* mustBeEnabled(repo);
          const payload = yield* Certificate.grant({
            repo: stored.genesis.repoId,
            publicKey,
            capabilities: capability.split(",").map((value) => value.trim()),
            expiresAt: expires === 0 ? null : new Date(Date.now() + expires * 1000),
            id: Log.newId(),
          });
          yield* Log.issue(payload, [signer]);
          return payload.subject;
        }),
      );

      yield* Console.log(`Granted ${capability} to ${printed}`);
    }),
);

const revoke = Command.make(
  "revoke",
  {
    root: rootFlag,
    key: keyFlag,
    subject: Flag.string("subject").pipe(
      Flag.withDescription("The member's key fingerprint (SHA256:…)"),
    ),
    reason: Flag.choice("reason", ["rotated", "left", "compromised", "superseded"]).pipe(
      Flag.withDefault("left"),
    ),
    repo: repoArgument,
  },
  ({ key, reason, repo, root, subject }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          const stored = yield* mustBeEnabled(repo);
          if (!isFingerprint(subject)) {
            return yield* new Invalid({
              field: "subject",
              reason: `'${subject}' is not a key fingerprint; \`hub members\` lists them`,
            });
          }
          yield* Log.issue(
            Certificate.revoke({
              repo: stored.genesis.repoId,
              subject,
              reason,
              id: Log.newId(),
            }),
            [signer],
          );
        }),
      );
      yield* Console.log(`Revoked ${subject} (${reason})`);
    }),
);

const members = Command.make("members", { root: rootFlag, repo: repoArgument }, ({ repo, root }) =>
  withRepo(
    root,
    repo,
    Effect.gen(function* () {
      const stored = yield* mustBeEnabled(repo);
      const projection = yield* project(stored.genesis);

      yield* Console.log(`${stored.genesis.repoId}`);
      yield* Console.log(
        `  ${projection.roots.length} root key(s), threshold ${projection.threshold}`,
      );
      for (const member of projection.members.values()) {
        const expiry =
          member.expiresAt === null ? "" : ` (expires ${member.expiresAt.toISOString()})`;
        yield* Console.log(`  ${member.fingerprint}  ${member.capabilities.join(",")}${expiry}`);
      }
      for (const revoked of projection.revoked.values()) {
        yield* Console.log(`  ${revoked.subject}  revoked (${revoked.reason})`);
      }
      // Said out loud rather than swallowed: a record that did not count is
      // exactly what somebody will be looking for.
      for (const rejected of projection.rejected) {
        yield* Console.error(`! ${rejected.commit} ignored: ${rejected.reason}`);
      }
    }),
  ),
);

const mustBeEnabled = Effect.fn("hub.mustBeEnabled")(function* (repo: string) {
  const stored = yield* readGenesis();
  if (stored === null) {
    return yield* new Invalid({
      field: "repo",
      reason: `${repo} has no genesis; run \`chr33s-git hub init ${repo} --key <key>\` first`,
    });
  }
  return stored;
});

// -- the client's view ------------------------------------------------------

/**
 * The identity a remote presents, without trusting it yet.
 *
 * The genesis is read from the commit the *remote* advertised, not from
 * whatever this directory happens to hold. Those differ in exactly the case
 * this function exists to catch: a local clone made earlier still has the old
 * genesis, the fetch refuses to move that ref because the new one is not a
 * fast-forward of it, and reading locally would then report the identity that
 * has changed as the identity that was expected.
 */
const presented = Effect.fn("hub.presented")(function* (url: string, token: string | undefined) {
  const target = { objects: yield* ObjectStore, refs: yield* RefStore };

  const advertised = yield* lsRemote(url, { token });
  const genesis = advertised.find((ref) => ref.name === GENESIS_REF);
  if (genesis === undefined) {
    return yield* new Invalid({
      field: "url",
      reason: `${url} is not hub-enabled: it has no genesis`,
    });
  }

  // Fetched into a name of our own, so the remote's genesis is a *value* here
  // rather than a claim about this repository's identity.
  yield* fetchRepository({
    url,
    stores: target,
    token,
    refspecs: [{ force: true, source: GENESIS_REF, destination: PRESENTED_REF }],
  });

  const record = yield* Record.read(genesis.oid, GENESIS_RECORD);
  return (yield* load(record.payload)).repoId;
});

/** Where a remote's genesis lands while it is still only a claim. */
const PRESENTED_REF = "refs/meta/presented/genesis";

const ask = (question: string): Effect.Effect<boolean> =>
  Effect.promise(async () => {
    const rl = readline.createInterface({ input: process.stdin, output: process.stderr });
    try {
      const answer = await rl.question(question);
      return answer.trim().toLowerCase() === "yes";
    } finally {
      rl.close();
    }
  });

const enable = Command.make(
  "enable",
  {
    root: rootFlag,
    yes: Flag.boolean("yes").pipe(
      Flag.withDescription("Accept a new repository identity without prompting"),
    ),
    name: Flag.string("as").pipe(
      Flag.withDefault("origin"),
      Flag.withDescription("Local repository directory to fetch into"),
    ),
    token: Flag.string("token").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Credential for a repository that requires one"),
    ),
    url: Argument.string("url"),
  },
  ({ name, root, token, url, yes }) =>
    Effect.gen(function* () {
      const key = yield* canonicalUrl(url);
      const directory = `${root}/${name}`;

      const credential = token === "" ? undefined : token;
      const identity = yield* presented(url, credential).pipe(
        Effect.provide(localRepository(directory)),
      );
      const decision = yield* decide(key, identity);

      if (decision.kind === "changed") {
        yield* Console.error(mismatchMessage(key, decision.expected, decision.presented));
        return yield* new Invalid({
          field: "url",
          reason: "repository identity has changed; hub operations refused",
        });
      }

      if (decision.kind === "new") {
        yield* Console.error(firstUseMessage(key, decision.repoId, decision.alias));
        const accepted = yes || (yield* ask("Trust this repository? [yes/no] "));
        if (!accepted) {
          return yield* new Invalid({ field: "url", reason: "not trusted; nothing was written" });
        }
        const store = yield* KnownRepos;
        yield* store.remember({ url: key, repoId: decision.repoId });
      }

      // Trust first, then hub: an event is judged against the membership graph,
      // so the grants have to be here before the events that lean on them.
      const fetched = yield* Effect.gen(function* () {
        const target = { objects: yield* ObjectStore, refs: yield* RefStore };
        const all: string[] = [];
        for (const spec of Refspec.HUB_FETCH) {
          const result = yield* fetchRepository({
            url,
            stores: target,
            token: credential,
            refspecs: [spec],
          });
          all.push(...result.refs.map((update) => update.name));
        }
        return all;
      }).pipe(Effect.provide(localRepository(directory)));

      yield* Console.log(`${key}`);
      yield* Console.log(`  ${identity}`);
      yield* Console.log(`  ${fetched.length} hub/trust ref(s) fetched into ${directory}`);
    }).pipe(Effect.provide(knownRepos)),
);

const status = Command.make("status", { url: Argument.string("url") }, ({ url }) =>
  Effect.gen(function* () {
    const key = yield* canonicalUrl(url);
    const store = yield* KnownRepos;
    const pinned = yield* store.lookup(key);

    yield* Console.log(key);
    yield* Console.log(pinned === null ? "  not trusted" : `  pinned ${pinned}`);
  }).pipe(Effect.provide(knownRepos)),
);

const forget = Command.make("forget", { url: Argument.string("url") }, ({ url }) =>
  Effect.gen(function* () {
    const key = yield* canonicalUrl(url);
    const store = yield* KnownRepos;
    const removed = yield* store.forget(key);
    yield* Console.log(removed ? `Forgot ${key}` : `${key} was not trusted`);
  }).pipe(Effect.provide(knownRepos)),
);

export const hubCommand = Command.make("hub", {}, () =>
  Console.log("chr33s-git hub <init|grant|revoke|members|enable|status|forget> — see --help"),
).pipe(
  Command.withSubcommands([
    init.pipe(Command.withDescription("Give a repository an identity and a root quorum")),
    grant.pipe(Command.withDescription("Issue a membership certificate")),
    revoke.pipe(Command.withDescription("Revoke a member's key")),
    members.pipe(Command.withDescription("Who this repository trusts, and with what")),
    enable.pipe(Command.withDescription("Trust a remote repository and fetch its hub state")),
    status.pipe(Command.withDescription("What identity is pinned for a URL")),
    forget.pipe(Command.withDescription("Drop a pinned repository identity")),
  ]),
);
