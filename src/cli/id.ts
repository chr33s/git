/** Stable principal identity repositories: bootstrap, rotate, revoke, inspect. */
import { Console, Effect, Result } from "effect";
import { Command, Flag } from "effect/unstable/cli";

import { formatPublicKey, isFingerprint } from "../crypto/SshSignature.ts";
import { Invalid } from "../git/Error.ts";
import { encodePrincipal } from "../social/Encode.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, signGenesis, writeGenesis, type Genesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import { principalId } from "../trust/Principal.ts";
import { openWindow, project } from "../trust/Projection.ts";
import {
  mustBeEnabled,
  readPrivateKey,
  readPublicKey,
  repoArgument,
  rootFlag,
  withRepo,
} from "./shared.ts";

const rootKeys = Flag.string("root-key").pipe(
  Flag.withDescription("Path to an offline root private key (repeat for a quorum)"),
  Flag.atLeast(1),
);

const enabled = Effect.fn("cli.id.enabled")(function* (repo: string) {
  return (yield* mustBeEnabled(repo)).genesis;
});

const confirm = Effect.fn("cli.id.confirm")(function* (genesis: Genesis, commit: string) {
  const projection = yield* project(genesis);
  const refused = projection.rejected.find((entry) => entry.commit === commit);
  if (refused !== undefined) {
    return yield* new Invalid({ field: "identity", reason: refused.reason });
  }
  return projection;
});

const init = Command.make(
  "init",
  {
    root: rootFlag,
    key: Flag.string("key").pipe(
      Flag.withDescription("Path to a root private key (repeat for a quorum)"),
      Flag.atLeast(1),
    ),
    threshold: Flag.integer("threshold").pipe(Flag.withDefault(1)),
    repo: repoArgument,
  },
  ({ key, repo, root, threshold }) =>
    Effect.gen(function* () {
      const signers = yield* Effect.forEach(key, readPrivateKey);
      const first = signers[0];
      if (threshold < 1 || signers.length < threshold || first === undefined) {
        return yield* new Invalid({
          field: "threshold",
          reason: `threshold ${threshold} needs at least ${threshold} root key(s)`,
        });
      }
      const genesis = yield* create(
        signers.map((signer) => formatPublicKey(signer.publicKey)),
        threshold,
      );
      yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          yield* writeGenesis(
            genesis,
            yield* Effect.forEach(signers, (signer) => signGenesis(genesis, signer)),
          );
          // A one-root identity is immediately usable. Larger quorums keep
          // roots offline and add devices explicitly with `id rotate`.
          if (threshold === 1) {
            const payload = yield* Certificate.grant({
              repo: genesis.repoId,
              publicKey: formatPublicKey(first.publicKey),
              capabilities: ["source.push", "social.write"],
              id: Log.newId(),
            });
            const commit = yield* Log.issue(payload, signers);
            yield* confirm(genesis, commit);
          }
        }),
      );
      const encoded = encodePrincipal({ id: principalId(genesis.repoId) });
      yield* Console.log(`PrincipalID: ${genesis.repoId}`);
      if (Result.isSuccess(encoded)) yield* Console.log(`Shareable: ${encoded.success}`);
    }),
);

const rotate = Command.make(
  "rotate",
  {
    root: rootFlag,
    rootKey: rootKeys,
    newKey: Flag.string("new-key").pipe(
      Flag.withDescription("Path to the new device's SSH public key"),
    ),
    old: Flag.string("revoke").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Old device fingerprint to revoke after granting the new one"),
    ),
    repo: repoArgument,
  },
  ({ newKey, old, repo, root, rootKey }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const genesis = yield* enabled(repo);
        const signers = yield* Effect.forEach(rootKey, readPrivateKey);
        const payload = yield* Certificate.grant({
          repo: genesis.repoId,
          publicKey: yield* readPublicKey(newKey),
          capabilities: ["source.push", "social.write"],
          id: Log.newId(),
        });
        const granted = yield* Log.issue(payload, signers);
        yield* confirm(genesis, granted);
        if (old !== "") {
          if (!isFingerprint(old)) {
            return yield* new Invalid({ field: "revoke", reason: `'${old}' is not a fingerprint` });
          }
          const revoked = yield* Log.issue(
            Certificate.revoke({
              repo: genesis.repoId,
              subject: old,
              reason: "rotated",
              id: Log.newId(),
            }),
            signers,
          );
          yield* confirm(genesis, revoked);
        }
        yield* Console.log(`Current device: ${payload.subject}`);
        if (old !== "") yield* Console.log(`Revoked old device: ${old}`);
      }),
    ),
);

const revoke = Command.make(
  "revoke",
  {
    root: rootFlag,
    rootKey: rootKeys,
    subject: Flag.string("subject").pipe(Flag.withDescription("Device key fingerprint")),
    reason: Flag.choice("reason", ["rotated", "left", "compromised", "superseded"]).pipe(
      Flag.withDefault("compromised"),
    ),
    repo: repoArgument,
  },
  ({ reason, repo, root, rootKey, subject }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const genesis = yield* enabled(repo);
        const signers = yield* Effect.forEach(rootKey, readPrivateKey);
        if (!isFingerprint(subject)) {
          return yield* new Invalid({
            field: "subject",
            reason: `'${subject}' is not a fingerprint`,
          });
        }
        const commit = yield* Log.issue(
          Certificate.revoke({ repo: genesis.repoId, subject, reason, id: Log.newId() }),
          signers,
        );
        const projection = yield* confirm(genesis, commit);
        const revoked = openWindow(projection.revoked.get(subject) ?? []);
        if (revoked === null) {
          return yield* new Invalid({ field: "subject", reason: `${subject} was not revoked` });
        }
        yield* Console.log(`Revoked ${subject} (${reason})`);
      }),
    ),
);

const status = Command.make("status", { root: rootFlag, repo: repoArgument }, ({ repo, root }) =>
  withRepo(
    root,
    repo,
    Effect.gen(function* () {
      const genesis = yield* enabled(repo);
      const projection = yield* project(genesis);
      yield* Console.log(`PrincipalID: ${genesis.repoId}`);
      yield* Console.log(`Trust head: ${projection.head ?? "empty"}`);
      for (const member of projection.members.values()) {
        yield* Console.log(`  ${member.fingerprint}  ${member.capabilities.join(",")}`);
      }
      for (const windows of projection.revoked.values()) {
        const revoked = openWindow(windows);
        if (revoked !== null)
          yield* Console.log(`  ${revoked.subject}  revoked (${revoked.reason})`);
      }
    }),
  ),
);

export const idCommand = Command.make("id", {}, () =>
  Console.log("git+ id <init|rotate|revoke|status> — see --help"),
).pipe(
  Command.withSubcommands([
    init.pipe(Command.withDescription("Create a stable PrincipalID repository")),
    rotate.pipe(Command.withDescription("Grant a new device, then optionally revoke the old one")),
    revoke.pipe(Command.withDescription("Revoke an identity device key")),
    status.pipe(Command.withDescription("Show the PrincipalID and current devices")),
  ]),
);
