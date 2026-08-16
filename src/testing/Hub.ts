/**
 * A hub-enabled repository, for the suites that need one.
 *
 * Every auth test opens the same way — a genesis, a root key, a member with
 * some capabilities, and a credential that member can present — so it lives
 * here once instead of five times.
 *
 * In `src/` rather than a test directory for the same reason as
 * `Store.contract.ts` and `testing/Git.ts`: it is shared test infrastructure
 * that test files import, not a `*.test.ts` file discovery should collect.
 */
import * as fs from "node:fs/promises";
import * as path from "node:path";

import { Effect, Layer } from "effect";

import { formatPublicKey, generate, type PrivateKey } from "../crypto/SshSignature.ts";
import { concatBytes } from "../git/Format.ts";
import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { mintDelegation } from "../server/Auth.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, type RepoId, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";

export interface HubFixture {
  readonly repoId: RepoId;
  /** The root key: holds everything, signs grants. */
  readonly root: PrivateKey;
  /** The member the fixture granted `capabilities` to. */
  readonly member: PrivateKey;
  /** A credential `member` can present, scoped to `capabilities`. */
  readonly credential: string;
}

/**
 * Give an existing bare repository a genesis and one member.
 *
 * `root` is a one-of-one quorum: the suites here are about what a member may
 * do, and a three-key threshold would only add signatures to every setup
 * without changing a single assertion.
 */
export const enableHub = (
  directory: string,
  capabilities: ReadonlyArray<string>,
): Promise<HubFixture> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const root = yield* generate("root@example.com");
      const member = yield* generate("member@example.com");

      const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
      yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);

      yield* Log.issue(
        yield* Certificate.grant({
          repo: genesis.repoId,
          publicKey: formatPublicKey(member.publicKey),
          capabilities,
          id: Log.newId(),
        }),
        [root],
      );

      const credential = yield* mintDelegation({
        key: member,
        repo: genesis.repoId,
        capabilities,
        ttlSeconds: 300,
      });

      return { repoId: genesis.repoId, root, member, credential };
    }).pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provide(stores(directory)),
        ),
      ),
    ),
  );

/** The same, addressed the way the hosts address repositories. */
export const enableHubUnder = (
  root: string,
  repo: string,
  capabilities: ReadonlyArray<string>,
): Promise<HubFixture> => enableHub(path.join(root, repo), capabilities);

/**
 * A second member of a repository that already has a genesis.
 *
 * `enableHub` cannot be called twice on one repository — writing the genesis a
 * second time is refused, which is the point of it — so a suite that needs a
 * reader *and* a writer grants the second one here.
 */
export const grantMember = (
  directory: string,
  root: PrivateKey,
  repoId: RepoId,
  capabilities: ReadonlyArray<string>,
): Promise<HubFixture> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const member = yield* generate("member@example.com");

      yield* Log.issue(
        yield* Certificate.grant({
          repo: repoId,
          publicKey: formatPublicKey(member.publicKey),
          capabilities,
          id: Log.newId(),
        }),
        [root],
      );

      const credential = yield* mintDelegation({
        key: member,
        repo: repoId,
        capabilities,
        ttlSeconds: 300,
      });

      return { repoId, root, member, credential };
    }).pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provide(stores(directory)),
        ),
      ),
    ),
  );

export const grantMemberUnder = (
  root: string,
  repo: string,
  signer: PrivateKey,
  repoId: RepoId,
  capabilities: ReadonlyArray<string>,
): Promise<HubFixture> => grantMember(path.join(root, repo), signer, repoId, capabilities);

/**
 * An OpenSSH private key file, for suites that drive the CLI.
 *
 * The CLI reads keys from disk, as a user's `ssh-keygen`-made key would be,
 * and this project deliberately has no private-key *writer* — the spec tells
 * users to run `ssh-keygen`, and a second implementation of that format would
 * be a second thing to keep correct. So the bytes are assembled here, in test
 * code, where being wrong fails a test rather than a user.
 */
export const opensshPrivateKey = (key: PrivateKey, comment: string): string => {
  const encoder = new TextEncoder();
  const string = (payload: Uint8Array): Uint8Array => {
    const out = new Uint8Array(4 + payload.length);
    new DataView(out.buffer).setUint32(0, payload.length);
    out.set(payload, 4);
    return out;
  };
  const text = (value: string) => string(encoder.encode(value));
  const uint32 = (value: number) => {
    const out = new Uint8Array(4);
    new DataView(out.buffer).setUint32(0, value);
    return out;
  };

  const point = key.publicKey.point;
  const check = uint32(0x01020304);
  const unpadded = concatBytes([
    check,
    check,
    text("ssh-ed25519"),
    string(point),
    string(concatBytes([key.seed, point])),
    text(comment),
  ]);
  const padding: number[] = [];
  for (let index = 1; (unpadded.length + padding.length) % 8 !== 0; index++) padding.push(index);

  const body = concatBytes([
    encoder.encode("openssh-key-v1\0"),
    text("none"),
    text("none"),
    text(""),
    uint32(1),
    string(concatBytes([text("ssh-ed25519"), string(point)])),
    string(concatBytes([unpadded, new Uint8Array(padding)])),
  ]);

  const base64 = Buffer.from(body).toString("base64");
  return `-----BEGIN OPENSSH PRIVATE KEY-----\n${(base64.match(/.{1,70}/g) ?? []).join("\n")}\n-----END OPENSSH PRIVATE KEY-----\n`;
};

/** A keypair on disk, private and public halves, as `ssh-keygen` leaves them. */
export const writeKeyPair = async (location: string, comment: string): Promise<PrivateKey> => {
  const key = await Effect.runPromise(generate(comment));
  await fs.writeFile(location, opensshPrivateKey(key, comment), { mode: 0o600 });
  await fs.writeFile(`${location}.pub`, `${formatPublicKey(key.publicKey)}\n`);
  return key;
};
