/**
 * SSHSIG against the real `ssh-keygen`.
 *
 * The claim this module makes is not "these bytes round-trip through my own
 * parser" — it is "OpenSSH made this signature" and "OpenSSH accepts the ones
 * I make". Only the binary can answer that, so both directions are checked
 * here, and the suite skips when `ssh-keygen` is not installed, the same way
 * the git interop suites skip without `git`.
 *
 * A `*.test.ts` rather than `*.integration.ts`: it runs in this process
 * against a subprocess, not inside workerd.
 */
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Result } from "effect";

import {
  fingerprint,
  formatPublicKey,
  NAMESPACE,
  parsePrivateKey,
  parsePublicKey,
  sign,
  verify,
} from "./SshSignature.ts";

/**
 * Whether this machine has an `ssh-keygen` that can do what the suite needs.
 *
 * A capability probe rather than a version check: `ssh-keygen` has no flag
 * that exits zero on its own, and the thing worth knowing is not the version
 * string but whether the binary can produce an Ed25519 key — which is also
 * what rules out the pre-8.0 builds that have no `-Y` at all.
 */
const hasSshKeygen: boolean = (() => {
  const directory = mkdtempSync(join(tmpdir(), "sshsig-probe-"));
  try {
    execFileSync("ssh-keygen", ["-q", "-t", "ed25519", "-N", "", "-f", join(directory, "probe")], {
      stdio: "ignore",
    });
    return true;
  } catch {
    return false;
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
})();

const encoder = new TextEncoder();

describe.skipIf(!hasSshKeygen)("SshSignature interop with ssh-keygen", () => {
  const workspace = (): string => mkdtempSync(join(tmpdir(), "sshsig-"));

  /** A real OpenSSH keypair, written by the binary that defines the format. */
  const keygen = (directory: string, comment: string): string => {
    const path = join(directory, "id_ed25519");
    execFileSync("ssh-keygen", ["-q", "-t", "ed25519", "-N", "", "-C", comment, "-f", path], {
      stdio: "ignore",
    });
    return path;
  };

  it.effect("reads a keypair ssh-keygen wrote", () =>
    Effect.promise(async () => {
      const directory = workspace();
      try {
        const path = keygen(directory, "interop@example.com");
        const parsed = parsePrivateKey(readFileSync(path, "utf8"));
        if (Result.isFailure(parsed)) throw new Error(JSON.stringify(parsed.failure));

        // The public half, as OpenSSH itself spells it.
        const line = readFileSync(`${path}.pub`, "utf8").trim();
        assert.equal(formatPublicKey(parsed.success.publicKey), line);
      } finally {
        rmSync(directory, { recursive: true, force: true });
      }
    }),
  );

  it.effect("agrees with ssh-keygen about a key's fingerprint", () =>
    Effect.promise(async () => {
      const directory = workspace();
      try {
        const path = keygen(directory, "print@example.com");
        const parsed = parsePublicKey(readFileSync(`${path}.pub`, "utf8"));
        if (Result.isFailure(parsed)) throw new Error(JSON.stringify(parsed.failure));

        // `256 SHA256:<base64> comment (ED25519)`
        const printed = execFileSync("ssh-keygen", ["-lf", `${path}.pub`], { encoding: "utf8" });
        const expected = printed.split(/\s+/).at(1);

        assert.equal(await Effect.runPromise(fingerprint(parsed.success)), expected);
      } finally {
        rmSync(directory, { recursive: true, force: true });
      }
    }),
  );

  it.effect("makes signatures ssh-keygen verifies", () =>
    Effect.promise(async () => {
      const directory = workspace();
      try {
        const path = keygen(directory, "signer@example.com");
        const parsed = parsePrivateKey(readFileSync(path, "utf8"));
        if (Result.isFailure(parsed)) throw new Error(JSON.stringify(parsed.failure));

        const message = "the payload a membership grant would carry\n";
        const armored = await Effect.runPromise(
          sign(parsed.success, encoder.encode(message), NAMESPACE),
        );

        const messagePath = join(directory, "message");
        const signaturePath = join(directory, "message.sig");
        const allowed = join(directory, "allowed_signers");
        writeFileSync(messagePath, message);
        writeFileSync(signaturePath, armored);
        writeFileSync(
          allowed,
          `signer@example.com ${readFileSync(`${path}.pub`, "utf8").trim()}\n`,
        );

        // Exits zero only if the signature, the namespace and the principal all
        // check out against the message on stdin.
        const output = execFileSync(
          "ssh-keygen",
          [
            "-Y",
            "verify",
            "-f",
            allowed,
            "-I",
            "signer@example.com",
            "-n",
            NAMESPACE,
            "-s",
            signaturePath,
          ],
          { input: message, encoding: "utf8", stdio: ["pipe", "pipe", "pipe"] },
        );
        assert.match(output, /Good "chr33s-git\/hub\/v1" signature/);
      } finally {
        rmSync(directory, { recursive: true, force: true });
      }
    }),
  );

  it.effect("verifies signatures ssh-keygen made", () =>
    Effect.promise(async () => {
      const directory = workspace();
      try {
        const path = keygen(directory, "theirs@example.com");
        const message = "a review of an exact revision\n";
        const messagePath = join(directory, "message");
        writeFileSync(messagePath, message);

        execFileSync("ssh-keygen", ["-Y", "sign", "-f", path, "-n", NAMESPACE, messagePath], {
          stdio: "ignore",
        });
        const armored = readFileSync(`${messagePath}.sig`, "utf8");

        const signer = await Effect.runPromise(verify(armored, encoder.encode(message), NAMESPACE));
        assert.notEqual(signer, null, "a signature from ssh-keygen must verify here");

        const expected = parsePublicKey(readFileSync(`${path}.pub`, "utf8"));
        if (Result.isFailure(expected)) throw new Error(JSON.stringify(expected.failure));
        assert.deepEqual(signer?.point, expected.success.point);
      } finally {
        rmSync(directory, { recursive: true, force: true });
      }
    }),
  );

  it.effect("rejects an ssh-keygen signature made under another namespace", () =>
    Effect.promise(async () => {
      const directory = workspace();
      try {
        const path = keygen(directory, "elsewhere@example.com");
        const message = "signed for a different application\n";
        const messagePath = join(directory, "message");
        writeFileSync(messagePath, message);

        execFileSync("ssh-keygen", ["-Y", "sign", "-f", path, "-n", "git", messagePath], {
          stdio: "ignore",
        });
        const armored = readFileSync(`${messagePath}.sig`, "utf8");

        const failure = await Effect.runPromise(
          verify(armored, encoder.encode(message), NAMESPACE).pipe(Effect.flip),
        );
        assert.match(failure.reason, /namespace/);
      } finally {
        rmSync(directory, { recursive: true, force: true });
      }
    }),
  );
});
