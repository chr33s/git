import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";

import { Effect } from "effect";

import { serve, type Server } from "../host/Node.ts";
import { writeKeyPair } from "../testing/Hub.ts";

const execFileAsync = promisify(execFile);
const entry = path.join(import.meta.dirname, "bin.ts");
let root = "";

const cli = async (args: ReadonlyArray<string>): Promise<string> => {
  const result = await execFileAsync(process.execPath, [entry, ...args], { encoding: "utf8" });
  return `${result.stdout}${result.stderr}`;
};

describe("cli identity and social graph", () => {
  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-id-"));
  });

  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  it.live("creates a PrincipalID and publishes a signed follow", () =>
    Effect.promise(async () => {
      const key = path.join(root, "identity-key");
      await writeKeyPair(key, "alice@example.com");

      const initialized = await cli(["id", "init", "--root", root, "--key", key, "alice"]);
      const principal = /PrincipalID: (SHA256:[A-Za-z0-9+/]{43})/.exec(initialized)?.[1];
      assert.notEqual(principal, undefined, initialized);
      assert.match(initialized, /Shareable: gid1/);
      if (principal === undefined) assert.fail(initialized);

      const status = await cli(["id", "status", "--root", root, "alice"]);
      assert.match(status, /source\.push,social\.write/);

      const followed = await cli([
        "social",
        "follow",
        "--root",
        root,
        "--key",
        key,
        "--name",
        "alice",
        principal,
        "alice",
      ]);
      assert.match(followed, /social\.follow/);

      const social = await cli(["social", "status", "--root", root, "alice"]);
      assert.match(social, /1 active statement/);
    }),
  );

  it.live("discovers attestations across locally synchronized identity repositories", () =>
    Effect.promise(async () => {
      const aliceKey = path.join(root, "alice-key");
      const bobKey = path.join(root, "bob-key");
      await writeKeyPair(aliceKey, "alice@example.com");
      await writeKeyPair(bobKey, "bob@example.com");

      const aliceInit = await cli(["id", "init", "--root", root, "--key", aliceKey, "alice"]);
      const bobInit = await cli(["id", "init", "--root", root, "--key", bobKey, "bob"]);
      const alice = /PrincipalID: (SHA256:[A-Za-z0-9+/]{43})/.exec(aliceInit)?.[1];
      const bob = /PrincipalID: (SHA256:[A-Za-z0-9+/]{43})/.exec(bobInit)?.[1];
      if (alice === undefined || bob === undefined) assert.fail(`${aliceInit}\n${bobInit}`);

      await cli([
        "social",
        "vouch",
        "--root",
        root,
        "--key",
        aliceKey,
        "--scope",
        "introduce.repo",
        bob,
        "alice",
      ]);
      await cli([
        "social",
        "attest",
        "--root",
        root,
        "--key",
        bobKey,
        "--url",
        "https://code.example/alice",
        alice,
        "bob",
      ]);

      const found = await cli(["social", "find", "--root", root, alice, "alice"]);
      assert.match(found, /https:\/\/code\.example\/alice/);
      assert.match(found, /1 path\(s\)/);
    }),
  );

  it.live("synchronizes followed identities from attested locations", () =>
    Effect.promise(async () => {
      const remoteRoot = await fs.mkdtemp(path.join(os.tmpdir(), "cli-id-remote-"));
      let server: Server | null = null;
      try {
        const aliceKey = path.join(root, "alice-key");
        const bobKey = path.join(remoteRoot, "bob-key");
        await writeKeyPair(aliceKey, "alice@example.com");
        await writeKeyPair(bobKey, "bob@example.com");

        const aliceInit = await cli(["id", "init", "--root", root, "--key", aliceKey, "alice"]);
        const bobInit = await cli(["id", "init", "--root", remoteRoot, "--key", bobKey, "bob"]);
        const alice = /PrincipalID: (SHA256:[A-Za-z0-9+/]{43})/.exec(aliceInit)?.[1];
        const bob = /PrincipalID: (SHA256:[A-Za-z0-9+/]{43})/.exec(bobInit)?.[1];
        if (alice === undefined || bob === undefined) assert.fail(`${aliceInit}\n${bobInit}`);

        server = await serve({ root: remoteRoot });
        const location = `${server.url}/bob`;
        await cli([
          "social",
          "follow",
          "--root",
          root,
          "--key",
          aliceKey,
          "--name",
          "bob",
          bob,
          "alice",
        ]);
        await cli([
          "social",
          "attest",
          "--root",
          root,
          "--key",
          aliceKey,
          "--url",
          location,
          bob,
          "alice",
        ]);

        const synced = await cli(["social", "sync", "--root", root, "alice"]);
        assert.match(synced, /1 followed identity repo\(s\) synchronized/);
        const cloned = (await fs.readdir(root)).find((name) => name.startsWith("gid1"));
        assert.notEqual(cloned, undefined);
        if (cloned === undefined) assert.fail("the followed identity was not materialized");
        const status = await cli(["id", "status", "--root", root, cloned]);
        assert.match(status, new RegExp(bob.replace(/[+/]/g, "\\$&")));
      } finally {
        if (server !== null) await server.close();
        await fs.rm(remoteRoot, { recursive: true, force: true });
      }
    }),
  );
});
