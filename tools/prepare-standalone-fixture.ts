/** Disposable identity fixture for the source-free installation rehearsal. */
import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { Result } from "effect";
import { enableHubUnder } from "../src/testing/Hub.ts";
import { encodeRepository } from "../src/social/Encode.ts";

const [destination, ...extra] = process.argv.slice(2);
assert.ok(destination !== undefined && extra.length === 0, "expected a new fixture directory");
await fs.mkdir(destination, { mode: 0o700 });
const fixture = await enableHubUnder(path.join(destination, "data"), "project", [
  "repo.read",
  "source.push",
]);
const encoded = encodeRepository({
  id: fixture.repoId,
  hints: ["http://127.0.0.1:18080/project"],
});
assert.ok(Result.isSuccess(encoded));
await fs.writeFile(
  path.join(destination, "fixture.json"),
  JSON.stringify({ remote: `git+id://${encoded.success}`, token: fixture.credential }),
  { flag: "wx", mode: 0o600 },
);
console.info("Prepared disposable standalone fixture; credentials expire in five minutes.");
