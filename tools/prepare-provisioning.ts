/** Prepare and verify identity material; does not upload or change a Worker. */
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { Effect, Layer } from "effect";
import { stores } from "../src/git/Node.ts";
import * as Repository from "../src/git/Repository.ts";
import { prepare } from "../src/server/Provisioning.ts";

const [directory, repo, output, ...extra] = process.argv.slice(2);
if (directory === undefined || repo === undefined || output === undefined || extra.length > 0)
  throw new Error(
    "usage: node tools/prepare-provisioning.ts <git-directory> <repo-name> <output.json>",
  );
await fs.access(path.join(path.resolve(directory), "HEAD"));
const bytes = await Effect.runPromise(
  prepare(repo).pipe(
    Effect.provide(
      Repository.layer.pipe(
        Layer.provide(Repository.hooksNoop),
        Layer.provide(stores(path.resolve(directory))),
      ),
    ),
  ),
);
await fs.writeFile(output, bytes, { flag: "wx", mode: 0o600 });
console.info(
  `Verified provisioning artifact written (${bytes.byteLength} bytes). No deployment was changed.`,
);
