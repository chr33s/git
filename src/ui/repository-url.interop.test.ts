import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { existsSync } from "node:fs";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { chromium } from "playwright";
import { formatPublicKey, generate } from "../crypto/SshSignature.ts";
import { stores } from "../git/Node.ts";
import * as Repository from "../git/Repository.ts";
import { serve } from "../host/Node.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import { browserBundle } from "../testing/Browser.ts";
import { fastImport, gitIn, hasGit, importCommit } from "../testing/Git.ts";

const entry = `
  import { Effect } from "effect";
  import { AtomRegistry } from "effect/unstable/reactivity";
  import { GitApi } from "./src/ui/api.ts";
  import { GitPlusApi, repoFromDocument } from "./src/ui/client.ts";
  import { registry } from "./src/ui/atoms.ts";
  export { describeIdentity, openTask } from "./src/ui/identity.ts";
  const repo = repoFromDocument();
  const api = new GitApi({ repo });
  export const refs = () => api.refs();
  export const cloneUrl = api.cloneUrl;
  export const tasks = () => Effect.runPromise(AtomRegistry.getResult(registry,
    GitPlusApi.query("hub", "tasks", { params: { repo }, query: {} })));
`;

describe.skipIf(!hasGit || !existsSync(chromium.executablePath()))(
  "repository URLs in Chromium",
  () => {
    it.live(
      "reads and authors in the literal repository through signed and generated clients",
      () =>
        Effect.promise(async () => {
          const root = await fs.mkdtemp(path.join(os.tmpdir(), "ui-repository-url-"));
          const heads = new Map<string, string>();
          for (const repo of ["project", "project.git", "project.git.git"]) {
            const directory = path.join(root, repo);
            await fs.mkdir(directory);
            gitIn(directory)("init", "--bare", "-q", "-b", "main");
            fastImport(
              directory,
              importCommit({ branch: "refs/heads/main", mark: 1, message: repo, files: [] }),
            );
            heads.set(repo, gitIn(directory)("rev-parse", "HEAD").trim());
          }
          const server = await serve({ root, allowAnonymousWrites: true });
          try {
            const bundle = await browserBundle(entry, "RepositoryUrlReview");
            const browser = await chromium.launch();
            try {
              for (const repo of ["project.git", "project.git.git"]) {
                const page = await browser.newPage();
                await page.goto(server.url);
                await page.evaluate((repo) => {
                  const meta = document.createElement("meta");
                  meta.name = "gp-repo";
                  meta.content = repo;
                  document.head.append(meta);
                }, repo);
                await page.addScriptTag({ content: bundle });
                // Before enrolment, stock Git reads the exact URL the UI exposes for cloning.
                const url = await page.evaluate<string>("RepositoryUrlReview.cloneUrl");
                const advertised = await promisify(execFile)("git", [
                  "ls-remote",
                  url,
                  "refs/heads/main",
                ]);
                assert.equal(advertised.stdout.split(/\s/)[0], heads.get(repo));
                const identity = await page.evaluate<{ publicKey: string }>(
                  "RepositoryUrlReview.describeIdentity()",
                );
                await Effect.runPromise(
                  Effect.gen(function* () {
                    const owner = yield* generate("owner@example.com");
                    const genesis = yield* create([formatPublicKey(owner.publicKey)], 1);
                    yield* writeGenesis(genesis, [yield* signGenesis(genesis, owner)]);
                    yield* Log.issue(
                      yield* Certificate.grant({
                        repo: genesis.repoId,
                        publicKey: identity.publicKey,
                        capabilities: ["repo.read", "source.push", "hub.task"],
                        id: Log.newId(),
                      }),
                      [owner],
                    );
                  }).pipe(
                    Effect.provide(
                      Repository.layer.pipe(
                        Layer.provide(Repository.hooksNoop),
                        Layer.provide(stores(path.join(root, repo))),
                      ),
                    ),
                  ),
                );
                const refs = await page.evaluate<Array<{ name: string; oid: string }>>(
                  "RepositoryUrlReview.refs()",
                );
                assert.equal(
                  refs.find((ref) => ref.name === "refs/heads/main")?.oid,
                  heads.get(repo),
                );
                const task = await page.evaluate<string>(
                  'RepositoryUrlReview.openTask({ title: "Correct repository", description: "" })',
                );
                const tasks = await page.evaluate<{ items: Array<{ task: string }> }>(
                  "RepositoryUrlReview.tasks()",
                );
                assert.ok(tasks.items.some((item) => item.task === task));
                await page.close();
              }
            } finally {
              await browser.close();
            }
          } finally {
            await server.close();
            await fs.rm(root, { recursive: true, force: true });
          }
        }),
    );
  },
);
