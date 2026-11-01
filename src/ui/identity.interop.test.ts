import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as http from "node:http";
import { describe, it } from "@effect/vitest";
import { Effect, Predicate, Schema } from "effect";
import { chromium, firefox, webkit, type BrowserContext } from "playwright";
import { browserBundle } from "../testing/Browser.ts";

const selected = process.env["GIT_TEST_BROWSER"];
const engine = Schema.decodeUnknownSync(Schema.Literals(["chromium", "firefox", "webkit"]))(
  selected ?? "chromium",
);
const browserType = { chromium, firefox, webkit }[engine];

const withBrowser = async (
  test: (context: BrowserContext, url: string, bundle: string, profile: string) => Promise<void>,
): Promise<void> => {
  const bundle = await browserBundle(
    'export { describeIdentity } from "./src/ui/identity.ts";',
    "IdentityReview",
  );
  const server = http.createServer((_request, response) => {
    response.writeHead(200, { "content-type": "text/html" });
    response.end("<!doctype html><title>Identity regression</title>");
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address !== null && !Predicate.isString(address));
  try {
    // A real temporary profile exercises ordinary persisted storage. WebKit's
    // off-the-record contexts can deny OPFS before a scenario injects its fault.
    const profile = await mkdtemp(join(tmpdir(), `git-identity-${engine}-`));
    try {
      const context = await browserType.launchPersistentContext(profile);
      try {
        await test(context, `http://127.0.0.1:${address.port}`, bundle, profile);
      } finally {
        await context.close();
      }
    } finally {
      await rm(profile, { recursive: true, force: true });
    }
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
};

// Explicit qualification runs must fail if their requested engine is missing.
describe.skipIf(selected === undefined && !existsSync(browserType.executablePath()))(
  "browser signing identity",
  () => {
    it.live("retains its identity after closing and reopening the browser profile", () =>
      Effect.promise(() =>
        withBrowser(async (context, url, bundle, profile) => {
          const page = await context.newPage();
          await page.goto(url);
          await page.addScriptTag({ content: bundle });
          const saved = await page.evaluate("IdentityReview.describeIdentity()");
          await context.close();
          const reopened = await browserType.launchPersistentContext(profile);
          try {
            const page = await reopened.newPage();
            await page.goto(url);
            await page.addScriptTag({ content: bundle });
            assert.deepEqual(await page.evaluate("IdentityReview.describeIdentity()"), saved);
          } finally {
            await reopened.close();
          }
        }),
      ),
    );

    for (const failure of ["SecurityError", "NotAllowedError"] as const) {
      it.live(`refuses a new identity while OPFS access raises ${failure}`, () =>
        Effect.promise(() =>
          withBrowser(async (context, url, bundle) => {
            const first = await context.newPage();
            await first.goto(url);
            await first.addScriptTag({ content: bundle });
            const saved = await first.evaluate("IdentityReview.describeIdentity()");
            const denied = await context.newPage();
            await denied.goto(url);
            await denied.addScriptTag({ content: bundle });
            await denied.evaluate((failure) => {
              Object.defineProperty(StorageManager.prototype, "getDirectory", {
                value: async () => {
                  throw new DOMException("origin storage access denied", failure);
                },
              });
            }, failure);
            await assert.rejects(
              denied.evaluate("navigator.storage.getDirectory()"),
              /origin storage access denied/,
            );
            for (let attempt = 0; attempt < 2; attempt++)
              await assert.rejects(
                denied.evaluate("IdentityReview.describeIdentity()"),
                /origin storage access denied/,
              );
            await denied.reload();
            await denied.addScriptTag({ content: bundle });
            assert.deepEqual(await denied.evaluate("IdentityReview.describeIdentity()"), saved);
          }),
        ),
      );
    }

    it.live("does not return a fresh signing identity before its record can be persisted", () =>
      Effect.promise(() =>
        withBrowser(async (context, url, bundle) => {
          const page = await context.newPage();
          await page.goto(url);
          await page.addScriptTag({ content: bundle });
          await page.evaluate(() => {
            // oxlint-disable-next-line typescript/unbound-method -- Restored on its prototype and invoked with the receiver through call.
            const original = FileSystemFileHandle.prototype.createWritable;
            FileSystemFileHandle.prototype.createWritable = async function (options) {
              if (this.name === "identity.json") {
                FileSystemFileHandle.prototype.createWritable = original;
                throw new DOMException("identity quota exhausted", "QuotaExceededError");
              }
              return original.call(this, options);
            };
          });
          await assert.rejects(
            page.evaluate("IdentityReview.describeIdentity()"),
            /identity quota exhausted/,
          );
          const saved = await page.evaluate<{ fingerprint: string; publicKey: string }>(
            "IdentityReview.describeIdentity()",
          );
          await page.reload();
          await page.addScriptTag({ content: bundle });
          const reopened = await page.evaluate<{ fingerprint: string; publicKey: string }>(
            "IdentityReview.describeIdentity()",
          );
          assert.equal(reopened.fingerprint, saved.fingerprint);
          assert.equal(reopened.publicKey, saved.publicKey);
        }),
      ),
    );

    it.live("labels an identity without OPFS as temporary and keeps it only within the page", () =>
      Effect.promise(() =>
        withBrowser(async (context, url, bundle) => {
          await context.addInitScript(() => {
            Object.defineProperty(StorageManager.prototype, "getDirectory", { value: undefined });
          });
          const page = await context.newPage();
          await page.goto(url);
          await page.addScriptTag({ content: bundle });
          const first = await page.evaluate<{ fingerprint: string; note: string }>(
            "IdentityReview.describeIdentity()",
          );
          assert.match(first.note, /only for this page visit/);
          assert.deepEqual(await page.evaluate("IdentityReview.describeIdentity()"), first);
          await page.reload();
          await page.addScriptTag({ content: bundle });
          const next = await page.evaluate<{ fingerprint: string; note: string }>(
            "IdentityReview.describeIdentity()",
          );
          assert.notEqual(next.fingerprint, first.fingerprint);
          assert.match(next.note, /only for this page visit/);
        }),
      ),
    );

    it.live("keeps the same persisted key when two tabs initialize together", () =>
      Effect.promise(() =>
        withBrowser(async (context, url, bundle) => {
          const firstMissing = Promise.withResolvers<void>();
          const secondStarted = Promise.withResolvers<void>();
          const releaseFirst = Promise.withResolvers<void>();
          try {
            await context.exposeFunction("identityMissing", async (tab: string) => {
              if (tab === "first") {
                firstMissing.resolve();
                await releaseFirst.promise;
              } else {
                secondStarted.resolve();
              }
            });
            await context.exposeFunction("identityLock", (tab: string) => {
              if (tab === "second") secondStarted.resolve();
            });
            const first = await context.newPage();
            const second = await context.newPage();
            for (const [page, tab] of [
              [first, "first"],
              [second, "second"],
            ] as const) {
              await page.goto(url);
              await page.addScriptTag({ content: bundle });
              // Pause the first tab after its real OPFS read finds no record.
              // The second either reads that same absence (the bug), or requests
              // the origin lock and waits until the first has saved its key.
              await page.evaluate(`(() => {
            const original = FileSystemDirectoryHandle.prototype.getFileHandle;
            FileSystemDirectoryHandle.prototype.getFileHandle = async function(name, options) {
              try { return await original.call(this, name, options); }
              catch (error) {
                if (name === "identity.json" && !options?.create && error.name === "NotFoundError")
                  await identityMissing(${JSON.stringify(tab)});
                throw error;
              }
            };
            const request = navigator.locks.request.bind(navigator.locks);
            navigator.locks.request = (...args) => {
              void identityLock(${JSON.stringify(tab)});
              return request(...args);
            };
          })()`);
            }
            await first.evaluate(async () => {
              const root = await navigator.storage.getDirectory();
              const scope = await root.getDirectoryHandle("git-plus", { create: true });
              await scope.getDirectoryHandle("identity", { create: true });
            });
            const openingFirst = first.evaluate<{ fingerprint: string; publicKey: string }>(
              "IdentityReview.describeIdentity()",
            );
            await firstMissing.promise;
            const openingSecond = second.evaluate<{ fingerprint: string; publicKey: string }>(
              "IdentityReview.describeIdentity()",
            );
            await secondStarted.promise;
            releaseFirst.resolve();
            const keys = await Promise.all([openingFirst, openingSecond]);
            assert.equal(keys[0].fingerprint, keys[1].fingerprint);
            assert.equal(keys[0].publicKey, keys[1].publicKey);

            const reloaded = await context.newPage();
            await reloaded.goto(url);
            await reloaded.addScriptTag({ content: bundle });
            assert.deepEqual(await reloaded.evaluate("IdentityReview.describeIdentity()"), keys[0]);
          } finally {
            releaseFirst.resolve();
          }
        }),
      ),
    );

    for (const failure of ["directory", "record", "legacy", "migration"] as const) {
      it.live(`preserves the saved key and retries after a temporary ${failure} failure`, () =>
        Effect.promise(() =>
          withBrowser(async (context, url, bundle) => {
            const first = await context.newPage();
            await first.goto(url);
            await first.addScriptTag({ content: bundle });
            const saved = await first.evaluate("IdentityReview.describeIdentity()");
            if (failure === "legacy" || failure === "migration") {
              const encoded = await first.evaluate(async () => {
                const root = await navigator.storage.getDirectory();
                const scope = await root.getDirectoryHandle("git-plus");
                const directory = await scope.getDirectoryHandle("identity");
                return await (
                  await (await directory.getFileHandle("identity.json")).getFile()
                ).text();
              });
              const record = Schema.decodeSync(
                Schema.fromJsonString(
                  Schema.Struct({
                    seed: Schema.String,
                    publicKey: Schema.String,
                  }),
                ),
              )(encoded);
              await first.evaluate(async (record) => {
                const root = await navigator.storage.getDirectory();
                const scope = await root.getDirectoryHandle("git-plus");
                const directory = await scope.getDirectoryHandle("identity");
                const seed = Uint8Array.from(atob(record.seed), (character) =>
                  character.charCodeAt(0),
                );
                for (const [name, bytes] of [
                  ["seed", seed],
                  ["public", new TextEncoder().encode(record.publicKey)],
                ] as const) {
                  const writable = await (
                    await directory.getFileHandle(name, { create: true })
                  ).createWritable();
                  await writable.write(bytes);
                  await writable.close();
                }
                await directory.removeEntry("identity.json");
              }, record);
            }
            const reopening = await context.newPage();
            await reopening.goto(url);
            await reopening.addScriptTag({ content: bundle });
            await reopening.evaluate((failure) => {
              if (failure === "migration") {
                // oxlint-disable-next-line typescript/unbound-method -- Restored on its prototype and invoked with the receiver through call.
                const original = FileSystemFileHandle.prototype.createWritable;
                FileSystemFileHandle.prototype.createWritable = async function (options) {
                  if (this.name === "identity.json") {
                    FileSystemFileHandle.prototype.createWritable = original;
                    throw new DOMException("temporary identity storage failure", "UnknownError");
                  }
                  return original.call(this, options);
                };
                return;
              }
              if (failure === "directory") {
                // oxlint-disable-next-line typescript/unbound-method -- Restored on its prototype and invoked with the receiver through call.
                const original = FileSystemDirectoryHandle.prototype.getDirectoryHandle;
                FileSystemDirectoryHandle.prototype.getDirectoryHandle = async function (
                  name,
                  options,
                ) {
                  if (name === "identity") {
                    FileSystemDirectoryHandle.prototype.getDirectoryHandle = original;
                    throw new DOMException("temporary identity storage failure", "UnknownError");
                  }
                  return original.call(this, name, options);
                };
                return;
              }
              // oxlint-disable-next-line typescript/unbound-method -- Restored on its prototype and invoked with the receiver through call.
              const original = FileSystemDirectoryHandle.prototype.getFileHandle;
              FileSystemDirectoryHandle.prototype.getFileHandle = async function (name, options) {
                if (
                  failure === "legacy"
                    ? name === "seed"
                    : name === "identity.json" && !options?.create
                ) {
                  FileSystemDirectoryHandle.prototype.getFileHandle = original;
                  throw new DOMException("temporary identity storage failure", "UnknownError");
                }
                return original.call(this, name, options);
              };
            }, failure);
            await assert.rejects(
              reopening.evaluate("IdentityReview.describeIdentity()"),
              /temporary identity storage failure/,
            );
            assert.deepEqual(await reopening.evaluate("IdentityReview.describeIdentity()"), saved);
            await reopening.reload();
            await reopening.addScriptTag({ content: bundle });
            assert.deepEqual(await reopening.evaluate("IdentityReview.describeIdentity()"), saved);
          }),
        ),
      );
    }
  },
);
