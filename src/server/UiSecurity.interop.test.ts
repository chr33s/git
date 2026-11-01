import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { createServer } from "node:http";
import { resolve } from "node:path";
import { describe, it } from "@effect/vitest";
import { Effect, Predicate } from "effect";
import { chromium } from "playwright";
import { assetResponse } from "./Static.node.ts";

describe.skipIf(!existsSync(chromium.executablePath()))("production UI security", () => {
  it.live("allows bootstrap but blocks injected scripts and framing", () =>
    Effect.promise(async () => {
      // A missing build is a failure, not a skip: release verification must
      // exercise the assets that will actually be installed.
      assert.ok(existsSync(resolve("dist/ui/index.html")), "build the production UI first");
      const server = createServer((request, response) => {
        void (async () => {
          if (request.url === "/frame") {
            response.setHeader("content-type", "text/html");
            response.end('<iframe src="/hub/"></iframe>');
            return;
          }
          const asset = await assetResponse(
            resolve("dist/ui"),
            new Request(`http://localhost${request.url ?? "/"}`),
          );
          if (asset === null) {
            response.writeHead(404).end();
            return;
          }
          response.writeHead(asset.status, Object.fromEntries(asset.headers));
          response.end(Buffer.from(await asset.arrayBuffer()));
        })().catch(() => response.writeHead(500).end());
      });
      await new Promise<void>((done) => server.listen(0, "127.0.0.1", done));
      const address = server.address();
      assert.ok(address !== null && !Predicate.isString(address));
      try {
        const browser = await chromium.launch({ headless: true });
        try {
          const page = await browser.newPage();
          const violations: string[] = [];
          page.on("console", (message) => {
            if (/Content Security Policy/i.test(message.text())) violations.push(message.text());
          });
          const origin = `http://127.0.0.1:${address.port}`;
          await page.addInitScript(() => localStorage.setItem("gp-theme", "dark"));
          await page.goto(`${origin}/hub/`);
          assert.equal(await page.locator("html").getAttribute("data-theme"), "dark");
          assert.deepEqual(violations, [], "the built application must boot under its policy");
          const executed = await page.evaluate(() => {
            const script = document.createElement("script");
            script.textContent = 'document.documentElement.dataset.injected = "yes"';
            document.head.appendChild(script);
            const button = document.createElement("button");
            button.setAttribute("onclick", 'document.documentElement.dataset.injected = "yes"');
            document.body.appendChild(button);
            button.click();
            return document.documentElement.dataset.injected;
          });
          assert.equal(executed, undefined, "inline scripts and event handlers must be blocked");
          await page.goto("about:blank");
          await page.goto(`${origin}/hub/#/settings`);
          await page.waitForURL(`${origin}/hub/settings`);
          await page.goto(`${origin}/frame`);
          assert.equal(
            await page
              .locator("iframe")
              .evaluate(
                (frame) => frame instanceof HTMLIFrameElement && frame.contentDocument === null,
              ),
            true,
            "even same-origin embedding must be denied",
          );
        } finally {
          await browser.close();
        }
      } finally {
        server.closeAllConnections();
        await new Promise<void>((done) => server.close(() => done()));
      }
    }),
  );
});
