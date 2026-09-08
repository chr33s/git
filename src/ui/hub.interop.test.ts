import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import * as http from "node:http";
import { describe, it } from "@effect/vitest";
import { Predicate } from "effect";
import { chromium } from "playwright";
import { browserBundle } from "../testing/Browser.ts";
import { isOid } from "../git/Store.ts";
import type {
  HubTask,
  HubPullSummary,
  HubSessionSummary,
  HubPullDetail,
} from "../server/ApiContract.ts";

const entrySource = `
  import { store } from "./src/ui/store.ts";
  export { refreshListings } from "./src/ui/hub.ts";
  export const snapshot = () => ({
    rows: store.rows().map(row => row.task.id).sort(),
    sessions: store.sessions.map(session => session.id).sort(),
  });
`;

const task = (id: string): HubTask => ({
  task: id,
  exists: true,
  title: id,
  description: "",
  refs: [],
  parent: null,
  children: [],
  available: true,
  claim: null,
  closed: null,
  sessions: [],
});
const pull = (id: string): HubPullSummary => ({
  id,
  title: id,
  base: "refs/heads/main",
  head: null,
  state: "open",
  author: null,
  approvals: 0,
  checks: { total: 0, passed: true },
  threads: { total: 0, unresolved: 0 },
  mergeable: { ok: false, reasons: ["no head"] },
  at: "2026-01-01T00:00:00.000Z",
});
const session = (id: string): HubSessionSummary => ({
  session: id,
  agent: null,
  refs: [],
  pulls: [],
  commits: 0,
  decisions: { total: 0, open: 0 },
  usage: { inputTokens: 0, outputTokens: 0 },
});

describe.skipIf(!existsSync(chromium.executablePath()))("hub listings in Chromium", () => {
  it("retains loaded pull details after an unrelated listing refresh", async () => {
    const bundle = await browserBundle(
      `
      import { store } from "./src/ui/store.ts";
      export { refreshListings, hydrate } from "./src/ui/hub.ts";
      export const snapshot = () => ({ pull: store.get("pr"), task: store.get("task") });
    `,
      "HubDetailReview",
    );
    let round = 0;
    let summary = pull("pr");
    let detail: HubPullDetail = {
      ...pull("pr"),
      description: "Discussion context",
      mergeCommit: null,
      commits: 3,
      reviews: [],
      threadList: [],
      checkList: [],
      rejected: [],
    };
    const server = http.createServer((request, response) => {
      const url = new URL(request.url ?? "/", "http://localhost");
      if (url.pathname === "/") {
        response.end("<!doctype html><title>Detail refresh</title>");
        return;
      }
      const paging = { next_cursor: null, has_more: false };
      const body = url.pathname.endsWith("/pulls/pr")
        ? detail
        : url.pathname.endsWith("/tasks")
          ? { ...paging, items: [{ ...task("task"), title: `round ${round}` }] }
          : url.pathname.endsWith("/pulls")
            ? { ...paging, enabled: true, reason: null, items: [summary] }
            : { ...paging, items: [] };
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify(body));
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    assert.ok(address !== null && !Predicate.isString(address));
    const browser = await chromium.launch();
    try {
      const page = await browser.newPage();
      await page.goto(`http://127.0.0.1:${address.port}`);
      await page.addScriptTag({ content: bundle });
      await page.waitForFunction("HubDetailReview.snapshot().pull?.hub === true");
      await page.evaluate("HubDetailReview.hydrate('pr')");
      await page.waitForFunction("HubDetailReview.snapshot().pull?.desc === 'Discussion context'");
      round++;
      await page.evaluate("HubDetailReview.refreshListings()");
      await page.waitForFunction("HubDetailReview.snapshot().task?.title === 'round 1'");
      assert.equal(await page.evaluate("HubDetailReview.snapshot().pull.desc"), detail.description);
      assert.equal(await page.evaluate("HubDetailReview.snapshot().pull.commitCount"), "3");
      const next = "a".repeat(40);
      assert.ok(isOid(next));
      summary = { ...summary, head: next };
      detail = { ...detail, head: next, description: "New proposal context", commits: 4 };
      await page.evaluate("HubDetailReview.refreshListings()");
      await page.waitForFunction(
        "HubDetailReview.snapshot().pull?.desc === 'New proposal context'",
      );
      assert.equal(await page.evaluate("HubDetailReview.snapshot().pull.commitCount"), "4");
      assert.equal(await page.evaluate("HubDetailReview.snapshot().pull.reviewHead"), next);
    } finally {
      await browser.close();
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });
  it("loads every page and refreshes the complete task, PR, and session listings", async () => {
    const bundle = await browserBundle(entrySource, "HubReview");
    let round = 0;
    const requests: string[] = [];
    const server = http.createServer((request, response) => {
      const url = new URL(request.url ?? "/", "http://localhost");
      if (url.pathname === "/") {
        response.writeHead(200, { "content-type": "text/html" });
        response.end("<!doctype html><title>Hub regression</title>");
        return;
      }
      const page = url.searchParams.get("cursor") === "second" ? 1 : 0;
      requests.push(`${round}:${url.pathname}:${page}`);
      const paging = { next_cursor: page === 0 ? "second" : null, has_more: page === 0 };
      const body = url.pathname.endsWith("/tasks")
        ? { ...paging, items: [task(`task-${round}-${page}`)] }
        : url.pathname.endsWith("/pulls")
          ? { ...paging, enabled: true, reason: null, items: [pull(`pr-${round}-${page}`)] }
          : { ...paging, items: [session(`session-${round}-${page}`)] };
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify(body));
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    assert.ok(address !== null && !Predicate.isString(address));
    const browser = await chromium.launch();
    try {
      const page = await browser.newPage();
      await page.goto(`http://127.0.0.1:${address.port}`);
      await page.addScriptTag({ content: bundle });
      for (round = 0; round < 2; round++) {
        if (round > 0) await page.evaluate("HubReview.refreshListings()");
        await page.waitForFunction(
          `(HubReview.snapshot().rows.includes('task-${round}-1') &&
            HubReview.snapshot().rows.includes('pr-${round}-1') &&
            HubReview.snapshot().sessions.includes('session-${round}-1'))`,
          undefined,
          { timeout: 3000 },
        );
        const snapshot = await page.evaluate<{ rows: string[]; sessions: string[] }>(
          "HubReview.snapshot()",
        );
        assert.deepEqual(snapshot.rows, [
          `pr-${round}-0`,
          `pr-${round}-1`,
          `task-${round}-0`,
          `task-${round}-1`,
        ]);
        assert.deepEqual(snapshot.sessions, [`session-${round}-0`, `session-${round}-1`]);
        for (const listing of ["tasks", "pulls", "sessions"]) {
          assert.ok(requests.includes(`${round}:/core/hub/${listing}:1`));
        }
      }
    } finally {
      await browser.close();
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });
});
