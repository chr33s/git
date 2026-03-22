import { after, before, describe, it } from "node:test";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import ts from "typescript";

import * as helpers from "./test.helpers.ts";

// Read and transpile git.storage.ts (for validateStoragePath) and client.storage.ts
const storageSrc = await readFile(join(helpers.__dirname, "git.storage.ts"), "utf-8");
const storageTranspiled = ts.transpileModule(storageSrc, {
  compilerOptions: {
    target: ts.ScriptTarget.ES2022,
    module: ts.ModuleKind.ESNext,
  },
});

const src = await readFile(join(helpers.__dirname, "client.storage.ts"), "utf-8");
const transpiled = ts.transpileModule(src, {
  compilerOptions: {
    target: ts.ScriptTarget.ES2022,
    module: ts.ModuleKind.ESNext,
  },
});

// Combine: inject validateStoragePath from git.storage.ts, then OpfsStorage from client.storage.ts
const storageCode = storageTranspiled.outputText
  .replace(/^export /gm, "")
  .replace(/^import .*/gm, "");
const clientCode = transpiled.outputText.replace(/^export /gm, "").replace(/^import .*/gm, "");
const browserCode = storageCode + "\n" + clientCode + "\nwindow.OpfsStorage = OpfsStorage;";

const worker = await helpers.worker();
const playwright = await helpers.playwright({ headless: false });

before(async () => {
  await worker.before();
  await playwright.before();

  await playwright.page.goto(`${worker.url}/test`);
  await playwright.page.addScriptTag({ content: browserCode, type: "module" });
  await playwright.page.waitForFunction(() => "OpfsStorage" in window);
});
after(async () => {
  await playwright.after();
  await worker.after();
});

void describe("OpfsStorage", async () => {
  await playwright.page.route("*/**/test", async (route) => {
    await route.fulfill({ body: "OK" });
  });

  void it("should initialize and store data", async () => {
    await playwright.page.evaluate(async () => {
      // @ts-ignore - OpfsStorage is injected via addScriptTag
      const storage = new window.OpfsStorage();
      await storage.init("test-repo");
      await storage.writeFile("test.txt", new TextEncoder().encode("Hello World"));
      const content = await storage.readFile("test.txt");
      const text = new TextDecoder().decode(content);
      if (text !== "Hello World") {
        throw new Error(`Expected "Hello World" but got "${text}"`);
      }
    });
  });

  void it("should check file existence", async () => {
    await playwright.page.evaluate(async () => {
      // @ts-ignore - OpfsStorage is injected via addScriptTag
      const storage = new window.OpfsStorage();
      await storage.init("test-repo-2");
      const existsBefore = await storage.exists("nonexistent.txt");
      if (existsBefore) {
        throw new Error("File should not exist before creation");
      }
      await storage.writeFile("test.txt", new TextEncoder().encode("test"));
      const existsAfter = await storage.exists("test.txt");
      if (!existsAfter) {
        throw new Error("File should exist after creation");
      }
    });
  });

  void it("should list directory contents", async () => {
    await playwright.page.evaluate(async () => {
      // @ts-ignore - OpfsStorage is injected via addScriptTag
      const storage = new window.OpfsStorage();
      await storage.init("test-repo-3");
      await storage.writeFile("file1.txt", new TextEncoder().encode("content1"));
      await storage.writeFile("file2.txt", new TextEncoder().encode("content2"));
      const files = await storage.listDirectory("");
      if (!files.includes("file1.txt") || !files.includes("file2.txt")) {
        throw new Error(`Expected files not found in directory. Got: ${files.join(", ")}`);
      }
    });
  });

  void it("should delete files", async () => {
    await playwright.page.evaluate(async () => {
      // @ts-ignore - OpfsStorage is injected via addScriptTag
      const storage = new window.OpfsStorage();
      await storage.init("test-repo-4");
      await storage.writeFile("deleteme.txt", new TextEncoder().encode("content"));
      const existsBeforeDelete = await storage.exists("deleteme.txt");
      if (!existsBeforeDelete) {
        throw new Error("File should exist before deletion");
      }
      await storage.deleteFile("deleteme.txt");
      const existsAfterDelete = await storage.exists("deleteme.txt");
      if (existsAfterDelete) {
        throw new Error("File should not exist after deletion");
      }
    });
  });
});
