import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { pathToFileURL } from "node:url";
import { it } from "@effect/vitest";

const execute = promisify(execFile);
const collector = path.resolve("tools/release-notices.ts");

it.each([
  "valid",
  "missing",
  "empty",
  "unmapped",
  "supplemental",
  "tampered",
  "upgrade",
  "missing-supplemental",
])("notice inventory handles %s package texts", async (scenario) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "release-notices-"));
  try {
    await fs.writeFile(path.join(root, "LICENSE"), "fixture project license\n");
    await fs.writeFile(path.join(root, "package.json"), '{"name":"project","version":"1.0.0"}');
    await fs.writeFile(
      path.join(root, "node-license.txt"),
      "Node.js is licensed for use: runtime notice fixture\n",
    );
    for (const directory of ["dist/sea", "dist/ui", "patches"])
      await fs.mkdir(path.join(root, directory), { recursive: true });
    for (const name of [
      "fixture",
      "@fontsource-variable/instrument-sans",
      "@fontsource-variable/jetbrains-mono",
      "@phosphor-icons/core",
    ]) {
      const directory = path.join(root, "node_modules", name);
      await fs.mkdir(directory, { recursive: true });
      await fs.writeFile(
        path.join(directory, "package.json"),
        JSON.stringify({ name, version: "1.0.0", license: "MIT" }),
      );
      if (name !== "fixture" || scenario !== "missing") {
        await fs.writeFile(
          path.join(directory, "LICENSE"),
          name === "fixture" && scenario === "empty" ? "" : `test notice for ${name}\n`,
        );
      }
    }
    const supplemental = ["supplemental", "tampered", "upgrade", "missing-supplemental"].includes(
      scenario,
    );
    if (supplemental) {
      const directory = path.join(root, "node_modules/@shikijs/langs");
      await fs.mkdir(directory, { recursive: true });
      await fs.writeFile(
        path.join(directory, "package.json"),
        JSON.stringify({
          name: "@shikijs/langs",
          version: scenario === "upgrade" ? "4.5.1" : "4.5.0",
          license: "MIT",
        }),
      );
      await fs.writeFile(path.join(directory, "LICENSE"), "fixture root notice");
      await fs.cp(path.resolve("tools/licenses"), path.join(root, "tools/licenses"), {
        recursive: true,
      });
      const notice = path.join(root, "tools/licenses/shiki-4.5.0/tm-grammars.NOTICE");
      if (scenario === "tampered") await fs.appendFile(notice, "tampered");
      if (scenario === "missing-supplemental") await fs.unlink(notice);
    }
    // Export subdirectories can have a package name but no package version.
    await fs.mkdir(path.join(root, "node_modules/fixture/sub"));
    await fs.writeFile(
      path.join(root, "node_modules/fixture/sub/package.json"),
      '{"name":"fixture-sub"}',
    );
    await fs.writeFile(
      path.join(root, "dist/sea/main.mjs.map"),
      JSON.stringify({
        sources: [
          ...(supplemental ? ["../../node_modules/@shikijs/langs/dist/typescript.mjs"] : []),
          scenario === "unmapped"
            ? "../../node_modules/missing/index.js"
            : "../../node_modules/fixture/sub/index.js",
        ],
      }),
    );
    await fs.writeFile(path.join(root, "patches/fixture.patch"), "fixture patch\n");
    const result = execute(
      process.execPath,
      [
        "--input-type=module",
        "-e",
        `import { writeReleaseNotices } from ${JSON.stringify(pathToFileURL(collector).href)}; writeReleaseNotices("node-license.txt");`,
      ],
      { cwd: root, timeout: 30_000 },
    );
    if (scenario === "valid" || scenario === "supplemental") {
      assert.match((await result).stdout, /Collected notices for [45] packages/);
      const notices = await fs.readFile(
        path.join(root, "dist/sea/THIRD_PARTY_NOTICES.txt"),
        "utf8",
      );
      assert.match(notices, /test notice for fixture/);
      assert.match(notices, /Node.js is licensed for use/);
      if (supplemental) {
        assert.match(notices, /tm-grammars@1.32.22/);
        assert.match(notices, /THIRD-PARTY SOFTWARE NOTICES AND INFORMATION/);
        assert.match(notices, /Files:   [^\n]*typescript\.json/);
      }
      const inventory = await fs.readFile(
        path.join(root, "dist/sea/dependency-inventory.json"),
        "utf8",
      );
      assert.match(inventory, /fixture.patch/);
      assert.ok(
        !inventory.includes(root),
        "inventory must not expose the builder's absolute paths",
      );
    } else if (
      scenario === "tampered" ||
      scenario === "upgrade" ||
      scenario === "missing-supplemental"
    ) {
      await assert.rejects(
        result,
        scenario === "tampered"
          ? /Supplemental notice checksum mismatch/
          : scenario === "upgrade"
            ? /Review supplemental notices/
            : /ENOENT/,
      );
    } else if (scenario === "unmapped") {
      await assert.rejects(result, /Cannot identify mapped package/);
    } else {
      await assert.rejects(
        result,
        scenario === "missing" ? /Missing license text for fixture/ : /Empty notice for fixture/,
      );
    }
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
