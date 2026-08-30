#!/usr/bin/env node
/** Keep docs/cli.md's command list tied to the CLI that actually ships. */
import * as fs from "node:fs";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const START = "<!-- BEGIN GENERATED CLI HELP -->";
const END = "<!-- END GENERATED CLI HELP -->";

const document = fileURLToPath(new URL("../docs/cli.md", import.meta.url));
const cli = fileURLToPath(new URL("../src/cli/bin.ts", import.meta.url));

// Runs bin.ts directly; relies on Node >=24 native TypeScript type stripping.
const rendered = spawnSync(process.execPath, [cli, "--help"], {
  encoding: "utf8",
  env: { ...process.env, FORCE_COLOR: "0", NO_COLOR: "1" },
});

if (rendered.error) {
  console.error(`failed to spawn ${cli}: ${rendered.error.message}`);
  process.exit(1);
}

if (rendered.status !== 0) {
  process.stderr.write(rendered.stderr);
  process.exit(rendered.status ?? 1);
}

const help = rendered.stdout.trimEnd();
if (help === "") throw new Error("git+ --help produced no stdout");

const block = `${START}\n\`\`\`text\n${help}\n\`\`\`\n${END}`;
const source = fs.readFileSync(document, "utf8");
const start = source.indexOf(START);
const end = source.indexOf(END);

if (start < 0 || end < start) {
  throw new Error(`docs/cli.md must contain ${START} and ${END}`);
}

const next = `${source.slice(0, start)}${block}${source.slice(end + END.length)}`;
const checking = process.argv.includes("--check");

if (checking) {
  if (next !== source) {
    console.error("docs/cli.md is stale; run `npm run docs:cli`");
    process.exitCode = 1;
  }
} else if (next !== source) {
  fs.writeFileSync(document, next);
  console.log("Updated docs/cli.md");
}
