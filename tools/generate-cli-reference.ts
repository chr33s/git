import { execFileSync } from "node:child_process";
import * as fs from "node:fs";

const help = execFileSync(process.execPath, ["src/cli/main.ts", "--help"], {
  encoding: "utf8",
});
const output = `# CLI reference

This reference is generated from the command tree. \`git+ --help\` and
\`git+ <command> --help\` are the canonical interface; regenerate this snapshot
with \`npm run docs:cli\` after changing commands.

## Top-level commands

\`GIT_ROOT\`, \`PORT\`, \`HOSTNAME\`, and \`GIT_HOSTS\` configure \`git+ serve\` when its
corresponding explicit flag is absent. Other local commands use \`--root\`.

\`\`\`text
${help.trimEnd()}
\`\`\`
`;
const destination = "docs/cli.md";

if (process.argv[2] === "--check") {
  if (fs.readFileSync(destination, "utf8") !== output) {
    throw new Error(`${destination} is stale; run npm run docs:cli`);
  }
} else {
  fs.writeFileSync(destination, output);
}
