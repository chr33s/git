import * as fs from "node:fs/promises";
import * as path from "node:path";
import ignore, { type Ignore } from "ignore";
import * as IgnoreConfig from "./IgnoreConfig.node.ts";

const read = async (file: string): Promise<string> => {
  try {
    return await fs.readFile(file, "utf8");
  } catch (cause) {
    if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return "";
    throw cause;
  }
};

/** Root-relative patterns let one matcher apply precedence across ignore files. */
const rebase = (contents: string, prefix: string): string[] =>
  contents.split(/\r?\n/).flatMap((line) => {
    if (line.startsWith("#") || line.trim() === "") return [];
    if (prefix === "") return [line];
    const negative = line.startsWith("!");
    const pattern = negative ? line.slice(1) : line;
    const anchored = pattern.replace(/ +$/, "").replace(/\/$/, "").includes("/");
    // The directory is a literal filesystem path, not part of the user's glob.
    const directory = prefix.replace(/[\\*?[\]]/g, "\\$&");
    return [
      `${negative ? "!" : ""}/${directory}/${anchored || pattern.startsWith("/") ? "" : "**/"}${pattern.replace(/^\//, "")}`,
    ];
  });

export interface IgnoreRules {
  readonly matcher: Ignore;
  readonly ignoreCase: boolean;
}

export const rootRules = async (
  gitDirectory: string,
  root: string,
  environment: IgnoreConfig.Environment,
): Promise<IgnoreRules> => {
  const relative = (await read(path.join(gitDirectory, "commondir"))).trim();
  const common = relative === "" ? gitDirectory : path.resolve(gitDirectory, relative);
  const config = await IgnoreConfig.load(common, gitDirectory, environment);
  const insensitive = config.ignoreCase;
  const excluded =
    config.excludesFile === "" ? "" : await read(path.resolve(root, config.excludesFile));
  return {
    matcher: ignore({ ignorecase: insensitive })
      .add(excluded)
      .add(await read(path.join(common, "info/exclude"))),
    ignoreCase: insensitive,
  };
};

export const directoryRules = async (
  root: string,
  prefix: string,
  inherited: IgnoreRules,
): Promise<IgnoreRules> => {
  const file = path.join(root, prefix, ".gitignore");
  try {
    if (!(await fs.lstat(file)).isFile()) return inherited;
  } catch (cause) {
    if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return inherited;
    throw cause;
  }
  return {
    matcher: ignore({ ignorecase: inherited.ignoreCase })
      .add(inherited.matcher)
      .add(rebase(await read(file), prefix)),
    ignoreCase: inherited.ignoreCase,
  };
};
