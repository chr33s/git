import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { Config, Effect } from "effect";

export const environment = Effect.fn("git.IgnoreConfig.environment")(function* () {
  const home = yield* Config.string("HOME").pipe(Config.withDefault(os.homedir()));
  const xdg =
    (yield* Config.string("XDG_CONFIG_HOME").pipe(Config.withDefault(""))) ||
    path.join(home, ".config");
  const global = yield* Config.string("GIT_CONFIG_GLOBAL").pipe(Config.withDefault(""));
  const system = yield* Config.string("GIT_CONFIG_SYSTEM").pipe(
    Config.withDefault("/etc/gitconfig"),
  );
  const noSystem = yield* Config.string("GIT_CONFIG_NOSYSTEM").pipe(Config.withDefault("false"));
  return {
    home,
    defaultExcludes: path.join(xdg, "git/ignore"),
    files: [
      ...(/^(?:false|no|off|0|)$/i.test(noSystem) ? [system] : []),
      ...(global === "" ? [path.join(xdg, "git/config"), path.join(home, ".gitconfig")] : [global]),
    ],
  };
});

export interface Environment {
  readonly home: string;
  readonly defaultExcludes: string;
  readonly files: ReadonlyArray<string>;
}

const expand = (file: string, home: string): string =>
  file.startsWith("~/") ? path.join(home, file.slice(2)) : file;

export const forRepository = async (gitDirectory: string, environment: Environment) => {
  let relative = "";
  try {
    relative = (await fs.readFile(path.join(gitDirectory, "commondir"), "utf8")).trim();
  } catch (cause) {
    if (!(cause instanceof Error && "code" in cause && cause.code === "ENOENT")) throw cause;
  }
  return load(
    relative === "" ? gitDirectory : path.resolve(gitDirectory, relative),
    gitDirectory,
    environment,
  );
};

/** Decode Git's quoted value syntax without treating quoted comment markers as comments. */
const configValue = (source: string): string => {
  let value = "";
  let spaces = "";
  let quoted = false;
  for (let index = 0; index < source.length; index += 1) {
    const character = source[index];
    if (character === '"') {
      if (!quoted && value !== "") value += spaces;
      spaces = "";
      quoted = !quoted;
    } else if (character === "\\") {
      const escaped = source[++index];
      const decoded =
        escaped === "n" ? "\n" : escaped === "t" ? "\t" : escaped === "b" ? "\b" : escaped;
      if (decoded === undefined || !['"', "\\", "n", "t", "b"].includes(escaped ?? ""))
        throw new Error("invalid escape in Git configuration");
      value += spaces + decoded;
      spaces = "";
    } else if (!quoted && (character === "#" || character === ";")) break;
    else if (!quoted && (character === " " || character === "\t")) {
      if (value !== "") spaces += character;
    } else {
      value += spaces + character;
      spaces = "";
    }
  }
  if (quoted) throw new Error("unterminated quote in Git configuration");
  return value;
};

/** Includes are expanded at their position, so later assignments retain Git's precedence. */
export const load = async (common: string, gitDirectory: string, environment: Environment) => {
  const values = new Map<string, string>();
  const visit = async (file: string, depth: number): Promise<void> => {
    if (depth > 10) throw new Error("Git configuration include depth exceeded");
    let contents: string;
    try {
      contents = await fs.readFile(file, "utf8");
    } catch (cause) {
      if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return;
      throw cause;
    }
    let section = "";
    for (let line of contents.replace(/\\\r?\n/g, "").split(/\r?\n/)) {
      const header = /^\s*\[([^\]]+)\]\s*/.exec(line);
      if (header !== null) {
        section = (header[1] ?? "").trim().toLowerCase();
        line = line.slice(header[0].length);
      }
      const entry = /^\s*([a-z][a-z0-9-]*)\s*(?:=\s*(.*))?\s*(?:[#;].*)?$/i.exec(line);
      if (entry === null) continue;
      const key = `${section}.${entry[1]?.toLowerCase()}`;
      if (
        ![
          "core.ignorecase",
          "core.filemode",
          "core.excludesfile",
          "include.path",
          "extensions.worktreeconfig",
        ].includes(key)
      )
        continue;
      const value = entry[2] === undefined ? "true" : configValue(entry[2]);
      if (key === "include.path") {
        if (value !== "")
          await visit(path.resolve(path.dirname(file), expand(value, environment.home)), depth + 1);
      } else values.set(key, value);
    }
  };
  for (const file of [...environment.files, path.join(common, "config")]) await visit(file, 0);
  if (/^(true|yes|on|1)$/i.test(values.get("extensions.worktreeconfig") ?? "false"))
    await visit(path.join(gitDirectory, "config.worktree"), 0);
  return {
    ignoreCase: /^(true|yes|on|1)$/i.test(values.get("core.ignorecase") ?? "false"),
    trustExecutableBit: /^(true|yes|on|1)$/i.test(values.get("core.filemode") ?? "true"),
    excludesFile: expand(
      values.get("core.excludesfile") ?? environment.defaultExcludes,
      environment.home,
    ),
  };
};
