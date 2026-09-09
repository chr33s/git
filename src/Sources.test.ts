import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

/** The three fields these checks read out of a package manifest. */
interface Manifest {
  readonly version?: string;
  readonly dependencies?: Readonly<Record<string, string>>;
  readonly devDependencies?: Readonly<Record<string, string>>;
  readonly peerDependencies?: Readonly<Record<string, string>>;
}

/**
 * One manifest, read for those fields alone.
 *
 * SAFETY: the path always names a `package.json` this repository just
 * resolved on disk, and every field below is optional — a manifest missing
 * one yields `undefined` and the assertion that wanted it fails by name
 * rather than throwing here.
 */
const manifestAt = (path: string): Manifest => JSON.parse(readFileSync(path, "utf8")) as Manifest;

describe("the source tree", () => {
  it.effect("spells control characters rather than embedding them", () =>
    Effect.sync(() => {
      // A NUL byte in a string literal — the separator this codebase uses to key
      // memos on several values at once — is invisible in an editor and changes
      // what the file *is*: grep, ripgrep and `git grep` all classify a file
      // holding one as binary and skip it, so the file silently drops out of
      // every search anybody makes across the codebase, including the searches a
      // reviewer makes to find the very code that put it there. The escape reads
      // the same to the compiler and leaves the file text.
      const listed = execFileSync("git", [
        "ls-files",
        "-z",
        "*.ts",
        "*.tsx",
        "*.js",
        "*.json",
        "*.md",
      ]).toString();
      const files = listed.split("\u0000").filter((path) => path.length > 0 && existsSync(path));

      const offending = files.filter((path) => readFileSync(path).includes(0));
      assert.deepEqual(offending, [], "these files hold a raw control byte and are unsearchable");
    }),
  );

  it.effect("resolves exactly one Vite, and one Foldkit peers against it", () =>
    Effect.sync(() => {
      // Foldkit's Vite plugin declares Vite as a peer, and Vite+ reaches its
      // own through `@voidzero-dev/vite-plus-core` rather than declaring it.
      // A second copy would give the plugin a different module registry from
      // the one running the build: the dev server would hold two HMR graphs
      // and the production build would silently drop the plugin's transform.
      // Cheaper to assert than to diagnose, so it is asserted.
      const copies = execFileSync("find", [
        "node_modules",
        "-maxdepth",
        "4",
        "-path",
        "*/vite/package.json",
      ])
        .toString()
        .split("\n")
        .filter((path) => path.length > 0);
      assert.deepEqual(
        copies,
        ["node_modules/vite/package.json"],
        "more than one Vite is installed",
      );

      assert.match(
        manifestAt("node_modules/vite/package.json").version ?? "",
        /^[78]\./,
        "Vite is outside @foldkit/vite-plugin's peer range",
      );

      // The pair is pinned, not ranged: Foldkit peers on one exact Effect, so
      // an Effect bump is a Foldkit bump and has to be made deliberately.
      const own = manifestAt("package.json");
      const declared = { ...own.dependencies, ...own.devDependencies };
      for (const name of ["foldkit", "@foldkit/vite-plugin", "@foldkit/devtools", "effect"]) {
        const pin = declared[name];
        assert.notEqual(pin, undefined, `${name} is not installed`);
        assert.match(pin ?? "", /^\d/, `${name} is ranged rather than pinned: ${pin ?? ""}`);
      }

      assert.equal(
        manifestAt("node_modules/foldkit/package.json").peerDependencies?.["effect"],
        declared["effect"],
        "Foldkit peers on an Effect this repository does not install",
      );
    }),
  );
});
