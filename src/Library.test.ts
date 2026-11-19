/**
 * The published package is the protocol, and only the protocol.
 *
 * `src/index.ts` is what npm ships (compiled by `tsconfig.lib.json`); the app
 * — server, CLI, UI, hub — stays in this repository. Nothing stops an import
 * from quietly pulling `Repository` or `hub/` into the library's graph except
 * a check, so the graph is asserted file by file: growing it is a decision
 * somebody makes here, not a side effect of a refactor.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

interface Manifest {
  readonly dependencies?: Readonly<Record<string, string>>;
  readonly devDependencies?: Readonly<Record<string, string>>;
  readonly peerDependencies?: Readonly<Record<string, string>>;
  readonly files?: ReadonlyArray<string>;
  /** Every entry but `./package.json`, which is a bare path and is never read. */
  readonly exports?: Readonly<Record<string, { readonly default: string }>>;
}

// SAFETY: this repository's own manifest; every field read is optional, so a
// missing one fails the assertion that wanted it rather than throwing here.
const manifest = JSON.parse(readFileSync("package.json", "utf8")) as Manifest;

const specifiers = (file: string): ReadonlyArray<string> =>
  [
    ...readFileSync(file, "utf8").matchAll(
      /^\s*(?:import|export)\s[^;]*?from\s+"([^"]+)"|^\s*import\s+"([^"]+)"/gm,
    ),
  ].flatMap((match) => match[1] ?? match[2] ?? []);

/** Every source file reachable from `entry`, and every package it names. */
const closure = (entry: string) => {
  const files = new Set<string>();
  const packages = new Set<string>();
  const pending = [entry];
  for (let file = pending.pop(); file !== undefined; file = pending.pop()) {
    if (files.has(file)) continue;
    files.add(file);
    for (const specifier of specifiers(file)) {
      if (specifier.startsWith(".")) pending.push(path.join(path.dirname(file), specifier));
      else packages.add(specifier.split("/")[0] ?? specifier);
    }
  }
  return { files: [...files].sort(), packages: [...packages].sort() };
};

describe("the published library", () => {
  it.effect("reaches only protocol modules, and only effect", () =>
    Effect.sync(() => {
      const { files, packages } = closure("src/index.ts");
      assert.deepEqual(packages, ["effect"]);
      assert.deepEqual(files, [
        "src/client/Authorize.ts",
        "src/client/Transport.ts",
        "src/client/Upload.ts",
        "src/client/Wire.ts",
        "src/git/Error.ts",
        "src/git/Format.ts",
        "src/git/Inflate.ts",
        "src/git/Oid.ts",
        "src/git/Pack.ts",
        "src/git/PackFile.ts",
        "src/git/PackIndex.ts",
        "src/git/Packed.ts",
        "src/git/Pkt.ts",
        "src/git/Refspec.ts",
        "src/git/Sha1.ts",
        "src/git/Store.ts",
        "src/index.ts",
      ]);
    }),
  );

  it.effect("exports each namespace of the barrel as its own entry point", () =>
    Effect.sync(() => {
      const barrel = [
        ...readFileSync("src/index.ts", "utf8").matchAll(/export \* as (\w+) from "\.\/(.+)\.ts"/g),
      ].map(([, name, file]) => [`./${name}`, `./dist/lib/${file}.js`]);
      const entries = Object.entries(manifest.exports ?? {}).flatMap(([key, target]) =>
        key === "." || key === "./package.json" ? [] : [[key, target.default]],
      );
      const byKey = (left: Array<string>, right: Array<string>) =>
        (left[0] ?? "").localeCompare(right[0] ?? "");
      assert.deepEqual(entries.sort(byKey), barrel.sort(byKey));
      assert.deepEqual(manifest.exports?.["."], {
        types: "./dist/lib/index.d.ts",
        default: "./dist/lib/index.js",
      });
    }),
  );

  it.effect("peers on the effect this repository pins, and ships nothing else", () =>
    Effect.sync(() => {
      // Consumers and this repository must be able to share one Effect: a
      // second copy splits every `Context` tag and `Schema` brand in two.
      assert.equal(
        manifest.peerDependencies?.["effect"],
        `^${manifest.devDependencies?.["effect"]}`,
      );
      assert.equal(manifest.dependencies, undefined);
      assert.deepEqual(manifest.files, ["dist/lib", "!dist/lib/**/*.tsbuildinfo"]);
    }),
  );
});
