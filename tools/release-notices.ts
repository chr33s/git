/** Collect shipped dependency notices from build maps, static fonts and Node. */
import { createHash } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import assert from "node:assert/strict";

/**
 * `bundle` is the directory whose top-level source maps are inventoried and
 * where the notices land. `runtime` is false for a bundle the user's own node
 * runs (`dist/bin`), which embeds no Node to attribute.
 */
export const writeReleaseNotices = (
  nodeLicense = path.resolve(path.dirname(fs.realpathSync(process.execPath)), "..", "LICENSE"),
  bundle = "dist/sea",
  runtime = true,
) => {
  const directories = new Set<string>();
  const maps = [
    ...fs
      .readdirSync(bundle)
      .filter((file) => file.endsWith(".map"))
      .map((file) => path.join(bundle, file)),
    ...fs
      .readdirSync("dist/ui", { recursive: true })
      .map(String)
      .filter((file) => file.endsWith(".map"))
      .map((file) => path.join("dist/ui", file)),
  ];
  for (const map of maps) {
    for (const source of JSON.parse(fs.readFileSync(map, "utf8")).sources) {
      if (!source.includes("node_modules/")) continue;
      let directory = path.dirname(path.resolve(path.dirname(map), source));
      while (directory.includes(`${path.sep}node_modules${path.sep}`)) {
        const manifest = path.join(directory, "package.json");
        if (fs.existsSync(manifest)) {
          const metadata = JSON.parse(fs.readFileSync(manifest, "utf8"));
          if (metadata.name && metadata.version) {
            directories.add(directory);
            break;
          }
        }
        directory = path.dirname(directory);
      }
      if (!directory.includes(`${path.sep}node_modules${path.sep}`))
        throw new Error(`Cannot identify mapped package: ${source}`);
    }
  }
  // CSS/font assets are not necessarily represented in JavaScript source maps.
  for (const name of [
    "@fontsource-variable/instrument-sans",
    "@fontsource-variable/jetbrains-mono",
    "@phosphor-icons/core",
  ]) {
    directories.add(path.resolve("node_modules", name));
  }
  const notices: string[] = [];
  const inventory = [...directories].sort().map((directory) => {
    const manifest = JSON.parse(fs.readFileSync(path.join(directory, "package.json"), "utf8"));
    const files = fs
      .readdirSync(directory, { withFileTypes: true })
      .filter(
        (entry) =>
          entry.isFile() && /^(licen[cs]e|notice|copying|copyright)([.-]|$)/i.test(entry.name),
      )
      .map((entry) => entry.name)
      .sort();
    // This published package carries its complete MIT notice in the README.
    if (files.length === 0 && manifest.name === "lru_map" && manifest.version === "0.4.1") {
      const readme = fs.readFileSync(path.join(directory, "README.md"), "utf8");
      if (
        !readme.includes("# MIT license\n") ||
        !readme.includes("Copyright (c) 2010-2016 Rasmus Andersson") ||
        !readme.includes("THE SOFTWARE IS PROVIDED")
      ) {
        throw new Error("lru_map README no longer contains the reviewed license text");
      }
      files.push("README.md");
    }
    if (files.length === 0)
      throw new Error(`Missing license text for ${manifest.name}@${manifest.version}`);
    const texts = files.map((file) => {
      const text = fs.readFileSync(path.join(directory, file), "utf8");
      if (text.trim() === "") throw new Error(`Empty notice for ${manifest.name}: ${file}`);
      notices.push(`===== ${manifest.name}@${manifest.version} — ${file} =====\n\n${text}\n`);
      return { file, sha256: createHash("sha256").update(text).digest("hex") };
    });
    const supplemental = [];
    if (manifest.name === "@shikijs/langs" || manifest.name === "@shikijs/themes") {
      const entries = JSON.parse(
        fs.readFileSync("tools/licenses/shiki-4.6.0/provenance.json", "utf8"),
      );
      const entry = entries.find((item: { package: string }) => item.package === manifest.name);
      assert.ok(entry, `Missing supplemental notices for ${manifest.name}`);
      assert.equal(
        manifest.version,
        entry.version,
        `Review supplemental notices for ${manifest.name} upgrade`,
      );
      for (const notice of entry.files) {
        const text = fs.readFileSync(notice.file, "utf8");
        const sha256 = createHash("sha256").update(text).digest("hex");
        assert.ok(text.trim(), `Empty supplemental notice: ${notice.file}`);
        assert.equal(
          sha256,
          notice.sha256,
          `Supplemental notice checksum mismatch: ${notice.file}`,
        );
        notices.push(
          `===== ${manifest.name}@${manifest.version} — ${entry.sourcePackage}@${entry.sourceVersion} ${path.basename(notice.file)} =====\n\n${text}\n`,
        );
      }
      supplemental.push(entry);
    }
    return {
      name: manifest.name,
      version: manifest.version,
      license: manifest.license,
      path: path.relative(process.cwd(), directory),
      notices: texts,
      supplemental,
    };
  });
  // Official Node binary distributions include the runtime and bundled-library
  // notices here. Fail rather than silently shipping only Node's MIT heading.
  const runtimeNotice = runtime ? fs.readFileSync(nodeLicense, "utf8") : undefined;
  if (runtimeNotice !== undefined) {
    if (!runtimeNotice.includes("Node.js")) throw new Error("Unexpected Node runtime license file");
    notices.push(`===== Node.js ${process.versions.node} — LICENSE =====\n\n${runtimeNotice}\n`);
  }
  const patches = fs
    .readdirSync("patches")
    .filter((file) => file.endsWith(".patch"))
    .sort()
    .map((file) => ({
      file,
      sha256: createHash("sha256")
        .update(fs.readFileSync(path.join("patches", file)))
        .digest("hex"),
    }));
  const metadata = {
    schemaVersion: 1,
    scope:
      "Source-map package inventory plus static fonts, raw SVG icons and embedded Node; includes mapped code even when partially eliminated. Review supplemental attribution requirements before publication.",
    packages: inventory,
    node:
      runtimeNotice === undefined
        ? null
        : {
            version: process.versions.node,
            licenseSha256: createHash("sha256").update(runtimeNotice).digest("hex"),
          },
    patches,
  };
  fs.writeFileSync(path.join(bundle, "THIRD_PARTY_NOTICES.txt"), notices.join("\n"));
  fs.copyFileSync("LICENSE", path.join(bundle, "LICENSE"));
  fs.writeFileSync(
    path.join(bundle, "dependency-inventory.json"),
    `${JSON.stringify(metadata, null, 2)}\n`,
  );
  console.info(
    `Collected notices for ${inventory.length} packages${runtimeNotice === undefined ? "" : ` and Node ${process.versions.node}`}.`,
  );
};

if (import.meta.main) writeReleaseNotices();
