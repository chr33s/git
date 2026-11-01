/** Offline local-emulator recovery rehearsal. Not a Cloudflare export/import tool. */
import assert from "node:assert/strict";
import { createReadStream } from "node:fs";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { createHash } from "node:crypto";

const inventory = async (root: string) => {
  const entries: Array<{ name: string; digest: string }> = [];
  const walk = async (relative: string): Promise<void> => {
    for (const entry of await fs.readdir(path.join(root, relative), { withFileTypes: true })) {
      const name = path.join(relative, entry.name);
      if (entry.isDirectory()) await walk(name);
      else {
        assert.ok(entry.isFile(), "backup fixture must not depend on external symlinks");
        const hash = createHash("sha256");
        for await (const bytes of createReadStream(path.join(root, name))) hash.update(bytes);
        entries.push({ name, digest: hash.digest("hex") });
      }
    }
  };
  await walk("");
  return entries.sort((a, b) => a.name.localeCompare(b.name));
};

/** Caller owns both paths and must stop every Worker/proxy before capture. */
export const restoreLocalState = async (alchemy: string, backup: string) => {
  assert.equal(path.basename(alchemy), ".alchemy");
  await fs.mkdir(backup, { mode: 0o700 });
  const names = ["state", "local"];
  for (const name of names) {
    const source = path.join(alchemy, name);
    assert.ok((await fs.lstat(source)).isDirectory());
    await fs.cp(source, path.join(backup, name), {
      recursive: true,
      errorOnExist: true,
      force: false,
      preserveTimestamps: true,
    });
    assert.deepEqual(await inventory(path.join(backup, name)), await inventory(source));
  }
  // Remove the actual source state, rather than reopening it under another name.
  // All bytes after this point must come from the independently verified copy.
  for (const name of names) await fs.rm(path.join(alchemy, name), { recursive: true });
  for (const name of names) {
    const destination = path.join(alchemy, name);
    await fs.cp(path.join(backup, name), destination, {
      recursive: true,
      errorOnExist: true,
      force: false,
      preserveTimestamps: true,
    });
    assert.deepEqual(await inventory(destination), await inventory(path.join(backup, name)));
  }
};
