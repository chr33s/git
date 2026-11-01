import * as fs from "node:fs/promises";
import * as path from "node:path";
import { check, inspect, type Limits } from "./MetadataCapacity.node.ts";

/** Under the namespace fork lock, before changing alternates or borrower records. */
export const prepare = async (
  root: string,
  links: ReadonlyMap<string, string>,
  child: string,
  parent: string,
  limits: Limits,
): Promise<string> => {
  check(limits, links.size, Buffer.byteLength(JSON.stringify(Object.fromEntries(links), null, 1)));
  const next = new Map(links).set(child, parent);
  check(limits, next.size, Buffer.byteLength(JSON.stringify(Object.fromEntries(next), null, 1)));

  const file = path.join(root, parent, "objects", "info", "borrowers");
  await inspect(file, limits);
  let written: string;
  try {
    written = await fs.readFile(file, "utf8");
  } catch (error) {
    if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
    written = "";
  }
  const existing = written
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  check(limits, existing.length, Buffer.byteLength(written));
  const borrowers = [...new Set([...existing, child])];
  const contents = `${borrowers.join("\n")}\n`;
  check(limits, borrowers.length, Buffer.byteLength(contents));
  check(limits, 1, Buffer.byteLength(`${path.resolve(root, parent, "objects")}\n`));
  return contents;
};
