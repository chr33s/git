/** Durable metadata files. Callers own mutation reservations and directory creation. */
import * as fs from "node:fs/promises";
import * as path from "node:path";

const syncDirectories = async (directory: string): Promise<void> => {
  let current = await fs.realpath(directory);
  for (;;) {
    const handle = await fs.open(current, "r");
    try {
      await handle.sync();
    } finally {
      await handle.close();
    }
    const parent = path.dirname(current);
    if (parent === current) return;
    current = parent;
  }
};

export const sync = async (target: string): Promise<void> => {
  let file;
  try {
    file = await fs.open(target, "r");
  } catch (cause) {
    // A missing optional registry represents an empty document. Its parent
    // still needs syncing when an edit created the directory before a no-op.
    if (!(cause instanceof Error && "code" in cause && cause.code === "ENOENT")) throw cause;
  }
  if (file !== undefined) {
    try {
      await file.sync();
    } finally {
      await file.close();
    }
  }
  // Existence does not prove a previous creation completed its sync. Registry
  // mutations repeat the physical chain; read-only operations do not call here.
  await syncDirectories(path.dirname(target));
};

/** Never recreate a missing parent: that could resurrect a deleted repository. */
export const replace = async (target: string, contents: string): Promise<void> => {
  const temporary = `${target}.${crypto.randomUUID()}.tmp`;
  try {
    const file = await fs.open(temporary, "wx", 0o600);
    try {
      await file.writeFile(contents);
      await file.sync();
    } finally {
      await file.close();
    }
    await fs.rename(temporary, target);
    await syncDirectories(path.dirname(target));
  } finally {
    await fs.rm(temporary, { force: true });
  }
};

export const append = async (target: string, contents: string): Promise<void> => {
  const file = await fs.open(target, "a", 0o600);
  try {
    await file.writeFile(contents);
    await file.sync();
  } finally {
    await file.close();
  }
  await syncDirectories(path.dirname(target));
};

export const remove = async (target: string): Promise<void> => {
  await fs.rm(target, { force: true });
  // A retry can follow removal of the containing repository. Sync the nearest
  // surviving ancestor without recreating any of the removed directories.
  let directory = path.resolve(path.dirname(target));
  for (;;) {
    try {
      await fs.realpath(directory);
      break;
    } catch (cause) {
      if (!(cause instanceof Error && "code" in cause && cause.code === "ENOENT")) throw cause;
      const parent = path.dirname(directory);
      if (parent === directory) throw cause;
      directory = parent;
    }
  }
  await syncDirectories(directory);
};
