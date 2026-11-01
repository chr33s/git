/** Capacity of the Node provider's namespace-wide administrative JSON files. */
import * as fs from "node:fs/promises";
import { check, type Limits } from "./MetadataCapacity.ts";
export {
  check,
  configuration,
  report,
  type Limits,
  type RegistryKind,
} from "./MetadataCapacity.ts";

/** Run under the file's writer lock, before parsing for a growth operation. */
export const inspect = async (file: string, limits: Limits): Promise<void> => {
  let size: number;
  try {
    size = (await fs.stat(file)).size;
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT") return;
    throw error;
  }
  check(limits, 0, size);
};
