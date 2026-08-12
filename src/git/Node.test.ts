/**
 * The filesystem backend against the same contract as the in-memory one.
 *
 * Two backends, one suite: that is the check on whether `ObjectStore` and
 * `RefStore` are real ports or just an interface the in-memory version happens
 * to satisfy.
 */
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";

import { stores } from "./Node.ts";
import { storeContract } from "./Store.contract.ts";

let directory = "";

storeContract("Node", {
  make: async () => {
    directory = await fs.mkdtemp(path.join(os.tmpdir(), "git-store-"));
    return stores(directory);
  },
  cleanup: async () => {
    if (directory !== "") await fs.rm(directory, { force: true, recursive: true });
    directory = "";
  },
});
