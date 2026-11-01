/** Match the Worker's public URL layout on disk for deploy and local workerd. */
import * as fs from "node:fs/promises";
import * as path from "node:path";

export const stageWorkerAssets = async (source = "dist/ui", target = "dist/worker-assets") => {
  // Refuse an incomplete UI before touching the previous staged build.
  for (const file of ["index.html", "bootstrap.js", "main.js", "main.css", "_headers"])
    await fs.access(path.join(source, file));
  await fs.rm(target, { recursive: true, force: true });
  await fs.mkdir(target, { recursive: true });
  await fs.cp(source, path.join(target, "hub"), { recursive: true });
  // Cloudflare reads routing/header configuration at the asset root; the rules
  // already name /hub/* and must not be moved or rewritten under that prefix.
  for (const file of ["_headers", "_redirects"]) {
    const from = path.join(target, "hub", file);
    try {
      await fs.rename(from, path.join(target, file));
    } catch (error) {
      if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
    }
  }
};

if (import.meta.main) await stageWorkerAssets();
