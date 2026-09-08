import * as path from "node:path";
import { build } from "vite-plus";

/** Bundle a browser regression against the actual modules without temporary source files. */
export const browserBundle = async (source: string, name: string): Promise<string> => {
  const root = path.resolve(import.meta.dirname, "../..");
  const entry = path.join(root, `${name}.virtual.ts`);
  const built = await build({
    root,
    configFile: path.join(root, "vite.config.ts"),
    logLevel: "error",
    plugins: [
      {
        name: "browser-regression",
        resolveId: (id) => (id === entry ? entry : undefined),
        load: (id) => (id === entry ? source : undefined),
      },
    ],
    build: {
      lib: { entry, name, formats: ["iife"] },
      target: "es2022",
      write: false,
    },
  });
  if ("on" in built) throw new Error("unexpected browser build watcher");
  const chunk = (Array.isArray(built) ? built : [built])
    .flatMap((output) => output.output)
    .find((output) => output.type === "chunk");
  if (chunk === undefined || !("code" in chunk)) throw new Error("no browser scenario emitted");
  return chunk.code;
};
