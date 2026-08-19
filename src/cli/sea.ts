/**
 * Single-executable entry.
 *
 * The SEA bundle compiles `import.meta.main` to `false` — inside one bundled
 * module the guards in `main.ts` and `host/Node.ts` would otherwise all see
 * themselves as "main" and fire at once — so this entry exists to call the
 * CLI unconditionally instead. See `sea.build.ts`.
 */
import { run } from "./main.ts";

run();
