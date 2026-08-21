/**
 * Ambient declarations for the two things the bundler understands and the type
 * checker does not.
 */

/**
 * Stylesheets imported for their side effect.
 *
 * `main.ts` imports the tokens and ownership-based files in `styles/` so that
 * esbuild emits them as one `main.css` beside the bundle. There is no value to
 * import — the modules exist only so the CSS joins the build graph.
 */
declare module "*.css";

/**
 * SVG source imported as text.
 *
 * esbuild's `loader: { ".svg": "text" }` turns Phosphor assets into strings
 * that `icons.ts` inlines. There is no React/URL default here.
 */
declare module "*.svg" {
  const content: string;
  export default content;
}

/**
 * The module that registers `<diffs-container>`.
 *
 * `@pierre/diffs@1.3.5` names `dist/components/web-components.js` in its
 * `sideEffects` but reaches it from no subpath in its `exports` map, so
 * TypeScript cannot resolve the specifier that `build.ts` aliases to the file.
 * Declared here as the side-effect-only module it is. Remove this once the
 * package exports it.
 */
declare module "@pierre/diffs/components/web-components";
