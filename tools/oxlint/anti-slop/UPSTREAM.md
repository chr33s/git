# Vendored anti-slop

Source: [dmmulroy/anti-slop](https://github.com/dmmulroy/anti-slop) `src/`, commit `13909c9e07590f5a85d93c62712259fe14a883f7` (2026-10-29), tests omitted, formatted with `vp fmt`.

Previous base: `9b80d9a` (2026-08-12). The pre-update tree was byte-identical to that commit after `oxfmt`, so there were no local deviations to carry forward; the update replaced the tree with formatted upstream.

## Local policy

- Registration and severities live in `vite.config.ts` (`antiSlopRules`). Rules that existed at the previous base stay `error`.
- Not enabled: `no-array-filter-map`, `no-reduce-accumulator-copy`, `require-readable-spacing` (generic) and the whole opt-in `effect/` plugin (`anti-slop-effect`, not registered in `jsPlugins`). At `13909c9` they report 22, 0, 12224 (autofixable) and 204 findings in this repository.
- `tools/oxlint/anti-slop/**` is exempt from anti-slop rules: the plugin parses untyped rule options and AST input with `typeof` and open dictionaries.

## Updating

Follow upstream `skills/install-anti-slop/references/update.md`. Use the commit above as the base, and compare after formatting incoming source with `vp fmt`.
