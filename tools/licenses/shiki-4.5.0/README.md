# Shiki 4.5.0 supplemental notices

The installed `@shikijs/langs` and `@shikijs/themes` packages contain Shiki's
root license but omit the upstream grammar/theme catalog notices.
[Shiki languages](https://shiki.style/languages) and
[Shiki themes](https://shiki.style/themes) identify those catalogs and their
separate attribution notices.

The `v4.5.0` annotated tag resolves to commit
`f7d0167873fd676fe4e190bc9b53832fba9ee01d`. Its
[pnpm lockfile](https://github.com/shikijs/shiki/blob/f7d0167873fd676fe4e190bc9b53832fba9ee01d/pnpm-lock.yaml)
resolves `tm-grammars` to `1.32.22` and `tm-themes` to `1.12.12`.
The LICENSE and NOTICE files here are unmodified bytes extracted from those
published npm tarballs. `provenance.json` records the tarball SHA-512 integrity,
file SHA-256 values, dependency versions, and upstream lockfile source. Tarball
integrity was checked before extraction. This preserves the entire pinned
catalog's notices, including entries not present in a particular final bundle.

The release collector checks package versions and notice hashes and includes
these files and provenance in its outputs. To upgrade, inspect the new Shiki
tag and exact lockfile resolutions, obtain the matching catalog packages with
scripts disabled, verify archive integrity, and replace the notice files and
provenance together. Never substitute a moving `main` notice for a pinned
release. Run the notice tests, rebuild the SEA, and verify embedded notices.
This records the upstream notices; it does not certify the complete artifact's
license compliance.
