# Operations

## Standalone CLI installation

`npm run build:sea` builds the executable for the builder's operating system and
architecture using Node 26. Its two Unix entry points are `dist/sea/git+` and
the relative symlink `dist/sea/git-remote-git+id`. Keep both names: stock Git uses
the second for `git+id://` remotes. The installed executable contains the UI and
does not need Node, npm, or a source checkout at runtime.

The build also emits `dist/sea/build-info.json` and `dist/sea/SHA256SUMS`.
`git+ --build-info` reads the metadata embedded in the executable, so an installed
copy identifies its package version, Git revision, dirty-tree flag, build Node
version, operating system, architecture, and package-lock SHA-256 without a
checkout or Node installation. A dirty build is development evidence only: its
revision names the base commit, not all compiled changes. Release candidates
must be built from a clean, version-tagged checkout and tested as exact artifacts.

From the artifact directory, verify `sha256sum -c SHA256SUMS` on Linux or
`shasum -a 256 -c SHA256SUMS` on macOS. The checksum is generated after the local
build signature is applied. Any later distribution signing or notarization that
changes executable bytes requires regenerating the checksum and retesting the
final artifact. Checksums detect changed bytes; they do not authenticate a
publisher. Signed provenance and platform distribution signing remain release
gates; the local macOS ad-hoc signature does not establish publisher identity.

The standalone build includes `THIRD_PARTY_NOTICES.txt` and
`dependency-inventory.json` both beside and inside the executable. Installed
copies print them with `git+ --licenses` and `git+ --dependency-inventory`.
The collector inventories CLI/UI source-map packages, explicitly includes static
fonts and raw SVG icons, copies their available root license/notice texts, and
includes the full embedded Node distribution's `LICENSE`. It records package
versions, notice hashes and applied-patch file hashes without absolute build
paths. It fails on missing or empty package notices. The build needs an official
Node distribution with `LICENSE` beside its `bin` directory; it does not fetch
or substitute a runtime license from another version. Source maps remain build
inputs to this audit and are not embedded in the executable.

For Shiki 4.6.0, the collector additionally embeds the complete LICENSE/NOTICE
files from its locked `tm-grammars@1.32.22` and `tm-themes@1.12.12` catalogs.
Their pinned provenance and SHA-256 checks live under `tools/licenses/shiki-4.6.0`;
missing/altered files or a Shiki version change fail the build until reviewed.
The inventory includes those supplemental sources and hashes. These catalogs
may include notices for languages/themes eliminated from a particular bundle.

This is a conservative mapped-package inventory, not a complete compliance
determination. Before distribution, review supplemental attribution in vendored
grammars/themes and other package assets, verify the patch inventory against the
shipped code. The root `LICENSE` implements the package manifest's MIT license;
it is copied into the artifact directory and printed by `git+ --license`.
Keep the project license, notice and inventory files with every distributed archive, and verify
their contents against the installed executable's output.

After building and testing a release candidate, run
`node tools/release-preflight.ts release` with the pinned build Node version on
the target platform. Tag CI runs this gate after interoperability tests. It
requires a clean checkout, `v<package-version>` pointing at HEAD, the adjacent
helper symlink, matching executable checksum and embedded metadata, and matching
source revision, package version, dependency lock, runtime, OS and architecture.
It then checks the standalone prerequisites. Development builds can still use
the `standalone` preflight mode; that mode does not certify a release candidate.
These are local consistency checks, not a signature or proof of build provenance.
Publication must additionally require the full CI matrix and the remaining
license, signing, provenance and platform rehearsal gates in `todo.md`.

CI attaches `tools/release-test-reporter.ts` to unit, workerd and interoperability
runs. It fails the process for unexpected skipped, TODO, pending or failed tests,
a failed/interrupted run, any unhandled runtime error, and a run with no passing
tests. A passing test count cannot override a failed teardown or an unhandled
rejection, even if Vitest's `dangerouslyIgnoreUnhandledErrors` is enabled. The
reporter emits its success verdict only after those checks. On macOS only, it permits the 11 explicitly
named `src/git/Node.test.ts` descriptor checks that inspect Linux `/proc/self/fd`.
Linux has no skip exemptions. Missing Git, SSH, Chromium or SEA prerequisites,
and running permission tests as root, are not exemptions. For the same coverage
gate locally, run:

```sh
npm test -- --reporter=default --reporter=./tools/release-test-reporter.ts
```

The reporter checks execution of collected tests. It does not prove that an
omitted suite, unsupported platform, or untested artifact was covered; retain the
explicit CI project selections, preflight checks, and platform release gates.

The browser CI job has six legs: Chromium, Firefox and WebKit on Node 24.12 and 26. Each installs its selected Playwright engine, builds the production UI,
runs the full UI/local Git verifier, then runs the identity contract with the
strict release reporter. Explicit engine selection prevents missing browsers
from becoming skips. Every leg retains runtime/browser revisions and verifier
logs, including failures. This is regression coverage on the existing Ubuntu
runner; it does not independently approve a production browser or OS support
matrix. Require passing remote results before treating that matrix as release
evidence. Chromium remains installed for the separate Worker and standalone
compatibility jobs.

Package a verified clean, tagged build with `node tools/package-standalone.ts`.
For local development only, `node tools/package-standalone.ts --rehearsal`
permits a dirty build and marks both the archive filename and its `RELEASE.json`
as a rehearsal. Neither command publishes anything. Output goes to `dist/releases`;
existing archive files are never overwritten. Retain or deliberately remove an
older rehearsal before rerunning with the same version/platform.

The `.tar.gz` contains the executable, its relative helper symlink, project and
third-party licenses, build/dependency metadata, operating instructions and
checksums. Bundles, source maps and other build intermediates are excluded.
Packaging extracts the archive into a fresh directory, compares every regular
file's hash and the helper target, and executes the installed metadata/license
commands with no Node on `PATH`. A changed external notice fails packaging even
if the executable checksum is unchanged. `CONTENTS.sha256` checks the regular
payload files; inspect the helper link separately to confirm it targets `git+`.
The adjacent archive `.sha256` checks the compressed bytes. Verify it before
extracting, then verify `CONTENTS.sha256` inside the extracted directory before
installation. These checks provide integrity, not publisher authentication.

CI runs on every push, version tags included; it tests the tagged source and
the rehearsal archive but no longer packages release candidates. The release
workflow's draft and npm publish wait on a green CI run for the same tag.

`.github/workflows/release.yml` builds, packages (`tools/package-standalone.ts`)
and smoke-tests the executable for linux-x64, linux-arm64, darwin-arm64 and
darwin-x64 on the same tag push (or a manual run naming an existing tag). Each
Linux archive is also installed into a clean Debian container with no Node or
source and exercised end to end; the installation report is retained as
`installation-<target>` for 30 days. The workflow then attests the archives,
verifies the attestation against this repository, `release.yml`, the run's ref
and commit and GitHub-hosted runners, and attaches them to a **draft** GitHub
Release. Publish the draft only after CI on that tag is green; the workflow
refuses to change assets on a release that is already published. Each release
carries:

| Asset                                         | What it is                                                  |
| --------------------------------------------- | ----------------------------------------------------------- |
| `git-plus-VERSION-OS-ARCH.tar.gz` (`.sha256`) | the executable with its helper link, licenses and notices   |
| `git+-OS-ARCH` (`.sha`)                       | the same executable, bare; `chmod +x` it after download     |
| `chr33s-git-VERSION.tgz` (`.sha`)             | the npm package, byte for byte the tarball published to npm |
| `SHA256SUMS`                                  | every checksum above                                        |

Every archive, executable and npm tarball is attested.

The macOS executables are signed with a Developer ID (hardened runtime, secure
timestamp, the V8 JIT entitlements in `tools/macos/entitlements.plist`) and
notarized, so Gatekeeper accepts a downloaded copy; a bare executable cannot
staple its ticket, so the first run checks it online. `npm run build:sea` signs
with the identity named by `MACOS_SIGNING_IDENTITY`, or ad hoc when it is unset.
The workflow fails without these repository secrets: `MACOS_CERTIFICATE`
(base64 Developer ID Application `.p12`), `MACOS_CERTIFICATE_PASSWORD`,
`MACOS_SIGNING_IDENTITY` (e.g. `Developer ID Application: Name (TEAMID)`),
and an App Store Connect API key for `notarytool`: `APPLE_API_KEY` (base64
`.p8`), `APPLE_API_KEY_ID`, `APPLE_API_ISSUER`. Linux executables are not signed.

The same workflow then publishes `@chr33s/git` to npm with
[trusted publishing](https://docs.npmjs.com/trusted-publishers): no npm token is
stored, and npm records provenance. It publishes the tarball attached to the
release rather than packing again. A prerelease publishes under its identifier
as the dist-tag (`2.0.0-alpha.1` → `alpha`), anything else under `latest`; a
version already on npm is skipped. Configure the package's trusted publisher on
npmjs.com as repository `chr33s/git`, workflow `release.yml`, environment `npm`,
and give the `npm` environment required reviewers so the publish waits for CI.

Before extracting a downloaded release archive, select the approved tag and full
commit from the release record, not from the downloaded archive. Use a GitHub
CLI with these [verification options](https://cli.github.com/manual/gh_attestation_verify):

```sh
gh attestation verify git-plus-VERSION-OS-ARCH.tar.gz \
  --repo chr33s/git \
  --signer-workflow chr33s/git/.github/workflows/release.yml \
  --source-digest APPROVED_FULL_COMMIT \
  --signer-digest APPROVED_FULL_COMMIT \
  --source-ref refs/tags/vVERSION \
  --deny-self-hosted-runners
```

This fetches the attestation from GitHub. Adding `--bundle attestation.json`
checks a retained bundle instead; then repository identity reflects certificate
issuance and cannot detect a subsequent repository rename/replacement. The saved
`verification.json` is a record, not a substitute for verifying the archive
bytes yourself. Missing attestations, identity/digest mismatches or verification
errors stop installation. Check the adjacent checksum and installed contents as
well. Remote workflow results and approval of the supported platform/channel
matrix remain release gates.

Install stock Git `2.56.0` or a compatible version on `PATH`. The Git executable
must retain its normal HTTP transport helpers. SSH remotes additionally need
OpenSSH and their usual host-key configuration. Building from source requires
the development prerequisites in the readme. Windows is outside the Unix CLI
release contract; a successful local build on another platform does not certify
that platform for distribution.

The Linux arm64 rehearsal ran on Debian 12 with glibc 2.36. Its Node-based
executable dynamically loads glibc, libstdc++, libgcc, libatomic, and the platform
loader; these are operating-system prerequisites, not files in the archive.
Check `ldd ./git+` on a trusted, verified artifact before installation and resolve
any missing libraries through the distribution's package manager. This rehearsal
does not establish a minimum glibc version or compatibility with musl-based
distributions; those require separate artifact verification.

For the Debian 12 arm64 rehearsal, install the runtime prerequisites with
`apt-get install --no-install-recommends git openssh-client ca-certificates libc6 libstdc++6 libgcc-s1 libatomic1`.
In particular, a minimal Debian image with Git and SSH can still lack
`libatomic.so.1`; resolve it with `libatomic1` before invoking the binary. Python
is needed only for the installation verifier below, not for the installed CLI.

After building and packaging a Linux artifact, run the clean-runtime rehearsal
from the source checkout on a Docker host of the matching architecture:

```sh
node tools/package-standalone.ts --rehearsal
bash tools/verify-standalone-container.sh \
  dist/releases/git-plus-0.1.1-linux-arm64-rehearsal.tar.gz \
  /tmp/new-standalone-installation.json
```

Use the actual artifact name/architecture and a new report destination. The
script creates a fresh Debian runtime container, installs the listed OS tools
and Python, and copies only the archive, verifier and disposable private Git
fixture. It does not mount the source checkout or install Node/npm. It checks
archive contents against their checksums, embedded UI responses, anonymous-read
denial, authenticated `git+id://` clone/push/fetch, repository identity pinning,
and native Git integrity. Test credentials expire after five minutes and are
created only after package installation; container and host scratch data are
removed on exit. The runtime listens only inside its container, with no
published port. Docker image/package downloads require network access.

The [Linux arm64 clean-runtime report](measurements/standalone-clean-2026-10-30-linux-arm64.json)
records the exact executable digest and Debian image digest. It passed with
Node 26.13 embedded in the binary, no separately installed Node, Debian Git
2.39.5 and glibc 2.36. This dirty rehearsal verifies only that environment; it
does not approve a platform support matrix, signing, publication or browser
rendering. Compatibility CI runs the same check and retains its report. Its
SEA build always uses Node 26, then source/interop checks run under each matrix
version (24.12 and 26); Node 24 is not used to invoke `--build-sea`.
On version tags, artifact certification and release packaging run only on the
Node 26 leg: the artifact verifier requires its runtime to match the embedded
build runtime exactly. The Node 24 leg still runs source/interop and clean
installation verification; its success does not waive the tagged Node 26 gate.

The [current Linux arm64 clean-runtime report](measurements/standalone-current-clean-2026-11-01-linux-arm64.json)
repeats that installation against the current source with a fresh dependency
installation and all pinned patches applied. Its 695-file source fingerprint
matches the full Node 24/26 macOS test baseline. The new Linux archive embeds
Node 26.13.0; Debian Git 2.39.5 verifies authenticated helper operations and
repository integrity without a separately installed Node or source checkout.
Build/runtime image digests, archive/executable hashes, logs and snapshot custody
are recorded in the report. This remains a dirty local rehearsal, not a tagged
or publisher-authenticated release.

The [tagged macOS arm64 rehearsal](measurements/tagged-artifact-gate-2026-10-30-darwin-arm64.json)
exercises the real release gate using an isolated clean snapshot tagged
`v0.1.1-release-rehearsal.20261030`. Its Node 26.10 executable passed release
preflight, 11 installed-host/helper/parity/archive checks with zero skips, and
release-mode packaging. Running the artifact verifier with Node 24.12 correctly
failed the build-runtime check. The main working repository was neither committed
nor tagged for this exercise. The snapshot used shared installed dependencies;
this is not evidence of a fresh dependency installation or publisher signing.

The report records archive and complete Git-bundle SHA-256 digests. Local
artifacts and logs are retained under `dist/rehearsals/tagged-gate-20261030/`.
The `source.bundle` there contains the rehearsal tag and its full ancestry,
so its source remains inspectable after the temporary checkout is removed.
Use `git bundle verify` and compare the report's checksum before inspecting it;
clone that bundle into a new directory to reproduce the source snapshot. A
fresh rebuild still needs the recorded Node version, Git baseline and development
dependencies. These local checksums are integrity evidence, not authenticated
provenance, notarization, distribution approval or a production release.

After checking the artifact, this installs version `0.1.1` under your own
account without changing system directories. Set `artifact_dir` to the extracted
archive directory; use `dist/sea` when installing a local build instead. Keep
the notices and build evidence beside the installed executable:

```sh
artifact_dir="/path/to/extracted/git-plus-0.1.1-darwin-arm64"
release_dir="$HOME/.local/lib/chr33s-git/0.1.1"
mkdir -p "$release_dir" "$HOME/.local/bin"
install -m 755 "$artifact_dir/git+" "$release_dir/git+"
for file in LICENSE THIRD_PARTY_NOTICES.txt dependency-inventory.json build-info.json SHA256SUMS; do
  install -m 644 "$artifact_dir/$file" "$release_dir/$file"
done
ln -sfn git+ "$release_dir/git-remote-git+id"
ln -sfn "$release_dir/git+" "$HOME/.local/bin/git+"
ln -sfn "$release_dir/git-remote-git+id" "$HOME/.local/bin/git-remote-git+id"
export PATH="$HOME/.local/bin:$PATH"
git+ --version
git --version
```

Use the actual release version in `release_dir` for subsequent releases.
Upgrade by installing into a new version directory, verifying it, and replacing
both links in `.local/bin`. Keep the previous directory until the new version
has passed your smoke checks. To roll back the executable, point both links at
the previous directory; first check that its data-format compatibility permits
rollback. Replacing a binary does not revert repository data or credentials.

Uninstall by removing only the two links in `.local/bin` and the version
directories you installed under `.local/lib/chr33s-git`. Keep repository storage
and `$XDG_CONFIG_HOME/chr33s-git` (or `~/.config/chr33s-git`) unless intentionally
removing identity pins and configuration. Keep backups independent of this
installation directory.

`serve --ui` embeds its browser files. Verify it from a repository with
`git+ serve --ui`, then open the printed server URL. Test a known `git+id://`
remote with stock Git to verify helper discovery and identity pinning. An
identity mismatch must fail before cloning or changing a saved pin.

## Browser security policy

Production UI assets carry the same policy on Node and in Cloudflare's asset
rules. Inline JavaScript and event handlers are refused; startup code loads from
the same origin. Framing is denied, including by same-origin pages. Base-URL
overrides, plugins, child frames, and form submissions are disabled. Responses
also disable content sniffing, omit referrers, and deny camera, microphone, and
geolocation access. Keep these headers when adding a reverse proxy.

The policy permits same-origin scripts and local or embedded fonts, WebAssembly compilation for code
rendering, and same-origin or blob workers. Inline styles remain allowed for
the editor's dynamic layout; images may use same-origin, data, and blob URLs.
HTTP(S) connections remain available because browser users can explicitly
select remote Git hosts; ordinary browser CORS restrictions still apply.
This is not the server's outbound-network policy. Use HTTPS for deployed UI
origins and configure TLS at the ingress; local loopback development is separate.

The real-browser regression checks startup, legacy links, blocked inline code,
and blocked embedding. Production UI verification exercises signing and local
repository workflows under the Node policy. Cloudflare asset-rule parity is
checked locally; deployed header behavior remains part of the staging gate.

## Browser identity and local data

The browser signing key belongs to an origin, not to an individual repository.
Settings shows its public key and fingerprint; grant that public key only the
repository permissions it needs. The private Ed25519 seed is stored in the
origin's OPFS `git-plus/identity/identity.json` record. It is not encrypted with
a user password. Protect the browser profile and the origin's served scripts;
code executing with that origin's authority can read its storage.

Tabs coordinate initial key creation using the origin's Web Locks API. Reloads
reuse the saved key. If OPFS access is denied or a read fails, initialization
fails rather than replacing the saved identity. A fresh identity is not returned
until its record has been written successfully. Retry after restoring storage
access or freeing quota. If OPFS is entirely unavailable, the page instead uses
an in-memory identity and Settings warns that it will change after reload. Do
not treat a grant to that temporary key as a durable browser setup.

Local repositories live under OPFS `git-plus/<repo>`. The UI requests persistent
storage, but a browser may refuse that request. Clearing site data, deleting the
profile, private-session teardown, or storage eviction can remove both local
work and the signing key. Push work that must survive loss of the browser;
unpushed commits are not part of the server backup. Keep an independent
administrator identity so a lost browser key can be revoked and its replacement
granted. Do not clear site data as a routine repair step for a storage error.
Changing scheme, hostname, or port creates a different origin and therefore a
different local identity and repository store.

The pinned Playwright Chromium is exercised by CI: the full UI verifier covers
OPFS clone, commit, reload, and push; `src/ui/identity.interop.test.ts` covers
simultaneous tabs, persisted keys, migration/read failures, denied access,
injected quota exhaustion, and the no-OPFS warning. Injected exceptions verify
application behavior, not a browser's actual disk quota or eviction policy.
The identity suite also accepts an explicit engine for local qualification:

```sh
npx playwright install chromium firefox webkit
GIT_TEST_BROWSER=chromium npx vp test run --project interop src/ui/identity.interop.test.ts
GIT_TEST_BROWSER=firefox npx vp test run --project interop src/ui/identity.interop.test.ts
GIT_TEST_BROWSER=webkit npx vp test run --project interop src/ui/identity.interop.test.ts
```

An explicit selection fails if the requested engine is missing; it cannot
silently skip. Each case uses an isolated temporary on-disk profile that is
removed afterward. Coverage includes retaining the key across a full browser
restart at the same origin, in addition to reload and simultaneous tabs.
On macOS arm64, the pinned Chromium 156.0.8078.4, Firefox 157.0 and WebKit 27.2
each pass the ten identity cases under Node 24.12 and 26. The
[qualification record](measurements/browser-identity-2026-10-30.json) lists scope
and exclusions. WebKit's off-the-record context returned
`UnknownError` for a direct OPFS probe on this machine; an on-disk profile
passed. That observation does not qualify Safari private mode or promise its
storage semantics. The fault tests override the storage prototype and verify
denial directly before checking that the application refuses a fresh identity.
The identity record covers only those scenarios. For full UI qualification,
build once and run the selected installed engine explicitly:

```sh
npm run build:ui
node src/ui/verify.ts --browser chromium
node src/ui/verify.ts --browser firefox
node src/ui/verify.ts --browser webkit
```

These runs cover rendering, editing, local OPFS clone/commit/push, signed hub
operations, keyboard focus and both UI entry points. Stock Git checks the
resulting repository with `fsck --strict`. The development entry must establish
its HMR WebSocket on the same origin as HTTP. Each scenario receives a temporary
profile and dynamically allocated origin port: this WebKit build was observed
reusing OPFS at a fixed origin across separate profile directories, so a fresh
directory alone is insufficient test isolation. The verifier does not erase
other browser profiles or their storage. The pinned Foldkit patch keeps Tab and
Shift+Tab within dialogs; application commands focus an explicit trigger before
opening so Escape can restore focus on engines that do not focus clicked buttons.
No local run qualifies all operating systems or stable branded browser releases.
The [full UI qualification record](measurements/browser-ui-2026-10-30.json)
records passing runs on all three pinned engines under Node 24.12 and 26 on macOS arm64,
including the tested entry and Foldkit patch hashes.
Firefox, WebKit, private-browsing modes, and browser-managed eviction have not
been qualified for production. The production browser support matrix remains
a release decision; Chromium test coverage alone does not qualify every Chrome
version or mobile browser.

## Standalone server verification

The standalone release suite also runs a copied executable as the HTTP host
from an installation directory containing no Node executable or source tree.
It checks anonymous/read-only refusal of receive-pack probes, a writer's empty
successful flush-only probe, and anonymous refusal/read-token success for
upload-pack probes in both protocol versions. It clones with a small
`http.postBuffer` and completes a stock-Git chunked atomic push larger than
that buffer. Both resulting refs and a freshly cloned payload are checked
with stock Git. This protects the packaged server against regressions hidden by
small buffered pushes; source-host coverage alone does not replace this check.

## Local verification of the deployable Worker

Run `npm run build:ui` followed by `npm run verify:worker` with Git and Playwright
Chromium installed. The UI build preserves `dist/ui` for Node/SEA and stages the
Worker's exact public paths under `dist/worker-assets/hub`, with `_headers` at the
asset root. Explicit paths avoid Alchemy's local runtime ignoring the deployment
manifest's `base` prefix. The Vite build hook also stages development and watch
builds. Build before starting `alchemy dev src/alchemy.run.ts`.

To retain a module-size report after a successful rehearsal, run
`npm run verify:worker -- new-bundle-report.json`. The report destination must
not already exist. The verifier measures the emitted bundle before removing its
isolated stage: each non-source-map file has a SHA-256, uncompressed byte count
and gzip byte count (zlib level 9); totals sum the separately compressed modules.
Source maps are listed as excluded, and static assets and multipart upload
metadata are not counted. These are local Alchemy build measurements, not a
Cloudflare upload receipt or evidence of startup CPU, runtime memory, or the
limits of the target account. Retain the deployment's actual upload and resource
measurements separately before closing R11.

The [2026-10-30 local measurement](measurements/worker-bundle-2026-10-30.json)
contains four modules totaling 1,084,750 bytes and 317,397 independently gzipped
bytes. The accompanying full Worker/DO rehearsal passed. CI retains these reports
for 30 days as `worker-bundle-node-<version>` after each successful rehearsal.

The verifier starts the real `src/alchemy.run.ts` stack in a unique local stage,
using `src/worker.ts` and `src/host/Cloudflare.ts`, without deployment credentials,
anonymous writes, or conformance endpoints. It selects an ephemeral local port
and supplies `GIT_DEV_PORT` with `GIT_DEV_STRICT_PORT=true` so another local
development server cannot silently move the rehearsal. It uses the loopback URL
reported by its own Alchemy child and refuses to continue if that origin changes
during a restart. Ordinary `alchemy dev` still defaults to port 1337 with fallback
enabled; these two settings configure its local listener only. The archived
source used for upgrade/rollback receives only the same strict local port setting
in its temporary copy, because that version predates these configuration keys.
It checks UI/CSP
and browser startup, clones through stock Git, and verifies that API
writes and Git pushes are refused and leave refs unchanged. It also exercises
the real LFS routes: missing-object lookup, refused anonymous upload with no
stored object, batch byte/count limits, and a successful request after both
limit refusals. Through the local operator R2 binding it installs a verified
identity, proves that the previous anonymous snapshot no longer serves reads,
pushes/clones with authenticated stock Git, and uploads/downloads private LFS.
It also exercises native `Hub-SSH-v1` challenges: a member can sign a read and
an LFS upload, an ungranted key is refused, an envelope cannot authorize a
different operation, and a successful envelope cannot be replayed. Refused
ungranted/wrong-operation attempts leave the valid member's nonce usable.
It restarts the complete stack with the original artifact retained, verifies
that later branch state survives, and checks privacy after artifact removal.
An unused pre-restart nonce is refused after restart; a fresh signed challenge
then succeeds.
The verifier also injects snapshot deletion and publication failures into its
own local R2 bucket. A unique sentinel identifies that bucket; SQLite triggers
target only its snapshot key and are installed/removed with runtime handles
stopped. Deletion failure leaves the old public snapshot present, but concurrent
reads return 503 while the provisioning artifact fences it. Publication failure
after successful invalidation leaves no snapshot; private authentication, Git,
and LFS continue through authoritative storage, including after restart. Removing
the fault permits publication of the private snapshot. Cleanup removes the fault
triggers even when a verification assertion fails.
The historical public snapshot contains a real branch and blob: an anonymous
stock-Git clone and fsck establish that its content was readable before fencing.
The fixture seeds only the verifier's new local namespace while it is stopped.
After the deletion-failure assertions, it removes only its exact seeded ref,
retaining the populated snapshot and objects for the bootstrap publication-failure
test. Provisioning requires empty authoritative refs; this is evidence for fencing
a populated historical cache, not support for migrating an existing repository
in place. No production provisioning check is bypassed or weakened.
After removing the provisioning artifact, it also uses authenticated stock Git
to revoke the populated repository's only reader. A failed snapshot deletion
refuses the push and leaves that member authorized. With deletion restored but
publication failing, the accepted revocation denies the member's existing
credential for API reads, Git, and LFS. Anonymous access stays denied, including
after restart and after a private snapshot with the new trust frontier is
published. This exercises the local R2 emulator, bootstrap privacy, and member
revocation. It also pushes a signed PR/comment and a redaction tombstone: failed
invalidation refuses the tombstone, while failed publication after acceptance
does not restore the comment to the projection. Authenticated GC completes
payload removal; fresh hub-aware fetches omit the payload before and after
restart/recovery, with stock Git checking the received tombstone and missing
blob. Deployed R2 outages and Cloudflare eviction remain unqualified.
Malformed operator input returns a redacted 503 and can be removed before retry.
It runs Alchemy in a temporary installation with its own `.alchemy/` directory
and removes that installation after closing the local server. CI runs this alongside the storage integration suite
on both supported source Node versions.

The pinned Alchemy package needs the repository's `patch-package` patch: its
unpatched Worker bridge initializes Node terminal services that workerd cannot
provide, causing every routed request to fail before reaching the application.
The local verifier covers that composition failure. It does not replace deployed
private-authentication, snapshot fault/restart, LFS, account-limit, backup, or
upgrade rehearsals. Those remain staging gates.

## Cloudflare operator provisioning

Provisioning is authorized through the operator's R2 write credentials, not an
HTTP administration route. The reserved `_operator/provisioning/v1/` prefix
cannot be addressed as a Git repository. Limit bucket administration to trusted
operators: permission to upload provisioning artifacts grants authority to
initialize empty repository names. Keep anonymous writes disabled.

Identity material can now be prepared locally, without changing a deployment:

```sh
node tools/prepare-provisioning.ts /path/to/identity.git canonical-repo-name provisioning.json
```

Use an existing Git metadata directory initialized with `git+ hub init` and a
current, directly granted `repo.admin` key. The command exports the genesis and
trust-log object closure, verifies it in a fresh isolated store, and creates a
new `0600` output file; it refuses to overwrite an existing file. Verification
checks genesis root signatures, identity binding, complete object connectivity,
accepted trust records, and a non-expired administrator. Unsupported names,
missing membership, corrupt packs, and mismatched identities are refused.
The source refs are unchanged. Unrelated source branches and filesystem key
files are not exported; any content committed into the trust closure itself
is included, so retain the artifact with restricted access.

The version-1 format is limited to 2 MiB of encoded JSON, a 1 MiB pack, 4,096
objects, 8 MiB of expanded object data, and 20,000 graph steps during validation.
It names the intended canonical repository and RepoID. Stock-Git interoperability
tests unpack the emitted pack and run `git fsck --strict`; they also confirm that
omitting a required blob fails validation even after signature caches are warm.
Preparation alone does not change the Worker. After reviewing the expected
repository name, RepoID, and administrator fingerprint, use the exact bucket
name from the intended Alchemy stage and authenticated operator credentials:

```sh
npx wrangler r2 object put "BUCKET/_operator/provisioning/v1/canonical-repo-name.json" --file provisioning.json --content-type application/json --remote
```

The next request to that repository installs its identity before authorization.
The Worker bypasses anonymous snapshots while the artifact exists. The Durable
Object validates the artifact, invalidates its old snapshot, reserves the
artifact digest, imports the objects, and atomically publishes authority. It
does not publish a snapshot while a reservation is pending. Errors return a
generic, non-cacheable 503 and a redacted `CloudflareHost/provisioning_unavailable`
signal; successful installation emits `provisioning_installed`.

Verify the expected identity using the granted administrator key, authenticated
Git clone/push, private LFS, and denied anonymous reads. Retain the preparation
artifact with your deployment backup. Only after successful verification may
the operator remove the uploaded artifact:

```sh
npx wrangler r2 object delete "BUCKET/_operator/provisioning/v1/canonical-repo-name.json" --remote
```

The installed SQLite marker remains authoritative after removal. A pending
reservation requires the original artifact; removing or replacing it does not
cancel the reservation and leaves requests refused. Restore the exact saved
artifact and repair storage before retrying. Do not manually delete the marker
or edit refs to bypass a failed provisioning check. A completed artifact may be
left in R2, but anonymous reads will continue to take the Durable Object path.

Back up the provisioning marker with the repository's Durable Object database
and retain the matching artifact alongside the R2 object backup. Restore these
together: refs alone do not preserve reservation/replay protection. Do not roll
a pending installation back to a version that does not understand provisioning
markers; first complete it with this version or restore a consistent pre-install
backup. Cloudflare backup and rollback rehearsals must include this state.

The SQLite provisioning state component now reserves the exact artifact digest,
RepoID, genesis, and initial trust tip as `pending`. It refuses repositories with
refs, reflog history, or shallow boundaries, and rechecks emptiness at publication.
Both authority refs and the `installed` marker publish in one synchronous
transaction. A changed artifact cannot take over a pending reservation; an
installed artifact cannot rewind a subsequently advanced trust log. Corrupt
markers, changed genesis, or absent/invalid trust refs fail closed. Native SQLite
tests cover reopening, publication failure, intervening writes, and SIGKILL
between ref inserts with a healthy retry. The actual local Worker verifier also
tests provisioning, old-snapshot privacy, restart, retained-artifact replay,
private Git/LFS, and artifact removal. Deployed R2/DO failures, eviction, and
account-specific behavior still require staging evidence before production.

## Node outbound network policy

Node `serve` applies the same host-owned transport to remote fetch/push/pull,
background replication, and webhook delivery. Registering a remote or webhook
does not grant permission to access the host's private network. The default
permits HTTPS to public-unicast addresses and rejects private destinations.
DNS answers are checked in the socket connection callback; only those checked
addresses are handed to the connector. A new connection checks DNS again.
Existing connections remain connected to their original address.

The address filter conservatively excludes special-purpose IPv4/IPv6 blocks,
including loopback, link-local, private, shared, mapped, and translation/tunneling
ranges. It also excludes documentation ranges and whole special-purpose blocks
that contain some public exceptions. Its classification is based on the IANA
[IPv4](https://www.iana.org/assignments/iana-ipv4-special-registry/) and
[IPv6](https://www.iana.org/assignments/iana-ipv6-special-registry/) registries;
it does not establish that an otherwise public address is trustworthy.

For intentional internal integrations, set `GIT_EGRESS_PRIVATE_ORIGINS` to a
comma-separated list of exact origins, for example
`https://mirror.internal,http://127.0.0.1:9000`. Paths, credentials, queries,
fragments, and wildcards are rejected. Each entry permits that origin's resolved
addresses, including private ones; choose only operator-trusted services.
The existing URL-registration rules still require HTTPS except for loopback
HTTP URLs. This setting belongs to the operator, not repository administration.

For local development, `GIT_EGRESS_ALLOW_LOOPBACK=true` explicitly allows
localhost/loopback destinations on any port. It does not grant access to private
LAN or link-local destinations. Keep it disabled on a shared production host
unless repository administrators are intentionally allowed to reach local
services. Programmatic `serve()` callers can use `egressPrivateOrigins` and
`allowLoopbackEgress` instead; anonymous writes do not implicitly enable either.

GET/HEAD discovery follows up to five same-origin redirects. Cross-origin
redirects are refused before credentials or bodies can reach the new origin;
configure the canonical destination explicitly. Writes are not automatically
replayed across redirects. Native signed-auth retries recreate their body and
use the same checked transport. Policy failures appear as failed remote
operations or the existing sanitized delivery warnings.

This controls the Node HTTP host. Direct browser/CLI client calls remain
caller-controlled, and operator-enabled wake subprocesses need OS/ingress
controls appropriate to their commands. The Node adapter is not imported into
Cloudflare.

## Cloudflare outbound network policy

The deployable Durable Object denies outbound remote operations, replication,
and webhooks by default. To enable integrations, the operator supplies
`GIT_EGRESS_TRUSTED_ORIGINS` as comma-separated exact HTTPS origins, for example
`https://mirror.example.com,https://hooks.example.com`. Repository administrators
cannot change this host policy through remote/subscriber registration. Missing
configuration denies all destinations; malformed configuration fails handler
initialization. Credentials, paths, queries, fragments, wildcards, literal IPs,
and localhost names are rejected in this setting. Node private-origin and
loopback settings do not apply to Cloudflare.

Both deployment and the workerd integration configuration enable
[`global_fetch_strictly_public`](https://developers.cloudflare.com/workers/configuration/compatibility-flags/#global-fetch-strictly-public).
Cloudflare documents that this makes global fetch use the public routing/security
path even for the Worker's own zone, instead of directly reaching its origin and
bypassing that path. It can also route back into a Worker on the destination URL;
avoid configuring an integration that loops back into itself. This is an explicit
platform routing safeguard, not an application DNS-pinning or CIDR filter.

An enabled origin is a trust decision about its service, DNS administration,
and routing. **This allowlist does not check resolved addresses or prevent DNS
rebinding within an enabled hostname.** Cloudflare's standard fetch uses the
platform resolver and does not expose the Node adapter's socket lookup callback;
see Cloudflare's [fetch/DNS limitations](https://developers.cloudflare.com/workers/platform/known-issues/).
Do not enable repository-owner-controlled or otherwise untrusted DNS names.
Supporting those destinations requires verified connection-level restrictions
or an operator-controlled egress gateway. That capability and actual deployed
policy verification remain production release gates under R04/R11.

The transport disables native redirect following. GET/HEAD requests may follow
at most five same-origin hops. Cross-origin redirects are refused even when
both origins are enabled, preventing credential forwarding between integrations.
Writes are sent once per explicit client attempt and are not automatically
replayed on redirects. Existing manual/error redirect behavior and explicit
signed-auth retries remain under the same host policy. Refusal surfaces through
remote-operation errors and sanitized delivery warnings. Verify effective
configuration and all three call paths on staging before enabling integrations.

## Request budgets

The Node host reads these positive-integer settings at startup. Invalid values
fail startup rather than disabling a limit. The Cloudflare host loads the same
configuration when initializing its handler; deployed overrides still require
staging verification. Library callers without an override get the same bounded
defaults.

| Setting                      |       Default | What it bounds                                                           |
| ---------------------------- | ------------: | ------------------------------------------------------------------------ |
| `GIT_NEGOTIATION_BYTES`      |     2,097,152 | Decoded upload-pack negotiation body                                     |
| `GIT_NEGOTIATION_LINES`      |        20,000 | Negotiation packets, including separators/capabilities                   |
| `GIT_FETCH_GRAPH_STEPS`      |       250,000 | Shared fetch-plan graph work across haves, wants, and shallow exclusions |
| `GIT_RECEIVE_BYTES`          |   536,870,912 | Complete decoded receive-pack request, including commands                |
| `GIT_RECEIVE_COMMANDS`       |        10,000 | Receive commands and shallow declarations                                |
| `GIT_RECEIVE_COMMAND_BYTES`  |     2,097,152 | Receive command prefix, including pkt-line headers                       |
| `GIT_RECEIVE_OBJECTS`        |       100,000 | Objects declared in one received pack, including duplicates              |
| `GIT_RECEIVE_EXPANDED_BYTES` |   536,870,912 | Sum of inflated entries plus reconstructed delta targets                 |
| `GIT_LFS_BATCH_BYTES`        |     1,048,576 | Batch JSON request bytes                                                 |
| `GIT_LFS_BATCH_OBJECTS`      |         1,000 | Objects in one LFS batch                                                 |
| `GIT_LFS_UPLOAD_BYTES`       | 1,073,741,824 | Bytes in one LFS upload, even without a batch or Content-Length          |
| `GIT_JSON_BODY_BYTES`        |     8,388,608 | Buffered JSON API request bytes, including base64 encoding               |

Fetch graph steps count roots and edges considered, including duplicate inputs,
tag peeling, shallow-history parents, tree entries, and excluded-ref work. Hub
history walks needed for redaction exclusions also consume this allowance,
including repeated parent edges and ref validation on cached exclusions. One
allowance is shared across the have/want walks and a redaction retry; it is not a
separate allowance per OID or walk. Exhaustion returns 413 before a pack response
is produced. A full clone may exceed the configured limit while a shallow clone
fits, but shallow fetches may still need unrelated hub histories to explain
redacted objects. Inspect repository size and available host capacity before increasing the
limit. The Cloudflare fast path uses the same configuration and refuses exhausted
budgets without repeating the work in the Durable Object.

This bounds the charged graph walks, not every unit of CPU time: object parsing,
compression, trust/hub projection, ref advertisement, persistent storage growth,
and other API operations need their own limits. Direct core library/maintenance
calls do not acquire an HTTP planning allowance automatically. These counts are
not a supported-repository-size guarantee.

Limits count bytes actually consumed, not the advertised Content-Length, and
Git gzip input is limited after decompression. Rejected readers are canceled.
JSON API, negotiation, command-prefix, LFS upload, and LFS batch overflow return HTTP `413`; a receive
limit reached during unpacking produces a failed Git unpack report and applies
no refs. Already-written unreachable objects can remain for maintenance to reclaim.
Anonymous inbox submissions retain their additional, lower 32 MiB pack ceiling.
Use the streaming commit-pack endpoint for bulk file writes rather than raising
the JSON ceiling. JSON API bodies do not support compressed request encoding.

Pack import limits apply before object inflation or delta reconstruction on
both loose-object and retained-pack paths. Duplicate entries consume the budget
again; delta instructions and their reconstructed targets both count. This is
an expansion budget, not a measurement of total CPU work across delta chains.
The same defaults protect library/CLI imports; library applications can provide
`Pack.ImportBudget` explicitly. The environment settings above configure HTTP
receive-pack. LFS batch upload requests over the upload ceiling receive a
per-object 413 error; sending a PUT directly cannot bypass the streaming ceiling.
Rejected Node uploads remove their temporary file; the R2 store's stream-error
path aborts multipart uploads. The R2 writer consumes bodies in the request's
Effect runtime, retains a newly created upload handle before sending its first
part, and waits for in-flight storage calls before running cleanup. Cancellation
therefore waits for multipart abort rather than leaving a detached body iterator.
An abort failure preserves the primary request failure and emits the redacted
`Lfs/multipart_cleanup_failed` signal; incomplete uploads then require operator
reconciliation. Local workerd checks cover rejected first parts, interruption
after a stored 8 MiB part, and injected abort failure. The actual Worker rehearsal
also checks timeout, absence of a readable partial object, and a complete retry.
Live R2 cleanup and incomplete-upload retention still require staging verification.

The managed bucket declares `abort-incomplete-uploads`: an enabled rule for the
entire bucket that makes incomplete multipart uploads eligible for abort after
604,800 seconds (seven days). This matches Cloudflare's documented default age.
The rule has no completed-object deletion or storage-class transition action.
It is explicit because the pinned Alchemy provider reconciles an omitted
`lifecycleRules` property to an empty policy. See
[Cloudflare object lifecycles](https://developers.cloudflare.com/r2/buckets/object-lifecycles/).

This is a fallback for isolate loss or failed application cleanup, not an
immediate cleanup guarantee or a byte quota. The local Alchemy provider does not
apply lifecycle rules, so a passing local rehearsal cannot prove enforcement.
After deploying to the selected staging account, set `GIT_R2_BUCKET` to the
actual bucket name from the deployment and read back its policy:

```sh
npx wrangler r2 bucket lifecycle list "$GIT_R2_BUCKET"
```

Confirm the enabled rule, full-bucket prefix, seven-day age, and absence of
completed-object expiry. Retain that readback and staging evidence of abandoned
upload cleanup before closing the Cloudflare storage-growth release gate.

These request byte/count budgets are separate from the retained Git/LFS limits
documented below. Aggregate account capacity, deployed qualification of Cloudflare admission/deadlines, and measured
supported sizes remain release gates. Do not infer a production repository-size guarantee from the
512 MiB request default. Raising limits requires host-specific load measurements.

### Node admission and deadlines

Both the Node entry point and `git+ serve` read the following settings at startup.
Values must be positive integers no greater than 2,147,483,647; invalid values
fail startup, including values that would overflow Node's timers.

| Setting                    | Default | Boundary                                                                                        |
| -------------------------- | ------: | ----------------------------------------------------------------------------------------------- |
| `GIT_HTTP_MAX_ACTIVE`      |      32 | Admitted requests across the host, including authentication, queued work, and response delivery |
| `GIT_HTTP_MAX_PER_REPO`    |       8 | Admitted requests for one repository, including queued work and response delivery               |
| `GIT_HTTP_MAX_CONNECTIONS` |     128 | Open TCP connections, including clients that have not finished sending headers                  |
| `GIT_HTTP_DEADLINE_MS`     | 300,000 | Absolute request/response deadline after headers, plus Node's request-receive timeout           |
| `GIT_HTTP_IDLE_MS`         |  30,000 | Socket inactivity timeout while serving requests                                                |
| `GIT_HTTP_HEADERS_MS`      |  15,000 | Header receive timeout, capped by the request deadline                                          |

Admission overflow returns HTTP 503 with `Retry-After: 1` and closes that
connection. A full connection pool refuses new sockets before parsing HTTP.
Incomplete headers receive Node's 408 response. Idle or absolute deadlines
close the transport, so clients must handle a connection failure. A continuously
trickling body cannot extend the absolute deadline. Slow response readers keep
their admission slot until delivery finishes or their connection closes.

A timed-out write has an unknown outcome: read the current refs or LFS object
before retrying. Already-started uninterruptible storage operations finish under
their original repository gate and retain their admission slot until they settle.
A deadline does not roll back a mutation or make it safe to run a second write
concurrently. A storage outage can therefore keep capacity occupied after a
client disconnects. These timers also cannot preempt synchronous JavaScript CPU
work; object-walk and CPU-work budgets remain required.

These socket and deadline controls apply to Node. Cloudflare Durable Objects
have a separate `GIT_CF_MAX_ACTIVE` limit (default 8, positive integer), configured
through the Worker environment. Each instance admits at most that many requests,
including provisioning, authentication, queued writes, and response delivery.
Overflow returns an uncached 503 with `Retry-After: 1` and emits a fixed-field
`CloudflareHost/admission_rejected` warning. Failed handlers and completed,
failed, or cancelled response streams release their slots; interrupted
uninterruptible writes retain capacity until they settle. This is a per-instance
limit, not a global Worker or per-client rate limit.

The edge has an independent `GIT_CF_EDGE_MAX_ACTIVE` pool (default 32, positive
integer) per Worker isolate. It includes public snapshot reads, forwarded
requests, and their response delivery, and emits `CloudflareEdge/admission_rejected`
with the same retryable 503 behavior. Static assets served directly by the
platform do not invoke the Worker and are outside this pool. Multiple isolates
have separate pools: these limits do not bound total deployment concurrency or
provide per-client fairness. Cloudflare ingress rate limits, deadlines,
storage quotas, and production capacity still need staging evidence.

Durable Object configuration is read in its initializer, which Alchemy executes
during binding discovery as well as instance activation. Reads deferred only to
the nested runtime effect can miss deployment bindings and silently use defaults.
Redeploy after changing host limits, `GIT_EGRESS_TRUSTED_ORIGINS`, or
`ALLOW_ANONYMOUS_WRITES`. The local rehearsal exercises a two-request DO limit,
enables and then disables writes on an uninitialized repository, and verifies
that a repository with genesis remains private throughout. A separate actual
stack startup test rejects an invalid egress allowlist before serving requests.

To add the native per-client limiter, set `GIT_CF_RATE_NAMESPACE` to a positive
decimal identifier (up to 18 digits) reserved for this deployment in the account.
Use different namespaces for staging and production; bindings sharing a
namespace share counters. Unset or empty means no binding is installed.
`GIT_CF_RATE_PER_MINUTE` defaults to 600 and accepts integers from 1 to 1,000,000.
These settings configure the Worker binding and require redeployment to change.
The limiter runs inside edge admission, before routing, public snapshot reads,
or DO forwarding. Platform-served static assets bypass it.

The key is the parsed, normalized `CF-Connecting-IP` supplied by Cloudflare;
forwarded headers, tokens, URLs and repository names do not choose buckets.
Missing/malformed client metadata shares one `unknown` bucket. Clients behind
one NAT share a limit. Only trust this header at the platform boundary: local
development and direct service callers do not establish public-client identity.
The client IP is supplied to the platform counter but never emitted in the
limiter's fixed-field logs. Denial returns uncached 429 with `Retry-After: 60`
and `rate_limit_exceeded`; native counter failures return uncached 503 with
`rate_limit_unavailable`. Refused bodies close through normal request cleanup.

Cloudflare documents this API as approximate, eventually consistent, and
scoped to each location. Concurrent requests can overshoot the configured
limit; it is neither a global cap nor billing/storage accounting. Production
still requires ingress configuration and target-account validation of client
identity, counter isolation, and overload behavior. See the
[Cloudflare rate-limit binding contract](https://developers.cloudflare.com/workers/runtime-apis/bindings/rate-limit/).

Both Cloudflare pools apply response-delivery timeouts:
`GIT_CF_RESPONSE_IDLE_MS` defaults to 30,000 milliseconds without forwarding a
body chunk, and `GIT_CF_RESPONSE_DEADLINE_MS` defaults to 300,000 milliseconds
from response creation. Both require positive integers. Progress resets the idle
timer but cannot extend the absolute deadline. Expiry errors the response stream
and cancels its source, including when a client stops reading; it cannot change
headers that have already been sent. Clients must treat this as an incomplete
transfer. The normal `response_body_failed` signal records the failure without
exposing its raw cause. Capacity is released only after source cancellation
settles, so a stuck underlying operation can continue occupying a slot.

The separate `GIT_CF_REQUEST_DEADLINE_MS` defaults to 300,000 milliseconds from
admission until the handler produces a response, including authentication,
queued work, uploads, and snapshot publication. It requires a positive integer.
Expiry selects an uncached 504, signals the router's separate runtime to cancel,
and emits `request_deadline_exceeded` with component and deadline fields. The
timeout waits for the interrupted request's cleanup: already-started
uninterruptible writes retain their admission slot and may delay the 504 beyond
the configured deadline. No mutation is detached or rolled back by the timer.
The edge and DO each start their own timer when admitting a request; response
delivery has its separate budget described above. Cancellation also errors the
incoming body and releases its reader. This stops an uninterruptible mutation
route from waiting indefinitely for more upload bytes; cancellation is an error,
never a successful end-of-file. Storage work already in progress still settles
before the admission slot is released. Unread bodies are closed after refusals.

`GIT_CF_UPLOAD_IDLE_MS` defaults to 30,000 milliseconds and requires a positive
integer. It bounds each pending read from the incoming body, returning an
uncached 408 with an `upload_idle_exceeded` warning. Time spent processing a
chunk or waiting for downstream storage does not count as sender inactivity.
Progress clears the timer; the next read starts a new one. The absolute request
deadline still bounds trickling uploads and work before body consumption. Idle
expiry selects its response before cancelling the body and waits for request
cleanup under the same admission rules.

These controls cannot preempt synchronous JavaScript or forcibly terminate
uninterruptible storage work. Deployed slow-client/storage-outage qualification
remains required. A 408, 504, or missing
response after a write is not proof that it failed; check current refs or LFS
state before retrying. The local Worker rehearsal sets a ten-second request
deadline and verifies expiry/recovery for incomplete public Git negotiation and
authenticated JSON uploads, LFS uploads, receive-pack, and commit-pack. The two
mutation probes verify that incomplete uploads leave refs unchanged. The LFS
probe verifies no partial object is readable and a complete retry succeeds.

Put the Node service behind TLS ingress with per-client rate
limits; concurrency limits do not prevent a client from repeatedly exhausting
the pool. Select limits for measured memory/CPU capacity and expected transfer
times rather than assuming the defaults certify a particular repository size.

## Credential files and upgrades

Run the Node host as a dedicated account. Its storage root should be owned by
that account and inaccessible to other users (`0700`). Avoid shared or
world-writable parent directories. Set `umask 077` in the service manager and
shell used for administration. Git object contents, session records, local
configuration, and backups can be sensitive even when they contain no tokens.

Remote credentials and signing keys are stored in each repository's
`remotes.json`; webhook secrets are in `webhooks.json`. The local Artifacts
provider keeps token digests and revocations in `<root>/.tokens.json`.
Repository and fork registry replacements (`.registry.json` and `.forks.json`)
also use private `0600` files and `0700` for newly created directories.
The writers create their temporary and final secret files with mode `0600`,
independently of umask. Replacement preserves that protection. New metadata
directories use `0700`; existing directories are not silently chmodded.

The Node repository registry refuses malformed rows, empty identity/default-branch
fields, and duplicate names or IDs. Duplicate records are not merged or silently
selected: they may disagree about identity, read-only restrictions or fork
ownership. Reads and edits return a redacted `ArtifactsError`, preserving the
file for offline recovery. Diagnostics omit parsed contents and private storage
paths. Preserve `.registry.json` and its associated repositories/credentials,
stop writers, and repair from trusted evidence before retrying. Do not resolve
duplicates merely by keeping the last row. Lock contention remains a
`PRECONDITION_FAILED` with the safe cause code `EEXIST`, so a caller can retry
without relying on a native error message or removing another writer's lock.

The Node Artifacts provider independently bounds each namespace's repository
and credential metadata files. `GIT_ARTIFACTS_REGISTRY_ROWS` defaults to 1,024
and `GIT_ARTIFACTS_REGISTRY_BYTES` to 1 MiB for `.registry.json`;
`GIT_ARTIFACTS_TOKEN_ROWS` defaults to 4,096 and `GIT_ARTIFACTS_TOKEN_BYTES` to
2 MiB for `.tokens.json`. Row limits must be integers from 1 to 1,000,000;
byte limits must be positive safe integers. Provider layers validate these
settings when opened. Reopen the provider after changing them. These are
retention budgets, not measured supported-workload guarantees.

Budgets count the entire serialized UTF-8 JSON file, including whitespace,
timestamps, descriptions and credential digests. Token capacity is shared by
all repositories in that namespace, and expired/revoked rows still count.
Creation, token issuance and repository branch/timestamp updates inspect the
current file size under its writer lock before parsing and check the candidate
row/byte totals before writing a temporary file. Exhaustion returns a redacted
`ArtifactsError` with `RESOURCE_EXHAUSTED` and emits
`ArtifactsMetadata/quota_exceeded` with a fixed registry category; the original
file is unchanged. Competing processes cannot independently spend the same
remaining capacity.

Reads, repository removal, initialization completion and credential revocation
remain available after limits are lowered, so exhaustion cannot prevent access
revocation. These recovery operations still parse and rewrite whole legacy files;
they do not establish a byte bound on previously oversized data. Reconcile very
large legacy files offline before exposing normal traffic. Removing repository
rows frees that file's capacity; revoking a token does not remove its row. Raise
the budget or perform reviewed offline retirement of obsolete token rows with
all writers stopped and a coordinated backup. There is no automatic expiry,
purge or eviction. A metadata update failure after a Git operation does not undo
that operation: inspect the current refs before retrying after recovery. These
Node limits exclude fork metadata, SQLite-backed Artifacts adapters, Git/LFS payloads,
other namespaces, filesystem overhead and account-wide quotas. Stop older
writers that do not enforce these limits and configure the same budgets on every
writer of a namespace before relying on the boundary.

Node fork metadata uses `GIT_ARTIFACTS_FORK_ROWS` (default 1,024) and
`GIT_ARTIFACTS_FORK_BYTES` (default 1 MiB), validated with the same ranges.
The row budget bounds the namespace's `.forks.json` links and, independently,
each parent's borrower lines. The byte budget bounds the serialized UTF-8
fork-link document and each borrower/alternate file written by the provider.
Before a fork changes disk, under the cross-process fork lock, it rejects an
oversized current fork document before parsing, checks the candidate links,
and checks existing/candidate borrower contents and the alternate path.
Refusal preserves existing links and does not create the child's alternates.

Repeated registrations retain one borrower line per child. Atomic replacement,
file fsync and ancestor-directory fsync preserve the existing durability
contract; external borrower names already in the file are retained. Reads,
forget and drop remain available after budgets are lowered, and can still
parse oversized legacy metadata during recovery. Quota refusal reports
`ArtifactsMetadata/quota_exceeded` with registry `forks`. These limits do not
bound files written by external Git commands, old providers or direct filesystem
writers; stop those writers during metadata changes. They are per-document
budgets, not a physical/account-wide storage quota. Recovery from partial
filesystem failures still follows the fork repair procedure; no automatic
purge or stale-lock stealing is introduced.

Cloudflare HEAD targets have a separate `GIT_CF_HEAD_BYTES` budget (default
1,024 UTF-8 bytes; positive integer, maximum 65,536). The DO initializer resolves
it for deployment binding discovery and passes it to both hosted stores and
same-bucket mirror stores. Redeploy to change it. A target that exceeds the
budget is refused before its KV value is replaced, with a typed storage failure
and the fixed `GitStorage/quota_exceeded` signal, resource `head_bytes`.
Reads also refuse valid but oversized legacy targets, preserving stored bytes.
The native KV get delivers the existing value before this check; this is a
target-size boundary, not a pre-allocation bound on legacy KV reads.

After lowering the budget, replace an oversized valid target with an in-budget
ref name through `setHead`, or raise the budget and redeploy. Replacing corrupt
non-string/invalid-ref data remains refused, so quota recovery cannot silently
erase corruption. Extremely small configured budgets may also reject the
implicit `refs/heads/main` default. HEAD remains one value per repository and
is not charged to the SQLite ref ledger; external writers, KV key overhead and
account-wide quotas remain separate operational concerns.

The SQLite Artifacts adapters enforce the corresponding
`GIT_CF_ARTIFACTS_REGISTRY_ROWS/BYTES` and
`GIT_CF_ARTIFACTS_TOKEN_ROWS/BYTES` settings, with the same defaults and validated
ranges. Each budget covers the entire namespace table. Bytes count UTF-8 values
of all stored columns (including numeric flags as text), excluding SQLite pages,
indexes and other physical overhead. These are separate from Node JSON budgets.
Creation, issuance and branch/timestamp updates check current and candidate usage
inside one synchronous SQL transaction; a refusal rolls back the entire mutation.
Both adapters require a transactional SQL port. Credential hashing occurs before
the transaction; plaintext is returned only after insertion commits.

SQL computes aggregate usage over at most the configured row limit plus one.
It does not materialize metadata in JavaScript or maintain a cached counter that
could miss legacy rows. This costs two bounded scans per growth operation;
production throughput and Durable Object CPU limits still need measurement,
particularly before increasing row limits. Verification and ordinary reads do not
scan for capacity. Revoked and expired tokens consume capacity until explicit
retirement. Reads, deletion, initialization completion and revocation remain
available when an existing table exceeds its budget; new growth fails closed.
Use coordinated offline repair or raise the limits and reopen the adapters.
Direct SQL writers and older adapter versions bypass enforcement and must be
stopped before relying on these budgets. Fork metadata, other KV data, physical
SQLite storage and account-wide quotas remain separate release requirements.

Token edits hold the cross-process reservation until the registry file and its
resolved ancestor chain through the filesystem root have synced. This applies
even when revocation returns `false` because the token is already revoked: a
previous failed edit may have left visible bytes without completing durability.
Read-only list/verify operations do not add syncs. As with other metadata writes,
the account needs access to open and sync the directory chain. A post-rename
failure is an unknown mutation outcome; inspect the current token list and retry
revocation by ID after fixing storage. Do not restore an older registry merely
because an edit returned an error. Local fault tests verify retry behavior and
continued denial of visible revoked tokens; they do not simulate power loss.

Repository registry mutations sync exclusive temporary contents before rename,
then sync the registry and physical ancestor chain before releasing the edit
reservation. No-op deletion or branch-setting retries also repeat the sync.
Fork registry replacement follows the same file/ancestor ordering, cleans up
temporary files on failure, and repeats syncs when the requested link is already
visible. A post-rename failure can therefore leave a complete changed document
while still returning an error.

Fork `alternates` use synced temporary-file replacement. Borrower additions
sync their append before closing, and pruning a borrower list uses synced
replacement rather than truncating the live file. Removing either metadata
file syncs the surviving directory chain, even when a previous attempt already
removed the file or its containing repository. These helpers never recreate a
deleted repository. The disk records are published before the fork JSON link.

Fault tests cover partial borrower rewrites, failed syncs, and a killed writer
after alternate publication but before recording the borrower. In that tested
crash window, stock Git still reads the child's objects and the parent's
borrower scan still protects them. The test confirms the writer exited before
removing only its known stale reservation and retrying. Follow the stale-lock
procedure below, preserve the files, and inspect Git integrity before recovery;
never erase a borrower list merely to force collection or parent deletion.
Recovery is operator-assisted under the Node durability contract below; no
transaction across the whole installation or physical power-loss guarantee is
claimed.

When upgrading from a version that created credential files with default modes:

1. Stop every host and CLI process that writes the storage root.
2. Confirm the root and its parent directories belong to the service account;
   do not follow storage symlinks into directories managed by someone else.
3. Set the root to `0700` and each existing `remotes.json`, `webhooks.json`, and
   `.tokens.json` to `0600`, as that account. Check all repositories, including
   ones not currently receiving traffic. Protect existing backups separately.
4. If other users could read a credential-bearing file, rotate the affected
   remote key/credential and webhook secret. Restricting permissions does not
   revoke copies already obtained. Revoke exposed Artifacts bearer tokens if
   their plaintext was copied elsewhere; `.tokens.json` contains only digests.
5. Restart the host and verify legitimate read/write operations before reopening
   ingress. Inspect file ownership and modes after the first registry update.

On Unix, inspect a known path with `ls -ld` and change only that reviewed path,
for example `chmod 700 /srv/git` and `chmod 600 /srv/git/project/remotes.json`.
Do not recursively apply `0600`: directories require traversal permission.

## Damaged metadata and stale locks

An absent optional metadata file means an empty registry. Invalid JSON, invalid
rows, and inaccessible files are errors. List and mutation operations preserve
the original file rather than replacing damaged contents with an empty list.
Errors intentionally omit parsing details that could quote a secret.
Webhook delivery logs `webhook registry unavailable` when it cannot load its
recipients; the source push remains accepted.
Stored webhook secrets must meet the registration minimum of 16 characters;
replace shorter hand-written legacy secrets on both sender and receiver before
restarting delivery.

Stop writers before recovery. Retain a private copy of the damaged file and
compare it with the last known-good backup. Repair its JSON/schema or restore
the complete document, preserving revocations and repository identity. Never
delete `.tokens.json` as a general repair step or restore an older token list
without reconciling revocations: old credentials must not become valid again.
Restart and verify that revoked credentials remain refused and that the complete
remote/subscriber lists survived. Fix filesystem permissions or mount errors
before retrying a storage failure.

Metadata mutation uses exclusive `.lock` files. Contention fails without changing
the registry; retry after the active writer completes. A process killed without
cleanup can leave a lock. Before removing a lock, stop all writers that can reach
that storage and verify that none still owns it. Age alone does not establish
that a lock is stale. Remove only the identified lock, preserve the underlying
data, and then restart writers. This also applies to `.registry.json.lock`,
`.forks.json.lock`, per-repository files under `.operations`, and Git ref/index
locks; use the owning tool's recovery rules for Git locks.

## Node liveness and readiness

Set `GIT_OPERATIONS_PORT=9091` to enable the separate operations listener at
`http://127.0.0.1:9091`. It is disabled when unset. The embedding API accepts
`operationsPort` and returns `operationsUrl`; port `0` chooses an ephemeral port.
Startup fails if the requested port cannot bind. This listener only accepts
GET/HEAD, requires its literal `127.0.0.1:port` Host, rejects foreign Origin
headers, and caps connections at 16 with short request/idle deadlines. It has no
repository routes and does not consume Git request admission slots.

- `/live` returns 200 while the listener is running. It does not check storage.
- `/ready` returns 200 only after a successful storage probe, while that result
  is less than ten seconds old and global request capacity is available;
  otherwise it returns 503. Its JSON reports `ready`, `storage`, and `capacity`.
- `/metrics` returns the cumulative request metrics described below.

Probe the literal loopback address from the same network namespace. Do not
publish this listener through the public reverse proxy or container port
mapping; it is an unauthenticated local operator interface. These endpoints
return `Cache-Control: no-store`. Configure readiness failures to remove traffic,
not to restart the process on every transient storage outage or overload.

Every five seconds, at most one storage probe creates an exclusive private
`.git-plus-health-<uuid>.tmp` in the data root (or the explicitly served repository
directory), writes and syncs a small payload, reads it back, removes it, and
syncs the directory. The probe never creates or repairs a missing root. Create
the configured root with the service owner's permissions before enabling probes.
A stalled probe prevents further probes and its last successful result expires.
Failure transitions emit a redacted `NodeHost/storage_probe_failed` JSON log.

A failed storage probe calls for checking the configured volume, free space,
permissions, and filesystem errors. A capacity failure calls for checking active
requests and slow clients. The probe is a small write test, not proof that every
repository, metadata file, alternate object store, or future large allocation is
healthy. It does not validate backup integrity or establish a power-loss
contract. A killed process can leave a probe temporary file; remove only those
files after confirming the owning process is stopped. Shutdown closes the
operations listener and waits for any current probe to clean up.

## Node request metrics

The Node host emits a JSON stderr record every 60 seconds with
`component: NodeHost` and `event: request_metrics`. Set
`GIT_HTTP_METRICS_INTERVAL_MS` to a positive integer interval in milliseconds, or
`0` to disable periodic export. Values above 2147483647 are rejected at startup.
The embedding API also exposes `server.metrics()`; no metrics HTTP endpoint is
exposed on the public Git listener. Collect these records through the service's
log collector and retain its process/instance identity.

Counters are cumulative for this server instance; `uptimeMs` identifies its age.
Use differences between records from the same instance for rates. `admitted`
counts requests admitted globally, `active` includes queued handlers, response
delivery, and timed-out work that has not settled. `completed` counts released
requests, including interrupted ones. `admitted = completed + active` always
holds. `refusedGlobal` counts requests outside that admitted population;
`refusedRepository` counts admitted requests refused by the repository limit.
Do not add either refusal count to `serverErrors` without accounting for overlap.

`deadlines` counts absolute request deadline expirations. `interrupted` counts
released requests whose responses did not finish. `unauthorized`, `forbidden`,
`oversized`, and `serverErrors` count finished 401, 403, 413, and 5xx responses.
`durationMsSum` and `durationMsMax` measure admission through release, including
queueing and delivery; the sum delta divided by completed-count delta gives the
interval mean. The maximum is a lifetime maximum, not a percentile. Counters
have fixed size and never label by repository, URL, credentials, or request body.

Alert on rising refusal/deadline/error rates and sustained active capacity. Check
storage and slow receivers before increasing configured limits; deadline expiry
does not terminate an uninterruptible write. These metrics exclude connections
rejected before HTTP dispatch, header parsing timeouts, and requests served by
development middleware. They do not supply storage quotas, snapshot freshness,
Cloudflare metrics, or repository-specific health checks.

## Operational failure rehearsal evidence

The following local checks exercise actual failure paths and their operator
signals. Run them with the repository's test prerequisites installed; a skipped
Git test is not failure-rehearsal evidence.

| Failure                                                      | Signal and recovery evidence                                                                                                                  | Regression                                   |
| ------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------- |
| Storage root becomes unavailable                             | Readiness changes to 503 while liveness stays 200; restoring storage restores readiness without exposing its path                             | `Operations.node.test.ts`                    |
| Object/ref/journal write or object sync returns ENOSPC       | Redacted `git.Node/storage_failure`; atomic push refused, original refs retained, cleanup and successful restart/retry checked with stock Git | `DiskFull.interop.test.ts`                   |
| Webhook TCP receiver has stopped                             | Terminal `Webhooks/delivery_result`, outcome `failed`, subscriber ID and timing; URL, secret, refs and object IDs absent                      | `Webhooks.test.ts`                           |
| Receiver repeatedly returns 503 through retry exhaustion     | Accepted source refs survive; after sender restart, explicit reconciliation repairs missed updates and deletions                              | `Delivery.recovery.interop.test.ts`          |
| Admission capacity or deadline exhausted                     | Refusal/deadline counters advance; occupied work retains its lease until settlement                                                           | `Admission.node.test.ts`                     |
| Canary storage, identity or content fails                    | Bounded, redacted unhealthy probe result; Git/LFS content and authorization checked through real HTTP                                         | `HostProbe.interop.test.ts`                  |
| Maintenance fails or detects corruption                      | Redacted failure/integrity events; original errors preserved and retry behavior checked                                                       | `Maintenance.signals.test.ts`                |
| Cloudflare response fails before headers or during streaming | Fixed-field error/body outcome events, correct status and cancellation behavior, no raw cause                                                 | `CloudflareRequest.test.ts`                  |
| R2 refuses LFS multipart abort                               | Redacted `Lfs/multipart_cleanup_failed`; original request failure preserved; operator reconciliation required                                 | `Lfs.cloudflare.contract.ts` through workerd |

The actual local Worker rehearsal separately checks snapshot failure signals
and the canary probe through the deployed entry modules. These checks do not
prove that a production log collector or alert destination receives events.
Before release, qualify collection, retention and alert delivery in the selected
environment, including platform-enforced CPU/memory/storage limits that can
terminate an invocation before application logging executes. Correlate platform
failures with the external canary; absence of an application event is not health.

## Private repository health probe

`node tools/probe-host.ts` performs a read-only synthetic check against the
ordinary Node or Cloudflare API. Run it from a monitoring checkout with Node
24.12 or later and installed dependencies; the served standalone host does not
need Node installed for this external check. Provision a dedicated private
canary repository, retain one known small commit on a fixed ref, and grant the
monitor only read access. Optionally retain a small LFS object with a known
SHA-256. Keep this canary out of ordinary branch updates and retention jobs.

Configure the monitor through its protected environment:

| Variable            | Meaning                                                                                                                                       |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `GIT_PROBE_URL`     | Repository URL, such as `https://git.example.com/health-canary`; HTTPS is required except for literal loopback/localhost development targets. |
| `GIT_PROBE_TOKEN`   | Fresh delegated read credential supplied by the scheduler's secret mechanism.                                                                 |
| `GIT_PROBE_REF`     | Fixed canary ref; defaults to `refs/heads/main`.                                                                                              |
| `GIT_PROBE_OID`     | Expected 40-character Git commit ID.                                                                                                          |
| `GIT_PROBE_LFS_OID` | Optional expected 64-character LFS SHA-256.                                                                                                   |

The token is not a command-line argument. Refresh it before expiry using the
normal delegation workflow; an expired or revoked credential is an unhealthy
probe and must not be mistaken for evidence that storage is corrupt. The probe
does not create identities, write a canary, or refresh its own authority.

Each run checks that anonymous smart-HTTP advertisement is refused with 401,
then verifies authenticated refs, reads the expected Git object and checks its
Git content hash, and optionally verifies the LFS digest. It follows no
redirects. Each request has a ten-second deadline, the run has a thirty-second
request budget, and response reads are limited to 1 MiB. Use a small dedicated
canary: a large ref list or object deliberately fails this budget.

The single JSON `HostProbe/probe_result` contains `healthy` and per-check names,
outcomes, statuses when available, and durations. It omits URLs, refs, object
IDs, tokens, and response contents. Exit 0 means all configured checks passed;
exit 1 means a check failed; exit 2 emits a redacted `probe_failed` configuration
error. `status` indicates an unexpected HTTP status; `content` indicates a
verified response disagreed with the known canary; `unavailable` covers transport,
stream, size-budget, or response-decoding failures. Keep configuration identity
in the monitor's protected inventory so responders can locate the target.

Schedule at an interval appropriate to the intended deployment, alert on
consecutive failures, and test the alert receiver during staging qualification.
Check monitor credentials and routing first, then correlate host storage,
snapshot, request, and maintenance signals. Never automatically rewrite the
canary or restart the host to silence this probe. A passing result establishes
these read/authentication paths for the canary only; it does not prove write
capacity, all repositories, backup integrity, or every smart-HTTP transfer.
Use Node's local write/readiness probe and the release capacity rehearsals for
their respective storage boundaries. No monitoring schedule is installed by
this tool.

## Cloudflare request signals

The application emits `response_ready` events with `component: CloudflareEdge`
for requests reaching the Worker script and `component: CloudflareHost` for
requests reaching the Durable Object handler. Each event contains numeric
`status` and `durationMs` fields. The status comes from the actual Web Response
when Alchemy wraps one; the wrapper's default status must not turn a 401 or 503
into a successful observation. Duration measures handler entry until the response
is ready. It excludes subsequent streaming-body delivery and platform queueing
before the handler starts. Assets served directly by the platform do not enter
this instrumentation.

Use edge events for client-visible status rates and response-ready latency;
use DO events to investigate the authoritative handler. A routed request can
produce both events, so do not sum them as independent requests. Count 401/403
for authentication/authorization refusals, 413 for request-budget refusals,
and 5xx for availability failures. A failing request whose platform signal is
aborted becomes 499 and does not emit a server-failure alarm. These events have
fixed fields and omit URLs, repository names, headers, bodies, credentials,
object/ref identifiers, and raw exception causes.

An unexpected failure before response creation produces a redacted
`request_failed` event and a 500 response with `Cache-Control: no-store`.
Inspect storage/service availability and the more specific provisioning,
snapshot, authorization, LFS, or maintenance signals before retrying. The edge
previously logged the raw DO failure cause; that path now uses the same fixed
failure record as the host. Responses already being streamed cannot be replaced
with a new 500 by this boundary.

The deployment definition explicitly enables persisted application logs with
head sampling set to 1, disables automatic invocation logs and traces, and
requests query-string redaction. This avoids relying on the pinned provider's
default invocation logging. The local actual-entry verifier checks that both
edge and DO events are emitted and that malformed provisioning payloads are not
echoed. Unit tests verify raw-response status, elapsed handler time, cancellation,
and failure redaction. These are configuration and local-runtime checks; verify
retention, sampling, access controls, alert delivery, and platform-added metadata
in the target account before releasing. Request events are operational telemetry,
not a durable audit ledger or proof that the client received or persisted the body.

Streamed responses also emit `response_body_complete`, `response_body_failed`,
or `response_body_cancelled` at each instrumented boundary, with numeric `bytes`
and duration from handler entry. This covers both raw Web Responses and Effect
HTTP streaming bodies, including DO responses forwarded by the edge. `bytes`
counts source bytes read for forwarding; completion means the local source was
exhausted, not that the client durably received them. Buffered responses and
responses with no body have no separate stream event. A body failure keeps the
status already sent, emits an error-level signal, and terminates the stream
with a fixed storage error instead of the underlying private exception.
Cancellation is informational. Alert on body failures independently of response
status, and require clients to verify the transfer before treating it as complete.
The wrapper retains backpressure and cancellation, including cancellation before
the first read; it does not buffer the response or read ahead of demand.

## Maintenance failure signals

The shared maintenance implementation emits error-level Effect logs with
`component: git.Maintenance`. Both hosts and the CLI use this implementation.
`event: maintenance_failed` with `operation: fsck` or `gc` means the operation
returned an error before producing its complete report. The signal preserves
that error for the caller and contains no raw cause, repository name, ref,
object ID, path, credential, or request payload. Failed collection can have
already completed some deletions or pack publication; the signal does not
promise rollback.

`event: integrity_failed` with `operation: fsck` means the completed check found
object problems or dangling refs. Its only variable fields are the numeric
`problems` and `danglingRefs` counts. The authenticated API still returns its
full report with `ok: false`, potentially with HTTP 200, so alert on this signal
as well as HTTP error rates. Healthy checks and successful retries do not emit
these failure signals. These events cover execution of the maintenance core;
authentication refusal and payload validation use the request-level signals.

For `maintenance_failed`, preserve storage and inspect the operation's private
report, storage permissions, free bytes/inodes, and filesystem or R2 errors.
Restore storage availability before running a read-only integrity check and
retrying collection. For `integrity_failed`, stop collection and other destructive
maintenance, preserve a complete copy, and investigate the reported refs and
objects against a verified backup. Do not remove damaged objects just to make
the report disappear: that can destroy the evidence needed to recover history.
Retain process/instance identity in the log collector to locate the affected
host without adding repository secrets to metric labels.

## Node request failures

LFS emits an error-level `Lfs/storage_failure` signal with operation `batch`,
`download`, or `upload` when the object store fails. The signal omits object
identifiers, repository names, paths, URLs, and underlying exception text.
Batch requests retain the LFS protocol's HTTP 200 envelope and report code 500
on each failed object, without an upload/download action. Alert on the signal
as well as HTTP status counters: batch failures are not HTTP 5xx responses.
Download failures before response headers return 500; a failure while streaming
aborts the body and logs the same signal. Upload byte-budget refusals remain 413
and do not emit a storage-outage signal.

For these signals, inspect the repository's LFS storage permissions, mount and
object/shard file types, free space, and the backing filesystem or R2 service.
An actual missing object remains 404; lookup errors do not establish absence.
Repair the storage cause before retrying. Verify the affected object's digest
after a failed upload because post-publication sync failures can leave complete
bytes visible. Do not delete or re-upload an existing object merely because a
batch lookup failed. Local HTTP tests exercise invalid shard/object paths and
successful service recovery after repair; production R2 outages still require
staging validation.

Unexpected host exceptions emit a JSON stderr record with `component: NodeHost`,
`operation: request`, and `event: request_failed`. `responseStarted` distinguishes
an error before headers from an interrupted response already in progress. The
former returns a non-cacheable generic HTTP 500; the latter closes the transport.
Raw causes, request URLs, headers, and bodies are omitted from this signal.
Client disconnects and request cancellation do not emit this failure signal.

Alert on repeated failures and check storage availability, permissions, and UI
asset installation using restricted operator access. A failed response does not
prove that a mutation was unapplied: inspect authoritative refs before retrying a
write. This signal covers unexpected failures escaping the host handler, not
all API error responses, and complements the request metrics and readiness probe.

## Authorization storage failures

Both hosts log `event: authorization_unavailable`, `operation: authenticate`,
and `component: NodeHost` or `CloudflareHost` when the authorization guard or
its storage layers fail. The request is refused with a non-cacheable 503 and
`authentication unavailable`; an outage is not treated as an empty membership
list or an ordinary credential denial. The signal omits raw causes, storage
paths, credentials, request bodies, and repository names.

Investigate storage availability and repository metadata integrity through
restricted operator access. Do not enable anonymous writes to work around this
failure. Restore storage access, preserve corrupt metadata for recovery, and
retry authorization. Ordinary 401/403 responses do not emit this outage signal;
Node request metrics count those separately. This signal covers typed guard
failures, not every possible failure in downstream handlers or platform startup.

## Cloudflare snapshot failures

Redaction has two steps: publishing a signed tombstone hides the event from hub
projections; authenticated `POST /:repo/gc` with `{"repack":true}` applies payload
removal to the remote object store. Until collection, raw transfers can still
include the stored bytes. Collection preserves bytes also needed by source
history or other protected roots, so check its report and the removal outcome.
Existing clones and backups are not erased. Stock Git rejects an explicit fetch
of collected hub history whose trees name the intentionally missing payload;
use the hub-aware client for that history. Ordinary source branches remain Git
compatible. Treat the tombstone response alone as insufficient evidence that
sensitive bytes have stopped being served.

The anonymous clone fast path uses an R2 snapshot of refs and read policy.
Before a mutation, the Durable Object removes the old snapshot while holding
the mutation/publication gate. If removal fails, the request returns `503`
without running the mutation. It logs `snapshot invalidation failed; mutation
refused`. Fix R2 access or availability and retry the request.

Publication runs after the handler settles, including on cancellation. A failed
publication logs `snapshot publication failed; authoritative reads required`;
the old public snapshot has already been removed. Reads fall back to the
Durable Object and an accepted source mutation remains accepted. Do not blindly
replay a write because its derived snapshot failed. Check authoritative refs,
repair storage access, and let a subsequent request publish the current view.
Journal retention failures are logged separately and now stop publication before
adding an entry or advancing the sequence. The invalidated snapshot stays absent
and reads use the Durable Object; a later attempt retries the same retirement,
including after restart. The 256-entry retention window cannot grow merely
because deletion keeps failing. A successful deletion followed by a failed write
can temporarily shorten the retained window. Existing excess entries left by
older writers require reviewed offline reconciliation; this change does not
discover or automatically purge them.

The stored journal sequence must be a nonnegative integer below 9,999,999,999
before publication advances it. This preserves the ten-digit key ordering and
refuses corrupt/overflowing counters without writing a journal or latest snapshot.
A sequence refusal emits the generic redacted publication-failure signal. Stop
writers and reconcile the counter against coordinated journal/storage evidence;
do not reset it to zero or guess from a partially restored bucket. These entry
and counter bounds alone do not establish a byte quota or an account-wide
storage guarantee; the separate snapshot byte limit is described below. Recovery of a snapshot is not a replacement for backing up
repository objects and administrative state.

`GIT_CF_SNAPSHOT_BYTES` bounds each latest snapshot and journal object written
by the publisher and read by the edge/publisher (default 16 MiB; positive integer,
maximum 64 MiB). Both host initializers resolve the setting for deployment
binding discovery; redeploy to change it. R2 object size is checked before
allocation. The reader allocates only that validated size and checks streamed
chunks against it, cancels refused bodies, and rejects truncation. Edge read
failures log the existing redacted `SnapshotReader/read` signal and fall back
to the Durable Object.

Over-limit writes fail before R2 replacement. As with other publication failures,
an accepted Git mutation remains accepted and the public snapshot stays absent.
An oversized existing latest view also prevents the writer from reading the
previous snapshot during invalidation, so that mutation is refused. Preserve
the object for inspection, then remove only the derived latest snapshot through
an approved operational path (or raise the limit and redeploy); do not delete
repository data or reset the journal counter. Removal remains available even
when read/write budgets are exceeded. Existing journal objects are not scanned
or automatically purged.

Together with the retention bound, new writers retain at most 256 journal
objects plus one latest snapshot per repository, each within this byte limit;
preexisting excess objects and external writers remain outside that claim.
Snapshot encoding still materializes derived ref metadata in memory before
checking the encoded write size. Git ref/request budgets remain required, and
these limits do not qualify supported repository sizes or account-wide capacity.

Snapshot failure logs carry `component: SnapshotPublisher` and `operation` equal
to `invalidate`, `publish`, or `retention`. Alert on these fields; invalidation
means a refused mutation, publication means degraded reads, and retention means
journal cleanup needs attention. These messages omit repository names and raw
storage errors, which can contain private paths or credentials. Use restricted
request tracing to investigate the affected repository. These failure logs do not supply host readiness checks.

Valid snapshot loads also emit an informational `SnapshotReader` record with
`operation: observed`, `ageState` and `ageMs`. Age is the elapsed wall-clock
time since capture. `known` carries a nonnegative millisecond value; `future`
(clock skew) and `invalid` (malformed/noncanonical timestamp) carry `null`.
These observations do not change snapshot eligibility or authorization and do
not turn an old unchanged repository into an outage. A decoded private snapshot
is observed too; observation does not prove that the edge served it.

Only successful latest-snapshot writes emit `SnapshotPublisher/published`,
with `durationMs`, `changed` and the same age fields. Duration covers capture,
journal retirement/publication, sequence persistence and latest-view writing;
it excludes permit wait and the Git handler. `changed` compares the published
view, excluding its timestamp; unchanged views are still republished after
invalidation. Retention/write failures do not emit a successful-publication
record. Negative wall-clock duration is clamped to zero.

These informational records omit repository names, refs, object IDs, authority
flags, raw timestamps and storage causes. Configure collection/sampling and
retention for their per-load/per-publication volume, then monitor age
distributions, clock anomalies and publication latency alongside failures and
the private canary. Age alone cannot distinguish an idle repository from missed
updates. Metrics are observations of invocations that reached logging; they do
not cover runtime termination before a signal is emitted or replace a scheduled
external availability probe.

The Worker fast path emits `component: SnapshotReader` with `operation: read`
when R2 lookup/body loading fails, `decode` when stored snapshot bytes are
invalid, or `serve` when constructing the snapshot response fails. These warnings
mean the request is falling back to the authoritative Durable Object; missing
snapshots and snapshots that intentionally restrict reads are normal and quiet.
Raw storage causes, snapshot contents, and repository names are omitted. Repair
R2 access or investigate invalid snapshot storage, then let the writer publish a
current view. These signals apply before returning a Response; a subsequent
streaming-body failure cannot be transparently replayed after bytes reach the
client. Snapshot-age metrics and deployed alert verification remain outstanding.

## Background delivery and reconciliation

Each attempted webhook or remote forward emits a structured `delivery_result`
with `component: Webhooks` or `Sending`, its registry identity, and numeric
`queueMs`/`durationMs`. For post-receive hooks, queue time starts before the host
background scheduler receives the job and includes registry lookup and waiting
for the batch's concurrency slot. Direct calls to webhook delivery measure from
that call's execution unless supplied the hook's enqueue time. Duration covers
the target attempt, including webhook retry backoff and timeouts. Their sum is
the observed enqueue-to-result delay for that target.

Webhook outcomes are `accepted`, `failed`, or `cancelled`. Replication also
reports `refused` when a remote returns a negative ref verdict and `skipped` when
there is nothing eligible to forward. A finished replication attempt with refused
refs must not be counted as a synchronized mirror. An accepted webhook means its
HTTP receiver accepted the request, not that downstream processing completed.
These timing records contain no destination URL, credentials, signed headers,
request payload, peer response, or ref values. Existing reconciliation warnings
retain registry identities and, for rejected pushes, affected ref names.

Alert on growing queue/delivery durations and any failed, refused, or cancelled
delivery. Check receiver availability, credentials, egress policy, and remote
divergence; reconcile using the steps below. Increasing concurrency cannot
repair an authorization failure or divergent history. The timers are in memory:
a process killed before the attempt finishes emits no terminal result, and a
missing event cannot establish that a destination is current. Continue periodic
reconciliation under the best-effort contract; these events do not provide a
durable queue depth or a remote's application lag.

Webhooks and automatic remote forwarding are **best effort**, not backups.
Accepted source writes do not wait for receiver success. Webhooks make at most
four attempts by default, with a ten-second timeout per attempt; permanent 4xx
responses stop retries. Automatic Git forwarding makes one attempt and never
forces a divergent remote. Neither mechanism has a persistent delivery queue.
A process/Worker restart or exhausted retries can permanently lose an event.
Events can arrive twice or out of order, and no stable delivery ID is promised.
Receivers must reconcile current authoritative state rather than assuming that
every before/after event arrived in sequence.

Collect structured host warning logs and alert on `webhook delivery failed`,
`replication failed`, `replication refused`, and registry-unavailable messages.
The warnings identify a subscriber or remote and require reconciliation; they
omit transport error objects, credential-bearing URLs, and peer response text.
Check the named registry entry through an authenticated administrator session,
then investigate receiver availability, expired grants, credentials, and ref
divergence. Do not enable HTTP header/body logging to diagnose credentials.
On restart, reconcile every receiver even if there was no warning: a killed
process cannot log its abandoned work. These logs are not a durable delivery ledger.

For a downstream that is exclusively a read-only copy of source branches and
tags, stop downstream writers and fetch the current state into its bare replica:

```sh
git -C /srv/replica.git fetch --prune https://source.example/project \
  '+refs/heads/*:refs/heads/*' '+refs/tags/*:refs/tags/*'
git -C /srv/replica.git fsck --strict
```

Configure the source credential helper first. The `+` permits replacement and
`--prune` removes refs absent at source: use this only for a replica whose refs
the source owns. Compare `git ls-remote` on both ends after recovery. For a
shared or writable destination, fetch into a separate recovery repository,
review divergences, and push reviewed updates without force. Delete only a ref
confirmed absent at source, with an explicit lease for the destination OID you
reviewed; never blanket-prune a shared destination.

A webhook receiver should enumerate the source's current refs and reconcile its
own previously known refs, including deletions. Rebuild caches from those refs;
an event-triggered external action that cannot be reconstructed needs its own
durable workflow and deduplication. There is no server endpoint that guarantees
replay of lost webhooks. Branch/tag reconciliation does not restore LFS,
registries, identity state, or hub/session/trace/task refs. Include only explicitly
authorized additional namespaces in a separate reconciliation plan; automatic
forwarding excludes sensitive session, trace, and task refs by default.

## Backup inventory and consistency

Until a complete restore rehearsal passes, treat backup support as a release
gate, not a guarantee supplied by ref snapshots or automatic forwarding.
Choose recovery-point and recovery-time objectives for the installation and
measure them with representative data. Store backups encrypted, restrict access
to the recovery operators, and retain an independently protected copy. Backup
read access can expose repository contents and administrative signing keys.

A complete Node capture includes the entire storage root, including hidden
files: loose objects and packs, all refs/reflogs and shallow state, LFS content,
trust and hub records, remote and webhook registries, `.registry.json`,
`.tokens.json`, `.forks.json`, and any SQLite databases with their required
sidecars. Include `.git-plus-ref-transaction.json` and all reservation files if
present; they are recovery evidence, not disposable backup noise.
Inventory alternates and fork dependencies before capture: a tar of
one child repository does not contain the objects it borrows. Include every
external alternate root or explicitly materialize borrowed objects first.
Back up service configuration, authorized hosts, operator-managed keys, identity
pins, and version/artifact manifests separately with equivalent protection.

For a filesystem backup, stop all host and CLI writers, close SQLite connections,
and verify that no maintenance or wake process can mutate the root. Take the
archive or filesystem snapshot while writers remain stopped; preserve file
modes, symlinks, and relative layout, and write the archive outside the source
root. Do not remove locks during capture. A live recursive copy is not a
consistent backup. Record the source revision, storage layout, capture time,
and any external alternate paths beside the protected backup.

Restore into an isolated root while the public host remains stopped. Verify
the archive's integrity before extraction, restore ownership/private modes,
and confirm that every alternate resolves inside the restored storage set,
not the live installation. Run stock `git fsck --strict` for every repository
and check acknowledged ref tips. Test authorized and denied reads/writes,
revoked credentials, LFS content hashes/downloads, representative hub records,
and parent/child fork reads before opening ingress. Retain the old root until
recovery has been accepted. Resolve stale locks only under the procedure above.

Restore into an empty destination rather than overlaying another installation.
For a changed root, inspect every `objects/info/alternates` entry: absolute paths
can still point at the original installation. Resolve relative entries against
the child repository's `objects` directory. Map each inventoried source object
directory to its restored location, then rewrite that entry explicitly; do not
perform a broad string replacement or assume external alternates were captured.
For sibling repositories, a relative entry such as `../../parent/objects` keeps
the relationship portable. Verify the resolved path and run Git's integrity
checks with the original root unavailable. Include linked-worktree paths in the
relocation inventory when the installation uses them.

Capture the common Git directory and every linked checkout together, including
each checkout's files and its private index under `worktrees/`. A repository-only
backup does not preserve staged, unstaged, or untracked checkout changes. After
restoring both the main repository and its linked checkouts, run
`git -C /restored/main worktree repair /restored/linked /restored/detached`, using
only paths from that inventory. Git repairs the checkout `.git` pointers and
the common directory's back-pointers. Do not prune worktrees to remove the stale
paths: that can discard their private administrative state. Check
`git worktree list --porcelain`, each checkout's HEAD and status, and
`git fsck --strict` before resuming writers. Preserve intentional worktree locks
and their reasons; they are distinct from stale transaction lock files.

Preserve symlink text during capture and restore; converting a relative key or
configuration link to an absolute source path can make a restored installation
depend on its lost original. The local Node rehearsal uses `fs.cp` with
`dereference: false` and `verbatimSymlinks: true`, preserves timestamps and modes,
and captures operator keys beside the data root while the host is stopped.

Run the functional rehearsal with:

```sh
node_modules/.bin/vp test run --project interop src/server/Backup.recovery.interop.test.ts
```

It removes the original fixture before verifying the fresh root. Checks cover
stock-Git history/integrity, private authenticated clone, denied anonymous access
and read-token push, successful write-token push, LFS bytes, accepted hub records,
a newly signed action using the restored operator key, token revocation, private
registry/key modes, preserved symlinks, and fork dependency/deletion protection.
The linked-worktree rehearsal removes the original root, repairs an attached
and a locked detached checkout, compares the private index byte-for-byte, and
verifies staged/unstaged/untracked content and Git+ status. A subsequent commit
advances only the attached checkout's branch, leaving the main and detached
HEADs unchanged.
The fixture subscriber is disabled after its restored registry is verified so
the rehearsal does not contact an external receiver. This is functional evidence
for a small Node installation, not a production-scale recovery-time measurement
or a Cloudflare restore rehearsal.

Reconcile revocations, membership changes, and redaction tombstones made since
the backup before exposing the restored instance. An old backup can re-enable
old authority or restore removed payloads. Physical erasure does not propagate
to backups: retention, legal deletion, and any required reapplication of
redaction need an explicit backup policy. Restrict recovery access while those
changes are applied.

Cloudflare recovery must capture the R2 object/pack and LFS namespace together
with per-repository Durable Object SQLite/key-value state, administrative
registries, identity/ref state, and deployment bindings. An R2 refs journal
alone omits this state. Quiesce mutation and background work for a coordinated
capture, or use a separately verified provider snapshot protocol. Restoring
only R2 or only a Durable Object is not a complete installation restore. The
automated capture/import mechanism and fresh-account restore rehearsal remain
open production gates.

### Cloudflare recovery inventory

Run `node tools/inventory-cloudflare-objects.ts ACCOUNT_ID NAMESPACE_ID NEW_DIRECTORY`
with `CLOUDFLARE_API_TOKEN` supplied through the operator's environment. Use a
scoped token permitted to read the selected namespace. The tool uses the
[account-level object listing API](https://developers.cloudflare.com/api/resources/durable_objects/subresources/namespaces/subresources/objects/methods/list/),
which returns object IDs and storage-presence flags, not repository names or
contents. It performs read-only requests against Cloudflare's fixed API origin,
refuses redirects, bounds each response to 2 MiB and each request to 30 seconds,
and follows at most 1,000 pages of 1,000 entries, refusing repeated cursors and
repeated object IDs within or across pages. A disposable SQLite uniqueness index
uses disk storage and a bounded cache rather than retaining every ID in memory.
It is removed before successful completion.

The new directory is private (`0700`); `objects.ndjson` and the completion report
`inventory.json` are private (`0600`). Version 2 includes `inventorySha256`: SHA-256
over the UTF-8 JSON array `["chr33s-git/namespace-inventory/v1", account, namespace]`,
a newline, and the exact `objects.ndjson` bytes. This binds even an empty inventory
to its account and namespace. Data and completion files, the inventory directory
and its parent are flushed before success. An existing directory is never replaced.
Handled failures remove this invocation's partial directory. After abrupt process
termination, a directory without a completed `inventory.json` is incomplete and
must not be used as recovery evidence. Run the token-free verifier before using
its digest in signed backup resource evidence:

```sh
node tools/inventory-cloudflare-objects.ts --verify ACCOUNT_ID NAMESPACE_ID DIRECTORY
```

Verification streams records, checks distinct IDs, exact digest, counts and expected
account/namespace, and refuses symlinked/non-regular files, invalid UTF-8, missing
terminal newlines, oversized records and legacy manifests without a digest. It
preserves the input files. This is internal consistency verification, not independent
proof of API origin or complete enumeration: protect the inventory and bind its
digest into the signed backup inventory. See the
[integrity evidence](measurements/cloudflare-inventory-integrity-2026-10-30.json).
Counts describe returned entries; concurrent
mutation can change enumeration, so reconcile the inventory under a verified
mutation barrier. Inventory files contain no API token. Treat object IDs as
administrative data and retain them within the protected backup inventory.

The inventory tool does not export or restore stored data.
`tools/copy-r2-backup.ts` supplies a separate internal R2 copy primitive, exercised
with two native local buckets. It streams each object body into an initially
empty destination, preserves HTTP/custom metadata and storage class, forwards an
available SHA-256 or MD5 checksum, and verifies copied sizes. After each PUT it
rereads the captured source version and the written destination version, computes
streaming SHA-256 digests, compares the bytes and validates HTTP/custom metadata
and storage class. Mismatches fail without including private metadata values in
the error. Conditional reads and source-version checks reject changed objects;
conditional writes refuse concurrent destination replacement. The implementation follows R2's
[conditional operations](https://developers.cloudflare.com/r2/api/workers/workers-api-reference/).
An interrupted/failed copy leaves partial destination data for inspection and
cannot be retried over that nonempty bucket. It never deletes source or partial
destination data. Object versions and upload timestamps are assigned anew by R2.

The returned `contentInventorySha256` hashes one JSON tuple plus newline per
object in listing order: `[key, byteLength, contentSha256]`. It commits to content
and names, not metadata or a quiescence claim. Version 2 reports additionally
return `metadataInventorySha256`, hashing newline-terminated JSON tuples of the
key, fixed-order HTTP metadata fields (content type/language/disposition/encoding,
cache control and ISO cache expiry), sorted custom-metadata entries, and storage
class. Version 1 reports lack this digest and cannot satisfy the signed final
inventory. Metadata-only changes alter this digest even when object bytes do not.
A native local rehearsal copies
3 MiB across paginated buckets and restores the copy into a third empty bucket
with a different page size; their content-inventory digests agree. Fault injection
checks same-size corruption, wrong metadata and replacement before verification.
Read-back verification requires two source reads and one destination read per
object in addition to the PUT; account bandwidth/operation budgets must include
that cost. A changed source version or failed verification leaves the destination
partial and unqualified.

Copy and restore first scan the complete source listing and reject invalid sizes
or any object above the single-PUT ceiling of 5 GiB before reading bodies or
writing destination objects. This catches an unsupported object even on a later
page, before earlier objects are copied. Sizes are checked again during transfer
and against the fetched source metadata. The preflight adds one full metadata
listing pass; the returned `pages` count describes the copy walk, not the total
number of listing requests. Writers must remain isolated across both passes.
The limit follows Cloudflare's [upload limits](https://developers.cloudflare.com/r2/objects/upload-objects/).
Metadata-fault tests verify refusal of oversized, negative, fractional, non-finite
and unsafe-integer sizes before any body read or destination write; they do not
qualify an actual 5 GiB transfer. See
[R2 size preflight evidence](measurements/r2-size-preflight-2026-11-01.json).

This is a component, not an operator command or a complete backup. Its caller
must quiesce **all** source/destination writers throughout enumeration and copy;
per-object conditions do not establish an installation-wide snapshot. The
single-object PUT path and invocation/page budgets still need production-scale
qualification and multipart transfer/resume for objects too large for one PUT.
The Workers API's [multipart completion method](https://developers.cloudflare.com/r2/api/workers/workers-api-reference/#r2multipartupload-definition)
does not expose the conditional-create option used by this component's PUTs.
Any multipart implementation must preserve the no-overwrite contract; replacing
conditional PUT with unconditional completion is insufficient. No production
route or privileged management worker invokes this component yet.

For recovery from an authenticated archive, `inventoryR2Backup` computes a
streamed read-only inventory of the bucket's current contents and metadata.
`restoreR2Backup` accepts the expected R2 evidence from the verified signed
namespace completion. It checks the entire archive bucket against that evidence
before any destination write, then copies missing keys with conditional creation.
Existing keys are read back and compared with the archive; matching keys are left
unchanged, while differing content, metadata, size or storage class fails without
overwrite. Version checks detect replacement during verification. A full final
destination scan must match the expected inventory, so unrelated keys also prevent
successful completion and are never silently deleted. Preflight and final scans add
full source/destination reads to the per-key comparison work; include those reads
in bandwidth and operation budgets.

This allows retry after partial copying or a lost PUT acknowledgement while
preserving already copied object versions. Source and destination writers must
remain isolated throughout; the primitive does not provide that barrier or release
any Durable Object fence. The ordinary `copyR2Backup` capture path still refuses
non-empty destinations. Native tests cover lost acknowledgement/resume, completed
retry without writes, mismatched existing content/metadata, concurrent replacement,
extra keys and changed source metadata before writes. The namespace recovery
rehearsal uses this expected-inventory restore path before object recovery. See
[R2 restore evidence](measurements/r2-restore-2026-10-30.json).

`tools/cloudflare-namespace-restore.ts` combines verified namespace archives,
R2 recovery and object import with a private local SQLite journal. Supply the
trusted source scope/signing fingerprint, archive directory and bucket, destination
account/namespace/bucket identity, privileged target client, and an NDJSON mapping
file with `sourceId` and `targetId` in saved discovery order. Each source and target
must occur exactly once. The caller must ensure the actual client and bucket
bindings match the declared destination identity; labels cannot authenticate
bindings. Source and destination namespaces must differ.

The journal directory must be private (0700) and its `restore.sqlite` file private
(0600). Mapping initialization commits before any remote operation. The journal
binds the complete signed archive evidence, trusted signer, target identity and
mapping; retries that change them are refused. SQLite exclusive locking prevents
concurrent coordinators using that journal. Use a local filesystem supporting
SQLite locking and durable sync, preserve the journal for the entire recovery,
and never create a second journal to bypass an in-progress restore.

Every target is held and bound to its object import plan before R2 recovery starts.
Object completion receipts are committed individually. On failure, retain all
fences and retry with the same inputs and journal; existing object checkpoints
and R2 versions are verified before continuing. A successful result marks the
journal `ready`, meaning imports completed, **not** permission to serve requests.
A retry resets this phase to `restoring` before remote operations and returns it
to `ready` only after successful verification. Perform application and authority
checks while all targets remain fenced. This component never retires checkpoints
or releases fences. Explicit retirement/release uses the separate coordinator
described below. Installation-wide isolation, full-coordinator process-kill
qualification and a deployed recovery rehearsal remain required. Local tests exercise concurrent journal access, failed holds, a lost
successful Apply acknowledgement, changed mappings/identity, eviction and retries;
they do not simulate power loss. See
[namespace restore evidence](measurements/namespace-restore-2026-10-30.json).

The shared journal opener in `tools/cloudflare-restore-journal.ts` also has
process-death tests: a child process receives SIGKILL before/after mapping commit,
before/after receipt commit and after readiness commit. A new connection recovers
the expected committed state, rolls back uncommitted changes, passes SQLite
integrity checking and accepts subsequent writes without manual lock removal.
This qualifies local journal recovery on the tested filesystem; it does not
simulate power loss, network filesystems or termination of the full coordinator
during remote operations. Private-path tests also reject symlinked paths and
group/world-accessible journal files/directories. See
[journal crash evidence](measurements/restore-journal-crash-2026-10-30.json).

`tools/cloudflare-namespace-release.ts` supplies `releaseNamespaceArchive` for
the explicit post-verification step. Keep installation-wide traffic and all other
writers isolated throughout release, including retries after some object fences
have already been removed. Supply the same archive, source scope, trusted signer,
target identity, mapping, journal and target bucket, plus `verificationSha256`:
the SHA-256 of a retained operator report documenting successful application,
authorization and recovery-policy checks. The component binds this digest but
cannot determine whether the report's checks were adequate or actually performed.
Never invoke release merely because import returned `ready`.

Before retirement, release re-verifies the archive, every mapped complete receipt,
the destination R2 inventory, and every target's current receipt. A single SQLite
transaction binds the verification digest and creates per-object release progress
while advancing the journal to `releasing`. No receipt is retired before that
intent is durable. It then retires every checkpoint, persisting each acknowledgement,
before releasing any fence. A lost successful retirement acknowledgement is retried
while its fence remains held. Once all retirements are durable, release advances
each target from `retired` to `released`; a lost release acknowledgement is retried
using the absent-fence postcondition. No release retry re-holds a target or imports
data. Import refuses journals already in `releasing` or `released`.

Retry with exactly the same inputs and verification digest. Preserve the archive,
mapping, journal and verification report. The coordinator checks current R2 on
each unfinished attempt; unexpected changes require investigation while global
isolation remains in place. After all acknowledgements are recorded, the journal
becomes `released`. Resume validates that retirements and releases each advanced
in discovery order, and rejects mixed released/pending progress or advancement
after a gap before any RPC. A terminal retry verifies the saved evidence and returns
historical completion without contacting target objects or their bucket, which
may now contain legitimate new writes. Restore/release share the same exclusive
journal lock. They do not coordinate across separate journal directories.

Native tests cover refusal of incomplete import, changed mapping/target/report,
extra R2 data and mismatched remote receipts; lost successful retirement and
release acknowledgements; eviction between retries; all-retirements-before-release
ordering; and terminal retries without RPCs. The rehearsal checks recovered
repository/LFS and authorization/registry state while both targets are still held.
These are local component tests, not a production operator command, a full
coordinator process-death test or a deployed recovery qualification. See
[namespace release evidence](measurements/namespace-release-2026-10-30.json).

A separate stock-Git interoperability rehearsal restores both the application
and authorization objects, evicts them, and compares recovered application/LFS
and registry/token state before release. It streams the recovered R2 pack/index
pair into a temporary bare repository. R2 loose objects use raw bytes plus type
metadata, so the rehearsal exports those too and has Git independently frame and
hash them; copying only packs would omit the retained empty tree. Git 2.56.0 then
runs `verify-pack`, strict full `fsck`, ref/HEAD and shallow checks, exact object
content comparisons, and a fresh clone. Both target fences remain held until
these checks finish; the release journal binds the verification-report digest.
This is a small local fixture with a shallow root commit, not complete deployed
hub/fork or production-scale recovery. See
[stock-Git recovery evidence](measurements/namespace-stock-git-2026-10-30.json).

The expanded fixture also seeds signed repository genesis and membership, a pull
request with a comment, and a session with production links, a note and usage.
After recovery and target eviction, it compares production trust, pull-request
and session projections with the source; the trust and pull-request projections
must reject no events. Stock Git explicitly fetches `refs/meta/*` and
`refs/hub/*` into the fresh clone and verifies the recovered ref tips. This
explicit fetch matters because an ordinary source clone is not a complete
operational backup of session and hub history. These checks still finish while
both targets are fenced, and the release report records successful hub comparison.
See [signed hub recovery evidence](measurements/hub-namespace-recovery-2026-10-30.json).
This does not qualify every redaction/revocation/concurrent-history scenario or
a deployed recovery.

The fixture's main branch now contains Unicode text, nested binary data, an
executable script and a symbolic link. The stock-Git clone performs a checkout
and checks exact file bytes, the symlink target, executable permissions and a
clean working tree before release. This catches recovery failures that an
empty-tree clone could not expose. See
[checkout recovery evidence](measurements/checkout-namespace-recovery-2026-11-01.json).

Additional journal process-death checks kill a child before and after each release
intent, retirement, per-target release and completion commit. Verification evidence
and release intent recover together; uncommitted progress is rolled back while
earlier receipts remain intact. Native recovery tests additionally alter progress
to impossible orderings and confirm rejection before RPC. The process-death tests
exercise the real journal opener and SQLite transactions, but do not kill the full
remote coordinator. See [release progress evidence](measurements/release-progress-2026-10-30.json).

`tools/cloudflare-backup-codec.ts` provides versioned transport frames for the
remaining SQL/KV component. It uses pinned
[`devalue` stringify/parse](https://github.com/sveltejs/devalue) rather than
plain JSON for stored values, with `schemaVersion: 1`, codec `devalue@6.0.3`,
and a 4 MiB UTF-8 frame ceiling. Decoding requires the caller's expected schema;
unknown versions, malformed payloads and unsupported values fail with diagnostics
that omit private contents. No generated JavaScript is evaluated. Serializer
versions are not assumed compatible: a dependency upgrade requires an explicit
format migration or retention of a decoder for existing backups. Encoding
materializes one frame, so callers must bound their SQL/KV batches before calling
it; this is not permission to buffer an entire object walk.

`tools/cloudflare-backup-auth.ts` signs bounded frames with the existing Ed25519
SSHSIG implementation under the distinct `chr33s-git/backup/v1` namespace.
The signed preimage binds protocol version, installation, snapshot, object,
frame kind, sequence number and the payload's SHA-256 digest. Verification
requires the expected coordinates and trusted signer fingerprint **from an
independent restore plan**, never from the packet being checked. It rejects
altered payloads/coordinates, untrusted signers and signatures from other
namespaces before returning payload bytes. Diagnostics omit frame contents.
The raw payload ceiling remains 4 MiB; the JSON wrapper has a bounded allowance
for escaping and signature overhead. Node 24.12/26 tests and independent
OpenSSH verification are recorded in the [authentication evidence](measurements/backup-authentication-2026-10-30.json).

This primitive does not encrypt data or configure key custody. A coordinator
must verify a frame before invoking any importer, maintain expected sequence
state and authenticate a final inventory before finalization. Individually valid
pages do not prove that an archive is complete, and replaying an authentic page
must never apply its mutations or advance inventory twice. The object session
may acknowledge an exact retry of its last committed packet.

`tools/cloudflare-backup-inventory.ts` maintains a constant-space SHA-256 chain
over authenticated frame positions, UTF-8 byte counts and payload digests. Its
signed final summary binds that ordered chain, frame/byte totals, the namespace
inventory digest, deployment digest, and verified R2 object/byte totals plus
content and metadata inventory digests. Verification requires independently
checked resource evidence. Missing, reordered, repeated or mixed frames and
changed resource evidence prevent completion. Capture and restore operate one
bounded frame at a time; the coordinator must durably commit inventory progress
with successful imports and keep the installation isolated until verification
and policy checks finish. The native SQL/KV/R2 rehearsal now authenticates every
application frame and verifies the final summary before removing restore markers;
a rejected R2 summary leaves those markers intact. This test coordinator is not
an exposed production administration service, and it does not establish that
namespace enumeration included every production object. The [inventory evidence](measurements/backup-inventory-2026-10-30.json)
records the Node 24.12/26 native/component checks and source hashes.

`tools/cloudflare-object-import.ts` provides a private, single-object SQL/KV
import session, superseding the earlier SQL-only session. The caller must
independently bind its immutable installation/snapshot/object and trusted signer
plan to the actual target Durable Object. Initialization requires fresh SQL and KV
storage except for a matching held backup fence. Authentication runs before the
transaction; imported data and the authenticated inventory checkpoint then commit
in the same native transaction. A checkpoint comparison prevents stale concurrent
work. A retry of the last committed packet returns its existing receipt without
advancing the inventory again; older packets are rejected. Acknowledgements wait
for `storage.sync()`, so a failed flush can be retried without repeating an already
committed page.

The session expects a SQL catalog, all SQL pages in catalog order, and then KV
pages in native UTF-8 key order. Empty SQL and KV stores still require their
catalog and terminal KV page. KV progress, data and inventory share a transaction;
reserved fence/checkpoint/legacy restore keys, absent values, replacement keys,
wrong snapshot/cursor and pages after completion are rejected. SQL indexes and
triggers remain deferred until the signed final inventory and independently
verified resource evidence pass. Finalization installs those schema objects and
commits a durable completion receipt atomically. It keeps the backup fence held:
one object's completion does not establish whole-installation recovery.

Native tests inject checkpoint-write faults after SQL and KV changes and verify
rollback of schema, data, progress and finalization. They also cover concurrent
retries, flush failure, actual eviction, empty stores and reserved-key attacks.
The production-service recovery fixture now imports SQL and typed KV through this
session, verifies its inventory against the R2 copy, evicts the target and checks
restored repository, quota, credential, snapshot, pack and LFS behavior. See the
[object session evidence](measurements/object-import-session-2026-10-30.json).
Whole-installation coordination and deployed qualification remain open. The standalone KV importer still requires an empty
target; only the object session permits its own control records alongside data.

The production repository Durable Object exposes `backup(request)` as a private
namespace-binding RPC through `src/host/CloudflareBackupAdmin.ts`. It is not routed
from the public Worker's HTTP interface. A caller holding this namespace binding
is treated as an installation operator; do not grant that binding to untrusted
Workers. Requests carry the actual target object ID, snapshot ID and a tagged
command. The target checks its own ID, serializes administrative calls, and
requires the matching held fence for every operation except `Hold` and an
already-completed `Release` retry.

Commands are `Hold`, `Catalog`, `SqlPage`, `KvPage`, `Start`, `Apply`, `Status`,
`Finish`, `Retire` and `Release`. `Start.plan.objectId` identifies the object in the
source archive; the request's outer `objectId` identifies the actual restore
target. The trusted coordinator owns this mapping and the signer fingerprint.
`Retire` requires the exact completed receipt, removes only that checkpoint and
flushes it while leaving the fence held. Repeating retirement after deletion is
safe. `Release` refuses current/legacy import checkpoints and unfinished SQL/KV
restore markers even when no fence exists. If the fence is already absent,
`Release` flushes storage and returns `released` without changing state; this
makes a lost successful acknowledgement retryable after eviction. It refuses a
malformed fence, an uncertain release in the current activation, or a fence held
for a different snapshot. The response establishes an unfenced postcondition,
not historical proof of which operation removed the fence. Snapshot identifiers
must be unique per operation, and the coordinator must retain durable retirement
and release progress. A coordinator must finish its whole-installation inventory
and policy checks before retiring receipts and releasing any target.

The native fixture checks wrong target/snapshot rejection, premature release,
incomplete or mismatched receipt rejection, retirement/release retry after eviction,
and refusal of a delayed release against a later snapshot's fence. See
[release retry evidence](measurements/fence-release-retry-2026-10-30.json).
The actual Worker/DO verifier also exercises repeated release and stale-snapshot
refusal through the Alchemy private RPC bridge, checking that HTTP admission stays
503 under the later fence. This full local rehearsal passes under Node 24.12 and
26; see [actual Worker release evidence](measurements/worker-release-retry-2026-10-30.json).
The actual Worker verifier additionally exercises this RPC through a private
cross-process namespace binding against an empty object: hold, capture,
authenticated SQL/KV import, completion, retirement and release. Its resource
digests are fixture placeholders, so that protocol check is not a backup or a
whole-installation recovery claim. Public snapshot reads, other installation
writers, namespace enumeration, archive custody and cross-object coordination
still require installation-wide isolation and a production coordinator. See
[administration evidence](measurements/backup-administration-2026-10-30.json).

`tools/cloudflare-object-archive.ts` supplies programmatic disk capture/restore
for this private RPC. The caller supplies a namespace-binding client, an immutable
source plan and independently verified resource evidence; capture additionally
requires the matching signing key, while restore requires the actual target ID.
It does not configure a remote operator connection or collect installation-wide
resource evidence itself.

Capture exclusively creates a new directory (mode `0700`) with sequential signed
page files (mode `0600`). It holds the source fence, captures the SQL catalog and
all SQL/KV pages, and signs their ordered inventory. Every file is flushed; the
`complete.json` record is written only after the page files and their directory
have been flushed, then the completion record and parent directory are flushed.
A failed capture preserves its partial directory and leaves the source fenced.
A failure during completion or its flush may leave a completion file behind; its
presence alone is not success. Restore always authenticates the full archive.
Existing destinations are never overwritten. Capture currently restarts into a
new directory after failure; it does not resume a partial source archive.

Restore checks the independently supplied source plan, authenticates every
bounded page and the final inventory/resource summary before contacting its
target. It reads one packet at a time, refuses symlinked packet/completion files,
and limits completion metadata to 64 KiB and the archive to one million frames.
It then holds the target fence and starts or resumes the durable object session.
A second authenticated pass compares the existing checkpoint with the archive's
exact prefix before applying any remaining page. A changed archive cannot extend
an unrelated checkpoint. Completion returns the durable receipt and leaves both
fence and receipt intact for installation-level verification and explicit
retirement/release.

These archives contain sensitive application data in plaintext. The signing key
is not stored in them. POSIX permission behavior is locally tested; encryption,
protected storage, retention and distribution remain operator responsibilities
under the backup policy above. Keep the archive directory immutable while
restoring. The native production-service fixture exercises disk capture, a lost
apply acknowledgement, eviction/resume, unchanged-prefix verification and final
repository/LFS recovery, alongside malformed/oversized/symlink/incomplete archive
rejection. See [archive evidence](measurements/object-archive-2026-10-30.json).
This remains one-object transport; namespace discovery, source/target mappings,
global isolation, R2/deployment capture and final installation inventory must be
coordinated before any production recovery claim.

Capture and restore require the exact `held` response before any catalog read or
import start. Restore also binds Start and Apply receipts to the requested plan,
checks each Apply packet digest and inventory, and accepts Finish only with a
complete receipt matching the plan, full archive inventory and signed-seal digest.
Namespace restore checks every Start receipt's plan before copying R2 data.
Unexpected responses stop the operation; existing fences and import checkpoints
remain available for inspection and retry. Native regressions cover these
response mismatches, including a wrong-source Start receipt that prevents R2
writes. See [receipt validation evidence](measurements/backup-receipts-2026-11-01.json).

`tools/cloudflare-namespace-archive.ts` coordinates capture for one verified DO
namespace and its R2 bucket. Supply the namespace-binding client, protected
inventory directory, matching signing key, source and fresh destination R2
buckets, installation/snapshot identity, and independently checked deployment
digest. The client binding must match the inventory's account and namespace.
The operator CLI below supplies these inputs from a private plan file.

`tools/cloudflare-backup-connection.ts` supplies a scoped remote connection for
these coordinators. Its configuration contains `account`, `namespace`, `worker`,
the live `bucket`, and a distinct `archiveBucket`; optional `jurisdiction` and
`archiveJurisdiction` select restricted R2 locations. Obtain these values from
independent deployment records. `connectCloudflareBackup` reads
`CLOUDFLARE_API_TOKEN` through Effect configuration, then fetches the Worker's
provider settings before opening a proxy. It requires exactly one repository
DO binding and one R2 binding, matching the expected namespace, `Repo` class,
Worker, live bucket and jurisdiction. Missing, ambiguous, cross-environment or
mismatched bindings fail before backup RPCs.

Opening the connection creates a temporary Cloudflare preview proxy through the
pinned runtime; it is not a read-only operation. Use an approved operator account
and token with the permissions needed for the provider metadata request and
remote service/R2 bindings. Keep the returned handles inside `Effect.scoped` so
the proxy and runtime close on success or failure. The return value contains
`client`, `bucket` and `archiveBucket`. Capture uses `bucket` as its source and
`archiveBucket` as its destination; restore uses them as its target and archive
respectively. The coordinators still require their independent trusted scope,
signer, mapping and journal inputs.

The production Worker's `backup` RPC forwards validated requests to its own
repository namespace by object ID. It is reachable through a service binding,
not an HTTP route. A service binding exposing this method grants administrative
backup authority; do not give it to untrusted callers or forward user-controlled
commands through it. The actual local Worker rehearsal exercises capture/import/
release through a service binding, rejects malformed IDs, and confirms an HTTP
request cannot release the held object. Freeze deployment changes as well as
writers during an operation: metadata validation is a preflight check, not a
global lock. See [connection evidence](measurements/backup-connection-2026-11-01.json).
The configured remote connection has not yet been exercised against an approved
Cloudflare account.

Run the explicit operator commands from the repository with Node 24 or 26:

```sh
node tools/backup-cloudflare.ts capture capture-plan.json
node tools/backup-cloudflare.ts restore restore-plan.json
node tools/backup-cloudflare.ts release-targets release-plan.json
node tools/backup-cloudflare.ts release-sources source-release-plan.json
node tools/backup-cloudflare.ts abandon-sources abandonment-plan.json
```

Each plan is a private regular JSON file (mode `0600`, at most 64 KiB).
The command argument must match its `command` field. For example, replace every
placeholder below with independently verified deployment and custody records:

```json
{
  "schemaVersion": 1,
  "command": "capture",
  "connection": {
    "account": "<32 lowercase hex digits>",
    "namespace": "<32 lowercase hex digits>",
    "worker": "git-source",
    "bucket": "git-source-objects",
    "archiveBucket": "git-backup-snapshot"
  },
  "scope": {
    "installationId": "production",
    "snapshotId": "<unique snapshot identity>",
    "account": "<source account>",
    "namespace": "<source namespace>",
    "deploymentSha256": "<64 lowercase hex digits>"
  },
  "signer": "SHA256:<independently trusted SSH fingerprint>",
  "directory": "snapshot-archive",
  "inventoryDirectory": "discovered-namespace",
  "keyFile": "backup-signing-key"
}
```

Paths resolve relative to the canonical plan file's directory. Keep the plan
outside the archive. Capture requires a private OpenSSH signing key whose
fingerprint matches `signer`, and an inventory produced by the discovery tool
above. Capture, source release and abandonment require the connection's account/namespace to
match the signed source scope. Restore and target release require a different
account/namespace pair; retain the original source scope and signer in those
plans. The live and archive bucket names must differ.

For `restore`, replace `inventoryDirectory` and `keyFile` with `mappingFile`
(the ordered source-to-target NDJSON mapping) and `journalDirectory`. Point the
connection at the recovery Worker and bucket. Keep both paths outside the
archive. For `release-targets`, use the same recovery inputs and add
`verificationFile`: a private, nonempty UTF-8 report of the checks performed
while recovery targets were held (at most 1 MiB, outside the archive). The CLI
hashes that exact report into the durable release journal; it does not establish
that the operator's checks are sufficient. For `release-sources`, use the source
connection and a separate `journalDirectory`; no mapping or verification report
is required. Source release is an explicit decision after capture, independent
of target recovery acceptance.

The CLI validates local inputs before opening its first remote connection and
closes that scoped connection on success or failure. Capture and restore leave
objects held. Completed release retries do not open a connection or clear a newer
hold. Successful commands emit a JSON report; failures emit a generic message
on stderr and exit nonzero. Retain plans, reports and journals under private
custody. Continue to enforce external writer, namespace-creation and deployment
isolation throughout the procedure. This command is a repository tool, not yet
part of the standalone `git+` distributable. Local native-workerd command tests
are recorded in [operator CLI evidence](measurements/backup-command-2026-11-01.json);
they do not qualify a deployed account or production recovery time.

The coordinator copies the verified discovery files into a new private archive
and verifies the copy before sending any RPC. It then writes a private signed
`capture-intent.packet`, syncs the file, archive directory and parent directory,
and only then begins holding source objects. The intent binds the independently
trusted scope to the copied inventory digest and object count. Its signature
position is separate from the completed namespace inventory.
`verifyCaptureIntent` authenticates this pre-hold source set even after capture
fails. It rejects a different signer, scope or source inventory. Intent is not a
backup: complete-archive verification still requires every object archive and
the signed completion record. Older partial captures without this intent cannot
be authenticated through this new path. The abandonment command below uses this intent solely to identify the sources
whose holds the operator has decided to release; it does not substitute for
successful backup verification or target recovery acceptance. It holds every listed object before
copying R2 or capturing object pages. A failed hold leaves prior holds intact and
prevents R2 copying. After R2 copy/readback verification, every object archive is
signed against the same namespace, deployment and R2 resource digests. A streamed
`index.ndjson` binds each source ID, original storage-presence flag, authenticated
object inventory and completion-seal digest. The signed `complete.packet` binds
the index digest, object count, scope and resources at the reserved `__namespace__`
position. It is written and flushed last, after the index and child archives.
Sources remain fenced on success and failure. A partial R2 destination or archive
must be inspected; capture retries require fresh destinations.

After a successful capture, `releaseCapturedSources` in
`tools/cloudflare-source-release.ts` can resume the captured sources. Supply the
archive directory, independently trusted `scope` and `signer`, the copied
`archiveBucket`, source namespace `client`, and a new private `journalDirectory`
dedicated to source release. Do not reuse the target restore journal. The binding
must resolve the source account/namespace recorded in the trusted scope; the
operator CLI supplies the configured transport described above.

The operation authenticates the complete disk archive and reads back copied R2
contents and metadata before releasing any source. It atomically records release
intent and the authenticated source list in an exclusive SQLite journal, then
records each successful `Release` acknowledgement. Keep the archive immutable
and retain global isolation until every source has been released. On failure,
keep the journal and retry with the same archive, signer, source binding and
copied bucket. Unfinished retries recheck the copied bucket and skip durably
recorded releases. A lost successful acknowledgement repeats only `Release`;
the coordinator never re-holds a source, imports data or retires a receipt.
Unexpected acknowledgements and inconsistent journal ordering stop progress.

A terminal retry verifies the disk archive and journal but performs no R2 or
source RPC. Its result records past completion; it does not establish current
source availability. In particular, it cannot release a later backup's fence.
This operation is for successful complete captures. It does not release restored
targets or qualify backup retention, global isolation, deployment-state capture
or production recovery targets. See
[source release evidence](measurements/source-release-2026-11-01.json).

To abandon a failed capture, stop its coordinator and keep external writer and
deployment isolation in place. Inspect and retain the partial archive and bucket.
Create a private nonempty UTF-8 `decisionFile` recording the reason for abandoning
this snapshot (at most 1 MiB). Use an `abandon-sources` plan with the original
source `connection`, independently trusted `scope` and `signer`, partial archive
`directory`, a separate private `journalDirectory`, and `decisionFile`. The
journal and decision file must be outside the archive. Do not reuse a normal
source-release or target-recovery journal. For example, the command is
`node tools/backup-cloudflare.ts abandon-sources abandonment-plan.json`.

The coordinator authenticates `capture-intent.packet` and its discovery list,
then atomically journals that source set, operation identity and decision-file
hash before the first `AbandonCapture` RPC. It does not verify or remove copied
R2 data and does not mark the backup complete. Native administration refuses
restore checkpoints, a corrupt fence or a different snapshot's fence. It can
release either a held or draining fence belonging to this snapshot, but waits
for admitted requests and their followups to finish and syncs storage first.
It does not create a fence on an untouched source. A live capture RPC may still
be waiting for active work; resolve that work or restart the fenced activation
before retrying, rather than running competing coordinators.

Keep the same journal, intent, trusted scope, signer and exact decision-file
bytes on retry. Successful acknowledgements are durable; a lost acknowledgement
repeats only the idempotent abandonment RPC. A completed retry verifies local
intent/journal state and makes no remote calls, preserving any later hold.
Normal release and abandonment share the source progress schema, but their
journal identities are distinct and cannot be interchanged. Restore or import
receipts are never retired by abandonment. Older partial captures without a
signed intent remain outside this command and require manual investigation.
Local native and subprocess checks, including SIGKILL around acknowledgements,
are recorded in [abandonment evidence](measurements/backup-abandon-2026-11-01.json).

`SourceRelease.process.test.ts` also runs the actual source-release coordinator
in child processes and sends SIGKILL before the first release, after a native
source has released but before acknowledgement, and when the next RPC proves
the first acknowledgement has been journaled. Native DO/R2 state remains alive
in the parent fixture. A new coordinator process reopens the journal after DO
eviction, repeats only unacknowledged work, and completes source release. A
further process performs a terminal retry without RPC/R2 access and preserves a
later snapshot's hold. This uses two empty native objects and an empty R2 bucket;
it verifies coordinator restart behavior, not production-scale transfer,
power-loss durability or target-import process death. See
[source coordinator process-death evidence](measurements/source-process-2026-11-01.json).

`TargetRecovery.process.test.ts` runs the actual namespace restore and target
release coordinators in child processes against two native objects containing
SQL blobs and typed/cyclic KV values. SIGKILL interrupts Apply and Finish after
remote success, the transition to the second object after the first receipt is
journaled, receipt retirement before/after its acknowledgement is journaled, and
fence release before/after its acknowledgement is journaled. New processes resume
after target eviction and verify recovered data while targets remain fenced.
The release phase records the verification report digest; retries finish
retirement and release without reimporting data. Terminal retries leave later
holds intact and make no remote requests. The source objects remain fenced.

The source and target process tests share a test-only loopback bridge that keeps
native state alive across child death. The target tests now include a 1 MiB R2
object with HTTP and custom metadata in all nine interruption scenarios. Two
additional boundaries kill the restore coordinator during its streamed PUT and
after native R2 commits but before its acknowledgement reaches the child. The
interrupted body leaves no object; the committed body survives, and retry checks
its content and metadata without changing its version. All resumed targets are
checked against the source SQL, typed KV and R2 bytes before release. The bridge
streams bodies through a native fixed-length stream; it does not buffer uploads.
A pinned Miniflare cleanup patch prevents a detached-writer rejection when an
upload is interrupted; direct stream regressions and the process tests require
zero unhandled errors. These tests do not qualify production-scale transfers,
global isolation, power loss or a deployed installation. See
[nonempty R2 process-death evidence](measurements/r2-process-2026-11-01.json).
The earlier [target coordinator evidence](measurements/target-process-2026-11-01.json)
records the seven scenarios before nonempty R2 coverage was added.

The native rehearsal captures repository and authorization objects together,
restores their signed object archives and the R2 copy into fresh storage, evicts
both targets and checks repository/LFS state, token permissions/revocation/expiry,
and registry/fork metadata. Both targets remain fenced through those checks.
Injected hold failure prevents R2 writes; injected page failure leaves no namespace
completion packet. See [namespace capture evidence](measurements/namespace-archive-2026-10-30.json).

Operators can run the offline preflight directly:

```sh
node tools/verify-cloudflare-backup.ts /protected/archive /operator/trusted-expected.json > /operator/archive-verification.json
```

The trusted JSON file must contain `scope` (the independently recorded
`installationId`, `snapshotId`, 32-character lowercase hexadecimal `account`
and `namespace`, and 64-character lowercase hexadecimal `deploymentSha256`)
and `signer` (the trusted SSH `SHA256:` fingerprint). For example, replace every
placeholder in this shape with values from protected operator records:

```json
{
  "scope": {
    "installationId": "<installation>",
    "snapshotId": "<snapshot>",
    "account": "<32 hex characters>",
    "namespace": "<32 hex characters>",
    "deploymentSha256": "<64 hex characters>"
  },
  "signer": "SHA256:<43 base64 characters>"
}
```

Keep this file outside the archive and obtain its values independently; accepting
scope or a signing key merely because the archive supplies it defeats
authentication. The command rejects a trust file inside the archive, symlinked
trust files, malformed UTF-8/JSON and files larger than 16 KiB. The archive must
remain immutable during verification. No Cloudflare credentials, namespace client
or target bucket are used. The command writes a JSON report only on success and
returns a nonzero exit status with a redacted message on failure; do not infer
success from the existence of a redirected output file.

The report authenticates archive signatures, discovery/index correspondence and
all object archives. Its `notVerified` list explicitly excludes R2 bucket
readback, installation-wide isolation and application recovery. Those checks
remain required before release; this report alone is not the application recovery
report requested by the release coordinator. See
[offline command evidence](measurements/offline-backup-verification-2026-10-30.json).

Before using a namespace archive for recovery, call `verifyNamespaceArchive`
with the independently trusted signer and expected installation/snapshot/account/
namespace/deployment scope. It verifies saved discovery, authenticates the signed
namespace completion, and streams the index alongside discovery records. Missing,
extra, repeated or reordered entries fail, including an index signed with the
trusted key that disagrees with discovery. Each object's complete archive is
then authenticated and matched to the indexed inventory and seal digest. The
shared file reader refuses symlinks and non-regular files, bounds record/file
sizes and requires valid UTF-8 and terminal newlines for indexes. Verification
performs no target RPC or R2 operation. Its returned R2 evidence is authenticated
expected evidence, not a readback check of the current bucket; independently
verify copied R2 bytes/metadata before releasing targets. See
[namespace verification evidence](measurements/namespace-verification-2026-10-30.json).

This does not establish installation-wide isolation by itself. Stop namespace
creation and every writer outside the listed repository objects before discovery;
per-object fences cannot prove that the discovery list was complete. Other
namespaces, public snapshot paths, R2 writers and deployment-state capture still
need the installation procedure. Production target mapping, a configured remote
operator transport and deployed qualification remain open. The programmatic
restore and release coordinators described above do not establish those properties.
The namespace completion signature does not waive
those checks or authorize automatic source/target release.

A local-only Durable Object fixture transfers one SQL table with a binary column
and three KV entries into a fresh object through this codec. It verifies HEAD,
snapshot counters, dates, maps/sets, big integers, typed arrays, non-finite numbers,
and shared/cyclic references. The codec itself needs no extra compatibility
flags; the expanded application fixture uses the production test Worker's
`nodejs_compat` and `global_fetch_strictly_public` flags for its storage services.
A separate native probe found `node:v8.serialize`
unimplemented even when its module flag was enabled. The fixture is not imported
by the production Worker.

`tools/cloudflare-kv-backup.ts` captures KV pages with a bounded lookahead
(default eight entries, maximum sixteen plus one application lookahead and
one possible fence record). A matching held fence is excluded from application
entries; mismatched/draining fences and retained current or legacy import
checkpoints prevent capture. Remove completed receipts only through the eventual
installation coordinator after its recovery checks. Each page
uses the versioned frame ceiling above; reduce the page size if encoding exceeds
that ceiling. Restore requires an empty target, commits each page and its progress
marker in one native transaction, and rejects replayed/out-of-sequence pages,
snapshot identifier changes and existing-key replacement. The reserved key
`git-plus:backup-restore:v1` persists progress across object eviction. Capture
refuses an unfinished restore. Only the outer coordinator may remove the marker,
after all pages and SQL/R2/policy verification succeed. Native tests cover eviction
and resumption, rollback after a rejected write, duplicate/out-of-order pages,
reserved keys and empty objects on Node 24.12 and 26.

`tools/cloudflare-sql-backup.ts` captures a catalog of up to 256 application
schema objects (table definitions, indexes, triggers and views) and row pages of
up to sixteen rows plus one lookahead row. It excludes SQLite and Cloudflare
internal objects. Row capture supports ordinary rowid tables, including quoted
identifiers, and rejects virtual/without-rowid tables, generated columns and
tables shadowing all three rowid aliases. It pages by rowid instead of offsets;
rowids and integer cells are decimal strings, preserving all signed 64-bit
values. Cells retain their SQL storage type, including blobs and nulls. Each
result uses the same 4 MiB transport ceiling; large rows/pages can exceed it and
must fail rather than truncate. Native tests exercise negative and large rowids,
integer extrema, blobs, nulls, fractional values and separate trigger/index DDL.
`tools/cloudflare-sql-restore.ts` imports this format into an empty application
SQL schema. **Catalog DDL is executable SQL and must come from an authenticated,
trusted backup**; schema decoding does not make untrusted statements safe.
Initialization, each page and finalization use native synchronous transactions.
The reserved `git_plus_sql_restore_v1` table retains the snapshot identifier,
catalog, table position and row cursor across eviction. Capture refuses to export
a catalog while this marker exists. Pages must match the expected table, column
order and cursor; row identifiers must increase. Each page is read back inside
its transaction and compared to its captured cells, detecting SQL affinity or
rowid alias changes before committing. Errors omit SQLite diagnostics that could
contain private values.

Finalization requires all tables, creates indexes/views/triggers after the rows,
checks foreign keys and compares the resulting catalog exactly before committing
marker removal. Accounting triggers therefore do not double-charge imported
rows. Native tests cover successful trigger behavior after restore, primary-key
rowid aliases, empty schemas, rejected/out-of-order pages, transactional rollback
of partial DDL and row pages, and resume after eviction on Node 24.12 and 26.
The native runtime rejected `PRAGMA integrity_check`; these checks establish
logical row/schema fidelity, not physical database integrity. AUTOINCREMENT
catalogs are explicitly rejected until sequence restoration exists. General
schema dependency ordering and production-scale CPU limits still need qualification.

The native fixture also runs the production Git, remote, webhook and provisioning
services against real DO/R2 bindings. It exports and imports eight application
tables (`git_capacity`, `git_plus_provisioning`, `git_ref_capacity`, `reflog`,
`refs`, `remotes`, `shallow`, `webhooks`), all nine metadata-accounting triggers,
HEAD and snapshot-counter KV entries, and the fixture's Git objects in R2.
After finalization and eviction, the production services return identical
objects, refs, reflogs, shallow state, private remote/webhook metadata, pending
provisioning state and quotas. A subsequent ref write charges the restored
metadata ledger correctly and leaves the source installation unchanged. This
rehearsal passes on Node 24.12 and 26. It also writes a 9 MiB LFS object through
the production multipart path, copies it with R2 read-back verification, and
checks every streamed byte and the reported size after restoration/eviction.
The restored inventory enforces the configured object quota. A separately seeded
persistent fence representing an uncertain upload is copied and still rejects
writes with `LfsQuotaBusy`; recovery must not silently clear that fence. This
tests preservation of the fence, not capture of an upload still in progress.
The fixture also runs production repacking and removes the loose commit copy,
then verifies object reads through the restored pack/index pair after eviction.
Production snapshot publication creates both the published view and its journal;
restore preserves sequence 24, and a later ref mutation publishes sequence 25
with the updated view. The native R2 copy contains seven objects, including those
metadata records. A separate stock-Git 2.56.0 rehearsal downloaded the recovered
pack/index into a fresh bare repository and passed `verify-pack` and
`fsck --strict` (with the conventional empty tree and fixture main ref supplied).
The [evidence record](measurements/cloudflare-component-recovery-2026-10-30.json)
identifies the source hashes and scope. This is a single-commit pack, not a
large/delta-pack or full deployment qualification.

The optional Artifacts namespace has a separate native rehearsal using its real
SQL Registry and Tokens services. It restores `repos` and `tokens` into a fresh
object, evicts it, and compares repository identities/metadata, pending-fork state,
and credential decisions. Read/write scopes survive; expired, revoked and
wrong-repository tokens remain rejected. Exported pages contain digests rather
than the generated plaintext tokens. A new revocation on the restored copy
survives another eviction and leaves the source unchanged. Node 24.12/26 results
and source hashes are in the [Artifacts evidence record](measurements/artifacts-recovery-2026-10-30.json).
This verifies namespace SQL state, not HTTP authorization or fork object resolution.

The fixtures exercise the private components together under explicit test
quiescence; they do not cover deployed routing/authentication or a production
writer barrier. Active multipart
uploads still require draining or explicit reconciliation in the coordinator.

The SQL/KV restore markers are **not writer barriers**. The production repository
host now wraps request admission with `host/CloudflareBackupFence.ts`, using the
separate durable KV marker `git-plus:backup-fence:v1`. A private caller of `hold`
closes admission before waiting for existing requests, their response streams and
registered detached work to settle, then durably marks the object held. Provisioning
schema initialization is lazy and occurs only inside admitted work. New requests
receive 503 while draining/held; malformed state also refuses admission. The fence
survives eviction and never expires automatically. Removing a held fence requires
the matching snapshot identifier and no active work. An already-absent fence
allows an idempotent release retry without mutating state. An uncertain release keeps this activation
closed and attempts to restore the durable marker; storage failure requires
inspection before an operator can claim release succeeded.

Native tests cover drain ordering, detached follow-ups, response cancellation,
eviction, mismatched release, malformed state and injected flush failures. These
pass with the admission tests on both Node versions; the full actual-Worker
rehearsal also passes for normal operation. The [fence evidence record](measurements/backup-fence-2026-10-30.json)
records the separate scopes and a successful retry after a connection reset. The
administrative methods have **no public HTTP route or exposed production RPC**.
The authenticated coordinator still needs to invoke them privately across every
object, gate Artifacts/other writers, reconcile uncertain native operations and
isolate restore routing. Stateless edge reads are not blocked by the per-object
fence. Capture/import must still have exclusive, quiesced source and destination
access throughout; page identifiers are not authenticated manifests. These
component tests do not qualify a complete installation restore.

The production recovery
mechanism must still map IDs to repository names, drain requests and background
work, capture all application SQL tables and KV values, and coordinate those with
R2 objects, packs, LFS, provisioning artifacts and deployment bindings. In this
repository, KV contains HEAD values and snapshot sequence counters; SQL includes
refs/reflogs/shallow state, remotes, subscribers, provisioning and quota state.
An enabled Artifacts provider adds its repository/token registry. Cloudflare's
[storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
keeps KV values in an internal table whose contents are inaccessible through SQL;
a SQL-only dump would omit them. A repository-name index cannot be reconstructed
from the object-list response alone. The private capture/import components still
need an authenticated coordinator and full recovery rehearsal; inventory alone
is one discovery step, not a completed backup.

Git-object R2 PUT/DELETE calls now settle before request interruption returns.
Once a pack publication starts, it waits for pack then index; deletion waits for
index then pack. A failed call stops that sequence and remains a storage error.
Four native-R2 regressions hold a successful mutation's response while cancelling
its caller and verify that cancellation cannot release the operation early.
This prevents an abandoned promise from mutating storage after handler cleanup;
it does not make an R2/SQLite transaction or resolve an uncertain native failure.
A lost response can still require inspection and reconciliation. Durable Object
handlers can interleave across external I/O; synchronous ref transactions are
explicit, rather than covering the whole asynchronous handler. See the
[SQLite storage transaction contract](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#transactionsync).

`npm run verify:worker` includes a local emulator recovery rehearsal against the
actual production entry modules. The verifier owns a temporary installation,
stops its Worker/DO and disposes proxy handles, then copies `.alchemy/state` and
`.alchemy/local` to a private backup directory. It hashes every regular file in
the source and copy, refuses symlink-dependent data, removes the original state
directories, restores from the copy, and compares hashes again before restart.
The restored stack must preserve private Git history and fsck, LFS content,
trust/provisioning state, a redaction tombstone with its payload still absent,
and remote/webhook registry entries containing secrets. The API and logs must
keep those secrets redacted. It removes the restored subscriptions before later
pushes, and verifies that a subsequent membership revocation still works.

This is a quiesced, filesystem-level emulator rehearsal with the same installed
application and bindings. Operator signing keys and test credentials remain
outside the emulator, as they must in the production backup inventory's separate
key custody. It does not implement production Cloudflare export/import, test a
different account, or establish recovery time and retention at production scale.

Cloudflare's [SQLite Durable Object recovery API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
offers bookmarks for restoring an object's SQL and key-value data within the
past 30 days, and is unavailable in local development. That is not a portable
installation backup: R2 and deployment bindings still need coordinated recovery.
The local emulator filesystem rehearsal cannot qualify this API, retention
behavior, or restoration into a different account. Do not substitute an R2
snapshot journal or a Durable Object bookmark alone for the complete inventory.

## Node capacity rehearsal

Run `node tools/measure-node.ts new-report.json [MiB] [concurrency] [commits] [rounds]`
to measure the source Node host locally. Defaults are 16 MiB, two concurrent
clones, 64 source commits, and one round; argument ceilings are 256 MiB, 16 clones,
4,096 commits, and 100 rounds. Git and `ps` must be available. The tool creates a temporary packed repository with deterministic
high-entropy 1 MiB files and private membership. It times a concurrent stock-Git
bare-clone burst, then verifies successful clones against the expected tip and
`git fsck --strict` outside the timing window. A subsequent clone must succeed
and pass fsck after every round. Clones are removed between rounds to bound disk
use. Schema-version-2 reports retain each round's request outcomes, burst duration
and post-recovery host RSS from `ps`; the final host report retains process peak
RSS and aggregate CPU. Post-recovery RSS samples are not per-round peaks, and
these repeated bursts have verification/recovery/cleanup gaps: they do not
establish continuous saturation behavior or a memory-leak-free lifetime.
The harness renews its five-minute read delegation before each timed round and
recovery clone, outside burst timing. It preserves normal authentication and
expiry checks; a long experiment must not accidentally measure an expired
fixture token as a capacity failure.
Temporary repositories and the child host are cleaned up. The report is created
exclusively, records transfer failures, and is removed if the rehearsal itself
cannot finish; an existing report is never overwritten.

Measurements on 2026-10-29 used macOS arm64, Apple M5 Max, 18 available logical
CPUs, 128 GiB RAM, Git 2.56.0, 64 source commits, and the dirty development tree.
Admission settings were 32 global / 8 per repository / 128 connections, with
the default 250,000-step fetch budget. Raw reports retain revision, OS/runtime,
object inventory, request outcomes, and resolved limits.

| Runtime / report                                                               | Data   | Concurrent clones | Verified / attempted | Successful clone range | Host peak RSS |
| ------------------------------------------------------------------------------ | ------ | ----------------- | -------------------- | ---------------------- | ------------- |
| [Node 26.10, small](measurements/node-capacity-2026-10-29-26-small.json)       | 16 MiB | 2                 | 2 / 2                | 0.81–0.87 s            | 325 MiB       |
| [Node 26.10, burst](measurements/node-capacity-2026-10-29-26-burst.json)       | 64 MiB | 8                 | 8 / 8                | 4.34–6.46 s            | 427 MiB       |
| [Node 26.10, overload](measurements/node-capacity-2026-10-29-26-overload.json) | 64 MiB | 16                | 8 / 16               | 4.44–6.52 s            | 431 MiB       |
| [Node 24.12, small](measurements/node-capacity-2026-10-29-24-small.json)       | 16 MiB | 2                 | 2 / 2                | 0.73–0.79 s            | 275 MiB       |

All eight failed overload clones received HTTP 503; the recovery clone succeeded
in every run. Peak RSS is the host process's lifetime high-water mark, including
startup and the recovery clone; it excludes the Git client processes and OS file
cache. Reported CPU likewise includes the recovery clone. These are individual
loopback bursts with a shared filesystem cache, not repeated trials, saturation
measurements, standalone-binary measurements, or measurements of a deployed
host. They establish observed behavior at these workloads, not supported maximum
repository sizes. Slow networks, sustained load, large individual objects,
different history/ref shapes, storage quotas, and Cloudflare capacity remain
unqualified. Use representative workloads on the intended host before choosing
production limits.

The [2026-10-30 Node 26 repeated-round report](measurements/node-capacity-2026-10-30-node26-repeated-64mib-8.json)
uses the same 64 MiB / 64-commit fixture with eight concurrent clones over 20
rounds. All 160 burst clones and 20 recovery clones passed. The measured rounds,
including verification and cleanup gaps, took 201 seconds; individual burst
durations were 6.37–6.70 seconds. Host peak RSS was 522 MiB. Post-recovery samples
ranged from 439 to 522 MiB, with the first at 439 MiB and last at 511 MiB. The
increase merits longer-run investigation; this short run cannot distinguish
bounded runtime caching/heap growth from a leak. It does not qualify a supported
repository size or continuous saturation behavior. Reports include the dirty
source revision, runtime, machine information and effective request limits.

The matching [Node 24.12 repeated-round report](measurements/node-capacity-2026-10-30-node24-repeated-64mib-8.json)
also verified all 160 burst clones and 20 recovery clones, over 234 seconds.
Burst durations were 7.80–8.33 seconds; host peak RSS was 517 MiB. Post-recovery
RSS increased from 399 MiB in the first round to 517 MiB in the last. Both
runtimes therefore need longer observation before interpreting growth as
bounded. These runs were sequential on the same machine, not a controlled
runtime-performance comparison.

The corrected [Node 24.12 100-round report](measurements/node-capacity-2026-10-30-node24-100rounds-64mib-8.json)
extends the same 64 MiB / eight-clone workload with credential renewal. All 800
burst clones and 100 recovery clones verified over 1,174 seconds (19.6 minutes).
Peak host RSS was 546 MiB. Post-recovery RSS increased initially, then levelled
off in this run: rounds 61–100 stayed between 544.95 and 545.41 MiB. Mean burst
duration by successive 20-round windows was 7.89, 8.07, 8.06, 8.00 and 7.94
seconds. This resolves the immediate question left by the shorter run: memory
did not keep rising at its initial rate under this fixed workload. It does not
prove a leak-free lifetime, continuous saturation capacity, or behavior with
changing repositories, large objects, slow clients or different host limits.
An earlier attempt expired its test credential after 25 rounds and produced no
completed report; it is excluded from capacity conclusions.

## Node durable publication and incomplete ref rollback

The Node storage contract is **acknowledged-operation persistence across process
death, with detectable incomplete atomic ref transactions and offline recovery**.
It applies to writes made through this application's Node stores on a local
filesystem supporting exclusive creation, atomic rename, and file/directory
sync. A successful Git push means the ref statuses report success, not merely
HTTP 200. A successful LFS upload means its final response reports success.
A successful administrative operation means its effect has completed. Reopening
after process termination must preserve those completed operations, including
repository/token registries, token revocation, and fork ownership. Later accepted
updates, explicit deletion, retention, and maintenance can intentionally change
that state; this is not an immutable history or backup promise.

Publication orders objects before refs and LFS file contents before their final
name. Administrative replacements sync private temporary contents before rename
and sync their directories before success. Atomic multi-ref operations persist a
recovery journal before changing refs and remove it only after completion or
successful rollback. A storage error is an **unknown outcome** until inspected:
a rename can have happened before a directory sync fails. A pending or corrupt
ref journal blocks ordinary application access. Failed rollback is reported as
`StorageFailure` rather than a successful rollback. Follow the offline recovery
procedure below before resuming service; automatic rollback after a crash is not
part of the contract. A crashed writer can leave locks and temporary files.
Confirm it has exited and preserve evidence before removing its reservations.

Each operation has its own acknowledgment boundary. There is no transaction
spanning a Git push, LFS upload, token edit, and repository lifecycle edit.
Reflogs and background deliveries have the weaker contracts described below:
a reflog failure does not undo an accepted ref, and delivery is best effort.
Stock-Git writers do not honor the application's journal. Stop all writers for
recovery and do not use stock Git to bypass an incomplete-transaction guard.
Filesystem corruption, external changes, network filesystems, unavailable sync
primitives, and loss of the storage device require operator recovery or backup;
they are not made safe by atomic rename alone.

The release evidence includes process-kill and native syscall-failure tests on
macOS arm64, actual filesystem exhaustion on Linux arm64 tmpfs, and one forced
Linux guest reset with persistent ext4 storage. The last exercise demonstrates
that recorded configuration, with its host storage still powered. **No physical
power-loss guarantee is offered.** Qualifying such a guarantee requires separate
filesystem, device, and write-cache evidence. The support matrix and deployment
qualification gates remain independent of this storage contract.

| Required boundary                                  | Evidence and recovery checked                                                                                                                                                                                    |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Partial object/ref/journal writes and ENOSPC       | `DiskFull.interop.test.ts` refuses the push, preserves both prior refs, checks Git integrity, then retries after restart; `verify-storage-capacity.ts` also exercises real kernel ENOSPC and LFS refusal.        |
| File/directory sync and existing-directory retries | `Node.durability.test.ts`, `RegistryFile.node.test.ts`, and `Lfs.durability.node.test.ts` reject failed syncs and retry after reopening, including existing ancestors and objects.                               |
| Pack/ref publication and rollback failure          | `Node.pack-durability.interop.test.ts`, `Node.ref-durability.interop.test.ts`, and `Node.test.ts` cover failed publication, surviving history, incomplete rollback reporting, and guarded access.                |
| Process death during atomic ref publication        | `Node.transaction.interop.test.ts` kills before the first ref, after its rename, and before journal removal; it rehearses offline repair, Git fsck, reopening, and another successful transaction.               |
| Acknowledged HTTP and installation writes          | `HostCrash.interop.test.ts` and `InstallationCrash.interop.test.ts` verify acknowledged Git/LFS data, private access, tokens/revocations, registries, fork dependencies, and subsequent mutations after SIGKILL. |
| Guest kernel restart                               | `verify-host-reset.ts` requires a different boot ID and verifies persisted refs, clone/LFS digests, credentials/revocations, fork ownership, and another atomic push. The retained report and limits are below.  |

For a Linux Node deployment, put all repository storage on a dedicated bounded
filesystem and set `GIT_STORAGE_FILESYSTEM_BYTES` to its maximum accepted total
capacity in bytes. For example, `107374182400` accepts a filesystem of at most
100 GiB. Before opening either listener, the host checks the existing data
directory (the explicit repository directory in single-repository mode),
rejects the system filesystem, and compares its total blocks times block size
with the ceiling using integer arithmetic. Missing directories, unavailable
filesystem statistics, unknown/zero capacity, oversized filesystems and invalid
configuration stop startup. The check is Linux-only; enabling it on another OS
fails startup. An unset setting preserves the development startup behavior.

This asserts an operator-provisioned limit; it does not partition disks, mount
volumes, configure filesystem quotas, or reserve free space. The kernel's
filesystem limit covers Git, LFS, metadata and temporary files across processes
on that volume. Provision inode limits and recovery headroom as well as bytes;
keep logs, backups and unrelated workloads elsewhere. Treat the mount topology
as immutable while serving: do not resize/remount the volume, replace the data
path, or place nested mounts/links to other storage under it. The startup check
does not police later administrator changes or storage outside that filesystem,
and is not a per-tenant allocation or a Cloudflare quota. A filesystem shared
with other writers can exhaust earlier than its total capacity suggests.
The [Linux filesystem statistics contract](https://man7.org/linux/man-pages/man2/statfs.2.html)
describes the reported total blocks and inode counts. Qualify the selected
persistent filesystem and its exhaustion/recovery behavior before deployment.

`tools/verify-storage-capacity.ts` exercises real filesystem-capacity refusal
in a disposable Linux container. It refuses to run unless
`/git-capacity-rehearsal` is an empty, separate tmpfs mount sized 16–64 MiB;
it never fills a persistent filesystem. It enables the startup ceiling, proves
the host refuses the system filesystem and a mount one byte over the ceiling,
and checks that single-repository mode validates its actual storage path.
The rehearsal fills that mount until
the kernel returns `ENOSPC`, frees only 512 KiB, and attempts an authenticated
atomic Git push and a 1 MiB LFS upload. Both old refs must survive, partial LFS
content must remain unpublished, and temporary files/reservations must be
cleaned. After freeing the filler and restarting the host, the same operations
must succeed; stock Git verifies the recovered clone and the LFS digest matches.

The [Node 26.13 report](measurements/storage-capacity-2026-10-29-linux-node26.json)
and [Node 24.12 report](measurements/storage-capacity-2026-10-29-linux-node24.json)
both pass on Linux arm64 with a 32 MiB mount and Debian Git 2.39.5. Both runs
emitted storage-failure signals and verified successful recovery. This is additional
capacity evidence, not a replacement for the pinned Git 2.56 release suite,
persistent-volume quota qualification, or power-loss testing. Tmpfs is volatile
and is used only for this isolated failure rehearsal.

The subsequent [Node 26.13 ceiling report](measurements/storage-ceiling-2026-10-30-linux-node26.json)
and [Node 24.12 ceiling report](measurements/storage-ceiling-2026-10-30-linux-node24.json)
repeat the same exhaustion/recovery exercise with the startup assertion enabled
and verify its refusal cases. They retain the same tmpfs and Git-version limits
of evidence.

To repeat it from a checkout with installed dependencies and applied patches,
use a fresh container name and report destination, then remove the container:

```sh
docker run -d --name git-storage-capacity \
  --mount "type=bind,src=$PWD,dst=/workspace,readonly" \
  --tmpfs /git-capacity-rehearsal:rw,size=32m --workdir /workspace \
  node@sha256:bc0a2afd3b74ce12222fc8bab99271e66257f021078f5f4a5d687a77d280c1d9 \
  sleep 1800
docker exec git-storage-capacity sh -c \
  'apt-get update && apt-get install -y --no-install-recommends git ca-certificates'
docker exec git-storage-capacity \
  node tools/verify-storage-capacity.ts /tmp/storage-capacity.json
docker cp git-storage-capacity:/tmp/storage-capacity.json ./new-storage-capacity.json
docker rm -f git-storage-capacity
```

To exercise inode exhaustion independently, use a fresh container with
`--tmpfs /git-capacity-rehearsal:rw,size=32m,nr_inodes=512` and run
`node tools/verify-storage-capacity.ts /tmp/inode-capacity.json inodes`.
This mode requires 128–4096 total inodes, creates empty filler files until the
kernel returns `ENOSPC`, and requires zero free inodes while more than 16 MiB
remains available. It verifies that `/ready` changes from 200 to 503 with
`storage:false`, that acknowledged Git state survives rejected writes, and that
partial LFS data is not published.
After removing only the rehearsal filler and restarting, readiness, writes,
clone/fsck and the LFS digest must recover. The default `bytes` mode retains
the original byte-exhaustion exercise. Both modes record free bytes and inodes.
The [kernel tmpfs documentation](https://docs.kernel.org/filesystems/tmpfs.html)
defines the independent `size` and `nr_inodes` limits; these temporary mounts
are test fixtures, not production storage recommendations.

The [Node 26.13 inode report](measurements/inode-capacity-2026-10-30-linux-node26.json)
and [Node 24.12 inode report](measurements/inode-capacity-2026-10-30-linux-node24.json)
pass on Linux arm64 with 512 inodes and approximately 31.9 MiB still free at
refusal. Both runs emitted `storage_probe_failed` and redacted Git/LFS
`storage_failure` signals. The same Debian Git 2.39.5 and volatile-storage
qualification limits apply; remote alert delivery is not exercised here.

Git loose-object writes and replacement of loose refs, HEAD, and shallow files
sync temporary contents before rename and sync the containing directory before
returning success. Publication and deletion now repeat the resolved ancestor
chain through the filesystem root, including retries in fresh processes.
Loose-object retries sync existing objects and this chain instead of treating
existence as proof that an earlier attempt completed. Bare initialization also
repeats the chain when its directories and HEAD already exist. The service
account must be able to open and sync those directories. This deliberately adds
filesystem work; production capacity measurements remain required.
A bounded cache retains up to 4,096 successfully synced directory identities
per process. Every traversal rechecks every ancestor's device, inode, birth time
and change time; unchanged ancestors can reuse a completed sync. The containing
directory is always synced. Failed chains never populate the cache, and a fresh
process repeats the full sync. Changing an ancestor invalidates its entry even
when a descendant directory is unchanged.
Failed writes remove their temporary
files after the writer settles. Local fault tests inject file and directory sync
failures and verify visible state, retries, cleanup, and ref rollback outcomes.
Packed-ref rewrites use the same synced replacement and invalidate the in-process
cache even if a sync fails after rename. Loose-ref, shallow-file, loose-object,
and pack/index deletions sync their directories; retries repeat that sync even
when an earlier attempt already removed the file. Failed index cleanup is
reported for operator retry. Multi-ref process-death detection and offline
recovery are covered below. These mechanisms implement the operation boundaries
above; they do not provide a transaction across the entire installation or a
physical power-loss guarantee.

Reflog appends and trimmed replacements attempt file and directory syncing.
If persistence fails, the ref update remains applied and a sanitized warning
reports the missing durability evidence. Do not interpret this warning as a
rejected push or rely on the reflog as a complete audit ledger. Preserve the
repository and investigate its storage before pruning history that may depend
on missing reflog roots. The warning omits ref values and underlying error text.

Pack publication replaces the pack through a synced temporary file, syncs its
directory, then does the same for the index. It never truncates a final pack or
index in place. Tests inject partial writes, file/directory sync failures, and
a killed writer before index rename; stock Git can still read the acknowledged
history and verifies it with `git fsck --strict`. A first publication interrupted
before its index can leave an undiscoverable pack, and process death can leave
temporary files. Preserve these during recovery; remove a temporary file only
after confirming its writer exited and verifying the surviving repository.
Automatic crash scavenging and power-loss validation are not established by
these tests.

Remote and subscriber registry edits sync their private temporary file before
rename and sync the containing directory afterward. Every mutation also syncs
the resolved directory chain through the filesystem root before publication,
even when all directories already exist. This prevents a fresh process from
acknowledging a retry after an earlier creation left an ancestor unsynced.
The service account must be able to open those directories and the filesystem
must support directory syncing; failures are reported, never silently bypassed.
This extra work applies to registry mutations, not reads. A file-sync failure preserves the previous registry;
a directory-sync failure after rename reports an error even though the new rows
may already be visible. On any failed edit, inspect the registry before retrying
an addition, especially a webhook whose generated ID may have been persisted.
Do not assume an error means no change occurred. Local child-process tests inject
both sync failures and verify preservation/visibility, lock release, cleanup,
and subsequent edits; they do not simulate loss of machine power.

Node LFS uploads use exclusive private temporary files and require their file
sync to succeed before rename. After rename, the store syncs the shard directory
and its resolved parent chain through the filesystem root on every upload,
including retries from a fresh process. Existing directories may remain from a
failed creation and do not establish a shorter safe sync boundary. The service
account must be able to open and sync this chain, as for registry mutations.
Publication is not interrupted between rename and these syncs. A sync failure
returns a storage error; if it happens after rename, a complete object can still
be visible. Check that object's digest and size before retrying. These ordered
sync calls have local functional coverage, not power-loss validation, and do
not make Git refs, repository metadata, or an entire installation crash-durable.
Cancelled LFS uploads abort the disk pipeline and close their source iterator
before removing the temporary file. Cleanup waits for the writer to settle;
this prevents a still-running writer from recreating a file after cleanup.

Node LFS enforces `GIT_LFS_RETAINED_BYTES` (default 10 GiB, positive safe integer)
and `GIT_LFS_RETAINED_OBJECTS` (default 100,000, integer 1–1,000,000) per repository.
An exclusive private `lfs/.quota.lock` serializes upload writers across processes.
Before reading an upload, a streaming directory scan counts retained files and
their payload bytes, including orphaned temporary files. Symlinks, unsupported
directory shapes and inspection errors fail closed. The scan bounds directory
entries too; empty directories cannot create an unlimited walk. Each arriving
chunk must fit the remaining byte allowance before reaching the disk pipeline.
Cancellation stops inventory traversal at native I/O boundaries, closes directory
iterators, and waits for active reads to settle before releasing the quota lock
and request admission. It does not detach the remaining scan or forcibly abort
an in-flight filesystem call.
Duplicate uploads need temporary-file headroom, even when they would replace
an existing object; use LFS batch/head results to avoid unnecessary uploads.

Quota exhaustion returns 507 and `Lfs/quota_exceeded` with the resource category.
A held writer lock returns retryable 503 and `Lfs/quota_lock_held`; readers remain
available. Cleanup retains the lock until the temporary file is removed or its
cleanup attempt fails, so the next writer includes any leftover bytes in its
scan. Cleanup failures emit `upload_cleanup_failed` or `quota_lock_cleanup_failed`
without filenames, tokens or payloads. A quota scan is proportional to the
repository's retained LFS inventory; these defaults do not certify supported
repository size or throughput.

`node tools/measure-lfs-capacity.ts /tmp/new-lfs-report.json 3` measures this
cost against 1,000, 10,000 and 99,999 valid retained objects using the default
quota. It creates and removes its own temporary tree, verifies each 1 MiB
upload's digest, verifies refusal at the object ceiling, and records scan/upload
latency, CPU, sampled process RSS and event-loop delay. Allow filesystem space
for 100,000 tiny files plus their shard directories; the payload total is only
about 2.6 MiB, but filesystem allocation is substantially larger.

The [Node 24.12 report](measurements/lfs-inventory-2026-10-30-node24.json) and
[Node 26.10 report](measurements/lfs-inventory-2026-10-30-node26.json) record three
rounds per size on macOS arm64 (Apple M5 Max), after the scan-cancellation fix:

| Retained objects | Node 24 median | Node 26 median |
| ---------------- | -------------- | -------------- |
| 1,000            | 104 ms         | 129 ms         |
| 10,000           | 674 ms         | 691 ms         |
| 99,999           | 4,395 ms       | 4,385 ms       |

Times include quota scanning, durable upload publication and streamed digest
verification, with warm filesystem caches. Sampled peak process RSS at the
largest inventory was 274 MiB and 257 MiB respectively; those processes also
seeded the fixture, so these values do not isolate quota-scan allocations.
This measures source-backend calls, not HTTP concurrency, cold-cache behavior,
production storage or R2. The several-second scan near the default ceiling
holds the writer lock: simultaneous uploads can receive retryable 503 even
though byte capacity remains. Set production inventory/throughput expectations
only after measuring the selected volume and request workload.

These are Node LFS payload/file quotas, not filesystem block/inode quotas. They
exclude Git objects, metadata, directory overhead and other repositories.
External writers that bypass this backend do not honor its lock. Keep filesystem
capacity monitoring and host/account quotas. R2 LFS and retained Git have the
separate accounting mechanisms below; deployed capacity remains a release gate.
Existing over-quota data stays readable and is never
automatically deleted. Raise the configured limit after capacity review or
reconcile abandoned scratch files offline to resume uploads.

Abrupt process death cannot run that cleanup. The LFS crash regression kills an
isolated writer after a partial disk write, before rename, after rename but
before directory syncing, and after the store acknowledges success. Fresh
processes verify previously acknowledged objects by length and SHA-256, confirm
that unpublished temporary bytes are not served as objects, and verify that
the stale quota lock refuses new uploads. After every writer has exited, the
fixture removes that lock offline and successfully retries the upload.
A post-rename crash can leave a complete object even when the
caller never received success; check its digest rather than assuming absence.
These are process-death tests on the test filesystem, not power-loss evidence.

After stopping all writers using the LFS directory and preserving recovery
evidence, inspect leftover `<oid>.<uuid>.tmp` files within its two-level shards.
Such files are private upload scratch space, not published objects. Confirm the
owning process is gone before removing them; a file's age alone does not prove
it is abandoned. These names are not served as objects but count against quota;
a retry uses a new exclusive temporary name. With all writers still stopped,
remove the stale `lfs/.quota.lock` before restarting. Do not remove a lock based
on age or an upload timeout: storage work may still be running. There is no
automatic stale-lock eviction. Do not delete the bare 64-character object files or
entire shard directories as temporary-file cleanup. No automatic crash-scratch
collector currently runs, so monitor their disk usage after abnormal exits.

Cloudflare LFS uses the same retained-byte/object settings and a persistent
`<repo>/lfs/.quota.lock` object, acquired with a conditional create. Before
consuming an upload it pages through the repository's LFS prefix, counting
completed objects and bytes while excluding that one control object. The
current upload consumes the remaining allowance before hashing or storing each
chunk. Object listing is paged and bounded; it does not accumulate the whole
inventory. Repeated uploads need headroom for existing content plus the incoming
copy. The scan's latency and R2 request cost grow with retained inventory.
An interrupted scan waits for the pending native listing before releasing its
lock/admission slot and does not request another page. R2 cannot abort that
pending listing; stopping the request is not proof that its native call settled.

The lock spans all multipart calls, publication, and cleanup. A confirmed
multipart abort or completed write allows release. Failed aborts, lost write
responses, or process death can leave the lock in place; subsequent uploads
return 503 while existing objects remain readable. Unknown write outcomes emit
`quota_reconciliation_required`; lock-deletion failures emit
`quota_lock_cleanup_failed`. There is no time-based lock stealing. The native
R2 contract relies on [conditional writes](https://developers.cloudflare.com/r2/api/workers/workers-api-reference/)
and [strongly consistent object listings](https://developers.cloudflare.com/r2/reference/consistency/).
Local workerd checks do not replace target-account failure testing.

Before deploying this quota to an existing bucket, stop every old and new LFS
writer and reconcile pre-existing multipart uploads through R2's multipart
administration API. Completed-object listings cannot inventory their parts.
Abort abandoned uploads and verify their cleanup before establishing the quota
baseline. The seven-day lifecycle policy remains a fallback, not proof that a
particular upload has disappeared. Never run an older writer that ignores the
quota lock alongside this writer.
A code rollback cannot make an older writer honor the new quota or lock. Keep
LFS writes disabled when reverting to a version that predates this mechanism.

To recover a retained R2 quota lock, keep writers stopped, establish that prior
storage calls have settled, inspect completed objects and incomplete multipart
uploads for that repository, and finish reconciliation before deleting the
exact `<repo>/lfs/.quota.lock` object. A missing HTTP acknowledgement is not
proof that an object was not published. Preserve this marker in bucket backups;
restoring data without it could discard an unresolved upload's fence. Neither
this quota nor the Node quota covers Git objects, other repositories, legacy
unreconciled multipart parts, metadata overhead, or an account-wide bill. Those
capacity and deployment qualification gates remain open.

Cloudflare Git storage separately enforces `GIT_CF_RETAINED_BYTES` (default
10 GiB, positive safe integer) and `GIT_CF_RETAINED_KEYS` (default 100,000,
integer 1–1,000,000) per repository. These count payload bytes and keys beneath
`<repo>/objects/` and `<repo>/pack/`, including orphan packs and indexes.
They exclude LFS, SQLite metadata, R2 metadata overhead and other repositories;
account-wide capacity and billing limits still require operator controls.
All writable DO stores use this boundary. Low-level object/pack factories also
serve the stateless read path and must not be used as independent writers.

The first mutation inventories both prefixes with bounded R2 pages. Later
mutations use one durable `git_capacity` SQLite row and a per-key R2 head;
they do not rescan the repository. The row's states are initializing (0), ready
(1), and reserved (2). A synchronous SQLite transaction reserves a ready row;
the reservation is persisted before sending an R2 mutation. A competing writer
is refused while that reservation is held. Reads remain available, including
when initialization or an uncertain mutation has fenced subsequent writes.
There must be exactly one DO/SQLite owner for each R2 repository prefix.

Writes reserve the entire incoming payload and one key before publication,
including replacements. Repacking therefore needs space for old and new data.
Successful replacement releases the old payload charge; successful deletion
releases its charge. A pack whose index cannot be published remains charged
until explicitly reconciled or removed. No objects are automatically deleted
to fit a lower limit. Quota refusal uses `StorageFailure` with operation
`quota.bytes` or `quota.keys` (HTTP 500 or a receive-pack unpack failure);
an unresolved reservation uses `quota.reservation`. A known quota refusal
releases its reservation, permitting retry after a limit increase. A failed
native mutation or persistence acknowledgement retains the fence rather than
assuming an unacknowledged write or deletion did not happen.

Mutation failures also emit structured signals with component `GitStorage`:
`quota_exceeded` (warning, resource `bytes` or `keys`), `quota_reserved` (warning),
`quota_reconciliation_required` (error), and `mutation_failed` (error).
These events omit repository paths, object names, native causes and credentials.
Alert on repeated capacity refusals, persistent reservations, or either error.
Capacity refusals require headroom review; a reservation can belong to an active
writer, so first wait for the operation to settle. Persistent reservations,
accounting mismatches and uncertain mutation outcomes require the offline
procedure below. A signal is not permission to clear a reservation automatically.
Use deployment/request trace context to correlate the event; the signal itself
does not identify a private repository. Remote alert delivery still needs
deployment-specific configuration and a staging outage rehearsal.

Before adopting these counters, restoring only one storage component, or
returning from any older/external writer, stop every writer and maintenance
task for the prefix. Preserve coordinated R2 and SQLite copies and resolve
every outstanding native operation; process age or a missing response does
not establish its outcome. Verify the retained objects and refs, account for
orphans, and ensure the configured limits accommodate the inventory and next
mutation. Only then remove the affected repository's `git_capacity` row through
the owning storage's administrative interface. The next mutation rebuilds it
from R2 before reserving space. Preserve other repositories' rows. An incomplete
or over-limit inventory remains fenced in state 0 and needs the same offline
procedure; there is no automatic reservation stealing or online reset route.
Keep old writers stopped once reconciliation begins. Restoring SQLite counters
from an unrelated point in time can silently undercount R2 and is unsupported.

The workerd contract exercises retained limits, existing/orphan inventory,
pack deletion, concurrent writers, lost PUT/DELETE responses, persistence failure
before PUT, and offline reconciliation. This is local emulator evidence;
deployed capacity, eviction and coordinated cloud recovery remain release gates.

An atomic ref batch that encounters a filesystem write error tries to restore
the failed update as well as every earlier update. A failed deletion can already
have changed `packed-refs`, so its failure is not evidence that it changed
nothing. Restoration continues after individual failures. If any required
restoration fails, the operation returns `StorageFailure` with operation
`rollback`; it does not return an ordinary all-unapplied batch result.

Cloudflare ref batches use one synchronous SQLite transaction for comparison,
ref mutations and their reflog entries. A SQL exception rolls back the entire
transaction, including earlier writes in the batch; an implicit DO persistence
batch alone does not provide this guarantee when application code catches the
exception. Real-workerd regressions inject failures on the second ref write,
second deletion and second reflog insertion, then verify that every original
ref and reflog entry remains unchanged and that retry succeeds after repair.
This transaction does not include R2 object uploads. Objects already uploaded
before a failed ref batch remain stored and charged to retained capacity.

Cloudflare decodes persisted ref and shallow OIDs, reflog OIDs/timestamps, and
the stored HEAD target before returning them. Malformed values produce
`StorageFailure` with `decode.refs`, `decode.shallow`, `decode.reflog`, or
`decode.head`; they are not reported as missing refs, omitted shallow boundaries,
null reflog entries, or a default HEAD. Missing HEAD alone retains the normal
`refs/heads/main` default. Ref compare-and-swap refuses a corrupt current OID,
and HEAD/shallow mutations refuse corrupt existing metadata before changing it.
These checks preserve evidence; they do not repair it or establish the intended
history. Unaffected records are not globally fenced merely because another
record may be corrupt.

On these errors, stop writers and capture coordinated SQLite/R2 state. Compare
the affected metadata with a trusted backup and known ref tips, then repair it
through the owning storage's administrative interface. Do not turn an unreadable
OID into an absent ref to make a push succeed. Reflog timestamps written by this
backend use canonical JavaScript ISO strings; preserve their original values
when recovering them. Verify history and object connectivity with native Git
after restoring valid metadata, then confirm normal reads and mutations before
reopening writes. The workerd fault tests preserve each corrupt fixture and
verify successful operations after explicit fixture repair; production recovery
still requires coordinated backup evidence.

Cloudflare also bounds retained ref metadata separately from R2 payload.
`GIT_CF_REF_METADATA_BYTES` (default 8 MiB, positive safe integer) limits UTF-8 bytes across every text column in
`refs`, `reflog` and `shallow`, including repeated repository keys and reflog
messages. `GIT_CF_REF_METADATA_ROWS` (default 100,000, integer 1–1,000,000)
limits the combined row count. These
settings exclude HEAD/KV records, administrative tables, SQLite indexes/page
overhead, R2 objects and account-wide storage; they are not a physical database
size ceiling or a measured supported workload.

Before the first mutation, bounded SQL aggregation inventories existing rows.
SQLite triggers then update `git_ref_capacity` within the same transaction as
each ref, reflog or shallow change. Normal mutations use these counters rather
than rescanning all history. A rejected batch rolls back its metadata and
counters together. Existing over-limit data remains readable; new mutations
fail with `StorageFailure` and `quota.metadata.bytes` or `quota.metadata.rows`.
The structured `GitStorage/quota_exceeded` signal uses `metadata_bytes` or
`metadata_rows`. Raising a limit takes effect after the owning runtime restarts.
No history or audit rows are automatically expired. Ref deletion itself writes
a reflog entry and can require additional capacity, so reserve maintenance
headroom instead of assuming deletion will always fit at the ceiling.

Back up the counter table and `git_ref_capacity_*` triggers with the underlying
metadata. Older writers may maintain counters through installed triggers but do
not enforce the new limits or guarantee atomic ref batches: keep them offline
when relying on these bounds. Dropping tables/triggers or restoring them from
different points invalidates accounting. To reconcile, stop every writer, retain
a coordinated copy, resolve the intended metadata using the recovery procedure
above, and remove only the affected repository's `git_ref_capacity` row. The
next mutation rebuilds accounting before making changes. An over-limit inventory
continues to refuse writes until capacity is raised or an explicit, reviewed
offline retention/recovery action reduces it. Administrative metadata and
deployed resource/throughput qualification remain separate release gates.

Cloudflare's SQL remote and webhook registries each enforce
`GIT_CF_REGISTRY_ROWS` (default 128, integer 1–4,096) and
`GIT_CF_REGISTRY_BYTES` (default 1 MiB, positive safe integer) per repository.
Bytes include every stored text column, including repository/name keys,
credentials, signing keys, webhook secrets, serialized standing instructions
and timestamps. The two registries have separate budgets; these limits exclude
SQLite pages/indexes, Artifacts registry/tokens, other KV metadata, other
repositories and account-wide billing. They do not apply to the Node JSON-file
registries.

An addition checks the bounded existing inventory and inserts in one SQL
statement. Concurrent additions cannot both spend the same capacity. Exhaustion
returns `StorageFailure` with operation `quota.registry` (HTTP 500) and emits
`Registry/quota_exceeded` with the fixed registry category, without URLs,
credentials, payloads or repository names. Existing registrations remain
readable, and explicit removal remains available even after a lower limit makes
the existing inventory exceed its budget. Remove unwanted registrations or
raise the limit and restart the owning runtime before retrying; there is no
automatic expiry or deletion. Backups retain the registrations themselves; no
separate reservation ledger needs reconciliation. Older writers do not enforce
these limits, so stop them before relying on the capacity boundary. Deployed
fan-out, memory and latency measurements remain necessary before choosing
production limits.
Lowering a limit does not paginate or shrink existing read results; inspect and
reconcile very large legacy registries offline before reopening normal traffic.

Node's remote and webhook JSON registries use separate settings:
`GIT_REGISTRY_ROWS` (default 128, integer 1–4,096) and
`GIT_REGISTRY_BYTES` (default 1 MiB, positive safe integer). Each file has its
own budget. Bytes count the complete serialized UTF-8 JSON, including property
names, whitespace, credentials and secrets. The Node host validates these
settings before listening; CLI adapters resolve them when opening their layers.
Restart the host or reopen the CLI adapter after changing the settings.

Additions hold the existing exclusive file lock while checking the current
file size, reading its rows and checking the replacement's row/byte totals.
An already oversized file is refused before parsing for an addition. A quota
refusal preserves the original bytes and creates no replacement file; normal
lock cleanup still runs. The error and redacted quota signal are the same as
for SQL registries. Reads and explicit removals remain available after lowering
limits, allowing removal followed by retry. Those operations still read the
whole JSON file: reconcile very large legacy files offline before serving them.
There is no automatic truncation or expiry. Stop older or external writers
that do not obey the lock and limits before relying on this boundary. These
settings exclude Artifacts metadata, Git objects, LFS, filesystem overhead and
aggregate storage across repositories.

The SQLite Artifacts registry also removes a repository row and its credentials
in one synchronous transaction. If either deletion fails, the original identity
and credentials remain intact and the caller receives a redacted
`ArtifactsError` with `STORAGE_FAILURE`. After successful deletion, reusing the
name creates a new identity and does not revive the old credentials.
The registry requires a `TransactionalSql` adapter; a raw statement executor
alone cannot provide this guarantee. Durable Object adapters must forward the
owning storage's `transactionSync`, not simulate transactions with separate
awaited calls.

SQLite token verification, listing and revocation validate persisted token
metadata before using it. Invalid scope, revocation flags, digest or timestamps
produce a redacted `ArtifactsError` with `STORAGE_FAILURE`; an invalid expiration
date must never make a credential indefinitely active. These failures preserve
the stored rows. Stop token writers, retain a coordinated backup and reconcile
damaged rows from trusted evidence before reopening traffic. When the intended
credential state cannot be established, retire the affected credential through
an explicit offline repair and issue a replacement; do not guess a future expiry
or reset its revocation flag. Ordinary revocation refuses malformed rows rather
than silently rewriting them. Timestamps must use the canonical UTC ISO format
written by this adapter.

SQLite repository records also validate their persisted fields before reads or
mutations. A malformed read-only flag must not become a writable repository;
invalid initialization states, timestamps and empty identity/default-branch
fields produce a redacted `STORAGE_FAILURE`. Listing refuses a page containing
an invalid record rather than omitting it. Creation at the same name, deletion,
initialization completion, timestamp updates and branch changes refuse damaged
current metadata and preserve its associated credentials. Repair requires
trusted original identity, read-only and initialization state, not merely a
syntactically valid replacement. Preserve the repository row, credentials and
storage together while performing offline recovery. Existing legacy rows with
no initialization column still migrate to the documented completed (`NULL`)
state; corruption in an existing column is not treated as a migration.

An earlier failed deletion or an inconsistent restore can leave token rows for
a missing repository. Creation now refuses that name with `ORPHANED_TOKENS`
and preserves the rows. Stop registry/token writers, preserve a coordinated
backup, and inspect the affected repository identity and storage before repair.
If the repository was meant to survive, recover its original identity and
matching data from the backup. If deletion was intended and the remaining
credentials must be retired, remove only that repository's token rows through
the owning administrative storage interface before retrying creation. Keep
the preserved evidence under the credential-backup access policy. Do not
silently transfer orphan credentials to a replacement identity. These adapter
guarantees do not replace the provider's lifecycle reservation around token
issuance, repository deletion and recreation.

Treat this error as an unknown batch outcome. Stop writes and maintenance for
the affected repository, preserve its entire storage directory and diagnostics,
and inspect the actual refs with `git show-ref` and object connectivity with
`git fsck --strict`. Compare every requested ref with its recorded pre-request
and intended values before repairing or retrying; successful restoration of one
ref does not establish restoration of the others. Keep the original copy until
recovery has been verified. A missing ref may require a known pre-request tip
or backup; an integrity check alone cannot reconstruct the intended ref names.

Atomic batches that change more than one ref now persist
`.git-plus-ref-transaction.json` in the common Git directory before changing
any ref. The private version-1 record names the worktree and each ref's raw
prior value (`from`) and intended value (`to`). A symbolic prior value retains
its `ref: ...` text. The journal is removed and its directory synced only after
successful completion or successful rollback. An incomplete rollback leaves it
intact. Multi-ref writers serialize journal publication with its `.lock`
reservation after reserving individual refs. Single-ref writes and HEAD/shallow
changes check for a pending journal around their own reservations, preserving
the usual concurrency for unrelated single-ref operations.

While a journal exists, ordinary ref reads, listings, resolution, HEAD reads,
and further ref mutations fail rather than silently serving or extending a
partially applied batch. A malformed journal is also preserved and blocks these
operations. Presence can mean a writer is still active; it is not permission to
remove a lock or declare a crash. Stock Git does not honor this application
journal and can expose partial state, so it must not be used to continue normal
service around the guard.

For offline recovery:

1. Stop all hosts, CLI writers, maintenance, and stock-Git writers sharing the
   repository or its linked worktrees. Verify the writer has exited before
   removing any reservation. Preserve a complete copy, including the journal,
   its locks, objects/packs, and linked storage, before changing anything.
2. Inspect every recorded ref through stock Git and compare its actual raw value
   with `from` and `to`. Validate ref names and the recorded worktree against the
   known installation, especially after relocation. Preserve a malformed record
   for manual investigation;
   do not replace it with an empty transaction. An unexpected third value needs
   investigation rather than an automatic overwrite.
3. Choose one outcome for the whole batch: restore all `from` values or complete
   all `to` values. Verify every chosen object exists and is intact. Remove only
   the confirmed stale reservations for the recorded refs, `packed-refs`, and
   the journal; keep the journal itself throughout repair. Restore symbolic
   values as symbolic refs and null values as deletions, not as literal text.
4. Verify every repaired ref and run `git fsck --strict`. Sync the repaired files
   and containing directories before removing the journal; then sync the common
   Git directory. Reopen through this application and verify the chosen outcome
   before resuming service. Keep the preserved evidence until recovery is accepted.

The crash regression performs this offline rollback after SIGKILL before the
first ref change, following the first ref rename, and after both refs change
but before journal removal. It then
reopens the store, verifies history with Git, and successfully runs another
transaction. Additional tests inject a partial journal write followed by ENOSPC
and a journal file-sync failure; both preserve the original refs and clean up
the temporary file before a successful retry.
The HTTP disk-full regression uses stock Git to attempt a two-ref atomic push
while native filesystem calls inject `ENOSPC` after partial object, ref, or
journal writes, or at object sync. The client sees failure, both prior refs
survive, temporary files/reservations are removed, and Git verifies the surviving
repository. After clearing the injected fault and restarting the Node host, the
same push succeeds and passes `git fsck --strict`. The test also checks that
private fault details do not reach client diagnostics or host output. These are
selected syscall faults, not a physically filled volume or a power-loss test.

A separate HTTP acknowledgment regression starts the real Node host in a child
process with private repository membership. Stock Git completes a two-ref atomic
push and an authenticated LFS upload returns success before the host is killed
with SIGKILL, without graceful cleanup. A new host process serves both accepted
refs and the LFS bytes with their expected SHA-256; anonymous reads stay denied.
Stock Git clones and verifies the recovered history, then completes another
atomic push. No pending transaction journal remains after the acknowledged
batch. This verifies process-death survival of those HTTP writes on the test
filesystem, not power-loss survival or a transaction across all installation
metadata.

The installation-metadata regression separately kills its writer after
repository creation, token issuance/revocation, fork creation, and remote and
subscriber registry writes have returned success. A fresh process verifies the
registries and private file modes, keeps revoked tokens denied even after new
issuance, and refuses deletion of a parent still used by its fork. Stock Git
verifies both repositories and their borrowed history. Later lifecycle operations
work without manually clearing reservations. The verifier then deletes the
child and parent, is killed after those deletions return, and a third process
checks that the empty registry and revoked access survive. Recreating the old
repository name does not revive either old token. This covers acknowledged
metadata operations under process death; it does not simulate a host reset,
filesystem failure, or loss of machine power.

The separate guest-reset rehearsal in `tools/verify-host-reset.ts` uses an
exclusively owned ext4 Docker volume in a disposable Linux VM. `prepare` persists
private test expectations before the last application writes, acknowledges an
HTTP atomic two-ref push, a namespace fork, and an LFS upload, then leaves the
server running with `HOST_RESET_READY`. Force-stop only that VM, restart its
existing volume, and run `verify <new-report.json>` in a fresh process. The
verifier requires a different kernel boot ID, checks both parent refs and the
fork's default branch with stock Git, verifies clone content and LFS digests,
checks token revocation/private permissions and parent-deletion protection, and
requires a subsequent atomic push to succeed. The volume must be mounted at
`/git-reset-rehearsal`; prepare refuses a nonempty volume or non-ext4 filesystem.
Fixture credentials expire after one hour. Never use an existing production
volume or force-stop a shared VM for this rehearsal.

[The retained Linux arm64 report](measurements/host-reset-2026-10-29-linux-node26.json)
records Node 26.13, Debian Git 2.39.5, kernel 6.8, and distinct boot IDs. It
used the dirty working tree based on `54c6199b79d3b121ad7964ac8d0edaeba85b4666`. The
isolated Colima 0.10.3 VZ profile had two CPUs, 4 GiB memory, and a 10 GiB data
disk; the source checkout was mounted read-only. Its Node image ID was
`sha256:bc0a2afd3b74ce12222fc8bab99271e66257f021078f5f4a5d687a77d280c1d9`.
`colima stop git-release-durability-20261029 --force` reported SIGKILL to the VM
driver. Recovery used the same named volume after restarting the guest. The
container, volume, and dedicated VM profile were removed after retaining the
report; the pre-existing default profile was left running. Verification first
exposed upload-pack's flush-only HTTP authentication-probe rejection; the final
clone/retry checks use the fix. Persisted-state checks required no repair.
This is one observed guest-reset recovery, with command latency after the
acknowledgments and the host storage still powered. It is not physical power-loss
qualification, proof of every interruption window, or a hardware write-cache
guarantee.

Atomic Git file replacement failures emit an Effect error log with
`component: git.Node`, `operation: write`, and `event: storage_failure`. This
signal is emitted before unpack/ref handlers translate failures into Git status
reports: a failed push can have HTTP status 200, so HTTP error counters alone
are insufficient. The signal includes no path, object/ref value, or raw cause.
Alert on it and inspect free bytes, free inodes, storage quotas, permissions,
and filesystem health. Pause writes while investigating, preserve any pending
transaction journal, and follow the recovery procedure above if one exists.
After restoring storage, verify integrity and retry the refused operation.

No automatic recovery command is shipped. Recovery is the verified offline
procedure above. Existing-directory retry tests cover failed creation syncs;
they do not establish survival of every physical power-loss window. Do not infer
a power-loss guarantee from these tests.

Empty-tree reads remain virtual for callers that have not written that object,
but creating a commit now writes and syncs the canonical empty tree before the
commit and its ref can be published. A tree write/sync failure refuses the commit,
including when the tree already exists. Earlier versions could acknowledge a
commit whose empty tree was never stored; check existing installations with
stock `git fsck --strict` before an upgrade or backup rehearsal. The corrected
write path does not automatically scan and repair all historical repositories.

## Deployment state and rollback

Alchemy uses local state under `.alchemy/`. Give one deployment identity
ownership of that state and allow only one deployment process per stage at a
time. Back up the state with restricted access before and after a successful
deployment; keep its record of resource identities with the source revision
and artifact manifest. Losing local state does not mean cloud resources were
deleted. Inspect remote resources before recovering state, and do not run
`destroy` or adopt unfamiliar resources as a shortcut.

Use separate staging and production stages, credentials, storage resources,
and state backups. A dry run validates planning only. Promote the exact tested
revision after authenticated Git, hub, LFS, UI, privacy-change, and restart
checks pass in staging. Never enable conformance endpoints or anonymous writes
on the production stack for bootstrap or testing.

Record persistent-schema compatibility for every release. A binary rollback is
valid only when the old version can read the new version's data. For an
incompatible change, restore application and data together from a verified
capture, then reconcile post-capture changes before reopening ingress. Rehearse
old-version data → candidate → supported rollback in staging. Do not infer that
a successful redeploy demonstrates data rollback or recovery of deployment state.
Resolve every pending ref transaction before downgrading: an older binary may
ignore the journal and serve or mutate the incomplete state.

The local source/data rehearsal is
`node_modules/.bin/vp test run --project interop src/server/Upgrade.interop.test.ts`.
It requires commit `54c6199b79d3b121ad7964ac8d0edaeba85b4666` in local Git history;
standalone/release preflight checks that prerequisite and compatibility CI checks
out full history. The test archives that revision into a temporary directory,
uses its actual source in a separate process to create data, runs current source
in a fresh process, then reopens the updated data through the old source after
all writers stop. Each phase checks private-read refusal, authenticated Git
clone/push, LFS bytes, namespace/fork ownership, token state, signed PR creation
and existing PR reads, and stock `git fsck --strict`. The upgrade revokes an old
token and issues a new one; the restarted old process must observe both changes.
It refuses the rollback rehearsal if a ref-transaction journal remains.

This test shares the installed dependency set between source revisions and does
not establish compatibility with an old distributed binary, older dependency
set, or Cloudflare deployment. The baseline is a pre-release revision with known
security/durability gaps: passing data compatibility does not make it an approved
production rollback target. It does not undo schema changes or preserve
post-backup writes automatically. R16 still requires the chosen supported
release artifacts and Cloudflare state to pass the staging upgrade/recovery
rehearsal before production promotion.

The actual local Worker verifier also archives `54c6199` and switches its source
symlink only with the entire runtime stopped. After candidate bootstrap, the
baseline writes a new Git commit; the candidate reopens that data and writes
another commit; the baseline then clones and fscks both before the candidate
resumes. Every phase checks anonymous denial and authenticated Git/LFS content
through the canary probe. A malformed operator artifact distinguishes the
running code versions: the baseline predates that feature, while the candidate
refuses it. This guards against accidentally exercising one cached build in
every phase. Later candidate-only revocation, redaction and complete local-state
restore checks still run.

This is source/data compatibility with shared installed dependencies and current
UI assets, not qualification of old distribution artifacts or old UI behavior.
It deliberately demonstrates an old version without today's provisioning guard;
it must not be interpreted as permission to roll production back to that version.
The CI Worker job fetches full history, and source preflight requires the archived
commit. Production account migrations and supported artifact rollback remain
unverified until the staging rehearsal is performed.
