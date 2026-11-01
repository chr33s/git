# Production release TODO

Remaining work as of 2026-11-01 for `0.1.1`. Scope: Git core, Node and
Cloudflare hosts, browser UI, standalone CLI, distribution and operations.
Only open release gates are listed; IDs are retained for existing references.
Implementation history and local validation records remain in
[docs/measurements](docs/measurements), with procedures in
[docs/operations.md](docs/operations.md).

All ten gates below remain required for the affected production-supported
surface. Local passing tests do not establish deployed-account qualification,
final artifact certification or operator acceptance. Deferring a surface requires
an explicit release-scope decision.

## Decisions needed to proceed

- Identify the Cloudflare account ID and isolated staging name for qualification.
  Supply identifiers, not credentials in chat.
- Decide whether to publish the prepared review snapshot to `chr33s/git` for CI.
  The pending proposal is commit `212bc3dfdf9def589e0a75f65254205fd03d0454` on
  `codex/production-release-review-20261101`; it includes previously staged work.
  Subsequent edits, including this checklist cleanup, are outside that snapshot.
  Confirm the revision to publish before running the final release matrix.
- Select supported OS/architectures, Node versions, browsers and distribution
  channels, including whether npm is in scope.
- Set production workload limits, recovery objectives, backup custody/retention,
  monitoring destinations, support/security commitments and a rollback target.

## Security and capacity

- [ ] **R04 — Qualify outbound network policy on the deployed Cloudflare host.**

  Configure the intended public-routing and trusted-origin restrictions on staging.
  Exercise Git remotes, direct remote operations and webhooks against loopback,
  private/link-local IPv4/IPv6, redirects, DNS changes and cross-origin credentials.
  Verify any intentionally internal destinations require explicit operator
  configuration. Document the deployed policy's guarantees and limitations;
  retain evidence that disallowed destinations and credential forwarding fail
  safely under that policy.

- [ ] **R05 — Establish and publish supported production capacity.**

  Measure representative workloads on each supported host and the target
  Cloudflare account: repository/object/ref counts, Git negotiation and uploads,
  LFS, administrative metadata, concurrent requests and slow clients. Qualify
  account-wide capacity, ingress/rate controls, CPU/memory limits and storage
  accounting. Exercise quota exhaustion, oversized/compressed requests, abandoned
  upload cleanup and recovery while confirming the service remains responsive.
  Publish supported sizes, concurrency, deadlines and operator settings from those
  measurements; do not infer them from local emulator tests.

## Backup and deployed runtime

- [ ] **R09 — Qualify complete production backup and recovery.**

  Exercise the configured remote capture/restore/release/abandonment commands
  against a representative deployed Cloudflare installation and fresh recovery
  storage. Verify isolation of all writers, namespace creation and deployments,
  and capture the complete relevant inventory: Git objects/packs/refs, LFS,
  trust/hub records, administrative state, credentials, SQL/KV and deployment state.
  Include fork/alternates relationships on hosts that support them.

  Verify history and integrity with stock Git, authorization/revocation, LFS
  downloads and application behavior before reopening targets. Rehearse interrupted
  transfers and the largest supported R2 objects on the target account. Define
  backup object-size support explicitly; the current single-PUT ceiling cannot
  support larger objects without a qualified multipart/no-overwrite design.

  Approve and document retention, encryption/access, signing-key and deployment-state
  custody, recovery-point/recovery-time objectives, stale-lock handling, and
  post-restore redaction/revocation procedures. Confirm these procedures and recovery
  objectives on representative Node and Cloudflare installations. Older incomplete
  captures without a signed capture intent require a documented inspection and
  recovery decision; they cannot use automatic abandonment.

- [ ] **R11 — Qualify the actual Cloudflare release candidate on staging.**

  Deploy the real stack to the selected isolated account/stage with production
  settings. Measure actual upload size, CPU/memory use and platform limits.
  Rehearse authenticated initial provisioning, default-deny writes, private reads,
  Git/native authentication, UI deep links/security headers, snapshots and LFS.
  Exercise real DO restart/eviction and storage transitions. Retain results for
  the exact candidate without enabling anonymous writes or conformance endpoints.

## Distribution and CI

- [ ] **R12 — Verify final standalone artifacts on the supported platform matrix.**

  Select the supported OS/architectures and build each final distributable from
  the release revision. Verify the exact archive on clean environments without
  a source checkout or separately installed Node: installation/uninstallation,
  `git+`, remote-helper discovery, embedded UI serving and authenticated
  `git+id://` clone/fetch/push. Confirm external Git/SSH and OS-library prerequisites
  and platform-specific upgrade instructions. Local macOS arm64 and Debian arm64
  rehearsals do not certify other platforms or later artifacts.

- [ ] **R13 — Finish release publication, provenance and signing.**

  Select channels and implement/rehearse publication of the exact tested artifacts
  from the tagged source. Finish attribution review for the shipped dependency
  and patch inventory. Publish integrity metadata and verified publisher
  provenance; apply required platform signing/notarization and revalidate any
  artifact bytes changed by signing. Document consumer verification and rollback.

  If npm is selected, finish its runtime dependency, lifecycle and package-content
  contract and prove a production-only tarball installation. Otherwise explicitly
  keep npm outside the release scope. CI candidate retention is not publication
  of a production release.

- [ ] **R14 — Run and retain the expanded remote release matrix.**

  Publish the approved review revision and run the configured unit, native Worker,
  interoperability and production-browser jobs across the supported runtimes and
  platforms. Resolve failures on that revision and retain exact commit/artifact
  results. Require prerequisites and the strict reporter: missing coverage,
  unexpected skips, failed teardown and unhandled errors must prevent success.
  Confirm that every supported configuration and the final distributables are
  covered; older baseline CI and local results cannot certify later changes.

## Operations and support

- [ ] **R15 — Configure and prove production monitoring and alert delivery.**

  Select collection/retention and alert destinations. Wire application signals,
  readiness checks and platform-limit visibility into the production monitoring
  system. Inject a storage outage, stopped delivery receiver, stale snapshot and
  exhausted quota; prove actionable alert delivery and recovery instructions.
  Include authentication failures, latency/errors, delivery lag and maintenance
  failures. Verify that exported data contains no tokens, private keys, request
  bodies or credential-bearing URLs.

- [ ] **R16 — Rehearse exact-artifact upgrades and supported rollback.**

  Choose the old/new distributables and an approved safe rollback target.
  Rehearse old-version data → release candidate → rollback/recovery on staging,
  including authenticated Git and hub operations. Qualify schema compatibility,
  exclusive deployment execution and restoration of local Alchemy deployment
  state. Assign deployment-state backup ownership and keep staging/production
  separate. Demonstrate recovery when code rollback cannot reverse persisted
  changes, coordinated with the backup procedure in R09.

- [ ] **R17 — Finalize and validate the production support/security contract.**

  Approve support and security-update commitments, vulnerability-reporting policy,
  supported Git features/formats, host/browser matrix and measured limits.
  Finalize operator guidance for TLS/reverse proxies and `GIT_HOSTS`, read-access
  defaults, bootstrap, credential rotation/revocation, sensitive session retention
  and redaction limits.

  Verify browser identity/OPFS persistence and denial behavior, and deployed UI
  security headers on the selected Node and Cloudflare environments. Have a new
  operator follow the published instructions to install, secure, back up, upgrade,
  diagnose and recover a supported deployment. Remove the pre-production warning
  only after the applicable release gates are closed.
