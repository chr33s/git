# CLI Reference & Design Guidelines

This document defines the CLI specification, command reference, and design guidelines for `@chr33s/git` (Universal Git smart-HTTP protocol server, browser client & Unix CLI built on Effect v4 and Web APIs).

---

## 1. Design & Authoring Principles

This documentation structure and command architecture synthesize key design patterns from leading developer documentation standards:

### Stripe Docs Design Principles

- **Developer-First DX & Runnable Snippets:** Every command, parameter, and flag is paired with real-world, executable code snippets and terminal commands.
- **Interactive & Self-Documenting:** Commands support localized interactive execution modes, JSON output for scriptability, and instant local verification without third-party dependencies.
- **Structured Command Hierarchy:** Clear separation between standard Git plumbing/porcelain actions and Hub-level collaboration primitives (identity, trust, pull requests, checks).
- **Rich Status & Error Handling:** Standardized error formats with granular exit codes and action-oriented error output.

### Dropbox Developer Docs Guidelines

- **Explicit Scope & Capability Matrices:** Clear designation of required cryptographic scopes (`source:push`, `hub:check`, `hub:review`, etc.) and authority levels (plain repo vs. genesis-guarded repo).
- **Comprehensive Parameter Tables:** Technical, tabular breakdown of flags, environment variables, argument types, defaults, and necessity.
- **Universal API-to-CLI Parity:** Every CLI subcommand maps 1:1 to the internal `@chr33s/git` Effect `HttpApi` schema and JSON endpoints.

---

## 2. Global Architecture & Options

The `@chr33s/git` CLI (invoked via `git+` or `npx @chr33s/git`) executes the exact same underlying TypeScript core engine across platforms (Node.js, Cloudflare Workers, browser runtime, native CLI).

```text
git+ [global-flags] <command> [subcommand] [arguments] [command-flags]
```

### Global Flags & Options

| Flag        | Short | Type      | Default | Description                                                         |
| :---------- | :---- | :-------- | :------ | :------------------------------------------------------------------ |
| `--help`    | `-h`  | `boolean` | `false` | Display help and usage information for command or subcommand.       |
| `--version` | `-v`  | `boolean` | `false` | Output current CLI engine version.                                  |
| `--json`    |       | `boolean` | `false` | Return structured JSON output to `stdout` for programmatic parsing. |
| `--quiet`   | `-q`  | `boolean` | `false` | Suppress non-essential informational output.                        |
| `--verbose` |       | `boolean` | `false` | Output detailed Effect trace logs and HTTP protocol frames.         |
| `--config`  | `-c`  | `string`  | `""`    | Path to custom repository/hub configuration file.                   |

### Environment Variables

| Variable         | Description                                                                           |
| :--------------- | :------------------------------------------------------------------------------------ |
| `GIT_ROOT`       | Base directory path for repository storage when running server or local operations.   |
| `GIT_AUTH_TOKEN` | Bearer/Capability token for authenticating against remote `@chr33s/git` endpoints.    |
| `SSH_AUTH_SOCK`  | Path to active SSH agent socket for signing cryptographic membership and review logs. |

---

## 3. Core Commands Reference

### `git+ init`

Initialize a new Git repository or convert an existing repository into a cryptographic hub.

```bash
git+ init [path] [--genesis]
```

#### Options

| Flag        | Short | Type      | Default | Required | Description                                                                      |
| :---------- | :---- | :-------- | :------ | :------- | :------------------------------------------------------------------------------- |
| `--genesis` | `-g`  | `boolean` | `false` | No       | Creates `refs/meta/trust/genesis` guarded by your active SSH key membership log. |
| `--bare`    |       | `boolean` | `false` | No       | Create a bare repository without a working directory.                            |

#### Example Usage

```bash
# Initialize standard local repository
git+ init my-repo

# Initialize an agent-first cryptographic hub guarded by SSH authority
git+ init my-secured-repo --genesis
```

---

### `git+ serve`

Start an embedded universal Smart-HTTP protocol server and web interface.

```bash
git+ serve [--port=<port>] [--host=<host>] [--open]
```

#### Options

| Flag     | Short | Type      | Default       | Required | Description                                                                          |
| :------- | :---- | :-------- | :------------ | :------- | :----------------------------------------------------------------------------------- |
| `--port` | `-p`  | `number`  | `8080`        | No       | Port number to bind the HTTP server.                                                 |
| `--host` | `-H`  | `string`  | `"127.0.0.1"` | No       | Network host address to bind.                                                        |
| `--open` | `-o`  | `boolean` | `false`       | No       | Permit write/push operations to un-guarded repos (useful for local scratch testing). |

#### Example Usage

```bash
# Start local server on port 8080
GIT_ROOT=./repos git+ serve --port=8080 --open
```

---

### `git+ clone`

Fetch and clone a repository over Smart-HTTP (v0/v2).

```bash
git+ clone <url> [destination]
```

#### Example Usage

```bash
git+ clone http://127.0.0.1:8080/my-repo ./my-copy
```

---

## 4. Advanced Collaboration & Capabilities

The following subcommands represent features that have been successfully merged into `main` and are now available in the core CLI.

---

### Distributed Agent Collaboration & Review (`git+ pr` / `git+ hub`)

**Status:** Merged (Formerly PR #1: _Agent-Native Collaboration DAG & Event Synthesis_)  
**Scope / Capabilities Required:** `hub:review`, `hub:check`, `hub:merge`

#### Overview

Extends standard Git with an offline-first, append-only event DAG stored natively inside `refs/meta/pull-requests/`. This allows parallel agents and humans to submit code reviews, attestations, and automated checks without relying on centralized API web hosts. All events are SSH-signed and bound to exact commit SHAs.

#### Commands & Syntax

##### `git+ pr list`

List active pull requests, approval statuses, and target branches.

```bash
git+ pr list [--state=<open|closed|all>]
```

##### `git+ pr create`

Open a new pull request by appending a PR genesis event to the DAG.

```bash
git+ pr create --title=<title> --target=<branch> [--body=<body>]
```

##### `git+ pr review`

Cryptographically sign and submit a review attestation (approve, request changes, or comment).

```bash
git+ pr review <pr-id> --status=<approve|reject|comment> [--message=<text>]
```

##### `git+ pr merge`

Perform a compare-and-swap atomic merge against the target ref if all cryptographic checks pass.

```bash
git+ pr merge <pr-id> [--strategy=<squash|rebase|merge>]
```

#### Options Matrix

| Flag         | Type     | Default    | Required     | Description                                        |
| :----------- | :------- | :--------- | :----------- | :------------------------------------------------- |
| `--title`    | `string` | —          | Yes (create) | Title summary of the pull request.                 |
| `--target`   | `string` | `"main"`   | No           | Target branch to merge into.                       |
| `--status`   | `string` | —          | Yes (review) | Review verdict: `approve`, `reject`, or `comment`. |
| `--strategy` | `string` | `"squash"` | No           | Merge strategy for combining commit DAGs.          |

#### Example Usage

```bash
# Create a new agent PR targeting main
git+ pr create --title="feat: Add WebCrypto signature verification" --target="main"

# Submit SSH-signed approval for PR #42
git+ pr review 42 --status=approve --message="Verified Effect v4 schema compatibility"

# Execute atomic compare-and-swap merge
git+ pr merge 42 --strategy=rebase
```

---

### Capability-Scoped Credential Minting (`git+ credential`)

**Status:** Merged (Formerly PR #2: _Delegated Cryptographic Capability Credentials_)  
**Scope / Capabilities Required:** `source:push`, `hub:check:test`, `hub:admin`

#### Overview

Introduces zero-trust, capability-scoped temporary credential generation. Rather than using long-lived personal access tokens or global SSH keys, developers and agents mint short-lived tokens scoped to specific actions (e.g., pushing to a single branch or posting a CI check) and pinned to the repository's SSH key log.

#### Commands & Syntax

##### `git+ credential mint`

Mint a short-lived, capability-restricted delegation token.

```bash
git+ credential mint --capability=<cap> [--ttl=<duration>] [--repo=<repo-id>]
```

##### `git+ credential verify`

Validate a token against a repository's membership authority log.

```bash
git+ credential verify --token=<token>
```

##### `git+ credential revoke`

Publish a tombstone event revoking an agent key or minted credential across the DAG.

```bash
git+ credential revoke --key-id=<ssh-key-fingerprint>
```

#### Options Matrix

| Flag           | Type     | Default | Required     | Description                                                |
| :------------- | :------- | :------ | :----------- | :--------------------------------------------------------- |
| `--capability` | `string` | —       | Yes (mint)   | Scoped capability (e.g., `source:push`, `hub:check:test`). |
| `--ttl`        | `string` | `"1h"`  | No           | Token lifetime validity (e.g., `30m`, `2h`, `1d`).         |
| `--key-id`     | `string` | —       | Yes (revoke) | SSH key fingerprint to retroactively revoke.               |

#### Example Usage

```bash
# Mint a 30-minute token for CI bot restricted to publishing test checks
git+ credential mint --capability="hub:check:test" --ttl="30m"

# Clone and push using minted capability token
git+ clone http://@host/repo --token="mnt_987654321xyz"

# Revoke a compromised agent key retroactively
git+ credential revoke --key-id="SHA256:abc123xyz789..."
```

---

## 5. Output Schemas & Errors

When invoking commands with the `--json` flag, responses adhere to standard JSON structures.

### Success Response Format

```json
{
  "success": true,
  "data": {
    "command": "pr review",
    "prId": 42,
    "status": "approved",
    "signature": "SSH-SIG-SHA256:...",
    "headCommit": "e2a1b0c93710bf3f0a"
  },
  "timestamp": "2026-08-21T11:00:25Z"
}
```

### Error Response Format & Exit Codes

| Exit Code | Meaning             | Description                                                    |
| :-------- | :------------------ | :------------------------------------------------------------- |
| `0`       | Success             | Command executed without errors.                               |
| `1`       | General Failure     | Invalid invocation, flag parse error, or missing repository.   |
| `2`       | Auth / Scope Denied | Key not present in `refs/meta/trust/` log or token expired.    |
| `3`       | DAG CAS Conflict    | Compare-and-swap failed due to target ref moving concurrently. |

```json
{
  "success": false,
  "error": {
    "code": "AUTH_SCOPE_DENIED",
    "exitCode": 2,
    "message": "Key 'SHA256:...' lacks capability 'hub:merge' for target branch 'main'.",
    "hint": "Mint a token with 'hub:merge' or request owner signature."
  }
}
```
