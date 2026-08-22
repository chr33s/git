# Core Git CLI compatibility

Status: proposed

Target: Git 2.55.0 command-line behavior for the supported core surface.

This document specifies how `git+` converges on the command-line contract of
stock Git for core repository work. The goal is not merely that both programs
can read the same repositories. For commands that `git+` claims as core Git
commands, the same invocation should mean the same thing, accept the same
argument shape, produce the same observable result, and be continuously checked
against stock Git.

The executable is still named `git+`, and `git+` may continue to expose hub,
identity, queue, social, session, server, webhook, credential, and other
project-specific commands. Those extensions are outside this compatibility
contract.

Reference documentation:

- https://git-scm.com/docs/git
- https://git-scm.com/docs/gitcli
- https://git-scm.com/docs/git-status

## 1. Compatibility promise

A command is **core-compatible** only when all of the following are true for its
declared supported surface:

1. The command name matches Git.
2. Positional arguments have Git's meaning and order.
3. Supported flags use Git's names, aliases, value grammar, defaults, and
   combination rules.
4. Repository discovery follows Git rather than requiring a `git+`-specific
   repository argument.
5. stdin is consumed using the same byte/text protocol when Git defines one.
6. stdout, stderr, and exit status match Git for deterministic cases, subject
   only to the explicit normalization rules in this document.
7. The resulting refs, objects, index state, work tree, configuration, and
   remote-visible state are equivalent to Git's result.
8. The repository remains usable interchangeably: stock Git must be able to
   consume state written by `git+`, and `git+` must be able to consume the
   corresponding state written by Git.
9. Every supported command grammar branch and flag is represented in the
   differential compatibility suite.
10. The SEA executable and the source CLI are byte-for-byte identical in their
    process-level behavior for the same argv, stdin, environment, and fixture.

Compatibility is a tested property, not an implementation claim.

## 2. Baseline and versioning

The initial baseline is Git 2.55.0. CI SHOULD install or pin that version for the
compatibility job instead of silently following the runner's system Git.

The compatibility manifest records the target Git version. A Git baseline
upgrade is a deliberate change: run the complete differential suite against the
new version, inspect behavior changes, update the manifest and this document,
and land implementation changes together.

`git+ --version` is not required to print Git's version string. Help/version
branding is therefore outside byte-identical differential output. Operational
core command output is not.

## 3. Repository selection and global invocation

Core commands operate on repositories the way Git does. They MUST NOT require a
`git+`-specific positional repository name, `--root`, or `--work` flag.

The v1 global compatibility surface is:

```text
git+ [-C <path>] [--git-dir=<path>] [--work-tree=<path>]
     [-c <name>=<value>] [--bare] [--no-pager]
     <command> [<args>]
```

Repository discovery follows these rules:

- From a work tree, discover `.git` from the current directory and parents as
  Git does.
- In a bare repository, operate on the current repository without a work tree.
- `-C <path>` changes directory before repository discovery and is repeatable
  with Git's semantics.
- `--git-dir` and `GIT_DIR` select the repository directory.
- `--work-tree` and `GIT_WORK_TREE` select the work tree.
- `--bare` disables work-tree assumptions.
- `-c name=value` supplies command-scoped configuration.
- `--` ends option parsing wherever Git uses it to disambiguate revisions,
  paths, and option-looking path names.

The implementation MAY keep `--root` / `--work` temporarily as deprecated
extension aliases during migration, but they MUST NOT be part of the core
compatibility manifest and MUST NOT change the behavior of Git-compatible
invocations.

## 4. Command ownership

A stock Git command name is reserved for Git-compatible behavior.
Project-specific behavior must not occupy that name with a different grammar.

In particular, the existing server JSON-API administration command named
`remote` must move under an extension namespace (for example `hub remote` or
`server remote`) before `git+ remote` is declared compatible. `git+ remote`
then means Git remote configuration and inspection.

Commands with no conflicting stock porcelain meaning, such as `hub`, `id`,
`queue`, `session`, `social`, `task`, `wake`, `serve`, `webhook`,
`credential`, and `credential-helper`, remain extensions.

Convenience commands such as `files`, `refs`, and `history` may remain as
extensions, but they do not substitute for compatibility of the corresponding
Git interfaces (`ls-tree`/`show`, `show-ref`/`for-each-ref`, and `log -- <path>`).

## 5. Core v1 surface

Core v1 is intentionally smaller than all of Git. Once a command is marked
compatible, however, its declared argument/flag surface is normative.

### 5.1 Repository and work-tree basics

#### `init`

Required grammar:

```text
git+ init [-q|--quiet] [--bare] [-b <branch>|--initial-branch=<branch>]
          [<directory>]
```

Required behavior includes work-tree and bare layouts, unborn HEAD, explicit
initial branch, existing-directory reinitialization, exit codes, and Git's
observable messages.

#### `add`

Required grammar:

```text
git+ add [-A|--all] [-u|--update] [-f|--force] [-N|--intent-to-add]
         [--] <pathspec>...
```

Required cases include new, modified, removed, ignored, missing, binary,
symlink, executable-bit, non-ASCII, whitespace, and option-looking paths.

#### `status`

Required grammar:

```text
git+ status [-s|--short] [-b|--branch] [--porcelain[=<v1|v2>]] [-z]
            [-u[<mode>]|--untracked-files[=<mode>]]
            [--ignored[=<mode>]] [--] [<pathspec>...]
```

The default invocation must behave like `git status`; it must not silently mean
`git status --porcelain`.

`--porcelain=v1` is the first machine-output hard gate. With deterministic
configuration its bytes, quoting, XY status codes, path ordering, branch
headers when requested, line endings, and `-z` NUL framing MUST match Git.

#### `commit`

Required grammar:

```text
git+ commit [-a|--all] [-m <msg>|--message=<msg>] [--allow-empty]
            [--amend] [--no-edit] [--author=<author>]
```

Identity, author/committer timestamps, parent ordering, message bytes, tree
selection, and resulting commit object must match Git when the environment is
fixed. A deterministic differential fixture therefore expects the same commit
OID.

#### `rm`

Required grammar:

```text
git+ rm [-r] [-f|--force] [--cached] [--ignore-unmatch]
        [--] <pathspec>...
```

#### `mv`

Required grammar:

```text
git+ mv [-f|--force] [-k] <source>... <destination>
```

#### `restore`

Required grammar:

```text
git+ restore [-s <tree>|--source=<tree>] [-S|--staged] [-W|--worktree]
             [--ours|--theirs] [--] <pathspec>...
```

The default source and destination (index versus work tree) follow Git exactly.

#### `reset`

Required grammar:

```text
git+ reset [--soft|--mixed|--hard] [<commit>]
git+ reset [<tree-ish>] [--] <pathspec>...
```

Mode-specific HEAD, index, and work-tree effects are differential state tests,
not merely output tests.

### 5.2 History and inspection

#### `log`

Required v1 options:

```text
git+ log [-n <n>|--max-count=<n>] [--oneline]
         [--pretty=<format>|--format=<format>] [--reverse] [--all]
         [<revision>...] [--] [<pathspec>...]
```

Default output, commit traversal order, path-limited history, format
placeholders in the supported subset, and empty-history behavior must match
Git. `git+ log` must not require a positional repository argument or a private
`--ref` flag to express ordinary Git revision syntax.

#### `show`

Required v1 options:

```text
git+ show [-s|--no-patch] [--stat] [--name-only] [--name-status]
          [--pretty=<format>|--format=<format>] [<object>...]
```

Blob output is binary and MUST be captured/compared as bytes, never decoded and
re-encoded by the test harness.

#### `diff`

Required v1 grammar:

```text
git+ diff [--cached|--staged] [--stat] [--name-only] [--name-status]
          [<commit>] [--] [<pathspec>...]
git+ diff <commit> <commit> [--] [<pathspec>...]
```

For the supported format the emitted patch bytes are a hard differential gate:
headers, modes, object abbreviations, hunk ranges, no-newline markers, binary
classification, quoting, ordering, and final newlines must match Git.

#### `grep`

Required v1 options:

```text
git+ grep [-n|--line-number] [-i|--ignore-case] [-v|--invert-match]
          [-F|--fixed-strings] [-E|--extended-regexp]
          <pattern> [<tree>...] [--] [<pathspec>...]
```

#### `reflog`

Core v1 requires `git+ reflog` / `git+ reflog show [<ref>]` with Git-compatible
ordering and formatting for the supported default and `--date` subset.

### 5.3 Branches, tags, and history mutation

#### `branch`

Required v1 grammar includes:

```text
git+ branch
git+ branch [--show-current]
git+ branch [-a|--all] [-r|--remotes]
git+ branch <branch> [<start-point>]
git+ branch [-d|--delete|-D] <branch>...
git+ branch [-m|-M] [<old>] <new>
```

Current-branch markers, sorting, detached/unborn behavior, validation, and
failure messages are compatibility surfaces.

#### `switch`

Required v1 grammar:

```text
git+ switch [-c <branch>|--create=<branch>] [-C <branch>]
            [-f|--force] [--detach] [<start-point-or-branch>]
```

Successful output and refusal on local modifications must follow Git rather
than printing a private `ref oid (N file(s))` result.

#### `tag`

Required v1 grammar:

```text
git+ tag [-l|--list] [<pattern>...]
git+ tag [-f|--force] <tag> [<object>]
git+ tag -d <tag>...
git+ tag -a <tag> [-m <msg>] [<object>]
```

Lightweight and annotated tag objects must be interchangeable with stock Git.
Cryptographic signing is out of core v1 unless separately declared.

#### `merge`

Required v1 options:

```text
git+ merge [--ff|--no-ff|--ff-only] [--squash] [--no-commit]
           [-m <msg>] <commit>...
git+ merge --abort
```

Fast-forward behavior, merge-base choice for supported histories, conflicts,
index stages, conflict markers, merge commit parent ordering, and abort state
must be cross-readable and differentially tested.

#### `rebase`

Required v1 options:

```text
git+ rebase [--onto <newbase>] <upstream> [<branch>]
git+ rebase --continue|--abort|--skip
```

Interactive rebase is outside core v1.

#### `cherry-pick`

Required v1 options:

```text
git+ cherry-pick [-n|--no-commit] [-m <parent-number>] <commit>...
git+ cherry-pick --continue|--abort|--skip
```

### 5.4 Remotes and transport

#### `clone`

Required v1 grammar:

```text
git+ clone [-b <branch>|--branch=<branch>] [--bare] [--single-branch]
           <repository> [<directory>]
```

The positional URL/path and optional directory follow Git; there is no private
required clone name argument in the compatible form.

The initial transport target may be the subset already supported by `git+`
(smart HTTP and local repositories if implemented), but unsupported transport
schemes are recorded explicitly in the compatibility manifest.

#### `remote`

Required v1 grammar:

```text
git+ remote [-v|--verbose]
git+ remote add <name> <url>
git+ remote remove <name>
git+ remote rename <old> <new>
git+ remote get-url [--push] <name>
git+ remote set-url [--push] <name> <newurl> [<oldurl>]
```

This operates on repository configuration, not a server JSON API.

#### `fetch`

Required v1 grammar:

```text
git+ fetch [--all] [--prune] [--tags] [-f|--force]
           [<repository> [<refspec>...]]
```

#### `pull`

Required v1 grammar:

```text
git+ pull [--ff-only] [--rebase] [<repository> [<refspec>...]]
```

#### `push`

Required v1 grammar:

```text
git+ push [-u|--set-upstream] [-f|--force] [--force-with-lease]
          [--delete] [--tags] [<repository> [<refspec>...]]
```

Network tests compare advertised/ref outcomes, object reachability, rejection
semantics, exit status, and deterministic stdout/stderr. Pack bytes are not
required to match: compression, delta choice, and object ordering may differ
while representing the same object graph.

### 5.5 Maintenance and utilities

Core v1 also claims these existing names with a deliberately small but
Git-shaped surface:

- `archive`: `--format`, `--output`, `--prefix`, tree-ish, pathspecs.
- `bisect`: `start`, `good`, `bad`, `reset`.
- `fsck`: default verification plus `--full`, `--unreachable`, and
  `--no-reflogs` when supported.
- `gc`: default collection plus `--prune`; exact pack bytes are not a
  compatibility requirement.

Adding more Git flags expands the manifest and therefore expands the required
differential coverage in the same change.

## 6. Observable equivalence

The differential harness evaluates several layers. A test case declares the
strongest layer that applies; weaker layers do not excuse an output mismatch
when Git defines stable deterministic output.

### 6.1 Process equivalence

Capture without text decoding:

```ts
type ProcessResult = {
  stdout: Buffer;
  stderr: Buffer;
  code: number | null;
  signal: NodeJS.Signals | null;
};
```

For deterministic core cases compare:

- stdout bytes,
- stderr bytes,
- exit status,
- signal behavior where relevant.

Do not trim, normalize line endings, call `.toString()` before comparison, or
ignore trailing newlines.

### 6.2 Object equivalence

After a mutating command, enumerate every reachable and relevant unreachable
object in each result repository. Compare object IDs and the canonical object
content (`type`, size, and bytes). When identity and timestamps are fixed,
commit/tag/tree/blob creation should normally produce identical OIDs.

Loose-object compression bytes are not required to match; canonical Git object
bytes are.

### 6.3 Ref and HEAD equivalence

Compare:

- symbolic `HEAD`,
- all refs and peeled targets,
- deletion/nonexistence,
- reflog entries in the declared compatibility surface.

### 6.4 Index equivalence

The primary gate is semantic index equivalence (paths, stages, modes, object
IDs, intent-to-add and conflict stages), plus bidirectional readability by
stock Git and `git+`.

Raw `.git/index` bytes are not a default equality gate because stat-cache data
and valid index extension choices may differ. A command-specific test may make
them a hard gate where the implementation intentionally promises canonical
bytes.

### 6.5 Work-tree equivalence

Compare recursively:

- file bytes,
- executable bit,
- symlink target,
- created/deleted paths.

Filesystem timestamps are not compared unless a Git behavior specifically
uses them in a way visible to the user.

### 6.6 Configuration equivalence

For commands such as `remote`, compare effective local config entries and their
ordering where Git exposes ordering. Stock Git must read changes written by
`git+` without repair or translation.

### 6.7 Transport equivalence

Smart-protocol tests compare protocol semantics and final repository state.
Wire bytes may be asserted when the protocol defines a canonical framing and
both implementations are configured identically. Packfile bytes are explicitly
not required to match.

## 7. Differential test architecture

The compatibility suite runs three programs:

```text
A. stock git 2.55.0
B. git+ source CLI
C. dist/sea/git+
```

For every core case:

1. Construct the fixture once from deterministic declarative data.
2. Materialize equivalent isolated copies for A, B, and C.
3. Run B and C with identical argv, stdin, environment, cwd shape, and fixture.
4. Require B and C process results to be byte-for-byte identical.
5. Run A and B with the same command argv after only substituting the
   executable name.
6. Compare the declared process and state equivalence layers.
7. Where a command mutates repository state, additionally cross-open B's
   result with Git and A's result with `git+` and run invariant probes.

After the CLI is aligned there are no per-command argument adapters in the
Git-vs-`git+` harness. Needing an adapter means the interface is not compatible.

Suggested files:

```text
src/cli/GitCompat.ts                 # manifest/types, no tests hidden here
src/cli/GitCompat.interop.test.ts    # stock Git differential suite
src/cli/Sea.interop.test.ts          # source CLI vs SEA exact process parity
src/testing/Process.ts               # Buffer-preserving spawn harness
src/testing/RepoSnapshot.ts          # refs/objects/index/work-tree probes
```

The stock-Git suite should be an integration/interop project, not a fast unit
test.

## 8. Coverage manifest

The suite maintains a machine-readable compatibility manifest. Conceptually:

```ts
export const coreCompatibility = {
  baseline: "2.55.0",
  commands: {
    status: {
      flags: ["--short", "-s", "--branch", "-b", "--porcelain", "-z"],
      cases: ["clean", "staged", "unstaged", "untracked", "quoted-path", "nul"],
    },
    // ...
  },
} as const;
```

CI enforces both directions:

- every command/flag declared compatible has at least one differential case;
- every Git-compatible command/flag registered in the CLI is present in the
  manifest.

A new supported flag that lacks a test fails with a diagnostic such as:

```text
Missing Git differential coverage:
  status --ignored
  push --force-with-lease
```

Unsupported Git options are listed explicitly per command rather than being
silently forgotten. The manifest is therefore also the precise statement of
how much of Git the current release implements.

## 9. Hermetic environment

Differential tests neutralize machine configuration and nondeterminism. At
minimum each run controls:

```text
LC_ALL=C
LANG=C
TZ=UTC
TERM=dumb
NO_COLOR=1
GIT_PAGER=cat
PAGER=cat
GIT_CONFIG_NOSYSTEM=1
GIT_AUTHOR_NAME=Compat
GIT_AUTHOR_EMAIL=compat@example.invalid
GIT_COMMITTER_NAME=Compat
GIT_COMMITTER_EMAIL=compat@example.invalid
GIT_AUTHOR_DATE=<fixed timestamp>
GIT_COMMITTER_DATE=<fixed timestamp>
```

`HOME`, XDG directories, global Git config, SSH config, credential stores, and
repository paths live under the fixture sandbox. Cases that test configuration
opt in to explicit fixture configuration instead of inheriting the developer's
machine.

Tests use the same locale and path spelling on both sides. When a command
necessarily prints its absolute fixture root, the harness may substitute only
the two sandbox roots with one fixed token before comparison. Normalization is
otherwise prohibited unless documented next to the case with a reason.

## 10. Required edge cases

Every applicable core command is exercised against a shared corpus containing:

- unborn HEAD and empty repository,
- one commit and linear history,
- merge history,
- detached HEAD,
- annotated and lightweight tags,
- staged/unstaged/untracked/deleted paths,
- conflicts and index stages,
- zero-byte and large blobs,
- binary blobs,
- executable files and symlinks,
- non-ASCII filenames,
- spaces, tabs, quotes, backslashes, leading dashes, and newlines in filenames
  where the platform permits them,
- empty and multiline commit messages,
- ambiguous revisions/ref names,
- invalid object IDs and missing refs,
- remotes with fast-forward and non-fast-forward changes,
- stdin that is empty, binary, unterminated, or chunked at awkward boundaries
  for commands/protocols that read stdin.

Platform-specific exclusions must be explicit. Windows path/mode behavior is a
separate compatibility axis, not grounds to weaken Unix cases.

## 11. Failure policy

A differential mismatch is treated as a product bug unless one of these is
true:

1. The flag/behavior is explicitly outside the compatibility manifest.
2. The mismatch is covered by an approved normalization in this document or
   next to the case.
3. Stock Git itself varies across the pinned environments and the test has a
   documented semantic assertion instead.

Do not convert a hard byte comparison into a semantic comparison merely to make
a regression pass.

## 12. Migration from the current CLI

The migration is incremental but the end state is not adapter-based.

### Phase 1 — invocation and harness

- Add `-C`, `--git-dir`, `--work-tree`, `GIT_DIR`, and `GIT_WORK_TREE` handling.
- Make core commands discover the current repository.
- Add the stock-Git differential process/state harness.
- Add source-vs-SEA exact process parity tests.
- Pin Git 2.55.0 in CI.

### Phase 2 — work-tree core

Align `init`, `add`, `status`, `commit`, `rm`, `mv`, `restore`, `reset`, and
`switch`.

Known current differences to remove include the private `--work` selector,
`status` always emitting a private `## <branch>` header, `add` printing staged
paths, `rm` printing removed paths, `mv` printing `from -> to`, and `switch`
printing `ref oid (N file(s))` rather than Git's command output.

### Phase 3 — inspection and refs

Align `log`, `show`, `diff`, `grep`, `branch`, `tag`, and `reflog`, including
Git revision/path disambiguation and `--` handling.

### Phase 4 — mutation

Align `merge`, `rebase`, and `cherry-pick`, with cross-readable conflict/index
state and deterministic commit tests.

### Phase 5 — remotes

Move the incompatible extension meaning of `remote`, implement Git-compatible
repository remotes, then align `clone`, `fetch`, `pull`, and `push`.

### Phase 6 — maintenance

Align the declared `archive`, `bisect`, `fsck`, and `gc` surface and add the
remaining manifest cases.

Deprecated private flags may survive one release as hidden aliases, but core
documentation and examples use only Git grammar. They are then removed.

## 13. Definition of done

Core v1 is complete when:

- every command in section 5 uses Git's invocation shape;
- no core command requires `--root`, `--work`, or a positional repository name
  that Git would not require;
- command-name conflicts with extension behavior are removed;
- every declared command and flag is represented in the compatibility
  manifest and differential tests;
- deterministic operational stdout/stderr and exit codes match pinned stock
  Git at the byte level unless this spec explicitly says otherwise;
- mutating commands produce equivalent refs, canonical Git objects, index
  semantics, work-tree bytes/modes, and config;
- Git can continue from repositories/states written by `git+` and vice versa;
- source `git+` and the SEA executable are byte-for-byte process-equivalent for
  the complete core differential corpus;
- the compatibility and SEA differential suites run in CI and are required to
  pass.

At that point, "Git-compatible core CLI" means a continuously measured contract
rather than a similarity of command names.
