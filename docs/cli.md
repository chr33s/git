# CLI reference

This reference is generated from the command tree. `git+ --help` and
`git+ <command> --help` are the canonical interface; regenerate this snapshot
with `npm run docs:cli` after changing commands.

## Top-level commands

`GIT_ROOT`, `PORT`, and `HOSTNAME` configure `git+ serve` when its
corresponding explicit flag is absent. Other local commands use `--root`.

```text
USAGE
  git+ <subcommand> [flags]

GLOBAL FLAGS
  --help, -h                                                          Show help information
  --version, -v                                                       Show version information
  --wizard                                                            Start wizard mode for a command
  --completions <bash|zsh|fish|sh>                                    Print shell completion script (choices: bash, zsh, fish, sh)
  --log-level <all|trace|debug|info|warn|warning|error|fatal|none>    Sets the minimum log level (choices: all, trace, debug, info, warn, warning, error, fatal, none)

SUBCOMMANDS
  add                 Stage paths as they are on disk
  archive             Write a tree as a tar, tar.gz or zip archive
  bisect              Name the next commit to test between a good and a bad one
  branch              List, create or delete branches
  cherry-pick         Replay one commit onto another
  clone               Clone a repository over smart HTTP
  commit              Commit what is staged
  diff                Unified diff between two revisions
  files               List the files a revision's tree holds
  fetch               Fetch a remote's branches and tags into a cloned repository
  fsck                Check every object and ref for damage
  gc                  Drop unreachable objects, optionally repacking
  grep                Search a revision's file contents
  hub                 Repository identity, membership and trust
  id                  Stable principal identity and device rotation
  history             Commits that changed one path
  init                Create an empty bare repository
  log                 Commit history, newest first
  maintenance         Plan or run desired-state repository maintenance
  merge               Three-way merge two revisions
  mv                  Move a tracked path, staging both halves
  pr                  Pull requests: open, review, discuss, check, merge
  pull                Fast-forward one branch from a remote
  push                Push refs to a remote over smart HTTP
  rebase              Replay a branch's commits onto another
  reflog              Where a ref has been: every move, newest first
  refs                Every ref and the object it points at
  server              Server JSON-API administration extensions
  reset               Move a ref, optionally compare-and-swap
  restore             Restore a path from the index or a commit
  rm                  Unstage a path, and delete it unless --cached
  serve               Run the node host over a directory of repositories
  show                Show one object: a commit, tree, tag or blob
  status              Working-tree status in git's porcelain format
  switch              Check out a branch, replacing index and work tree
  session             Record what an agent was told, and what came of it
  social              Social graph: follows, vouches and discovery
  tag                 List, create or delete tags
  task                What needs doing, and who is on it
  queue               Land approved pull requests as one tested batch
  wake                Run local rules for hub events since the last run
  webhook             Administer a server's webhooks over its JSON API
  credential          Mint a short-lived credential stock git can present
  credential-helper   Answer git's credential helper protocol
```
