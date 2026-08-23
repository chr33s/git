# Search

Search is intentionally a reader feature, not a query language. The UI's `⌘K`
search currently combines Tasks from the client-side hub store with code hits
from `grep`; code search is literal and case-insensitive, returns matching
lines, and caps the answer rather than asking a reader to understand regular
expressions.

That contract is good. The implementation can become much faster without
changing it.

This document describes the current code-search path, a Git-native index built
around immutable blob OIDs, and the parts of
[FFF](https://github.com/dmtrKovalenko/fff) worth borrowing. The important
adaptation is that FFF indexes mutable filesystem files while this repository
already owns a stronger identity primitive: Git objects.

## Today

`src/ui/screen.search.ts` chooses `refs/heads/main` (or the first branch when
there is no `main`) and calls `SearchApi.grep(pattern, ref)`. The local client in
`src/ui/local.ts` implements the same surface as the server: resolve the ref,
walk its tree, read blobs, skip unsuitable content, decode text, and look for
the lower-cased literal query line by line. Results are capped at 50 by default.

The UI has a generation counter so an old request cannot replace the result of
a newer query. That protects presentation state, but it does not cancel the old
work: a superseded grep can still finish walking and reading the repository.

The shape is approximately:

```text
query
  │
  ▼
resolve ref
  │
  ▼
walk tree
  │
  ▼
read searchable blob at every path
  │
  ▼
decode + lower-case + scan lines
  │
  ▼
first 50 matches
```

This is a good baseline because it is simple and correct on every storage
backend. Its cost is proportional to the searchable bytes in the selected
revision for every query, including the second, third, and tenth query over the
same content.

## The Git-native observation

A filesystem search index normally has to answer: "what are the contents of
this path now?" Paths are mutable identities. A watcher or rescan must repair
the index after edits, deletes, and renames.

Git separates those concerns already:

```text
commit / ref
    │
    ▼
  tree
    │
    ├── path ──► blob OID
    │
    └── path ──► blob OID
                   │
                   ▼
             immutable bytes
```

The invariant to exploit is:

```text
same blob OID  =>  same bytes forever
```

Index content identity, not file location.

A blob only needs content indexing once. A rename has no indexing cost because
the path changes while the blob OID does not. A commit that changes three files
in a 50,000-file tree introduces at most three new content objects to index;
the rest of the index is reused. Two branches share index work automatically
for every blob they share. Old commits are equally cheap once their blobs have
been seen.

The ref still decides *which paths are in scope*. The blob index only decides
*which contents could match*.

## Proposed shape

Keep four concerns separate:

```text
BlobIndex
  oid <-> ordinal
  bigram -> bitset<blob ordinal>

RefView
  selected commit/ref -> paths and reachable blob ordinals

Verifier
  candidate blob -> exact line matches

Result projection
  matching blob -> path(s) in selected ref
```

`BlobIndex` is derived, content-addressed cache state. It must never become a
source of truth. `Repository` and the object store remain authoritative.

`RefView` does not need to be a permanent index for every historical commit.
For a query it can be produced by the normal tree walk and cached where useful:

```text
path -> blob ordinal
blob ordinal -> path(s)
reachable blob bitset
```

The reverse map matters because one blob can appear at more than one path. Such
a blob should be verified once and its matches projected to every path that
references it.

### Minimal blob index

Start much simpler than FFF's production implementation:

```text
ordinalByOid : Map<Oid, BlobOrdinal>
oidByOrdinal : Oid[]
bigram       : Map<Bigram, BitSet<BlobOrdinal>>
```

A `Bigram` is a pair of normalized bytes. For an indexed blob containing
`repository`, representative keys are `re`, `ep`, `po`, `os`, and so on. Each
key's bitset records the blobs in which that pair occurs at least once.

For a query, intersect its useful bigram bitsets:

```text
"repository"
     │
     ▼
re ∩ ep ∩ po ∩ os ∩ si ∩ it ∩ to ∩ or ∩ ry
     │
     ▼
candidate blob ordinals
```

The index is only a prefilter. A candidate still has to be read and checked by
the exact literal matcher. False positives are fine; false negatives are not.

### Query path

The indexed search path becomes:

```text
query
  │
  ├── normalize / extract usable bigrams
  ▼
intersect blob-index bitsets
  │
  ▼
candidate blob ordinals
  │
  ├── intersect with blobs reachable from selected ref
  ▼
read only candidate blobs
  │
  ▼
exact literal, case-insensitive verification
  │
  ▼
map matches back to path(s)
  │
  ▼
cap / return results
```

The branch/ref filter is what makes this a Git search rather than a global
object-database search. An unreachable blob may remain in the derived index
without affecting answers because it is removed by the selected revision's
reachable set.

### Incremental indexing

Indexing should follow object identity, not filesystem events:

```text
clone / fetch / commit / first search
          │
          ▼
     encounter blob OIDs
          │
          ▼
   already indexed? ── yes ──► reuse
          │ no
          ▼
       read blob
          │
          ├── binary / too large ──► mark non-searchable
          ▼
    extract bigram set
          │
          ▼
      update index
```

There is no rename event to process. There is no "modified file" to update:
a modification creates another blob OID. Deletion only removes reachability
from a tree; it does not require deleting the old blob's search metadata.

That makes the index naturally append-heavy. A rebuild or Git/object GC can
compact unreachable entries later; correctness does not require eager cleanup.

## Correctness rules

Optimization is allowed to make an answer faster, never different.

### Preserve the current search contract

The primary mode stays literal and case-insensitive. Do not make regex or fuzzy
matching implicit in successful searches.

An initial byte bigram index will probably normalize printable ASCII because it
keeps the representation compact and cheap. The current verifier operates on
decoded strings, so a byte prefilter must not silently narrow Unicode behavior.
Use the full scan/verifier path when the query cannot be represented safely by
the index, including at least:

- queries shorter than two useful bytes;
- non-ASCII case-folding the index cannot conservatively model;
- an unavailable, corrupt, or still-warming index.

Likewise, newly discovered but not-yet-indexed blobs must be included in the
verification set. An index that is 90% built may reduce 90% of the work; it may
not hide the other 10%.

### Verify candidates

A bigram index proves only that a blob *might* contain a literal. For example,
a blob may contain all query bigrams in unrelated places. Read the candidate
and run the same exact matcher used by the unindexed path before returning it.

This property is useful operationally: the index can be thrown away at any
time. A bad cache should degrade to a slower search, not a wrong search.

### Keep result ordering intentional

Bitset/ordinal order is an implementation detail and should not accidentally
become user-visible ranking. Project verified matches back through the selected
ref and return a stable order (for example path/line order) unless the product
explicitly introduces ranking later.

## What to borrow from FFF

FFF is useful because it has already explored the repeated-search problem at a
much larger scale. Borrow the ideas that fit this repository's domain; do not
copy its mutable-filesystem architecture wholesale.

### 1. Bigram prefilter — adapt it to blobs

FFF maintains an inverted bigram index and intersects posting bitsets to reject
most files before running the real matcher. It also compresses/filters bigram
columns based on usefulness and has a "skip-1" bigram index to make candidate
sets more selective.

The first idea is the important one here. Start with ordinary consecutive
bigrams over blob OIDs. Only add density heuristics, skip-1 bigrams, SIMD-shaped
layouts, or other tuning after repository benchmarks show the simpler index is
insufficient.

The Git adaptation is materially simpler than FFF's filesystem update model:
immutable blob OIDs replace file watchers, modified-file overlays, and
tombstones for content changes.

### 2. Real cancellation, budgets, and partial answers

FFF treats interactive search as bounded work: it supports cancellation, time
budgets, page limits, and continuation metadata.

The first change here should be cancellation. Thread an `AbortSignal` (or the
Effect equivalent at the domain boundary) through `SearchApi.grep` and the
repository walk so a new debounced query actually stops the superseded one.
The UI's generation counter should remain as the final stale-result guard, but
it should not be the only guard.

After cancellation, useful controls are:

- a maximum result count;
- a maximum per-request work/time budget where a host needs one;
- explicit `truncated` / continuation information rather than silently dropping
  work;
- cancellation checks below the route boundary, especially while walking trees
  and reading blobs.

A partial answer should be labelled as partial. This repository already follows
that principle in the search UI when one side of search is unavailable.

### 3. Exact first, fuzzy only as a fallback

FFF can retry unsuccessful exact searches fuzzily. That is good UX when kept
out of the primary contract.

The safe shape here is:

```text
42 exact matches
  -> show exact matches only

0 exact matches for "repsoitory"
  -> "No exact matches"
  -> optional "Possible matches" from a fuzzy pass
```

Do not mix approximate hits into a successful literal result set. A reader
should always know whether a returned code line actually contains the query.

### 4. Later: richer match metadata

FFF's result model carries useful information such as byte offsets, match
ranges, optional context lines, and a cheap "looks like a definition" bit.
Those are worth considering if search grows into an agent/API surface or the UI
adds previews/highlighting.

They are not prerequisites for the index. Keep the first performance change
focused on avoiding unnecessary blob reads.

## What not to borrow yet

Several FFF features solve problems this repository does not have, or would
prematurely enlarge the search contract:

- **Filesystem watchers and modified-file overlays.** Git object identity makes
  them unnecessary for committed/local Git state; new content means a new OID.
- **A frecency database.** Search currently answers "where is this text?", not
  "which file do I probably want?" Ranking is a separate product decision.
- **Git-status annotations inside the search index.** Status is path/ref state,
  not blob content. Keep it outside `BlobIndex` if the UI later wants it.
- **Regex as an implicit mode.** The current literal contract is deliberately
  simpler for readers.
- **FFF's Rust/Rayon-specific parallel search machinery.** Browser, Node, and
  Workers share this repository's domain. Optimize behind portable ports and
  measure each host rather than importing a runtime assumption.
- **The full skip-bigram/query-decomposition machinery.** It is clever and may
  be valuable later, but consecutive bigrams capture most of the architectural
  win with far less code.

## Where the code should live

Search should follow the repository's existing boundary rule: routes and UI do
not reach through `Repository` into storage to implement domain behavior.

A plausible decomposition is:

```text
src/git/Search.ts
  pure normalization, bigram extraction, candidate-set helpers,
  exact blob verification result types

SearchIndex port / implementation
  derived oid <-> ordinal and posting-bitset cache
  host-specific persistence only if benchmarks justify it

Repository search operation
  resolves ref, walks tree, combines reachability with BlobIndex,
  reads/verifies candidates, returns path + line matches

src/server/Api.ts / src/ui/local.ts
  expose the same repository operation through remote/local clients
```

The exact module names are not a commitment. The boundary is: indexing is a
repository capability over Git objects, not a special-case optimization owned
by `screen.search.ts` or the HTTP handler.

Because the index is derived state, the first implementation can be in-memory
and lazily built. Persistence in OPFS, filesystem cache, R2/SQLite, or another
host store should come only after measuring startup/index cost. A persistent
index needs a version header so representation changes can invalidate it with a
rebuild instead of migration complexity.

## Rollout

Prefer small independently measurable steps.

1. **Cancel stale searches.** Add end-to-end cancellation while preserving the
   current brute-force verifier.
2. **Extract one verifier.** Server and local search should share the exact
   literal matching behavior below the transport boundary.
3. **Add an in-memory blob bigram prefilter.** Index each searchable OID once,
   intersect candidates with the selected ref, then use the same verifier.
4. **Make partial indexing safe.** Unknown/unindexed blobs are always scanned;
   short/unsupported queries fall back to full grep.
5. **Benchmark real repositories.** Record cold index build, warm query latency,
   candidate ratios, memory per indexed blob, and branch-switch cost.
6. **Persist only if useful.** Content-addressed reuse may make a lazy in-memory
   index sufficient for some hosts; OPFS/browser sessions are the most obvious
   place to test persistence.
7. **Add fuzzy suggestions only after exact search is solid.** Keep them visibly
   separate from exact results.

## What success looks like

The search box should still feel like the same feature:

```text
reader types text
  -> literal, case-insensitive matches from the selected revision
```

But repeated searches should stop paying for unchanged content:

```text
                     brute force              blob-OID index
new query            scan revision bytes      filter + verify candidates
rename               scan again               no content-index change
small commit          scan whole new tree      index only new blob OIDs
branch switch         scan other revision      reuse shared blob index
old commit            scan its revision        reuse every seen blob
stale UI query        keeps running            cancelled
```

The architectural goal is not "build a search engine." It is to let Git's
content-addressed model remove work the system already knows is redundant.

## FFF reference and license

FFF is MIT licensed. The design above borrows general ideas, not source code.
If implementation later copies or substantially adapts FFF code, preserve the
required MIT copyright and license notice. Keep any such copied implementation
clearly attributable so the boundary between independent Git-native design and
upstream code remains obvious.
