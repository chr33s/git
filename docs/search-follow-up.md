# Search follow-up implementation pass

> **Status: landed.** Every scope item below shipped; measurements are in
> [`search-benchmarks.md`](search-benchmarks.md). The host size limits remain
> the initial hypotheses — no measured corpus has approached a soft limit yet.

This pass completes the operational hardening and optional fuzzy fallback left
outside the initial Git-native literal-search implementation.

## Scope and order

1. **Durable Object persistence flag**
   - Add `SEARCH_PERSISTENCE`, default `false`, to the Worker/Durable Object
     environment contract.
   - Build the persistent `SearchIndex` layer only when enabled; otherwise
     provide `Search.memory`.
   - Keep OPFS persistence enabled and Node persistence behind
     `git+ serve --search-persistence`.

2. **Persistence regression coverage**
   - Add real-OPFS tests covering index snapshot write, a fresh layer reload,
     checksum corruption, and version mismatch falling back to a full scan.
   - Add workerd Durable Object tests covering the same path across
     `evictDurableObject`.
   - Assert answers match the unindexed verifier, not merely that a snapshot
     key exists.

3. **Request cancellation**
   - Establish the HTTP request-abort signal/equivalent at the server boundary.
   - Thread it into `Repository.search` and verify tree walking/blob reads stop
     after disconnect.
   - Retain the UI generation guard as presentation protection; it is not the
     cancellation mechanism.

4. **Fuzzy fallback contract**
   - Extend `GrepRequest` with optional `fuzzy: boolean`, default `false`.
   - Extend `GrepResponse` with optional `suggestions`.
   - Exact matches remain `matches`; never mix approximate hits into them.
   - Run suggestions only when exact search returned zero matches and fuzzy was
     requested.
   - Define `FuzzyMatch` as `path`, `line`, `text`, matched character ranges,
     and score. Cap at 20 and tie-break by path then line.

5. **Fuzzy implementation**
   - Keep literal indexed search as the first pass.
   - Use a bounded subsequence matcher only for the fallback candidate pass.
   - Score contiguous runs above fragmented runs, then shorter spans.
   - Preserve ref/path scope, file-size limits, binary skips, cancellation, and
     stable ordering.

6. **Budgets and continuations**
   - Add explicit request fields for maximum results and bounded work/time.
   - Return `truncated` plus an opaque continuation only when the answer is
     partial; never silently drop unvisited work.
   - Define continuation scope precisely: pattern, ref, path, matching mode,
     and index/version assumptions must remain stable or the continuation is
     refused/restarted.
   - Check cancellation and budget consumption while walking trees, loading
     blobs, and verifying lines.

7. **Compression and chunking**
   - Replace monolithic snapshots with a versioned manifest plus independently
     checksummed blob-table and posting-list chunks.
   - Keep chunk identity, ordinals, posting layouts, codecs, and manifests
     private to each host; the HTTP contract remains `pattern + ref -> hits`.
   - Use OPFS files in the browser, filesystem files on Node, and Durable
     Object storage values or SQLite rows on Workers.
   - Write chunks first and publish the manifest atomically last. A missing,
     corrupt, unknown-version, or quota-limited chunk must become an unknown
     verifier-scanned blob, never a negative match.
   - Start with a manifest carrying format version, codec, chunk target size,
     checksums, compressed sizes, and blob/posting chunk ranges.
   - Set and measure host-specific soft/hard limits before enabling broadly:
     OPFS 25/50 MiB, Durable Object 10/20 MiB, Node 100/250 MiB are initial
     hypotheses, not defaults. Compact reachable entries after GC; beyond the
     hard limit, retain only in-memory indexing.
   - The primary benefit is incremental persistence: indexing a few new blobs
     rewrites only their chunks instead of a whole snapshot, while startup can
     load only postings needed by the query.

8. **UI**
   - Request fuzzy suggestions only after an exact empty result.
   - Render a separate “Possible matches” section.
   - Clearly label it approximate and use ranges for highlighting when present.

## Acceptance criteria

- A corrupt, missing, or old index cannot alter exact answers.
- A cancelled search stops below the UI/route boundary.
- Exact results never contain fuzzy suggestions.
- OPFS and enabled Durable Object indices survive a restart/eviction.
- `npm run check`, `npm run test:unit`, and `npm run test:integration` pass.
- Re-run `bench:search`, `bench:search:opfs`, and `bench:search:durable` and
  record host, corpus, restart, snapshot, and candidate-ratio measurements.
