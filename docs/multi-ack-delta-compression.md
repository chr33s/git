# Multi-ack & delta compression — design sketch

> **Status:** implemented on this branch, with one deliberate narrowing.
> `multi_ack_detailed` landed on both sides plus the honest v2 `ready`
> (`Repository.canServe`). Delta creation landed as `createDelta` and an
> ofs-delta window behind `PackOptions.deltify`, enabled in
> `Maintenance.repack` only — live fetch responses stay full-object, and
> thin packs remain unbuilt, until measurement shows the wire savings pay
> for serve-time delta search.

The two protocol items still on the [artifacts branch](https://github.com/chr33s/git/tree/artifacts)
roadmap, explained against the code as it stands and sketched against its
seams. File references below are to the artifacts branch. Neither item changes
which objects arrive — both change how many bytes and round trips it takes,
which is also what makes each safe to land incrementally: every intermediate
state is a valid dialect of the protocol.

## Where the branch stands

Smart-HTTP v0 and v2, shallow in all three spellings, `side-band-64k` both
directions; stock `git` clones, pushes, and fetches. Both deferrals left
receipts:

- `server/Protocol.ts:13` — "Deliberately not advertised: `multi_ack` (a
  stateless round-trip either concludes with `done` or restarts, so the client
  sends everything it has and the worst case is a larger pack, never a wrong
  one)."
- `git/Pack.ts:18` — "Writing emits full objects only, no deltas: valid by the
  format, larger on the wire, and enough for upload-pack until delta
  compression pays its way."

Delta _application_ is complete (ofs-delta, ref-delta, thin packs, verified
against real `git repack` output). Missing is the other direction of each
item: acknowledging more than one common commit, and producing deltas.

## multi_ack_detailed

### Explain

Negotiation finds the common base — the newest commits both sides share — so
the pack can be cut there. Today both sides speak baseline single-ACK v0: the
client offers haves newest-first, 32 per round, cap 256 (`client/Fetch.ts`),
and the server ACKs the first have it holds (`server/Protocol.ts:256`). One
ACK ends the offering: the client sends `done` and takes the pack.

The failure mode is a client with several branches. The first common commit
closes the conversation, bases on the other branches are never offered, and
everything reachable only from an unoffered base is re-sent.
`multi_ack_detailed` fixes both dimensions: the server tags _every_ common
have (`ACK <oid> common`) so the client keeps offering, and says
`ACK <oid> ready` once it can prove a pack is cuttable — usually fewer rounds
than walking the cap down.

```
today — single ack                 with multi_ack_detailed
------------------                 -----------------------
C: want 4f3a…  (no caps)           C: want 4f3a… multi_ack_detailed
C: have ×32                        C: have ×32
S: NAK                             S: ACK 7d02… common
C: have ×64  (prefix repeats)      S: ACK 88aa… common
S: ACK 7d02…                       S: NAK            (keep offering)
C: done                            C: have ×64
S: ACK 7d02… + PACK                S: ACK 91bc… common
                                   S: ACK 91bc… ready
   bases on other branches         C: done
   never offered → fat pack        S: ACK 91bc… + PACK
                                      pack cut at every common base
```

Two things make this smaller than it looks. The client already parses the
richer dialect defensively (`client/Fetch.ts:284` reads `continue` and
`ready`) — it just never requests the capability. And the v2 handler already
answers per-have ACKs plus `ready` (`server/Protocol.ts:464`); its gap is
declaring `ready` as soon as _any_ common commit exists, which ends
negotiation with the same possibly-too-small base set. The genuinely new
piece is one honest predicate shared by both paths.

### Sketch

1. **Predicate.** `Repository.canServe(wants, common)` — git's
   `ok_to_give_up`: walk each want backwards, stopping at common or visited;
   true iff every walk bottoms out in common. Bound the walk (a few thousand
   commits); over budget returns false — another round, never wrong.
2. **Server v0** (`server/Protocol.ts`): advertise `multi_ack_detailed`; parse
   it off the first `want` where `side-band-64k` parses today. Round without
   `done`: `ACK <oid> common` per held have, `ACK <last> ready` when
   `canServe`, then `NAK`. The `done` round keeps its shape.
3. **Server v2:** replace the eager `ready` with the predicate.
4. **Client** (`client/Fetch.ts`): request the capability when advertised
   (derive the empty `CAPABILITIES` from the advertisement); widen
   `acknowledged` to `{ common, ready }`; offer rounds until `ready` or
   exhaustion. Stateless prefix repetition already works.
5. **Proof.** Interop: stock `git -c protocol.version=0 fetch` under
   `GIT_TRACE_PACKET`, asserting `common`/`ready` appear and the round count
   drops; plus a pack-size assertion — two diverged branches, incremental
   fetch, no object reachable from any offered have lands in the pack.

## Delta compression

### Explain

A pack object is full (deflated bytes) or a delta — a copy/insert program
against a base: `ofs-delta` (base named by backward offset in the same pack)
or `ref-delta` (base named by oid; what makes thin packs possible). Read side
complete: `applyDelta` (`git/Pack.ts:212`), both kinds in `unpack`, thin-pack
bases resolved from the store (`git/Pack.ts:289`). Write side: full objects
only.

One writer, three call sites, so the item pays three times:

| call site                           | what gets smaller                    |
| ----------------------------------- | ------------------------------------ |
| `git/Repository.ts:1064` `packOids` | every fetch and clone response       |
| `git/Maintenance.ts:200` repack     | packs at rest in R2 / DO storage     |
| `client/Push.ts` (same writer)      | every push, including from a browser |

### Sketch

1. **Codec.** `createDelta(base, target)` beside `applyDelta` — the same
   vocabulary in reverse. Fingerprint the base in 16-byte blocks into a hash
   map; greedy-scan the target emitting copy ops (≤64 KiB, git's flag-byte
   encoding) and literal inserts (≤127 bytes). Return `null` past ~90% of
   target size — a delta that barely wins isn't worth a chain link.
2. **Plan metadata.** `packOids` gets bare oids, but delta ordering needs
   type, path, size. The closure walk in `Repository.fetch` visits tree
   entries with names in hand — extend plan entries to
   `{oid, type, pathHash, size}`. The only cross-module change.
3. **Window.** Sort candidates by type · path-hash · size descending so
   successive versions of a file are neighbours; try `createDelta` against the
   last W=10 same-type candidates; emit whichever spelling is smaller. Chain
   depth ≤50; objects over ~1 MiB skip deltification — bounding resident
   memory near W×1 MiB, comfortable in a 128 MiB Durable Object and a browser
   tab. All three knobs live in `PackOptions`, so repack can press harder than
   a live fetch.
4. **Emission.** Prefer `ofs-delta` — the writer already tracks emission
   offsets for the `.idx`, which is exactly what `ofs-delta` needs.
   `ref-delta` is reserved for thin packs: a fetch response deltas against a
   base in the client's common closure once the client advertises
   `thin-pack`. This is where the two items compose — an accurate common set
   from `multi_ack_detailed` is exactly the base set a thin pack deltas
   against. Storage repack stays self-contained, never thin.
5. **Proof.** The interop harness already reads git's deltified packs; add the
   inverse: our deltified pack through `git index-pack --strict` and
   `git verify-pack -v`, asserting deltas present, chains ≤50, every object
   byte-identical. The `.idx` writer needs nothing — crc32 covers stored
   bytes, whichever spelling.

## Order of work

`multi_ack_detailed` first: two files plus one predicate, the client already
half-speaks it, and the win is observable with nothing but
`GIT_TRACE_PACKET`. Delta compression second, in three increments that each
stand alone: the codec with its own tests, the window behind `PackOptions`,
then thin packs last — which depend on the negotiation work anyway for an
honest base set.
