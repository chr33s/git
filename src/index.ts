/**
 * `@chr33s/git` — portable Git wire protocol, repository, and in-memory
 * working-tree primitives for Effect.
 *
 * Hashing, object and pack codecs, pkt-line framing, storage ports, typed
 * errors, smart-HTTP fetch/push clients, in-memory stores/worktree, checkout,
 * repository operations, diff, search, merge, and path history. Portable — no
 * `node:*` — and the only dependency is `effect`, as a peer.
 *
 * Each namespace is also its own entry point (`@chr33s/git/Pkt`), and the
 * package is side-effect free, so a bundler keeps only what is imported.
 */
export * as Authorize from "./client/Authorize.ts";
export * as Checkout from "./git/Checkout.ts";
export * as Diff from "./git/Diff.ts";
export * as Errors from "./git/Error.ts";
export * as Fetch from "./client/Fetch.ts";
export * as Format from "./git/Format.ts";
export * as History from "./git/History.ts";
export * as Index from "./git/Index.ts";
export * as Inflate from "./git/Inflate.ts";
export * as Memory from "./git/Memory.ts";
export * as Merge from "./git/Merge.ts";
export * as MergeState from "./git/MergeState.ts";
export * as Oid from "./git/Oid.ts";
export * as Pack from "./git/Pack.ts";
export * as PackFile from "./git/PackFile.ts";
export * as PackIndex from "./git/PackIndex.ts";
export * as Packed from "./git/Packed.ts";
export * as Pkt from "./git/Pkt.ts";
export * as Refspec from "./git/Refspec.ts";
export * as Repository from "./git/Repository.ts";
export * as Search from "./git/Search.ts";
export * as Sha1 from "./git/Sha1.ts";
export * as Store from "./git/Store.ts";
export * as Transport from "./client/Transport.ts";
export * as Wire from "./client/Wire.ts";
export * as Work from "./git/Work.ts";
