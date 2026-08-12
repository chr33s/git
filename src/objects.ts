/**
 * The object store as a resource value. Its own module because both the
 * stack and the Durable Object that binds it need it, and the DO importing
 * the stack would be a cycle.
 */
import * as Alchemy from "alchemy/Cloudflare";

/** Git objects and LFS payloads. One bucket, prefixed per repository. */
export const Objects = Alchemy.R2.Bucket("git-objects");
