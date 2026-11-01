/**
 * The object store as a resource value. Its own module because both the
 * stack and the Durable Object that binds it need it, and the DO importing
 * the stack would be a cycle.
 */
import * as Alchemy from "@alchemy.run/cloudflare";

// Alchemy reconciles omitted lifecycle rules to an empty policy. Keep the
// incomplete-upload fallback explicit, without expiring completed Git/LFS data.
export const lifecycleRules: Alchemy.R2.BucketLifecycleRule[] = [
  {
    id: "abort-incomplete-uploads",
    enabled: true,
    prefix: "",
    abortMultipartUploadsTransition: {
      condition: { type: "Age", maxAge: 7 * 24 * 60 * 60 },
    },
  },
];

/** Git objects and LFS payloads. One bucket, prefixed per repository. */
export const Objects = Alchemy.R2.Bucket("git-objects", { lifecycleRules });
