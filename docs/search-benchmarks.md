# Search benchmark record

Measured locally on the development checkout with the built-in benchmarks after
chunked v3 persistence landed (deflate-compressed, lazily restored posting
chunks). These are regression reference points, not capacity limits.

| host                   | corpus                                            | cold build | restart query |                     persisted |        candidates |
| ---------------------- | ------------------------------------------------- | ---------: | ------------: | ----------------------------: | ----------------: |
| Node filesystem        | this checkout's `.git` (329 reachable blobs)      |  105.39 ms |      73.08 ms |     272,939 bytes in 2 chunks | 64 / 329 (19.45%) |
| Chromium OPFS          | generated 100 text blobs                          |   74.30 ms |       8.00 ms | v3 manifest + deflated chunks |  n/a (miss query) |
| workerd Durable Object | generated 100 text blobs, eviction between passes |   45.03 ms |      10.67 ms | v3 manifest + deflated chunks |  n/a (miss query) |

Commands:

```sh
npm run bench:search -- .git
npm run bench:search:opfs
npm run bench:search:durable
```

The first row's candidate ratio is measured by `bench:search`; the restart
benchmarks intentionally use a guaranteed miss to measure index restoration
rather than result projection.

## Size limits vs. measurement

The persisted cache for this checkout is ~270 KB — three orders of magnitude
below the enforced hard limits, so the limits are not yet measurement-driven:

| host           | soft (warn) | hard (memory-only) |
| -------------- | ----------: | -----------------: |
| browser OPFS   |      25 MiB |             50 MiB |
| Durable Object |      10 MiB |             20 MiB |
| Node           |     100 MiB |            250 MiB |

They remain the initial hypotheses from the follow-up plan; revisit them when a
real corpus approaches a soft limit. Posting chunks load on demand at query
time, so the restore costs above are dominated by the blob table, not postings.
