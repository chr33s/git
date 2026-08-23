# Call stack

A practical map of how requests and commands move through `@chr33s/git`.

This is a control-flow guide, not a module inventory. The important architectural rule is that transport edges call `Repository`; HTTP handlers do **not** reach through it to `ObjectStore` or `RefStore`.

## Mental model

```text
entry point
  -> host / CLI / client adapter
    -> auth + route selection (server paths)
      -> protocol / JSON API / porcelain operation
        -> Repository
          -> Pack / Merge / Maintenance / codecs as needed
          -> ObjectStore + RefStore + PackStore
        -> hooks / post-push delivery
```

`Repository` is the domain boundary. Storage is supplied by layers:

```text
Node host / CLI       -> git/Node.ts        -> filesystem stores
Durable Object        -> git/Cloudflare.ts  -> R2 + DO SQLite
Browser               -> adapters/Opfs.ts   -> OPFS stores
Tests                  -> git/Memory.ts      -> in-memory stores
```

## CLI bootstrap

The installed `git+` binary starts here:

```text
package.json bin["git+"]
  -> src/cli/bin.ts
     -> enableCompileCache(...)
     -> dynamic import("./main.ts")
     -> run()
        -> parseInvocation(...)
        -> process.chdir(parsed.invocation.cwd)
        -> runCoreCompatibility(parsed.invocation)
           -> handled: return
           -> not handled:
              NodeRuntime.runMain(...)
                -> main(parsed.invocation.argv)
                   -> Command.runWith(git, { version })
                      -> selected subcommand handler
```

All commands are collected under the `git` command in `src/cli/main.ts`. Most local repository commands enter a filesystem-backed repository with `withRepo(...)` or `stores(...)`, then call `Repository` methods.

Typical local command:

```text
git+ <command>
  -> src/cli/main.ts command handler
     -> withRepo(root, repo, effect)
        -> git/Node.ts stores(directory)
        -> GitRepository.layer
        -> Repository
           -> read/write domain operation
           -> ObjectStore / RefStore
```

Examples:

```text
git+ log
  -> log command
  -> Repository.resolve / resolveRev
  -> Repository.log
  -> Stream.runForEach(...)

git+ clone
  -> clone command
  -> openStores(target directory)
  -> client/Fetch.ts fetchRepository(...)
  -> smart-HTTP fetch
  -> target ObjectStore / RefStore
```

## `git+ serve` bootstrap

```text
git+ serve
  -> src/cli/serve.ts serveCommand
     -> host/ServeConfig.ts resolve(...)
     -> dynamic import("../host/Node.ts")
     -> host/Node.ts serve(options)
        -> http.createServer(...)
        -> listen(...)
```

The standalone Node host reaches the same `serve(...)` function.

## Node HTTP request

`src/host/Node.ts` is the Node transport adapter. It converts `node:http` requests into Web `Request` objects and keeps request bodies streaming.

```text
node:http incoming request
  -> host/Node.ts http.createServer callback
     -> optional Static.assetResponse(...)
     -> Route.routeOf(url.pathname)
     -> normalize repository path
     -> build streaming Web Request
     -> Auth.guard(request)
        -> guardLayer(repo)
           -> GitRepository.layer
           -> git/Node.ts stores(repo directory)
     -> denied?
        -> yes: deliver denial
        -> no: dispatch(repo, request, deliver, authenticated)
```

Per-repository state is cached by `stateFor(repo)`:

```text
stateFor(repo)
  -> GitRepository.layer
     -> AfterPush hooks
     -> git/Node.ts stores(directory)
  -> HttpRouter.toWebHandler(Api.layer(...))
  -> LFS store
  -> per-repository promise gate
```

The gate serializes handlers for one repository so object writes and ref compare-and-swap updates cannot interleave with another request.

### Node dispatch order

Large streaming bodies are deliberately tested before the JSON API:

```text
dispatch(...)
  -> wait for in-flight response bodies if this request collects objects
  -> per-repository gate
     -> Lfs.handle(request)
        -> matched? return
     -> CommitPack.handle(request)
        -> matched? return
     -> Archive.handle(request)
        -> matched? return
     -> Protocol.handle(request)
        -> matched? return
     -> state.api(request, requesterContext)
        -> Api.layer(...)
```

The response body is delivered outside the serialization gate. Active streaming bodies are tracked separately so `gc` does not delete objects that an upload-pack/archive response is still reading.

## Smart-HTTP routing

`src/server/Protocol.ts::handle` owns the Git smart-HTTP suffixes after the host has already resolved the repository.

```text
Protocol.handle(request)
  -> GET */info/refs?service=git-upload-pack
     -> v2? advertiseV2()
     -> otherwise advertise("git-upload-pack")

  -> GET */info/refs?service=git-receive-pack
     -> advertise("git-receive-pack")

  -> POST */git-upload-pack
     -> uploadPack(request)

  -> POST */git-receive-pack
     -> receivePack(request)

  -> otherwise null
```

## Fetch / clone server path

### Advertisement

```text
Protocol.advertise("git-upload-pack")
  -> Repository.refs
  -> Repository.head
  -> pkt-line advertisement
```

Protocol v2 advertises capabilities first and gets refs later through `ls-refs`.

### Upload-pack

```text
POST git-upload-pack
  -> Protocol.handle
  -> Protocol.uploadPack(request)
     -> PktReader(body(request))
     -> parse wants / haves / shallow negotiation
     -> Repository.contains(...)          # common-have checks
     -> Repository.canServe(...)          # negotiation ready check
     -> planFor(...)
        -> Repository.fetch(...)
        -> optional redaction-aware retry
     -> Repository.packOids(plan.oids)
        -> lazy Stream<Uint8Array>
     -> side-band / pkt-line framing
     -> streaming Response
```

The pack is produced lazily: the response can start before the entire object walk has been materialized.

## Push server path

```text
POST git-receive-pack
  -> Protocol.handle
  -> Protocol.receivePack(request)
     -> PktReader(body(request))
     -> parse RefUpdate commands + capabilities
     -> Policy.uncovered(updates)
     -> Policy.mayWrite("source.push")      # early refusal where possible
     -> Repository.unpack(pack stream)      # object phase
     -> Repository.contains(new tips)       # ensure target objects exist
     -> Policy.gate(allowed, atomic)         # branch / capability policy
     -> Repository.receive(judged.updates)  # ref phase
        -> Hooks.preReceive(...)
        -> Hooks.update(...) per ref
        -> RefStore.apply(...)               # compare-and-swap path
        -> Hooks.postReceive(...)
     -> report-status response
```

The request body is drained even on refusal paths where needed, so Git receives the report instead of seeing the connection terminate mid-push.

## Repository service

`src/git/Repository.ts` is the domain seam used by the CLI, smart-HTTP handlers, JSON API, browser client, and hosts.

```text
GitRepository.layer
  requires:
    ObjectStore
    RefStore
    PackStore
    SearchIndex
    Hooks

  provides:
    Repository
```

Important protocol-facing operations:

```text
Repository.fetch(...)     # object plan for upload-pack
Repository.packOids(...)  # exact lazy pack stream
Repository.canServe(...)  # negotiation cutoff decision
Repository.unpack(...)    # ingest incoming pack
Repository.receive(...)   # hooks + ref update phase
Repository.contains(...)  # object existence checks
```

Other operations fan into focused modules rather than accumulating algorithms in `Repository.ts`:

```text
Repository.merge* / mergeBase -> git/Merge.ts
Repository.gc / fsck           -> git/Maintenance.ts
pack transport                 -> git/Pack.ts
pack-at-rest reads             -> git/PackFile.ts / Packed.ts / PackIndex.ts
codecs + hashing               -> git/Format.ts
history                        -> git/History.ts and repository walks
replay                         -> git/Rebase.ts
bisect                         -> git/Bisect.ts
```

## Storage calls

At the bottom of the domain stack:

```text
Repository operation
  -> ObjectStore
     -> read / write / contains / enumerate object data

Repository ref mutation
  -> RefStore
     -> compare-and-swap apply(...)
     -> reflog / HEAD / refs
```

The backend is selected by the layer composition, not by branching inside `Repository`.

For Node requests the composition is effectively:

```text
host/Node.ts stateFor(repo)
  -> GitRepository.layer
     -> AfterPush hooks
     -> git/Node.ts stores(directory)
        -> ObjectStore
        -> RefStore
        -> PackStore
        -> SearchIndex
```

## Post-push hooks

A successful ref phase stays inside the repository pipeline and then fans out through `Hooks`.

Node host:

```text
Repository.receive(...)
  -> Hooks.postReceive(...)
     -> server/AfterPush.node.ts chain
        -> webhook delivery
        -> configured remote forwarding
        -> optional wake rules
```

Durable Object:

```text
Repository.receive(...)
  -> Hooks.postReceive(...)
     -> Webhooks.service(...)
     -> Sending.service(...)
     -> ctx.waitUntil(...) for detached delivery
```

Post-push delivery is intentionally detached from the push response.

## JSON API path

The JSON surface is declared once in `src/server/Api.ts`; hosts build an Effect HTTP router from that declaration.

Node:

```text
host/Node.ts
  -> stateFor(repo)
  -> HttpRouter.toWebHandler(Api.layer(remotes), ...)
  -> state.api(request, requesterContext)
  -> API handler
  -> Repository operation
```

Durable Object:

```text
GitRepo.fetch(request)
  -> Auth.guard(request)
  -> special streaming routes first
  -> cached HttpRouter.toWebHandler(Api.layer(...))
  -> api(request, requesterContext)
  -> API handler
  -> Repository operation
```

The authenticated requester is request-scoped context, not instance state.

## Durable Object request

`src/git/Durable.ts::GitRepo` is one repository per Durable Object instance.

```text
GitRepo.fetch(request)
  -> Route.routeOf(...)
  -> normalize(request, matched)
  -> Auth.guard(request)
     -> #live(repo)
        -> GitRepository.layer
        -> git/Cloudflare.ts stores({ bucket, repo, storage })
  -> route dispatch
     -> Lfs.handle
     -> CommitPack.handle
     -> Archive.handle
     -> Protocol.handle
     -> cached Api.layer router
```

The Durable Object input gate supplies the per-repository serialization that the Node host implements with its promise chain.

## Client transport

The CLI and browser client reuse the smart-HTTP client modules rather than implementing another Git transport.

```text
clone / fetch
  -> client/Fetch.ts fetchRepository(...)
  -> GET info/refs / protocol-v2 negotiation
  -> POST git-upload-pack
  -> parse streamed pack
  -> target ObjectStore / RefStore

push
  -> client/Push.ts push(...)
  -> GET receive-pack advertisement
  -> build ref commands + pack
  -> POST git-receive-pack
  -> parse report-status
```

The destination stores can be Node filesystem stores or browser OPFS stores depending on the caller.

## Concurrency and lifetime boundaries

```text
Node host
  per repository:
    Promise gate         -> serialize handler critical sections
    delivering Set       -> track lazy response bodies
    cached Repository    -> reuse built layers

Durable Object
  input gate             -> serialize request critical sections
  #delivering Set        -> track lazy response bodies
  instance fields        -> cache repository/API layers
```

Collection (`gc`) is special: it waits for bodies still reading objects. Ordinary requests do not wait for response delivery, so a slow client cannot wedge the repository.

## Streaming boundaries

The intended data path is streaming end-to-end:

```text
incoming HTTP body
  -> Web Request body
  -> PktReader / Stream
  -> Pack decoder
  -> ObjectStore writes

ObjectStore reads
  -> Repository.packOids / archive stream
  -> Response.body
  -> node pipeline / Workers response
  -> client
```

Avoid inserting `arrayBuffer()`, whole-body collection, or an eager object-walk materialization into these paths.

## Useful tracing names

Public and non-trivial operations use `Effect.fn("Domain.operation")`. A trace should therefore read roughly like:

```text
Protocol.receivePack
  -> Repository.unpack
  -> Policy.gate
  -> Repository.receive
  -> <backend RefStore.apply>
```

or:

```text
Protocol.uploadPack
  -> Protocol.planFor
  -> Repository.fetch
  -> Repository.packOids
  -> <backend ObjectStore.read>
```

When debugging a request, start at the host, identify which dispatcher claimed it, then follow the named `Repository` operation down to the selected storage layer.
