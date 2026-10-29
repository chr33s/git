# Documentation

Git+ adds three planes beside ordinary Git history:

```text
WORK
  tasks / pull requests / sessions / decisions

KNOWLEDGE
  OKF-compatible Knowledge Concepts
  bounded Repository Memory

AUDIT
  Invocations
  exact repository context
  runtime telemetry
```

They share the same Git object store, signed identity, capabilities, and repository trust model.

## Start here

1. **[agents.md](agents.md)** — Work: membership, authorization, sessions, tasks, decisions, PR workflow, and Repository Memory.
2. **[knowledge.md](knowledge.md)** — Knowledge: the OKF-compatible `.gitplus/knowledge/` corpus, Git+ provenance extensions, freshness, and bounded Memory projection.
3. **[context-pack.md](context-pack.md)** — Audit/context protocol: Repository Views, typed blob/gitlink evidence, exact ContextRender framing, reachability, and Context Exposure.
4. **[telemetry.md](telemetry.md)** — Audit/runtime protocol: OTel GenAI ingestion, logical Invocations, attempts, capture health, signed `refs/hub/trace/*`, and Flight Recorder projection.
5. **[cli.md](cli.md)** — CLI conventions and common Work / Knowledge / Audit workflows. Detailed syntax comes from `git+ ... --help`.

## Internal layering

The public vocabulary stays small. The audit protocol keeps separate durable records underneath it:

```text
Session
  policy-visible distilled work record

Knowledge
  OKF Markdown + optional gitplus provenance

Invocation
  user-facing audit projection
      │
      ├── Context Exposure
      │     Repository View / Context Pack / ContextRender
      │
      └── Invocation Telemetry
            OTel-normalized runtime facts
```

Storage is split by cost and policy relevance:

```text
refs/hub/session/<session>  distilled provenance / policy-visible
refs/hub/trace/<session>    detailed audit / policy-invisible
refs/notes/hub/memory       bounded recall projection
.gitplus/knowledge/          ordinary versioned OKF-compatible knowledge
```

## External interoperability

**OpenTelemetry GenAI** is the preferred runtime semantic input. Git+ preserves the declared upstream meaning at ingestion and normalizes selected audit facts into stable signed Git records. OTel IDs are correlation metadata, not Git identity.

**Open Knowledge Format (OKF)** is the on-disk knowledge representation. Git+ stores ordinary OKF Markdown/YAML and adds stronger signed citations and evidence under a `gitplus:` extension. OKF editorial metadata never grants Git+ authority.

## Other docs

- **[hub.md](hub.md)** — Git-native collaboration, trust, policy, pull requests, and hub storage.
- **[web-of-trust.md](web-of-trust.md)** — trust and identity.
- **[queue.md](queue.md)** — queue behavior and scheduling.
- **[internals.md](internals.md)** — contributor-facing implementation structure.

## Documentation rule

Put normative interoperability and security rules in the protocol docs. Keep the README and CLI guide on workflows. Do not duplicate evolving OTel/OKF specifications or hand-maintain a second CLI flag schema.
