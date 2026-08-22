# Documentation

The repository documentation is split by responsibility so protocol rules, knowledge architecture, runtime integration, and product behavior can evolve without becoming one brittle specification.

## Agent and provenance docs

Read these in roughly this order:

1. **[agents.md](agents.md)** — existing agent membership, authorization, session provenance, decisions, bounded Repository Memory, and end-to-end workflow.
2. **[knowledge-durability.md](knowledge-durability.md)** — the Capture → Retention → Recall architecture, including Durable Knowledge Concepts, structured evidence dependencies, freshness, Memory projection, and optional Open Knowledge Format (OKF) interoperability.
3. **[context-pack.md](context-pack.md)** — normative repository and invocation-exposure provenance: Repository Views, typed blob/gitlink evidence, semantically framed ContextRender, Git reachability, and Context Exposure records.
4. **[telemetry.md](telemetry.md)** — runtime audit model plus OTel GenAI ingestion, signed `refs/hub/trace/*` storage, harness integration, API projection, and Flight Recorder UI guidance.

The core layering is:

```text
signed sessions / decisions / repository evidence
        │
        ↓
Durable Knowledge Concepts
        │
        ├──────────────→ bounded Repository Memory
        │
        └──────────────→ task-specific retrieval
                                │
                                ↓
                           Context Pack
                                │
                                ↓
                         Context Exposure
                                │
                                ↓
                   OTel runtime correlation
                                │
                                ↓
                    Git+ durable audit trace
```

The layers intentionally answer different questions:

```text
session / decision
  what happened, and who said or did it?

Knowledge Concept
  what reusable thing does the repository currently claim to know?

Repository Memory
  what small set of current knowledge should every session receive cheaply?

Context Pack / Context Exposure
  what exact Git-grounded repository context crossed this invocation boundary?

Telemetry
  under what observable runtime conditions did the invocation occur?
```

## Knowledge interoperability

Durable Knowledge Concepts use boring, portable Markdown + YAML frontmatter and may be made compatible with the Open Knowledge Format (OKF):

```text
https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md
```

OKF is an optional interchange target, not a Git+ trust or protocol dependency.

Useful OKF ideas such as stable source IDs, generated-versus-verified metadata, lifecycle, temporal freshness, per-claim attribution, and progressive-disclosure indexes can round-trip while Git+ retains stronger repository semantics under namespaced metadata:

```text
Git record commit OIDs
signed provenance
blob/gitlink dependencies
repository trust/capabilities
Context Exposure
```

Imported knowledge is data. OKF `verified` metadata, a human-reviewed label, or any other imported field does not grant repository membership, capability, instruction authority, or policy authority.

## Repository provenance versus runtime observability

OpenTelemetry does not replace Git-native provenance:

```text
Context Pack / ContextRender
  exact repository evidence + exposure commitment

OTel GenAI
  runtime operation semantics + trace correlation

refs/hub/trace/<session>
  selected normalized signed audit facts
```

High-frequency audit provenance remains separate from the policy-critical session DAG:

```text
refs/hub/session/<session>  distilled provenance / policy-visible
refs/hub/trace/<session>    detailed audit trace / policy-invisible
```

The Git+ audit path should not silently inherit ordinary observability sampling or filtering. A normal OTel backend may use sampling, transforms, aggregation, and shorter retention; a Git+ deployment claiming durable runtime audit should ingest audit-relevant signals through a loss-intolerant branch or surface the resulting trace as partial/transformed.

## Other docs

- **[hub.md](hub.md)** — Git-native collaboration, trust, policy, pull requests, and hub storage model.
- **[web-of-trust.md](web-of-trust.md)** — trust and identity model.
- **[queue.md](queue.md)** — queue behavior and scheduling.
- **[cli.md](cli.md)** — CLI surface.
- **[internals.md](internals.md)** — implementation structure and contributor-facing internals.

## Documentation rules

Normative Git+ wire/storage rules belong in protocol specs. Architecture notes should link to those rules instead of restating them.

OpenTelemetry semantic conventions and OKF are versioned external interoperability surfaces. Git+ should preserve their declared meaning at ingestion/import boundaries without freezing their evolving field names into Git-native protocol identity.

Documentation and UI wording should distinguish:

```text
signed
observed
reported
derived
editorially verified
repository-evidence current
temporally fresh
```

from stronger claims about truth, authority, cognition, or causation.
