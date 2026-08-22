# Documentation

The repository documentation is intentionally split by responsibility so protocol rules, architecture goals, runtime integration, and product behavior can evolve without becoming one brittle specification.

## Agent and provenance docs

Read these in roughly this order:

1. **[agents.md](agents.md)** — existing agent membership, authorization, session provenance, decisions, Memory, and end-to-end workflow.
2. **[knowledge-durability.md](knowledge-durability.md)** — the Capture → Retention → Recall objective, including team-member departure, cited claims, and structured evidence dependencies.
3. **[content-pack.md](content-pack.md)** — normative repository and context-exposure provenance: Repository Views, blob/gitlink evidence, ContextRender, reachability, and Context Exposure records.
4. **[invocation-telemetry.md](invocation-telemetry.md)** — normative runtime audit-trace model. Harness-native OpenTelemetry is the preferred capture/correlation input; Git+ normalizes selected observations into signed `refs/hub/trace/*` records.
5. **[telemetry-integration.md](telemetry-integration.md)** — non-normative OTel-first harness, OTLP ingestion, fallback-hook, server-projection, and Flight Recorder UI guidance.

The core separation is:

```text
Capture / Retention / Recall goal
        ↓
repository + exposure protocol
        ↓
OTel runtime capture / correlation
        ↓
Git+ durable audit projection
        ↓
harness / API / UI implementation
```

OpenTelemetry does not replace Git-native provenance:

```text
Context Pack / ContextRender
  exact repository evidence + exposure commitment

OTel
  runtime spans / events / logs + trace correlation

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

## Documentation rule

Normative Git+ wire/storage rules belong in the protocol specs. OpenTelemetry semantic conventions are treated as versioned external input to a normalization layer rather than frozen into Git+ protocol identity. Architecture notes should link to protocol rules instead of restating them, and UI wording should distinguish signed, observed, reported, and derived facts from claims about model cognition.
