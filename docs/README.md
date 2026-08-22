# Documentation

The repository documentation is intentionally split by responsibility so protocol rules, architecture goals, runtime integration, and product behavior can evolve without becoming one brittle specification.

## Agent and provenance docs

Read these in roughly this order:

1. **[agents.md](agents.md)** — existing agent membership, authorization, session provenance, decisions, Memory, and end-to-end workflow.
2. **[knowledge-durability.md](knowledge-durability.md)** — the Capture → Retention → Recall objective, including team-member departure, cited claims, and structured evidence dependencies.
3. **[context-pack.md](context-pack.md)** — normative repository and context-exposure provenance: Repository Views, blob/gitlink evidence, ContextRender, reachability, and Context Exposure records.
4. **[telemetry.md](telemetry.md)** — runtime audit model plus OTel GenAI ingestion, harness integration, API projection, and Flight Recorder UI guidance. Harness-native OpenTelemetry is the preferred capture/correlation input; Git+ normalizes selected observations into signed `refs/hub/trace/*` records.

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

When an incoming signal declares OpenTelemetry GenAI semantic-convention compatibility, the declared convention controls interpretation of that signal. Git+ may normalize its representation into stable fields, but does not redefine the upstream meaning. In particular, one compliant inference span is one logical invocation; automatic provider retries may remain subordinate attempt detail.

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

Normative Git+ wire/storage rules belong in the protocol portions of the docs. OpenTelemetry semantic conventions are treated as versioned external input to a normalization layer rather than frozen into Git+ protocol identity. Architecture/UI guidance should link to those rules rather than restating them, and UI wording should distinguish signed, observed, reported, and derived facts from claims about model cognition.
