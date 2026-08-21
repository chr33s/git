# Documentation

The repository documentation is intentionally split by responsibility so protocol rules, architecture goals, implementation guidance, and product behavior can evolve without becoming one brittle specification.

## Agent and provenance docs

Read these in roughly this order:

1. **[agents.md](agents.md)** — existing agent membership, authorization, session provenance, decisions, Memory, and end-to-end workflow.
2. **[knowledge-durability.md](knowledge-durability.md)** — the Capture → Retention → Recall objective, including team-member departure, cited claims, and structured evidence dependencies.
3. **[content-pack.md](content-pack.md)** — normative repository and context-exposure provenance: Repository Views, blob/gitlink evidence, ContextRender, reachability, and Context Exposure records.
4. **[invocation-telemetry.md](invocation-telemetry.md)** — normative runtime audit-trace model: invocation usage, context limits, retries, lifecycle events, tool summaries, workspace transitions, and trace visibility.
5. **[telemetry-integration.md](telemetry-integration.md)** — non-normative harness, CLI, server-projection, and Flight Recorder UI guidance.

The core separation is:

```text
Capture / Retention / Recall goal
        ↓
repository + exposure protocol
        ↓
runtime audit-trace protocol
        ↓
harness / API / UI implementation
```

High-frequency audit provenance is deliberately separate from the policy-critical session DAG:

```text
refs/hub/session/<session>  distilled provenance / policy-visible
refs/hub/trace/<session>    detailed audit trace / policy-invisible
```

## Other docs

- **[hub.md](hub.md)** — Git-native collaboration, trust, policy, pull requests, and hub storage model.
- **[web-of-trust.md](web-of-trust.md)** — trust and identity model.
- **[queue.md](queue.md)** — queue behavior and scheduling.
- **[cli.md](cli.md)** — CLI surface.
- **[internals.md](internals.md)** — implementation structure and contributor-facing internals.

## Documentation rule

Normative wire/storage rules belong in the protocol specs. Architecture notes should link to those rules rather than restating them, and UI wording should distinguish signed/observed/reported facts from derived diagnostics or claims about model cognition.
