# AGENTS.md

## Project overview

trpc-agent-service is a Go single-module service for building a multi-tenant,
node-based Agent deployment platform on tRPC-Agent-Go.

The module path is:

```text
github.com/liuzengh/trpc-agent-service
```

The module requires Go 1.24.1 and pins tRPC-Agent-Go v1.11.2. The service entry
point is `cmd/trpc-service`.

## Engineering principles

Preserve syntax, semantic, behavioral, serialization, persistence, and protocol
compatibility unless the task explicitly requires a documented change.

Prefer minimal, focused changes. Avoid unrelated refactoring and avoid creating
new abstractions before their responsibility and ownership are clear.

New behavior should preserve existing defaults and should be opt-in when
practical.

Platform abstractions should be capability-oriented. Keep
implementation-specific concepts in their owning packages unless their
semantics are genuinely shared across implementations.

Update documentation and examples whenever public behavior, defaults,
configuration, or recommended usage changes.

## Go design conventions

Follow Effective Go and the Go Code Review Comments, while preserving
established APIs when compatibility requires it.

- Use `gofmt` and `goimports`.
- Use short, lowercase, single-word package names, and make exported names read
  naturally with their package qualifier without stutter.
- Use MixedCaps and spell common initialisms consistently.
- Prefer small, consumer-oriented interfaces and concrete constructor return
  types. Do not add an interface for a hypothetical abstraction.
- Make zero values useful when practical; use constructors to establish
  invariants when necessary.
- Pass `context.Context` first when needed, propagate cancellation, and avoid
  storing contexts in structs unless the type owns that lifetime.
- Keep error strings lowercase and without trailing punctuation. Preserve
  causes with `%w` when callers need them; expose inspectable errors only for a
  stable caller contract.
- Make goroutine, channel, resource, cancellation, and shutdown ownership
  explicit.
- Write Godoc for exported declarations as complete sentences beginning with
  the declared name and describing the caller-visible contract.

Do not refactor established public APIs or raise local style comments merely to
apply an idiom when the existing code is clear and compatible.

## Implementation workflow

### Understand the existing design

Before implementation:

- inspect the owning package and adjacent abstraction layers;
- search for existing types, methods, interfaces, options, callbacks, and
  extension points related to the requested capability;
- inspect relevant tests, documentation, examples, and recent design decisions;
- identify compatibility constraints and external implementations; and
- determine the smallest surface required by external consumers.

Do not add a parallel API until the distinction from existing APIs is clear.

### Implement conservatively

Keep implementation details unexported unless external consumers require them.

Prefer extending a coherent existing contract over adding overlapping types or
entry points.

Do not change default, zero-value, nil, error, ordering, cancellation, retry,
persistence, or lifecycle behavior accidentally.

Tests must cover the intended public behavior, meaningful boundary conditions,
and regression cases. Avoid tests that only execute code or assert language
properties without protecting a project contract.

## Public API and service contract design

Treat externally observable behavior and exported APIs intended for external
consumers as long-lived compatibility commitments.

Public surface includes:

- exported types, functions, methods, interfaces, fields, constants, and
  variables documented for external consumers;
- options, callbacks, plugin contracts, and sentinel errors;
- default, zero-value, nil, error, ownership, concurrency, and lifecycle
  behavior;
- JSON and other serialization fields;
- persistence schemas and migration behavior;
- protocol and wire contracts; and
- event ordering, streaming, cancellation, retry, and tool invocation behavior.

### Mandatory second-pass design review

After implementation and before final validation, perform a separate review of
the complete diff for public API and service contract design.

This second pass is mandatory whenever the change adds or modifies public
surface or externally observable behavior.

For every added or changed public symbol, verify:

1. **Export necessity**
   The symbol is required by external consumers and cannot reasonably remain
   unexported.

2. **Package ownership**
   The concept belongs to the declaring package and abstraction layer.

3. **API overlap**
   The symbol does not duplicate or partially overlap an existing type, method,
   option, or extension point without a distinct user-facing contract.

4. **Naming semantics**
   The name represents a stable user-facing concept rather than an incidental
   implementation or deployment detail.

5. **Extensibility**
   The design can support foreseeable variants without parallel APIs,
   duplicated types, or incompatible renames.

6. **Compatibility**
   Existing source, behavior, defaults, serialization, persistence, protocols,
   and external implementations remain compatible unless an intentional change
   is explicitly documented.

7. **Contract completeness**
   Zero values, nil inputs, errors, ownership, mutation, concurrency,
   cancellation, cleanup, and lifecycle behavior are defined where relevant.

8. **Documentation**
   Every exported symbol has meaningful Godoc describing its contract,
   constraints, defaults, and errors where relevant.

9. **Validation**
   Tests exercise the public contract and externally observable behavior rather
   than only implementation details.

If an export cannot be justified, keep it private.

If two public entry points perform substantially the same operation, consolidate
them or establish and document clearly distinct contracts.

### Public API naming and local naming

Public API naming is a service contract design concern. Review exported names
for semantic accuracy, discoverability, package fit, abstraction boundaries,
implementation leakage, and future evolution.

Unexported helpers, local variables, and test names are implementation details.
Do not block a change based only on a personal naming or refactoring preference.
Raise a local naming issue only when the name is misleading, conflicts with an
established convention, or is likely to cause incorrect behavior.

## Service-specific constraints

- Treat the pinned tRPC-Agent-Go v1.11.2 API as the source of truth. Reuse its
  Runner, server, OpenClaw, Session, Memory, Knowledge, Artifact, storage, Tool,
  MCP, Skill, Plugin, Guardrail, Callback, and telemetry capabilities before
  adding a platform abstraction.
- Use `event.Event.IsRunnerCompletion()` for Runner completion and
  `runner.ManagedRunner` for supported cancellation and status operations. Do
  not introduce parallel framework event markers or lifecycle APIs.
- Derive `tenant_id` only from authenticated claims or verified channel
  bindings. Do not trust tenant identity supplied by an external payload.
- Include tenant and application scope in persistence keys, queries, cache
  keys, message and side-effect idempotency keys, object paths, and vector
  filters.
- Restrict in-memory providers to tests and local development. Horizontally
  scaled production workers must use shared state backends.
- Enforce multi-node session ordering and concurrency in the authoritative
  backend, and use stable idempotency keys and a transactional outbox for
  cross-backend propagation.
- Consume Runner event channels until closed, including after context
  cancellation, and make goroutine and shutdown ownership explicit.
- Verify IM callback signatures and deduplicate deliveries. Never expose IM
  tokens, model API keys, database credentials, complete PII, or raw Tool
  arguments in logs, traces, or error reports.

## Language and documentation

Write source-code comments and Godoc in English.

Translated documentation and test data that intentionally verifies localized
behavior are exempt from the English requirement.

Comments should explain contracts, constraints, invariants, non-obvious
behavior, or design reasoning. Avoid comments that merely restate the code.

## Validation

Use validation proportional to the affected packages and risk.

- Build the module with `go build ./...`.
- Test the module with `go test ./...`.
- Run static analysis with `go vet ./...`.
- Run lint with `golangci-lint run --timeout=10m`.
- Check formatting and `any` usage with
  `gofmt -r 'interface{} -> any' -l .`.
- Check imports with `goimports -l .`.

Run targeted tests while iterating and broader validation before delivery.

## Repository-specific caveats

- The repository is a single Go module. Running `go test ./...` from the
  repository root covers all project packages.
- Tests use mocks and should not require external API credentials. Credentials
  are only needed for examples that call external services.
- SQLite-backed providers require CGO and a C compiler when enabled.
- `golangci-lint` and `goimports` may require the Go binary directory to be on
  `PATH`.
