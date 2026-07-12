# Accepted Design Decisions

This document records decisions that define the initial implementation. It is not a chronological meeting log.

## D1. Product mode

**Decision:** Consumer-only command using direct Kafka partition assignment.

**Consequences:**

- no producer code;
- no group subscription;
- no commits;
- no exactly-once claim;
- explicit partitions are required.

## D2. Topic scope

**Decision:** One topic per invocation, one or more selected partitions.

**Reason:** Keeps offsets, formatting, snapshot state, and command-line semantics simple while covering the intended large-topic workflows.

## D3. Record actions

**Decision:** Every input record resolves to drop, tombstone, pass through, or project.

**Reason:** These outcomes are semantically distinct and should not be encoded through special JSON values.

## D4. Cardinality

**Decision:** One input produces zero or one output.

**Reason:** Multiple results complicate counts, ordering, offsets, framing, and downstream interpretation without serving the primary use case.

## D5. Action precedence

**Decision:**

1. input tombstone preservation;
2. drop predicates;
3. tombstone predicates;
4. projection;
5. pass-through.

Repeated predicate options short-circuit in command-line order.

## D6. Tombstone formatting

**Decision:** `%S` emits `-1` and `%R` emits signed big-endian `-1` for a tombstone. `%s` emits no bytes.

**Consequences:** Tombstones remain distinguishable from empty values when length framing is used.

## D7. Projected null

**Decision:** A projection yielding JSON `null` emits the bytes `null`, with length `4`.

**Reason:** Kafka tombstone is transport-level null, not JSON null.

## D8. Language surface

**Decision:** Use explicit CLI options compiled into one restricted jq-inspired program.

**Reason:** Common filtering and projection do not require a general jq runtime.

## D9. Parser implementation

**Decision:** Use a handwritten lexer and Pratt parser.

**Conditions:**

- thorough unit tests;
- positioned errors;
- small grammar;
- no parser generator dependency.

## D10. JSON backend

**Decision:** Begin with `simd-json` behind an interchangeable internal backend boundary.

**Reason:** It is mature, high-performance, and appropriate for large JSON workloads. The boundary preserves the option of a custom path scanner later.

## D11. JSON edge semantics

**Decision:**

- missing is distinct from null;
- boolean operators are strict;
- practical native number representations are sufficient;
- duplicate source keys follow backend effective behavior;
- duplicate projection keys are rejected;
- projection missing values are errors;
- source tombstones bypass parsing.

**Reason:** Keep behavior useful, understandable, and inexpensive without solving uncommon JSON pathology comprehensively.

## D12. Runtime concurrency

**Decision:** Use dedicated threads and bounded crossbeam channels.

**Reason:** The workload combines librdkafka polling, CPU-bound JSON work, and blocking stdout. An async runtime would still require a compute pool and add lifecycle complexity.

## D13. Worker parallelism

**Decision:** Records from the same partition may be processed concurrently.

**Consequence:** A per-partition completion frontier restores source order.

## D14. Ordering contract

**Decision:** Preserve order within each partition by default. Provide no global order across partitions.

**Optional mode:** `--unordered` bypasses partition restoration.

## D15. Count semantics

**Decision:** Count admitted Kafka input records, not emitted records.

**Includes:** source tombstones and records later dropped.

## D16. Snapshot semantics

**Decision:** Capture startup high watermarks as exclusive fixed boundaries and drain all admitted work before exit.

## D17. Backpressure

**Decision:** Bound records and retained bytes globally and per partition. Pause and resume Kafka partitions while continuing event polling.

**Oversized record rule:** admit one record larger than the byte budget only when no other payload bytes are retained.

## D18. Output ownership

**Decision:** The writer is single-threaded.

**Reason:** Prevent byte interleaving and centralize broken-pipe and flush behavior.

## D19. Pass-through fidelity

**Decision:** Pass-through preserves exact source value bytes.

**Consequence:** When mutable JSON parsing is needed and pass-through remains possible, parse a copy rather than mutating the only original buffer.

## D20. Configuration

**Decision:** Rely on librdkafka for protocol, authentication, TLS, and client properties.

**Supported inputs:** broker option, config file, and repeated property options.

## D21. Error policies

**Decision:** Invalid JSON, evaluation errors, and Kafka record errors have separate explicit policies.

**Defaults:** fail.

## D22. Dependency policy

**Decision:** Initial direct dependencies are limited to `rdkafka`, `simd-json`, `clap`, `crossbeam-channel`, and `signal-hook`, unless implementation evidence justifies another.

## D23. Distribution

**Decision:** Publish executable archives, not a package-registry crate.

**Platforms:** Linux AMD64, Linux ARM64, macOS ARM64.

**Assets:** one tar archive and one SHA-256 file per platform.

## D24. Documentation and comments

**Decision:** Keep source comments sparse and meaningful. Tests and project documents carry most behavioral explanation.

## D25. Test philosophy

**Decision:** Tests freeze meaningful behavior and invariants, not coverage percentages.

## D26. Initial defaults

**Decision:** Start with 1,024 global in-flight records, 256 MiB retained payload bytes, and 256 in-flight records per partition. The worker count is `max(1, available_parallelism - 2)` and is capped at 1,024 when explicitly configured.

**Reason:** These are conservative operational defaults, not performance claims. All limits remain explicit CLI configuration.

## D27. String length

**Decision:** `length(string)` counts Unicode scalar values.

**Reason:** This matches user-visible character expectations without adding Unicode segmentation dependencies.

## D28. JSON envelope bytes

**Decision:** Keys, header values, and post-transform payloads use UTF-8 strings when valid and RFC 4648 base64 otherwise. Each byte field has an explicit encoding and byte length. Null uses JSON `null`, null encoding, and length `-1`.

Payload JSON is not embedded as a JSON value. It remains a byte representation so pass-through output is exact and invalid-JSON pass policy has one schema. Field order, tombstones, timestamps, headers, and newline framing are frozen by golden tests.

## D29. Deferred questions

The following are deliberately deferred until implementation or benchmark evidence exists:

- whether to add a custom single-pass path extractor;
- whether unordered mode provides enough measured value to remain public;
- whether implicit default config-file discovery is desirable;
- whether additional platforms should be released;
- whether metadata or offset-query commands belong in a later product scope.

Deferred questions must be resolved in the relevant specification before a stable release makes the behavior contractual.
