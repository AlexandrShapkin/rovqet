# AI agent operating instructions

Applies to the whole rovqet repository. These are execution rules, not a request to manufacture process overhead.

## Authority and context

Read \`docs/requirements/BROKER_REQUIREMENTS_v1.1.1.en.md\` (normative), \`docs/architecture/README.md\` (design index), the target component document, \`docs/architecture/16-mvp-scope.md\` (prototype exceptions), and \`docs/roadmap/README.md\` (execution order). The historical change register is explanatory, never normative.

Precedence: (1) durability/correctness and failure semantics in normative requirements, (2) approved architecture interfaces and state machines, (3) roadmap task contracts, (4) local implementation choices. If prototype mode relaxes production security or operations, label the deviation and keep it strictly isolated; do not silently claim production compliance.

## Autonomy: development over bureaucracy

- Start a bounded ready work package without asking for administrative permission. Prefer directly committing to \`main\` when the working head is current and tests pass. PRs are optional transport, **never** mandatory review gates.
- Do not create approval flows, sign-off requirements, CODEOWNERS gates, change-control tickets, release jobs on each commit, sprawling issue hierarchies or full-suite CI on every edit.
- No standing obligation to get independent AI approvals. Targeted second-pass review is useful for proof-critical WAL, Raft, ACK/cursor and wire behavior, but does not create a human approval requirement.
- Complete the task, including minimal tests and documentation updates. If blocked, report the exact blocker and smallest concrete next action; do not feign completion.
- Parallel agents can own disjoint crates/docs; avoid concurrent edits to the same file, incompatible changes to shared interfaces, and force pushing main. Rebase/read current main before writing.
- No speculative platform, plugin framework, abstraction layer or deployment plumbing before its roadmap dependency requires it.

## Architecture-first invariant

Do not start broker feature implementation until the D0 architecture package has explicit component interfaces, data structures, concurrency ownership, wire contract, fault transitions and dependency backlog. If a concrete choice remains experimental (transport/KV/shard sizing), implement behind the specified seam with the default selected for MVP, not by reopening whole-system design. A missing contract is a narrowly scoped architecture correction before coding.

## Prototype profile

Development-only listeners default to loopback. For fast research runs, plaintext, static test identities, simple configuration and reduced observability are allowed **only** as clearly marked prototype profiles. No real customer data or untrusted ingress. Never fake Q1 fsync, Q2 durable quorum, consumer ACK, bounded queues or failure results to satisfy a benchmark.

## Focused definition of done

Every code task needs (a) compiling changed code, (b) relevant unit/integration/contract test, (c) an honest result for each affected invariant, (d) no undocumented change to externally observable behavior, (e) an updated task ledger and, if interfaces change, architecture documentation. Use property/fault tests on durability and ownership changes, simple smoke tests for pure Q0 work. Manual or nightly soak is not required per commit.

Use small commits with purpose and test evidence. Direct main updates are authorized; never rewrite published history. No claim of 'PASS' without actual execution. A failed check is a defect to fix, not a reason to introduce approval bureaucracy.

## Agent handoff format

Provide: completed work package IDs; changed paths; short behavioral result; commands/tests run and observed outcome; known limitations; next unblocked IDs. Do not write a long process narrative.
