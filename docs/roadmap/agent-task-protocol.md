# Minimal AI agent task execution protocol

This file is a low-friction convention, not an administrative gate.

1. Select a task in \`tasks.json\` whose dependencies are DONE and whose status is TODO; avoid working concurrently on a shared file another agent owns.
2. Read task's architecture chapter and relevant normative FR/INV IDs. No full-system research for a local crate.
3. Implement the smallest working vertical slice, not a speculative general framework.
4. Write a focused automated test and at least one negative/failure case relevant to the behavior; use the existing oracle for durable paths.
5. Execute local commands. If Rust exists, \`cargo fmt --check\`, \`cargo check --workspace\`, and specific \`cargo test -p <crate> <filter>\`; avoid full suite unless touched safety-critical state.
6. Commit directly to main if unchanged upstream and authorized, or merge a short-lived branch without a mandatory reviewer. Never force-push shared main.
7. Update task status and observed evidence. Do not mark DONE if command did not actually execute.
8. Return a compact handoff: task IDs, commit SHA, tested command/outcome, remaining limitations, next unlocked task.

For a suspected contradiction: isolate the exact requirement, state a failure sequence, change the narrow architecture contract and its test in the same change, and continue. Do not convert the discovery into a week-long review process.

Do not bypass producer or consumer durability barriers, ownership fencing, bounded resources, or QoS class policy to make demos appear functional. Relax nonessential prototype concerns only inside an explicitly isolated lab profile.

See [work packages](work-packages.md), [MVP environment](../architecture/16-mvp-scope.md), and [AGENTS.md](../../AGENTS.md).
