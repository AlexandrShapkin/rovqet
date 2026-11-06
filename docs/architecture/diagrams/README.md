# Architecture diagrams as source

- \`workspace.dsl\`: C4 architecture context, containers, server components and research lab deployment.
- \`flows.puml\`: Q0, Q1, Q2 and durable consumer ACK/DLQ sequences.
- \`states.puml\`: session, publisher, consumer, ownership, overload and Q1 incarnation state machines.

These text sources are canonical; generated PNG/SVG are optional and must never be edited manually. Render using Structurizr CLI/PlantUML in a local environment when available. Diagrams describe the design, **not** a running deployed service. No Java/diagram tool is required for the everyday docs check. Keep diagrams in sync with contracts in chapters 01–15.
