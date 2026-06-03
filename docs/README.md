# Documentation

Architecture and data design for **Unified Lakehouse Engine**.

| Document | Contents |
|----------|----------|
| [data-flow.md](./data-flow.md) | End-to-end data flow, workload routing, pipeline paths |
| [sequence-diagrams.md](./sequence-diagrams.md) | CLI, streaming, lakehouse sync, federated query sequences |
| [data-models.md](./data-models.md) | Event schema, per-engine tables, ER diagram, config model |

Diagrams use [Mermaid](https://mermaid.js.org/). They render on GitHub, in VS Code (Markdown Preview), and many IDEs.

## Quick links

- Reference DDL: [`../sql/schemas/init.sql`](../sql/schemas/init.sql)
- Seed data: [`../sql/seeds/`](../sql/seeds/) · [`../examples/sample_events.json`](../examples/sample_events.json)
- Example config: [`../config/platform.example.yaml`](../config/platform.example.yaml)
