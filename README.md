# @us-all/airflow-mcp

> Airflow MCP server — read DAGs, runs, task instances, log tails; trigger and clear (write-gated). Built on `@us-all/mcp-toolkit`.

A focused MCP for the Airflow Stable REST API. Read by default; `airflow-trigger-dag` and `airflow-clear-task` are gated behind `AIRFLOW_ALLOW_WRITE=true`.

For deeper dbt integration (manifest parsing, run-results history, source freshness, per-column test coverage, lineage walks, custom DQ result tables), install the companion **[@us-all/dbt-mcp](https://www.npmjs.com/package/@us-all/dbt-mcp)** alongside.

- 7 tools in `airflow` + `meta` categories
- 2 MCP Prompts for DAG triage workflows
- 1 aggregation tool that replaces the list-runs → get-task-instances combo

## Install

```bash
pnpm add -D @us-all/airflow-mcp
```

## Run

```bash
AIRFLOW_API_URL=http://airflow.example.com:8080/api/v1 \
AIRFLOW_USERNAME=admin AIRFLOW_PASSWORD=... \
npx @us-all/airflow-mcp
```

The server speaks MCP stdio; wire it into Claude Desktop / Cursor / any MCP client. Set `MCP_TRANSPORT=http` to opt in to Streamable HTTP transport (Bearer auth, `/health` endpoint).

## Tools

### `airflow` (6 + 1 aggregation)

| Tool | Description |
|------|-------------|
| `airflow-list-dags` | List active DAGs with tag/search filters |
| `airflow-list-runs` | Recent runs of one DAG (state filter, ordered newest first) |
| `airflow-get-task-instances` | Task instances for a specific DAG run |
| `airflow-get-task-logs` | Tail (last N kB) of one task instance log |
| `airflow-trigger-dag` | Trigger a new run (write-gated) |
| `airflow-clear-task` | Clear specific task instances → re-run (write-gated) |
| `dag-health-rollup` | Aggregated DAG health: success rate + avg duration + last failed run + failing tasks |

### `meta`

`search-tools` — natural-language tool discovery.

### Prompts

| Prompt | Use when |
|--------|----------|
| `dag-failure-triage` | "Why did DAG X fail?" — pulls runs, isolates failure, dumps logs, proposes remediation |
| `dag-schedule-audit` | "Sweep all DAGs for low success rate and stuck runs" |

## Environment variables

| Env | Required | Notes |
|-----|----------|-------|
| `AIRFLOW_API_URL` | yes | Airflow REST API base, e.g. `http://airflow.example.com:8080/api/v1` |
| `AIRFLOW_USERNAME` | no | Basic-auth username |
| `AIRFLOW_PASSWORD` | no | Basic-auth password (secret) |
| `AIRFLOW_ALLOW_WRITE` | no | `true` enables `airflow-trigger-dag` / `airflow-clear-task` |
| `AIRFLOW_TOOLS` / `AIRFLOW_DISABLE` | no | Category toggles |

## Tested-against schemas

- Airflow Stable REST API (2.x). Airflow 3.x exposes the same surface via the `/api/v1/` Stable endpoints, so this server works with both 2.x and 3.x deployments.

## Companion server

For dbt artifact parsing, run-results history, and DQ result tables, install [`@us-all/dbt-mcp`](https://www.npmjs.com/package/@us-all/dbt-mcp) alongside.

## Build

```bash
pnpm install
pnpm run build      # tsc → dist/
pnpm test           # vitest
pnpm run smoke      # spawns dist/index.js, calls initialize + tools/list (set env first)
```

## License

MIT — see [LICENSE](./LICENSE).
