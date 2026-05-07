# @us-all/airflow-mcp

> Airflow MCP server — read DAGs, runs, task instances, log tails; trigger and clear (write-gated). Built on `@us-all/mcp-toolkit`.

A focused MCP for the Airflow 3.x REST API (`/api/v2`). Read by default; `airflow-trigger-dag` and `airflow-clear-task` are gated behind `AIRFLOW_ALLOW_WRITE=true`. Auth is JWT via SimpleAuthManager — supply `AIRFLOW_USERNAME` + `AIRFLOW_PASSWORD` and the server mints/refreshes the token transparently.

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
AIRFLOW_API_URL=http://airflow.example.com:8080 \
AIRFLOW_USERNAME=admin AIRFLOW_PASSWORD=... \
npx @us-all/airflow-mcp
```

Pass the host base only — the server prepends `/api/v2` internally. A trailing `/api/v1` or `/api/v2` is stripped if supplied. JWT tokens are cached for the lifetime of the session and refreshed 1 minute before they expire.

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
| `AIRFLOW_API_URL` | yes | Airflow host base, e.g. `http://airflow.example.com:8080`. Trailing `/api/v1` or `/api/v2` is stripped if present |
| `AIRFLOW_USERNAME` | yes | Username for JWT minting via SimpleAuthManager `/auth/token` |
| `AIRFLOW_PASSWORD` | yes | Password for JWT minting (secret) |
| `AIRFLOW_ALLOW_WRITE` | no | `true` enables `airflow-trigger-dag` / `airflow-clear-task` |
| `AIRFLOW_TOOLS` / `AIRFLOW_DISABLE` | no | Category toggles |

## Tested-against schemas

- Airflow 3.x `/api/v2` (current default). Airflow 2.x is **not** supported by v0.2 because basic auth + `/api/v1` were removed; pin to `@us-all/airflow-mcp@0.1.0` for Airflow 2.x deployments.

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
