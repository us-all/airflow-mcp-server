#!/usr/bin/env node

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { startMcpServer } from "@us-all/mcp-toolkit/runtime";
import { inferToolAnnotations } from "@us-all/mcp-toolkit";
import { validateConfig } from "./config.js";
import { wrapToolHandler } from "./tools/utils.js";

import {
  airflowListDagsSchema, airflowListDags,
  airflowListRunsSchema, airflowListRuns,
  airflowGetTaskInstancesSchema, airflowGetTaskInstances,
  airflowGetTaskLogsSchema, airflowGetTaskLogs,
  airflowTriggerDagSchema, airflowTriggerDag,
  airflowClearTaskSchema, airflowClearTask,
} from "./tools/dags.js";
import {
  airflowListAssetsSchema, airflowListAssets,
  airflowGetAssetSchema, airflowGetAsset,
  airflowListAssetEventsSchema, airflowListAssetEvents,
} from "./tools/assets.js";
import {
  airflowListImportErrorsSchema, airflowListImportErrors,
  airflowListDagWarningsSchema, airflowListDagWarnings,
  airflowListPoolsSchema, airflowListPools,
  airflowListVariablesSchema, airflowListVariables,
  airflowListConnectionsSchema, airflowListConnections,
  airflowListEventLogsSchema, airflowListEventLogs,
} from "./tools/admin.js";
import { dagHealthRollupSchema, dagHealthRollup } from "./tools/aggregations.js";
import { registry, searchToolsSchema, searchTools, type Category } from "./tool-registry.js";
import { registerPrompts } from "./prompts/index.js";

const pkgPath = join(dirname(fileURLToPath(import.meta.url)), "..", "package.json");
const { version: pkgVersion } = JSON.parse(readFileSync(pkgPath, "utf-8")) as { version: string };

validateConfig();

const server = new McpServer({
  name: "airflow",
  version: pkgVersion,
});

let currentCategory: Category = "airflow";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function tool(name: string, description: string, schema: any, handler: any, annotations?: any): void {
  registry.register(name, description, currentCategory);
  if (registry.isEnabled(currentCategory)) {
    server.tool(name, description, schema, inferToolAnnotations(name, annotations), handler);
  }
}

// --- airflow ---
currentCategory = "airflow";

tool("airflow-list-dags", "List Airflow DAGs (active by default) with optional tag filter and dag_id substring search", airflowListDagsSchema.shape, wrapToolHandler(airflowListDags));
tool("airflow-list-runs", "List recent runs of one Airflow DAG, optionally filtered by state, ordered newest first", airflowListRunsSchema.shape, wrapToolHandler(airflowListRuns));
tool("airflow-get-task-instances", "List task instances for a specific Airflow DAG run with state, try_number, duration", airflowGetTaskInstancesSchema.shape, wrapToolHandler(airflowGetTaskInstances));
tool("airflow-get-task-logs", "Fetch the tail (last N kB) of an Airflow task instance log for a specific try_number", airflowGetTaskLogsSchema.shape, wrapToolHandler(airflowGetTaskLogs));
tool("airflow-trigger-dag", "Trigger a new Airflow DAG run with optional conf payload and note. Write-gated by AIRFLOW_ALLOW_WRITE.", airflowTriggerDagSchema.shape, wrapToolHandler(airflowTriggerDag));
tool("airflow-clear-task", "Clear specific task instances in an Airflow DAG run (re-run them); supports include_upstream / include_downstream. Write-gated by AIRFLOW_ALLOW_WRITE.", airflowClearTaskSchema.shape, wrapToolHandler(airflowClearTask));

tool("airflow-list-assets",
  "List Airflow 3.x assets (replaces the legacy 'dataset' concept) with optional URI substring filter and DAG-id filter. Returns producing tasks + consuming DAGs per asset — the cross-DAG lineage answer to 'who writes this, who reads this?'",
  airflowListAssetsSchema.shape, wrapToolHandler(airflowListAssets));

tool("airflow-get-asset",
  "Get one Airflow 3.x asset by numeric id, including producing tasks and consuming DAGs and the asset's extra metadata blob.",
  airflowGetAssetSchema.shape, wrapToolHandler(airflowGetAsset));

tool("airflow-list-asset-events",
  "List Airflow 3.x asset materialization events (newest first by default). Each event names the source DAG/task/run plus any downstream DAG runs the materialization triggered. Filterable by assetId, sourceDagId, sourceRunId, sourceTaskId, and a timestamp window — pair with dbt-mcp lineage to trace cross-tool data flow.",
  airflowListAssetEventsSchema.shape, wrapToolHandler(airflowListAssetEvents));

tool("airflow-list-import-errors",
  "List DAG files that failed to parse (import errors) with filename, timestamp, and stack trace — the first thing to check when a DAG is 'missing' from list-dags. Newest first.",
  airflowListImportErrorsSchema.shape, wrapToolHandler(airflowListImportErrors));

tool("airflow-list-dag-warnings",
  "List non-fatal DAG warnings (e.g. references to a non-existent pool, duplicate task ids) optionally filtered by dagId or warningType. Newest first.",
  airflowListDagWarningsSchema.shape, wrapToolHandler(airflowListDagWarnings));

tool("airflow-list-pools",
  "List Airflow pools with slot utilization (slots / occupied / running / queued / open) — diagnose why tasks are stuck queued due to pool capacity.",
  airflowListPoolsSchema.shape, wrapToolHandler(airflowListPools));

tool("airflow-list-variables",
  "List Airflow Variable keys + descriptions (values omitted to avoid leaking secrets) — see what configuration knobs exist.",
  airflowListVariablesSchema.shape, wrapToolHandler(airflowListVariables));

tool("airflow-list-connections",
  "List Airflow connections (connection_id / conn_type / host / schema / login / port) — diagnose which external targets DAGs talk to. Passwords are never returned by the API.",
  airflowListConnectionsSchema.shape, wrapToolHandler(airflowListConnections));

tool("airflow-list-event-logs",
  "List the Airflow event-log audit trail (newest first): DAG run / task state changes, config edits, etc. Filterable by dagId, taskId, runId, event.",
  airflowListEventLogsSchema.shape, wrapToolHandler(airflowListEventLogs));

tool("dag-health-rollup",
  "Aggregated DAG health: success-rate over the last N runs + count breakdown (succeeded/failed/queued) + average duration + last-failed-run id + (optional) failing task instances. Replaces the airflow-list-runs + airflow-get-task-instances combo for 'is this DAG healthy right now?'.",
  dagHealthRollupSchema.shape, wrapToolHandler(dagHealthRollup));

// --- meta ---
currentCategory = "meta";

tool("search-tools",
  "Discover available Airflow MCP tools by natural language query.",
  searchToolsSchema.shape, wrapToolHandler(searchTools));

registerPrompts(server);

startMcpServer(server).catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
