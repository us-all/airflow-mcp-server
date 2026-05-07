#!/usr/bin/env node

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { startMcpServer } from "@us-all/mcp-toolkit/runtime";
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
function tool(name: string, description: string, schema: any, handler: any): void {
  registry.register(name, description, currentCategory);
  if (registry.isEnabled(currentCategory)) {
    server.tool(name, description, schema, handler);
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
