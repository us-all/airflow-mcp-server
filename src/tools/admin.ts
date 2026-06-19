/**
 * Airflow 3.x operational read surfaces that complement the DAG/run tools.
 * All read-only GET endpoints, stable across Airflow 3.0–3.2 `/api/v2`.
 *
 * Endpoints used:
 *   GET /api/v2/importErrors   — DAG files that failed to parse ("why is my DAG missing?")
 *   GET /api/v2/dagWarnings    — non-fatal DAG warnings (e.g. non-existent pool, duplicate task ids)
 *   GET /api/v2/pools          — pool slot utilization (capacity diagnosis)
 *
 * These close the most common operational-triage gap: list-dags shows what
 * parsed, importErrors/dagWarnings explain what didn't, and pools explain why
 * tasks are stuck queued.
 */
import { z } from "zod";
import { extractFieldsDescription } from "@us-all/mcp-toolkit";
import { airflowFetch } from "../clients/airflow.js";

const ef = z.string().optional().describe(extractFieldsDescription);

// --- list-import-errors ---

interface AirflowImportError {
  import_error_id?: number;
  timestamp?: string;
  filename?: string;
  bundle_name?: string;
  stack_trace?: string;
}

export const airflowListImportErrorsSchema = z.object({
  limit: z.coerce.number().int().min(1).max(200).default(50).describe("Max import errors to return"),
  extractFields: ef,
});

export async function airflowListImportErrors(
  args: z.infer<typeof airflowListImportErrorsSchema>,
): Promise<unknown> {
  const qs = new URLSearchParams();
  qs.set("limit", String(args.limit));
  qs.set("order_by", "-timestamp");
  const data = await airflowFetch<{ import_errors: AirflowImportError[]; total_entries: number }>(
    `/importErrors?${qs.toString()}`,
  );
  return {
    totalEntries: data.total_entries,
    count: data.import_errors.length,
    importErrors: data.import_errors.map((e) => ({
      importErrorId: e.import_error_id,
      filename: e.filename,
      bundle: e.bundle_name,
      timestamp: e.timestamp,
      stackTrace: e.stack_trace,
    })),
  };
}

// --- list-dag-warnings ---

interface AirflowDagWarning {
  dag_id?: string;
  warning_type?: string;
  message?: string;
  timestamp?: string;
}

export const airflowListDagWarningsSchema = z.object({
  dagId: z.string().optional().describe("Restrict to a single DAG id"),
  warningType: z.string().optional().describe("Filter by warning type (e.g. 'non-existent pool')"),
  limit: z.coerce.number().int().min(1).max(200).default(50).describe("Max warnings to return"),
  extractFields: ef,
});

export async function airflowListDagWarnings(
  args: z.infer<typeof airflowListDagWarningsSchema>,
): Promise<unknown> {
  const qs = new URLSearchParams();
  qs.set("limit", String(args.limit));
  qs.set("order_by", "-timestamp");
  if (args.dagId) qs.set("dag_id", args.dagId);
  if (args.warningType) qs.set("warning_type", args.warningType);
  const data = await airflowFetch<{ dag_warnings: AirflowDagWarning[]; total_entries: number }>(
    `/dagWarnings?${qs.toString()}`,
  );
  return {
    totalEntries: data.total_entries,
    count: data.dag_warnings.length,
    dagWarnings: data.dag_warnings.map((w) => ({
      dagId: w.dag_id,
      warningType: w.warning_type,
      message: w.message,
      timestamp: w.timestamp,
    })),
  };
}

// --- list-pools ---

interface AirflowPool {
  name?: string;
  slots?: number;
  occupied_slots?: number;
  running_slots?: number;
  queued_slots?: number;
  scheduled_slots?: number;
  deferred_slots?: number;
  open_slots?: number;
  description?: string;
  include_deferred?: boolean;
}

export const airflowListPoolsSchema = z.object({
  limit: z.coerce.number().int().min(1).max(200).default(50).describe("Max pools to return"),
  extractFields: ef,
});

export async function airflowListPools(
  args: z.infer<typeof airflowListPoolsSchema>,
): Promise<unknown> {
  const qs = new URLSearchParams();
  qs.set("limit", String(args.limit));
  const data = await airflowFetch<{ pools: AirflowPool[]; total_entries: number }>(
    `/pools?${qs.toString()}`,
  );
  return {
    totalEntries: data.total_entries,
    count: data.pools.length,
    pools: data.pools.map((p) => ({
      name: p.name,
      slots: p.slots,
      occupiedSlots: p.occupied_slots,
      runningSlots: p.running_slots,
      queuedSlots: p.queued_slots,
      scheduledSlots: p.scheduled_slots,
      deferredSlots: p.deferred_slots,
      openSlots: p.open_slots,
      description: p.description,
      includeDeferred: p.include_deferred,
    })),
  };
}

// --- list-variables ---
// Returns keys + descriptions only. Values are intentionally omitted: bulk-listing
// variable values would leak secrets (Airflow masks sensitive keys, but many
// non-"sensitive-named" variables still hold tokens/URLs). Fetch a specific value
// via the Airflow UI when needed.

interface AirflowVariable {
  key?: string;
  description?: string;
}

export const airflowListVariablesSchema = z.object({
  limit: z.coerce.number().int().min(1).max(200).default(100).describe("Max variables to return"),
  extractFields: ef,
});

export async function airflowListVariables(
  args: z.infer<typeof airflowListVariablesSchema>,
): Promise<unknown> {
  const qs = new URLSearchParams();
  qs.set("limit", String(args.limit));
  const data = await airflowFetch<{ variables: AirflowVariable[]; total_entries: number }>(
    `/variables?${qs.toString()}`,
  );
  return {
    totalEntries: data.total_entries,
    count: data.variables.length,
    note: "values omitted to avoid leaking secrets — keys/descriptions only",
    variables: data.variables.map((v) => ({ key: v.key, description: v.description })),
  };
}

// --- list-connections ---
// The Airflow REST API never returns the connection password, so this is safe to
// surface. conn_type/host/schema/login/port help diagnose 'which target does this
// DAG talk to?' without exposing credentials.

interface AirflowConnection {
  connection_id?: string;
  conn_type?: string;
  host?: string;
  schema?: string;
  login?: string;
  port?: number;
  description?: string;
}

export const airflowListConnectionsSchema = z.object({
  limit: z.coerce.number().int().min(1).max(200).default(100).describe("Max connections to return"),
  extractFields: ef,
});

export async function airflowListConnections(
  args: z.infer<typeof airflowListConnectionsSchema>,
): Promise<unknown> {
  const qs = new URLSearchParams();
  qs.set("limit", String(args.limit));
  const data = await airflowFetch<{ connections: AirflowConnection[]; total_entries: number }>(
    `/connections?${qs.toString()}`,
  );
  return {
    totalEntries: data.total_entries,
    count: data.connections.length,
    connections: data.connections.map((c) => ({
      connectionId: c.connection_id,
      connType: c.conn_type,
      host: c.host,
      schema: c.schema,
      login: c.login,
      port: c.port,
      description: c.description,
    })),
  };
}

// --- list-event-logs ---
// Audit trail of Airflow events (DAG runs, task state changes, config edits, etc).
// Newest first. Filterable by dag_id / run_id / task_id / event.

interface AirflowEventLog {
  event_log_id?: number;
  when?: string;
  event?: string;
  dag_id?: string;
  task_id?: string;
  run_id?: string;
  owner?: string;
}

export const airflowListEventLogsSchema = z.object({
  dagId: z.string().optional().describe("Restrict to a single DAG id"),
  taskId: z.string().optional().describe("Restrict to a single task id"),
  runId: z.string().optional().describe("Restrict to a single DAG run id"),
  event: z.string().optional().describe("Filter by event name (e.g. 'success', 'failed', 'cli_task_run')"),
  limit: z.coerce.number().int().min(1).max(200).default(50).describe("Max event-log rows to return"),
  extractFields: ef,
});

export async function airflowListEventLogs(
  args: z.infer<typeof airflowListEventLogsSchema>,
): Promise<unknown> {
  const qs = new URLSearchParams();
  qs.set("limit", String(args.limit));
  qs.set("order_by", "-when");
  if (args.dagId) qs.set("dag_id", args.dagId);
  if (args.taskId) qs.set("task_id", args.taskId);
  if (args.runId) qs.set("run_id", args.runId);
  if (args.event) qs.set("event", args.event);
  const data = await airflowFetch<{ event_logs: AirflowEventLog[]; total_entries: number }>(
    `/eventLogs?${qs.toString()}`,
  );
  return {
    totalEntries: data.total_entries,
    count: data.event_logs.length,
    eventLogs: data.event_logs.map((e) => ({
      eventLogId: e.event_log_id,
      when: e.when,
      event: e.event,
      dagId: e.dag_id,
      taskId: e.task_id,
      runId: e.run_id,
      owner: e.owner,
    })),
  };
}
