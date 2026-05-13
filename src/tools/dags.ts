import { z } from "zod";
import { extractFieldsDescription } from "@us-all/mcp-toolkit";
import { airflowFetch, type AirflowDag, type AirflowDagRun, type AirflowTaskInstance } from "../clients/airflow.js";
import { assertWriteAllowed } from "./utils.js";

const ef = z.string().optional().describe(extractFieldsDescription);

export const airflowListDagsSchema = z.object({
  onlyActive: z.boolean().default(true).describe("Filter out paused DAGs (Airflow 3.x: is_paused=false)"),
  tag: z.string().optional().describe("Filter to DAGs that carry this tag"),
  search: z.string().optional().describe("Substring match on dag_id (case-insensitive)"),
  limit: z.coerce.number().int().min(1).max(500).default(100),
  extractFields: ef,
});

export async function airflowListDags(args: z.infer<typeof airflowListDagsSchema>): Promise<unknown> {
  const qs = new URLSearchParams();
  qs.set("limit", String(args.limit));
  if (args.onlyActive) qs.set("paused", "false");
  if (args.tag) qs.append("tags", args.tag);
  const data = await airflowFetch<{ dags: AirflowDag[]; total_entries: number }>(
    `/dags?${qs.toString()}`,
  );
  let dags = data.dags;
  if (args.search) {
    const s = args.search.toLowerCase();
    dags = dags.filter((d) => d.dag_id.toLowerCase().includes(s));
  }
  return {
    totalEntries: data.total_entries,
    count: dags.length,
    dags: dags.map((d) => ({
      dagId: d.dag_id,
      displayName: d.dag_display_name,
      isPaused: d.is_paused,
      isStale: d.is_stale,
      description: d.description,
      schedule:
        d.timetable_summary ??
        (typeof d.schedule_interval === "object" ? d.schedule_interval?.value : d.schedule_interval),
      timetable: d.timetable_description,
      bundle: d.bundle_name,
      fileloc: d.relative_fileloc ?? d.fileloc,
      tags: d.tags?.map((t) => t.name) ?? [],
      nextDagrun: d.next_dagrun_logical_date ?? d.next_dagrun,
      lastParsedTime: d.last_parsed_time,
    })),
  };
}

export const airflowListRunsSchema = z.object({
  dagId: z.string().describe("Airflow DAG id"),
  state: z.string().optional().describe("Filter by state (success | running | failed | queued | up_for_retry | ...)"),
  limit: z.coerce.number().int().min(1).max(200).default(20),
  extractFields: ef,
});

export async function airflowListRuns(args: z.infer<typeof airflowListRunsSchema>): Promise<unknown> {
  const qs = new URLSearchParams();
  qs.set("limit", String(args.limit));
  // Airflow 3.x v2 uses logical_date (formerly execution_date in v1)
  qs.set("order_by", "-logical_date");
  if (args.state) qs.append("state", args.state);
  const data = await airflowFetch<{ dag_runs: AirflowDagRun[]; total_entries: number }>(
    `/dags/${encodeURIComponent(args.dagId)}/dagRuns?${qs.toString()}`,
  );
  return {
    dagId: args.dagId,
    totalEntries: data.total_entries,
    count: data.dag_runs.length,
    runs: data.dag_runs.map((r) => ({
      dagRunId: r.dag_run_id,
      state: r.state,
      logicalDate: r.logical_date ?? r.execution_date,
      runType: r.run_type,
      startDate: r.start_date,
      endDate: r.end_date,
      duration: r.duration,
      queuedAt: r.queued_at,
      runAfter: r.run_after,
      triggeredBy: r.triggered_by,
      triggeringUser: r.triggering_user_name,
      conf: r.conf,
      note: r.note,
    })),
  };
}

export const airflowGetTaskInstancesSchema = z.object({
  dagId: z.string(),
  dagRunId: z.string().describe("dag_run_id (e.g. 'scheduled__2026-05-06T00:00:00+00:00')"),
  extractFields: ef,
});

export async function airflowGetTaskInstances(
  args: z.infer<typeof airflowGetTaskInstancesSchema>,
): Promise<unknown> {
  const data = await airflowFetch<{ task_instances: AirflowTaskInstance[]; total_entries: number }>(
    `/dags/${encodeURIComponent(args.dagId)}/dagRuns/${encodeURIComponent(args.dagRunId)}/taskInstances`,
  );
  return {
    dagId: args.dagId,
    dagRunId: args.dagRunId,
    totalEntries: data.total_entries,
    taskInstances: data.task_instances.map((t) => ({
      taskId: t.task_id,
      state: t.state,
      tryNumber: t.try_number,
      maxTries: t.max_tries,
      startDate: t.start_date,
      endDate: t.end_date,
      duration: t.duration,
      operator: t.operator,
      pool: t.pool,
    })),
  };
}

export const airflowGetTaskLogsSchema = z.object({
  dagId: z.string(),
  dagRunId: z.string(),
  taskId: z.string(),
  tryNumber: z.coerce.number().int().min(1).default(1),
  tailKb: z.coerce.number().int().min(1).max(64).default(16).describe("Return only the last N kilobytes of log"),
  extractFields: ef,
});

export async function airflowGetTaskLogs(args: z.infer<typeof airflowGetTaskLogsSchema>): Promise<unknown> {
  const path = `/dags/${encodeURIComponent(args.dagId)}/dagRuns/${encodeURIComponent(args.dagRunId)}/taskInstances/${encodeURIComponent(args.taskId)}/logs/${args.tryNumber}?full_content=true`;
  const data = await airflowFetch<{ content?: string; continuation_token?: unknown }>(path);
  const fullContent = data.content ?? "";
  const limit = args.tailKb * 1024;
  const bytes = Buffer.from(fullContent, "utf8");
  const truncated = bytes.length > limit;
  let content = fullContent;
  if (truncated) {
    let start = bytes.length - limit;
    while (start < bytes.length && (bytes[start]! & 0xc0) === 0x80) start += 1;
    content = bytes.subarray(start).toString("utf8");
  }
  return {
    dagId: args.dagId,
    dagRunId: args.dagRunId,
    taskId: args.taskId,
    tryNumber: args.tryNumber,
    truncated,
    bytesReturned: Buffer.byteLength(content, "utf8"),
    content,
  };
}

export const airflowTriggerDagSchema = z.object({
  dagId: z.string(),
  dagRunId: z.string().optional().describe("Optional run id; auto-generated if omitted"),
  conf: z.record(z.string(), z.unknown()).optional().describe("DAG run conf payload"),
  note: z.string().optional().describe("Optional note attached to the run"),
  logicalDate: z.string().optional().describe("Optional logical_date (ISO 8601) — Airflow 3.x replacement for execution_date"),
});

export async function airflowTriggerDag(args: z.infer<typeof airflowTriggerDagSchema>): Promise<unknown> {
  assertWriteAllowed();
  const body: Record<string, unknown> = {};
  if (args.dagRunId) body.dag_run_id = args.dagRunId;
  if (args.conf) body.conf = args.conf;
  if (args.note) body.note = args.note;
  if (args.logicalDate) body.logical_date = args.logicalDate;
  const data = await airflowFetch<AirflowDagRun>(
    `/dags/${encodeURIComponent(args.dagId)}/dagRuns`,
    { method: "POST", body: JSON.stringify(body) },
  );
  return {
    triggered: true,
    dagId: data.dag_id,
    dagRunId: data.dag_run_id,
    state: data.state,
    logicalDate: data.logical_date ?? data.execution_date,
  };
}

export const airflowClearTaskSchema = z.object({
  dagId: z.string(),
  dagRunId: z.string(),
  taskIds: z.array(z.string()).min(1).describe("List of task_ids to clear (and re-run)"),
  includeDownstream: z.boolean().default(false),
  includeUpstream: z.boolean().default(false),
});

export async function airflowClearTask(args: z.infer<typeof airflowClearTaskSchema>): Promise<unknown> {
  assertWriteAllowed();
  const body = {
    dry_run: false,
    task_ids: args.taskIds,
    include_downstream: args.includeDownstream,
    include_upstream: args.includeUpstream,
    only_failed: false,
    only_running: false,
    reset_dag_runs: false,
    dag_run_id: args.dagRunId,
  };
  const data = await airflowFetch<{ task_instances?: AirflowTaskInstance[] }>(
    `/dags/${encodeURIComponent(args.dagId)}/clearTaskInstances`,
    { method: "POST", body: JSON.stringify(body) },
  );
  return {
    cleared: true,
    dagId: args.dagId,
    dagRunId: args.dagRunId,
    affectedTasks:
      data.task_instances?.map((t) => ({ taskId: t.task_id, state: t.state })) ?? [],
  };
}
