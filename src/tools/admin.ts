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
