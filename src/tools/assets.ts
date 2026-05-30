/**
 * Airflow 3.x assets surface — replaces the legacy "dataset" concept. Lets a
 * model walk producer→consumer lineage (which DAG writes an asset, which DAGs
 * read it) without scraping logs. Pairs with @us-all/dbt-mcp lineage for
 * cross-tool data-flow tracing.
 *
 * Endpoints used (Airflow REST API v2 stable):
 *   GET /api/v2/assets              — list assets
 *   GET /api/v2/assets/{asset_id}   — single asset
 *   GET /api/v2/assets/events       — asset materialization events
 *
 * All read-only.
 */
import { z } from "zod";
import { airflowFetch } from "../clients/airflow.js";

// --- list-assets ---

export const airflowListAssetsSchema = z.object({
  uriPattern: z
    .string()
    .optional()
    .describe(
      "Substring match on the asset URI (Airflow's `uri_pattern` query param). Example: 's3://my-bucket/'",
    ),
  dagIds: z
    .array(z.string())
    .optional()
    .describe(
      "Filter to assets produced or consumed by any of these DAG ids. Combines producer/consumer relationships.",
    ),
  limit: z.coerce.number().int().min(1).max(1000).optional().default(50),
  offset: z.coerce.number().int().min(0).optional().default(0),
  orderBy: z
    .string()
    .optional()
    .describe("Order-by field, e.g. 'id' or '-id' (descending)."),
});

export interface AirflowAsset {
  id: number;
  uri: string;
  name?: string | null;
  group?: string | null;
  extra?: Record<string, unknown> | null;
  created_at?: string;
  updated_at?: string;
  producing_tasks?: Array<{ dag_id: string; task_id: string }>;
  consuming_dags?: Array<{ dag_id: string }>;
}

interface AirflowAssetListResp {
  assets: AirflowAsset[];
  total_entries: number;
}

export async function airflowListAssets(
  args: z.infer<typeof airflowListAssetsSchema>,
): Promise<unknown> {
  const params = new URLSearchParams();
  if (args.uriPattern) params.set("uri_pattern", args.uriPattern);
  if (args.dagIds && args.dagIds.length > 0) {
    for (const dagId of args.dagIds) params.append("dag_ids", dagId);
  }
  params.set("limit", String(args.limit));
  params.set("offset", String(args.offset));
  if (args.orderBy) params.set("order_by", args.orderBy);

  const data = await airflowFetch<AirflowAssetListResp>(
    `/assets?${params.toString()}`,
  );
  return {
    totalEntries: data.total_entries,
    assets: (data.assets ?? []).map(slimAsset),
  };
}

function slimAsset(a: AirflowAsset) {
  return {
    id: a.id,
    uri: a.uri,
    name: a.name ?? null,
    group: a.group ?? null,
    producingTasks: a.producing_tasks ?? [],
    consumingDags: (a.consuming_dags ?? []).map((d) => d.dag_id),
    updatedAt: a.updated_at ?? null,
  };
}

// --- get-asset ---

export const airflowGetAssetSchema = z.object({
  assetId: z.coerce.number().int().describe("Airflow asset numeric id"),
});

export async function airflowGetAsset(
  args: z.infer<typeof airflowGetAssetSchema>,
): Promise<unknown> {
  const data = await airflowFetch<AirflowAsset>(`/assets/${args.assetId}`);
  return {
    id: data.id,
    uri: data.uri,
    name: data.name ?? null,
    group: data.group ?? null,
    extra: data.extra ?? null,
    producingTasks: data.producing_tasks ?? [],
    consumingDags: (data.consuming_dags ?? []).map((d) => d.dag_id),
    createdAt: data.created_at ?? null,
    updatedAt: data.updated_at ?? null,
  };
}

// --- list-asset-events ---

export const airflowListAssetEventsSchema = z.object({
  assetId: z.coerce.number().int().optional().describe("Filter by asset id"),
  sourceDagId: z.string().optional().describe("Filter to events produced by this DAG"),
  sourceRunId: z.string().optional().describe("Filter to events produced by this DAG run"),
  sourceTaskId: z.string().optional().describe("Filter to events produced by this task id"),
  timestampGte: z
    .string()
    .optional()
    .describe("ISO-8601 lower bound on event timestamp (inclusive)."),
  timestampLte: z
    .string()
    .optional()
    .describe("ISO-8601 upper bound on event timestamp (inclusive)."),
  limit: z.coerce.number().int().min(1).max(1000).optional().default(50),
  offset: z.coerce.number().int().min(0).optional().default(0),
  orderBy: z
    .string()
    .optional()
    .describe(
      "Order-by field; default '-timestamp' is most useful for 'what changed recently?'",
    )
    .default("-timestamp"),
});

interface AirflowAssetEvent {
  id: number;
  asset_id: number;
  asset_uri?: string;
  extra?: Record<string, unknown> | null;
  source_dag_id?: string | null;
  source_task_id?: string | null;
  source_run_id?: string | null;
  source_map_index?: number | null;
  timestamp?: string;
  created_dagruns?: Array<{ dag_id: string; dag_run_id: string }>;
}

interface AirflowAssetEventListResp {
  asset_events: AirflowAssetEvent[];
  total_entries: number;
}

export async function airflowListAssetEvents(
  args: z.infer<typeof airflowListAssetEventsSchema>,
): Promise<unknown> {
  const params = new URLSearchParams();
  if (args.assetId !== undefined) params.set("asset_id", String(args.assetId));
  if (args.sourceDagId) params.set("source_dag_id", args.sourceDagId);
  if (args.sourceRunId) params.set("source_run_id", args.sourceRunId);
  if (args.sourceTaskId) params.set("source_task_id", args.sourceTaskId);
  if (args.timestampGte) params.set("timestamp_gte", args.timestampGte);
  if (args.timestampLte) params.set("timestamp_lte", args.timestampLte);
  params.set("limit", String(args.limit));
  params.set("offset", String(args.offset));
  params.set("order_by", args.orderBy);

  const data = await airflowFetch<AirflowAssetEventListResp>(
    `/assets/events?${params.toString()}`,
  );
  return {
    totalEntries: data.total_entries,
    events: (data.asset_events ?? []).map((e) => ({
      id: e.id,
      assetId: e.asset_id,
      assetUri: e.asset_uri ?? null,
      timestamp: e.timestamp ?? null,
      sourceDagId: e.source_dag_id ?? null,
      sourceRunId: e.source_run_id ?? null,
      sourceTaskId: e.source_task_id ?? null,
      triggeredDagRuns: (e.created_dagruns ?? []).map((r) => ({
        dagId: r.dag_id,
        dagRunId: r.dag_run_id,
      })),
    })),
  };
}
