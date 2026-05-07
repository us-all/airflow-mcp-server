import { config } from "../config.js";
import { AirflowApiError } from "../tools/utils.js";

function authHeader(): string {
  const credentials = `${config.username}:${config.password}`;
  return "Basic " + Buffer.from(credentials, "utf-8").toString("base64");
}

export async function airflowFetch<T = unknown>(
  path: string,
  init: RequestInit & { method?: string; body?: string } = {},
): Promise<T> {
  if (!config.apiUrl) {
    throw new Error("AIRFLOW_API_URL is not configured");
  }
  const url = `${config.apiUrl}${path.startsWith("/") ? "" : "/"}${path}`;
  const headers = new Headers(init.headers);
  if (config.username) {
    headers.set("Authorization", authHeader());
  }
  if (init.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  headers.set("Accept", "application/json");
  const res = await fetch(url, { ...init, headers });
  const text = await res.text();
  let body: unknown;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = text;
  }
  if (!res.ok) {
    throw new AirflowApiError(
      res.status,
      body,
      `Airflow API ${init.method ?? "GET"} ${path} failed: ${res.status} ${res.statusText}`,
    );
  }
  return body as T;
}

export interface AirflowDag {
  dag_id: string;
  is_active?: boolean;
  is_paused?: boolean;
  description?: string | null;
  schedule_interval?: { __type?: string; value?: string } | string | null;
  owners?: string[];
  tags?: Array<{ name: string }>;
  next_dagrun?: string | null;
  last_parsed_time?: string | null;
}

export interface AirflowDagRun {
  dag_run_id: string;
  dag_id: string;
  state: string;
  execution_date?: string;
  start_date?: string | null;
  end_date?: string | null;
  data_interval_start?: string | null;
  data_interval_end?: string | null;
  external_trigger?: boolean;
  conf?: Record<string, unknown>;
  note?: string | null;
}

export interface AirflowTaskInstance {
  task_id: string;
  dag_id: string;
  dag_run_id?: string;
  execution_date?: string;
  state: string | null;
  try_number?: number;
  max_tries?: number;
  start_date?: string | null;
  end_date?: string | null;
  duration?: number | null;
  operator?: string | null;
  pool?: string;
  queued_when?: string | null;
}
