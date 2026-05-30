import { config } from "../config.js";
import { AirflowApiError } from "../tools/utils.js";

interface CachedToken {
  token: string;
  expiresAt: number; // epoch ms
}

let tokenCache: CachedToken | null = null;
let inFlightTokenFetch: Promise<string> | null = null;
const TOKEN_REFRESH_MARGIN_MS = 60_000; // refresh 1min early

async function fetchJwtToken(): Promise<string> {
  if (!config.username || !config.password) {
    throw new AirflowApiError(
      0,
      null,
      "Airflow 3.x SimpleAuthManager requires AIRFLOW_USERNAME + AIRFLOW_PASSWORD to mint a JWT token",
    );
  }
  const url = `${config.apiBase}/auth/token`;
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username: config.username, password: config.password }),
  });
  const text = await res.text();
  let body: unknown;
  try { body = text ? JSON.parse(text) : null; } catch { body = text; }
  if (!res.ok) {
    throw new AirflowApiError(res.status, body, `Airflow auth/token failed: ${res.status} ${res.statusText}`);
  }
  const accessToken = (body as { access_token?: string })?.access_token;
  if (!accessToken) throw new AirflowApiError(res.status, body, "Airflow auth/token returned no access_token");
  return accessToken;
}

function decodeJwtExp(token: string): number | null {
  // JWT: header.payload.signature, payload base64url-encoded JSON with 'exp' (seconds)
  const parts = token.split(".");
  if (parts.length < 2) return null;
  try {
    const b64 = parts[1]!.replace(/-/g, "+").replace(/_/g, "/").padEnd(Math.ceil(parts[1]!.length / 4) * 4, "=");
    const payload = JSON.parse(Buffer.from(b64, "base64").toString("utf-8"));
    return typeof payload.exp === "number" ? payload.exp * 1000 : null;
  } catch {
    return null;
  }
}

async function getValidToken(): Promise<string> {
  // External Bearer mode: caller supplies a pre-minted token. We don't cache
  // or refresh — the env var is the source of truth and the caller owns its
  // lifetime. Expired tokens surface as Airflow's own 401.
  if (config.bearerToken) {
    return config.bearerToken;
  }

  const now = Date.now();
  if (tokenCache && tokenCache.expiresAt - TOKEN_REFRESH_MARGIN_MS > now) {
    return tokenCache.token;
  }
  // Coalesce concurrent refreshes — without this, N parallel tool calls that
  // arrive on a cache miss each mint their own JWT, which Airflow may rate-limit
  // or block as suspicious. The in-flight singleton is cleared in `finally` so a
  // failed mint doesn't permanently poison the cache.
  if (inFlightTokenFetch) return inFlightTokenFetch;
  inFlightTokenFetch = (async () => {
    try {
      const token = await fetchJwtToken();
      const expiresAt = decodeJwtExp(token) ?? Date.now() + 23 * 60 * 60 * 1000;
      tokenCache = { token, expiresAt };
      return token;
    } finally {
      inFlightTokenFetch = null;
    }
  })();
  return inFlightTokenFetch;
}

export function _resetTokenCacheForTest(): void {
  tokenCache = null;
  inFlightTokenFetch = null;
}

export async function airflowFetch<T = unknown>(
  path: string,
  init: RequestInit & { method?: string; body?: string } = {},
): Promise<T> {
  if (!config.apiBase) {
    throw new Error("AIRFLOW_API_URL is not configured");
  }
  const url = `${config.apiBase}/api/v2${path.startsWith("/") ? "" : "/"}${path}`;
  for (let attempt = 0; attempt < 2; attempt += 1) {
    const token = await getValidToken();
    const headers = new Headers(init.headers);
    headers.set("Authorization", `Bearer ${token}`);
    if (init.body && !headers.has("Content-Type")) headers.set("Content-Type", "application/json");
    headers.set("Accept", "application/json");
    const res = await fetch(url, { ...init, headers });
    const text = await res.text();
    let body: unknown;
    try { body = text ? JSON.parse(text) : null; } catch { body = text; }
    if (!res.ok) {
      if (res.status === 401 && attempt === 0) {
        tokenCache = null;
        continue;
      }
      throw new AirflowApiError(
        res.status,
        body,
        `Airflow API ${init.method ?? "GET"} ${path} failed: ${res.status} ${res.statusText}`,
      );
    }
    return body as T;
  }
  throw new Error("unreachable");
}

// --- Airflow 3.x v2 response shapes ---

export interface AirflowDag {
  dag_id: string;
  dag_display_name?: string;
  is_paused?: boolean;
  is_stale?: boolean;
  description?: string | null;
  timetable_summary?: string | null;
  timetable_description?: string | null;
  bundle_name?: string;
  fileloc?: string;
  relative_fileloc?: string;
  tags?: Array<{ name: string; dag_id?: string }>;
  next_dagrun_logical_date?: string | null;
  last_parsed_time?: string | null;
  // legacy v1 fields kept for forward-compatibility (may appear in some deployments)
  is_active?: boolean;
  schedule_interval?: { __type?: string; value?: string } | string | null;
  owners?: string[];
  next_dagrun?: string | null;
}

export interface AirflowDagRun {
  dag_run_id: string;
  dag_id: string;
  state: string;
  logical_date?: string | null;        // v2 (replaces v1 'execution_date')
  execution_date?: string | null;      // v1 fallback
  start_date?: string | null;
  end_date?: string | null;
  duration?: number | null;
  queued_at?: string | null;
  run_after?: string | null;
  data_interval_start?: string | null;
  data_interval_end?: string | null;
  run_type?: string | null;
  triggered_by?: string | null;
  triggering_user_name?: string | null;
  external_trigger?: boolean;
  conf?: Record<string, unknown>;
  note?: string | null;
}

export interface AirflowTaskInstance {
  task_id: string;
  dag_id: string;
  dag_run_id?: string;
  logical_date?: string | null;
  execution_date?: string | null;
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
