import dotenv from "dotenv";

dotenv.config({ quiet: true });

function parseList(raw: string | undefined): string[] | null {
  if (!raw) return null;
  return raw.split(",").map((s) => s.trim().toLowerCase()).filter(Boolean);
}

/**
 * Normalize the user-supplied AIRFLOW_API_URL into the host base — strip a
 * trailing /api/v1 or /api/v2 since this server (v0.2+) targets Airflow 3.x
 * v2 endpoints exclusively and prepends /api/v2 internally.
 */
function normalizeBase(raw: string): string {
  const trimmed = raw.replace(/\/+$/, "");
  return trimmed.replace(/\/api\/v[12]$/, "");
}

const rawUrl = (process.env.AIRFLOW_API_URL ?? "").trim();

export const config = {
  apiBase: rawUrl ? normalizeBase(rawUrl) : "",
  username: process.env.AIRFLOW_USERNAME ?? "",
  password: process.env.AIRFLOW_PASSWORD ?? "",
  // Pre-minted Bearer token from an external IDP (Auth0, Okta, kubectl/OIDC, etc.).
  // When set, the server skips the SimpleAuthManager /auth/token mint path and
  // sends `Authorization: Bearer <token>` as-is. The caller owns refresh;
  // expired tokens surface as the original 401 from Airflow.
  bearerToken: process.env.AIRFLOW_BEARER_TOKEN ?? "",
  allowWrite: process.env.AIRFLOW_ALLOW_WRITE === "true",
  enabledCategories: parseList(process.env.AIRFLOW_TOOLS),
  disabledCategories: parseList(process.env.AIRFLOW_DISABLE),
};

export function validateConfig(): void {
  if (!config.apiBase) {
    throw new Error("AIRFLOW_API_URL environment variable is required");
  }
  if (config.bearerToken) return; // external Bearer wins; username/password optional
  if (!config.username || !config.password) {
    process.stderr.write(
      "[airflow-mcp] WARN: neither AIRFLOW_BEARER_TOKEN nor (AIRFLOW_USERNAME + AIRFLOW_PASSWORD) is set — JWT cannot be minted; calls will fail\n",
    );
  }
}
