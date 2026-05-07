import dotenv from "dotenv";

dotenv.config({ quiet: true });

function parseList(raw: string | undefined): string[] | null {
  if (!raw) return null;
  return raw.split(",").map((s) => s.trim().toLowerCase()).filter(Boolean);
}

export const config = {
  apiUrl: (process.env.AIRFLOW_API_URL ?? "").replace(/\/+$/, ""),
  username: process.env.AIRFLOW_USERNAME ?? "",
  password: process.env.AIRFLOW_PASSWORD ?? "",
  allowWrite: process.env.AIRFLOW_ALLOW_WRITE === "true",
  enabledCategories: parseList(process.env.AIRFLOW_TOOLS),
  disabledCategories: parseList(process.env.AIRFLOW_DISABLE),
};

export function validateConfig(): void {
  if (!config.apiUrl) {
    throw new Error("AIRFLOW_API_URL environment variable is required");
  }
  if (!config.username) {
    process.stderr.write(
      "[airflow-mcp] WARN: AIRFLOW_USERNAME not set — calls will be sent unauthenticated\n",
    );
  }
}
