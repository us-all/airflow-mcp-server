import { ToolRegistry, createSearchToolsMetaTool } from "@us-all/mcp-toolkit";
import { config } from "./config.js";

/**
 * Categories used by AIRFLOW_TOOLS / AIRFLOW_DISABLE env toggles.
 *
 * Default: all categories enabled.
 * `AIRFLOW_TOOLS=airflow` → only airflow tools (allowlist)
 * `AIRFLOW_DISABLE=meta`  → exclude meta tools (denylist)
 */
export const CATEGORIES = [
  "airflow",
  "meta",
] as const;

export type Category = (typeof CATEGORIES)[number];

export const registry = new ToolRegistry<Category>({
  enabledCategories: config.enabledCategories,
  disabledCategories: config.disabledCategories,
});

const meta = createSearchToolsMetaTool(
  registry,
  CATEGORIES,
  "Discover available Airflow MCP tools — call this first to find the right tool.",
);

export const searchToolsSchema = meta.schema;
export const searchTools = meta.handler;
