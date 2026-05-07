import { z } from "zod";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";

export function registerPrompts(server: McpServer): void {
  server.registerPrompt(
    "dag-failure-triage",
    {
      title: "Triage a failing Airflow DAG",
      description:
        "Pull recent runs of a DAG, isolate the most recent failed run, dump the failing task tail logs, and propose a remediation. If @us-all/dbt-mcp is also installed, suggest cross-referencing with dbt run results when the DAG runs dbt.",
      argsSchema: {
        dagId: z.string().describe("Airflow DAG id to triage"),
        recentRuns: z.string().optional().describe("How many recent runs to scan (default '10')"),
      },
    },
    ({ dagId, recentRuns }) => {
      const n = recentRuns ?? "10";
      return {
        messages: [
          {
            role: "user",
            content: {
              type: "text",
              text: [
                `Triage Airflow DAG '${dagId}'.`,
                "",
                "Steps:",
                `1. Call \`dag-health-rollup\` with dagId=${JSON.stringify(dagId)}, recentRuns=${n}, includeFailingTasks=true. Capture successRatePct, counts, avgDurationSec, lastFailedRunId, and lastFailureTasks.`,
                "2. If there is a lastFailedRunId, for each failing task call `airflow-get-task-logs` with that dagRunId, taskId=<task>, tryNumber=<max try_number>, tailKb=16.",
                "3. Read the log tail to extract the actual error message (Python traceback / SQL error / connection error / timeout).",
                "4. If @us-all/dbt-mcp is installed and the DAG runs dbt models (e.g. dag_id contains 'dbt' or tags include 'dbt'), suggest the user also call `dbt-failed-tests` and `dbt-get-run-results` for additional context.",
                "5. Produce a triage summary:",
                "   - Health snapshot: successRatePct, recent state distribution, avg duration trend.",
                "   - Last failure: task ids, error category (data / config / infra / dependency), root-cause hypothesis (1 sentence).",
                "   - Recommended actions (3-5, ranked):",
                "     * 'Re-run via airflow-clear-task' if transient",
                "     * 'Investigate connector / secret rotation' if auth/permission",
                "     * 'Roll back upstream code change' if regression after deploy",
                "     * 'Page oncall' if Tier 1 + repeated failure",
                "6. IMPORTANT: do NOT call `airflow-clear-task` or `airflow-trigger-dag` automatically — list them as next steps the user should run manually after confirming the diagnosis.",
              ].join("\n"),
            },
          },
        ],
      };
    },
  );

  server.registerPrompt(
    "dag-schedule-audit",
    {
      title: "Audit DAG scheduling health",
      description:
        "Sweep all active DAGs, identify schedule misses (next_dagrun far in the past), abnormally low success rates, and stuck/queued runs.",
      argsSchema: {
        successRateThresholdPct: z.string().optional().describe("Flag DAGs below this success rate over recent runs (default '95')"),
      },
    },
    ({ successRateThresholdPct }) => {
      const thr = successRateThresholdPct ?? "95";
      return {
        messages: [
          {
            role: "user",
            content: {
              type: "text",
              text: [
                `Audit all active Airflow DAGs (success-rate threshold: ${thr}%).`,
                "",
                "Steps:",
                "1. Call `airflow-list-dags` with onlyActive=true, limit=200.",
                "2. For each DAG (or sample top-50 by tag priority), call `dag-health-rollup` with recentRuns=10, includeFailingTasks=false.",
                `3. Flag DAGs where successRatePct < ${thr}, or runningOrQueued > 0 with start_date older than the schedule interval × 2.`,
                "4. Group findings:",
                "   - **Critical**: Tier 1 / production-tagged DAGs below threshold.",
                "   - **Warning**: Other DAGs below threshold or with stuck runs.",
                "   - **Healthy**: Everything else (just count, don't list).",
                "5. Produce a one-page audit:",
                "   - Headline: total DAGs, % healthy.",
                "   - Critical / Warning sections with: dag_id, successRatePct, lastFailedRunId, suggested action.",
                "   - 'Next 24h watchlist': DAGs trending down even if still above threshold.",
              ].join("\n"),
            },
          },
        ],
      };
    },
  );
}
