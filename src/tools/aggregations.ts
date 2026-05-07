import { z } from "zod";
import { airflowListRuns, airflowGetTaskInstances } from "./dags.js";

export const dagHealthRollupSchema = z.object({
  dagId: z.string().describe("Airflow DAG id"),
  recentRuns: z.coerce.number().int().min(1).max(100).default(10),
  includeFailingTasks: z.boolean().default(true).describe("If true, fetch task instances for the most recent failed run"),
});

interface DagRunOut {
  dagRunId: string;
  state: string;
  startDate?: string | null;
  endDate?: string | null;
  durationSec?: number | null;
}

interface TaskInstance {
  taskId: string;
  state: string | null;
  duration?: number | null;
}

export async function dagHealthRollup(args: z.infer<typeof dagHealthRollupSchema>): Promise<unknown> {
  const runs = (await airflowListRuns({
    dagId: args.dagId,
    limit: args.recentRuns,
  })) as { runs: Array<{ dagRunId: string; state: string; startDate?: string | null; endDate?: string | null }> };

  const enriched: DagRunOut[] = runs.runs.map((r) => {
    let durationSec: number | null = null;
    if (r.startDate && r.endDate) {
      durationSec = (new Date(r.endDate).getTime() - new Date(r.startDate).getTime()) / 1000;
    }
    return { ...r, durationSec };
  });

  const total = enriched.length;
  const succeeded = enriched.filter((r) => r.state === "success").length;
  const failed = enriched.filter((r) => r.state === "failed").length;
  const running = enriched.filter((r) => r.state === "running" || r.state === "queued").length;
  const successRatePct =
    total === 0 ? null : Math.round((succeeded / total) * 1000) / 10;

  const completed = enriched.filter((r) => r.durationSec != null);
  const avgDurationSec =
    completed.length === 0
      ? null
      : Math.round(
          completed.reduce((acc, r) => acc + (r.durationSec ?? 0), 0) /
            completed.length,
        );

  let lastFailureTasks: TaskInstance[] | null = null;
  const lastFailedRun = enriched.find((r) => r.state === "failed");
  if (args.includeFailingTasks && lastFailedRun) {
    try {
      const ti = (await airflowGetTaskInstances({
        dagId: args.dagId,
        dagRunId: lastFailedRun.dagRunId,
      })) as { taskInstances: Array<{ taskId: string; state: string | null; duration?: number | null }> };
      lastFailureTasks = ti.taskInstances
        .filter((t) => t.state !== "success")
        .map((t) => ({ taskId: t.taskId, state: t.state, duration: t.duration }));
    } catch {
      lastFailureTasks = null;
    }
  }

  return {
    dagId: args.dagId,
    window: { recentRuns: args.recentRuns, runsScanned: total },
    successRatePct,
    counts: { succeeded, failed, runningOrQueued: running, total },
    avgDurationSec,
    lastFailedRunId: lastFailedRun?.dagRunId ?? null,
    lastFailureTasks,
    runs: enriched,
  };
}
