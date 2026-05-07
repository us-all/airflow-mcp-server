import { describe, it, expect, beforeAll, beforeEach, afterEach, vi } from "vitest";

beforeAll(() => {
  process.env.AIRFLOW_API_URL = "http://airflow.example.com:8080/api/v1";
  process.env.AIRFLOW_USERNAME = "admin";
  process.env.AIRFLOW_PASSWORD = "test-pw";
});

describe("airflow tools", () => {
  let originalFetch: typeof globalThis.fetch;

  beforeEach(() => {
    originalFetch = globalThis.fetch;
  });
  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  it("airflow-list-dags passes only_active and tag filters; basic-auth is sent", async () => {
    let captured: { url: string; headers: Headers } | null = null;
    globalThis.fetch = vi.fn(async (url: string | URL, init?: RequestInit) => {
      captured = { url: String(url), headers: new Headers(init?.headers) };
      return new Response(
        JSON.stringify({
          dags: [
            { dag_id: "load_users", is_active: true, owners: ["data-eng"], tags: [{ name: "tier1" }] },
            { dag_id: "score_dq", is_active: true, owners: ["data-eng"], tags: [{ name: "dq" }] },
          ],
          total_entries: 2,
        }),
        { status: 200 },
      );
    }) as unknown as typeof globalThis.fetch;

    const { airflowListDags } = await import("../src/tools/dags.js");
    const r = (await airflowListDags({ onlyActive: true, tag: "tier1", limit: 50 })) as {
      dags: { dagId: string }[];
    };
    expect(captured!.url).toContain("only_active=true");
    expect(captured!.url).toContain("tags=tier1");
    expect(captured!.headers.get("Authorization")).toMatch(/^Basic /);
    expect(r.dags.length).toBeGreaterThan(0);
  });

  it("airflow-trigger-dag is blocked when AIRFLOW_ALLOW_WRITE is not 'true'", async () => {
    delete process.env.AIRFLOW_ALLOW_WRITE;
    vi.resetModules();
    const { airflowTriggerDag } = await import("../src/tools/dags.js");
    await expect(airflowTriggerDag({ dagId: "load_users" })).rejects.toThrow(/disabled/i);
  });

  it("airflow-trigger-dag posts to dagRuns when AIRFLOW_ALLOW_WRITE=true", async () => {
    process.env.AIRFLOW_ALLOW_WRITE = "true";
    vi.resetModules();
    let captured: { url: string; method: string; body: string } | null = null;
    globalThis.fetch = vi.fn(async (url: string | URL, init?: RequestInit) => {
      captured = { url: String(url), method: init?.method ?? "GET", body: String(init?.body ?? "") };
      return new Response(
        JSON.stringify({
          dag_id: "load_users",
          dag_run_id: "manual__2026-05-06T12:00:00",
          state: "queued",
          execution_date: "2026-05-06T12:00:00Z",
        }),
        { status: 200 },
      );
    }) as unknown as typeof globalThis.fetch;
    const { airflowTriggerDag } = await import("../src/tools/dags.js");
    const r = (await airflowTriggerDag({
      dagId: "load_users",
      conf: { foo: "bar" },
    })) as { triggered: boolean; dagRunId: string };
    expect(captured!.method).toBe("POST");
    expect(captured!.url).toContain("/dags/load_users/dagRuns");
    expect(JSON.parse(captured!.body)).toMatchObject({ conf: { foo: "bar" } });
    expect(r.triggered).toBe(true);
  });

  it("airflow-get-task-logs returns the tail when content exceeds tailKb", async () => {
    process.env.AIRFLOW_ALLOW_WRITE = "false";
    vi.resetModules();
    const longContent = "A".repeat(40 * 1024) + "TAIL_MARKER";
    globalThis.fetch = vi.fn(async () =>
      new Response(JSON.stringify({ content: longContent }), { status: 200 }),
    ) as unknown as typeof globalThis.fetch;
    const { airflowGetTaskLogs } = await import("../src/tools/dags.js");
    const r = (await airflowGetTaskLogs({
      dagId: "load_users",
      dagRunId: "x",
      taskId: "extract",
      tryNumber: 1,
      tailKb: 16,
    })) as { truncated: boolean; content: string };
    expect(r.truncated).toBe(true);
    expect(r.content.endsWith("TAIL_MARKER")).toBe(true);
    expect(r.content.length).toBeLessThanOrEqual(16 * 1024);
  });

  it("dag-health-rollup computes success rate + avg duration + last failed run", async () => {
    vi.resetModules();
    const fakeRuns = [
      { dag_run_id: "r1", state: "success", start_date: "2026-05-06T00:00:00Z", end_date: "2026-05-06T00:01:00Z" },
      { dag_run_id: "r2", state: "failed",  start_date: "2026-05-06T01:00:00Z", end_date: "2026-05-06T01:01:30Z" },
      { dag_run_id: "r3", state: "success", start_date: "2026-05-06T02:00:00Z", end_date: "2026-05-06T02:00:45Z" },
      { dag_run_id: "r4", state: "running", start_date: "2026-05-06T03:00:00Z", end_date: null },
    ];
    let nthCall = 0;
    globalThis.fetch = vi.fn(async (url: string | URL) => {
      nthCall++;
      const u = String(url);
      if (u.includes("/dagRuns?")) {
        return new Response(JSON.stringify({ dag_runs: fakeRuns, total_entries: fakeRuns.length }), { status: 200 });
      }
      if (u.includes("/taskInstances")) {
        return new Response(
          JSON.stringify({
            task_instances: [
              { task_id: "extract", state: "success", duration: 10 },
              { task_id: "transform", state: "failed", duration: 80 },
            ],
            total_entries: 2,
          }),
          { status: 200 },
        );
      }
      throw new Error(`unexpected fetch: ${u}`);
    }) as unknown as typeof globalThis.fetch;

    const { dagHealthRollup } = await import("../src/tools/aggregations.js");
    const r = (await dagHealthRollup({ dagId: "load_users", recentRuns: 4, includeFailingTasks: true })) as {
      successRatePct: number;
      counts: { succeeded: number; failed: number; runningOrQueued: number };
      avgDurationSec: number | null;
      lastFailedRunId: string | null;
      lastFailureTasks: { taskId: string; state: string }[] | null;
    };
    expect(r.counts.succeeded).toBe(2);
    expect(r.counts.failed).toBe(1);
    expect(r.counts.runningOrQueued).toBe(1);
    expect(r.successRatePct).toBe(50);
    expect(r.avgDurationSec).toBeGreaterThan(0);
    expect(r.lastFailedRunId).toBe("r2");
    expect(r.lastFailureTasks).toEqual([{ taskId: "transform", state: "failed", duration: 80 }]);
    expect(nthCall).toBeGreaterThanOrEqual(2);
  });
});
