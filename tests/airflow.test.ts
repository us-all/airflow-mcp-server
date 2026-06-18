import { describe, it, expect, beforeAll, beforeEach, afterEach, vi } from "vitest";

beforeAll(() => {
  process.env.AIRFLOW_API_URL = "http://airflow.example.com:8080";
  process.env.AIRFLOW_USERNAME = "admin";
  process.env.AIRFLOW_PASSWORD = "test-pw";
});

function tokenResponse() {
  // JWT with exp far in the future. Header.payload.signature; payload b64url-encoded JSON.
  // We don't need a real signature for tests.
  const header = Buffer.from(JSON.stringify({ alg: "HS512", typ: "JWT" })).toString("base64url");
  const payload = Buffer.from(JSON.stringify({ sub: "admin", role: "ADMIN", exp: Math.floor(Date.now() / 1000) + 86400 })).toString("base64url");
  const sig = "fake-signature";
  return `${header}.${payload}.${sig}`;
}

function makeFetchMock(handlers: Record<string, (init?: RequestInit) => Response>) {
  const calls: Array<{ url: string; method: string; headers: Headers; body?: string }> = [];
  const fn = vi.fn(async (url: string | URL, init?: RequestInit) => {
    const u = String(url);
    calls.push({
      url: u,
      method: init?.method ?? "GET",
      headers: new Headers(init?.headers),
      body: init?.body ? String(init.body) : undefined,
    });
    for (const [needle, handler] of Object.entries(handlers)) {
      if (u.includes(needle)) return handler(init);
    }
    return new Response(JSON.stringify({ error: "no handler", url: u }), { status: 500 });
  }) as unknown as typeof globalThis.fetch;
  return { fn, calls };
}

describe("airflow-mcp v0.2 (Airflow 3.x v2 API + JWT)", () => {
  let originalFetch: typeof globalThis.fetch;

  beforeEach(async () => {
    originalFetch = globalThis.fetch;
    delete process.env.AIRFLOW_ALLOW_WRITE;
    vi.resetModules();
    const mod = await import("../src/clients/airflow.js");
    mod._resetTokenCacheForTest();
  });
  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  it("list-dags: mints JWT then calls /api/v2/dags with Bearer auth + paused=false", async () => {
    const { fn, calls } = makeFetchMock({
      "/auth/token": () => new Response(JSON.stringify({ access_token: tokenResponse() }), { status: 200 }),
      "/api/v2/dags": () => new Response(
        JSON.stringify({
          dags: [
            {
              dag_id: "dbt_daily",
              dag_display_name: "dbt_daily",
              is_paused: false,
              is_stale: false,
              tags: [{ name: "tier1", dag_id: "dbt_daily" }],
              timetable_summary: "0 3 * * *",
              last_parsed_time: "2026-05-07T06:00:00Z",
            },
          ],
          total_entries: 1,
        }),
        { status: 200 },
      ),
    });
    globalThis.fetch = fn;
    const { airflowListDags } = await import("../src/tools/dags.js");
    const r = (await airflowListDags({ onlyActive: true, tag: "tier1", limit: 10 })) as { dags: { dagId: string; schedule?: string; lastParsedTime?: string }[] };
    expect(r.dags.length).toBe(1);
    expect(r.dags[0]?.dagId).toBe("dbt_daily");
    expect(r.dags[0]?.schedule).toBe("0 3 * * *");
    // First call: auth/token (POST). Second: /api/v2/dags with Bearer.
    expect(calls[0]!.url).toContain("/auth/token");
    expect(calls[0]!.method).toBe("POST");
    expect(calls[1]!.url).toContain("/api/v2/dags");
    expect(calls[1]!.url).toContain("paused=false");
    expect(calls[1]!.url).toContain("tags=tier1");
    expect(calls[1]!.headers.get("Authorization")).toMatch(/^Bearer eyJ/);
  });

  it("token cache: subsequent calls reuse the JWT (no re-auth)", async () => {
    const { fn, calls } = makeFetchMock({
      "/auth/token": () => new Response(JSON.stringify({ access_token: tokenResponse() }), { status: 200 }),
      "/api/v2/dags": () => new Response(JSON.stringify({ dags: [], total_entries: 0 }), { status: 200 }),
    });
    globalThis.fetch = fn;
    const { airflowListDags } = await import("../src/tools/dags.js");
    await airflowListDags({ onlyActive: true, limit: 10 });
    await airflowListDags({ onlyActive: true, limit: 10 });
    const tokenCalls = calls.filter((c) => c.url.includes("/auth/token"));
    const apiCalls = calls.filter((c) => c.url.includes("/api/v2/dags"));
    expect(tokenCalls.length).toBe(1);
    expect(apiCalls.length).toBe(2);
  });

  it("401 from API invalidates cached JWT and transparently retries once", async () => {
    let tokenMintCount = 0;
    let apiCount = 0;
    const { fn, calls } = makeFetchMock({
      "/auth/token": () => {
        tokenMintCount += 1;
        return new Response(JSON.stringify({ access_token: tokenResponse() }), { status: 200 });
      },
      "/api/v2/dags": () => {
        apiCount += 1;
        if (apiCount === 1) {
          return new Response(JSON.stringify({ detail: "token expired" }), { status: 401 });
        }
        return new Response(JSON.stringify({ dags: [], total_entries: 0 }), { status: 200 });
      },
    });
    globalThis.fetch = fn;
    const { airflowListDags } = await import("../src/tools/dags.js");
    const r = (await airflowListDags({ onlyActive: true, limit: 10 })) as { count: number };
    expect(r.count).toBe(0);
    expect(tokenMintCount).toBe(2);
    expect(calls.filter((c) => c.url.includes("/api/v2/dags")).length).toBe(2);
  });

  it("trigger-dag is blocked when AIRFLOW_ALLOW_WRITE != 'true'", async () => {
    delete process.env.AIRFLOW_ALLOW_WRITE;
    vi.resetModules();
    const { airflowTriggerDag } = await import("../src/tools/dags.js");
    await expect(airflowTriggerDag({ dagId: "dbt_daily" })).rejects.toThrow(/disabled/i);
  });

  it("trigger-dag posts to /api/v2/dags/{id}/dagRuns when allowed", async () => {
    process.env.AIRFLOW_ALLOW_WRITE = "true";
    vi.resetModules();
    const { fn, calls } = makeFetchMock({
      "/auth/token": () => new Response(JSON.stringify({ access_token: tokenResponse() }), { status: 200 }),
      "/api/v2/dags/dbt_daily/dagRuns": () => new Response(
        JSON.stringify({
          dag_id: "dbt_daily",
          dag_run_id: "manual__2026-05-07T07:00:00",
          state: "queued",
          logical_date: "2026-05-07T07:00:00Z",
        }),
        { status: 200 },
      ),
    });
    globalThis.fetch = fn;
    const { airflowTriggerDag } = await import("../src/tools/dags.js");
    const r = (await airflowTriggerDag({ dagId: "dbt_daily", conf: { foo: "bar" } })) as { triggered: boolean; dagRunId: string; logicalDate: string };
    const post = calls.find((c) => c.method === "POST" && c.url.includes("/dagRuns"));
    expect(post).toBeTruthy();
    expect(JSON.parse(post!.body!)).toMatchObject({ conf: { foo: "bar" } });
    expect(r.triggered).toBe(true);
    expect(r.logicalDate).toBe("2026-05-07T07:00:00Z");
  });

  it("get-task-logs returns the last N kB when content exceeds tailKb", async () => {
    const longContent = "A".repeat(40 * 1024) + "TAIL_MARKER";
    const { fn } = makeFetchMock({
      "/auth/token": () => new Response(JSON.stringify({ access_token: tokenResponse() }), { status: 200 }),
      "/logs/": () => new Response(JSON.stringify({ content: longContent }), { status: 200 }),
    });
    globalThis.fetch = fn;
    const { airflowGetTaskLogs } = await import("../src/tools/dags.js");
    const r = (await airflowGetTaskLogs({ dagId: "dbt_daily", dagRunId: "x", taskId: "compile", tryNumber: 1, tailKb: 16 })) as { truncated: boolean; content: string };
    expect(r.truncated).toBe(true);
    expect(r.content.endsWith("TAIL_MARKER")).toBe(true);
    expect(r.content.length).toBeLessThanOrEqual(16 * 1024);
  });

  it("get-task-logs tails by UTF-8 bytes without splitting multibyte characters", async () => {
    const longContent = "A".repeat(2048) + "한".repeat(20) + "TAIL";
    const { fn } = makeFetchMock({
      "/auth/token": () => new Response(JSON.stringify({ access_token: tokenResponse() }), { status: 200 }),
      "/logs/": () => new Response(JSON.stringify({ content: longContent }), { status: 200 }),
    });
    globalThis.fetch = fn;
    const { airflowGetTaskLogs } = await import("../src/tools/dags.js");
    const r = (await airflowGetTaskLogs({ dagId: "dbt_daily", dagRunId: "x", taskId: "compile", tryNumber: 1, tailKb: 1 })) as { truncated: boolean; content: string; bytesReturned: number };
    expect(r.truncated).toBe(true);
    expect(r.content).not.toContain("\uFFFD");
    expect(r.content.endsWith("TAIL")).toBe(true);
    expect(r.bytesReturned).toBeLessThanOrEqual(1024);
    expect(Buffer.byteLength(r.content, "utf8")).toBe(r.bytesReturned);
  });

  it("get-task-logs flattens Airflow 3.x structured JSON content (array of entries)", async () => {
    const structured = [
      { event: "::group::Log message source details", sources: ["/opt/airflow/logs/.../attempt=2.log"] },
      { timestamp: "2026-05-27T18:07:47.109322Z", level: "info", event: "Failure in model view_crm_campus_profiles" },
      {
        timestamp: "2026-05-27T18:07:47.110281Z",
        level: "info",
        event: "Database Error in model view_crm_campus_profiles",
        error_detail: "Not found: Dataset us-service-data:us_campus was not found in location asia-northeast3",
      },
    ];
    const { fn } = makeFetchMock({
      "/auth/token": () => new Response(JSON.stringify({ access_token: tokenResponse() }), { status: 200 }),
      "/logs/": () => new Response(JSON.stringify({ content: structured }), { status: 200 }),
    });
    globalThis.fetch = fn;
    const { airflowGetTaskLogs } = await import("../src/tools/dags.js");
    const r = (await airflowGetTaskLogs({ dagId: "dbt_daily", dagRunId: "x", taskId: "dbt_run", tryNumber: 2, tailKb: 16 })) as { truncated: boolean; content: string };
    expect(r.truncated).toBe(false);
    expect(r.content).toContain("[info] Failure in model view_crm_campus_profiles");
    expect(r.content).toContain("us_campus was not found in location asia-northeast3");
    expect(r.content).toContain("2026-05-27T18:07:47.109322Z");
  });

  it("dag-health-rollup computes success rate + last failed run + failing tasks (v2 logical_date)", async () => {
    const { fn } = makeFetchMock({
      "/auth/token": () => new Response(JSON.stringify({ access_token: tokenResponse() }), { status: 200 }),
      "/dagRuns?": () => new Response(
        JSON.stringify({
          dag_runs: [
            { dag_run_id: "r1", dag_id: "x", state: "success", logical_date: "2026-05-06T00:00:00Z", start_date: "2026-05-06T00:00:00Z", end_date: "2026-05-06T00:01:00Z" },
            { dag_run_id: "r2", dag_id: "x", state: "failed",  logical_date: "2026-05-06T01:00:00Z", start_date: "2026-05-06T01:00:00Z", end_date: "2026-05-06T01:01:30Z" },
            { dag_run_id: "r3", dag_id: "x", state: "success", logical_date: "2026-05-06T02:00:00Z", start_date: "2026-05-06T02:00:00Z", end_date: "2026-05-06T02:00:45Z" },
            { dag_run_id: "r4", dag_id: "x", state: "running", logical_date: "2026-05-06T03:00:00Z", start_date: "2026-05-06T03:00:00Z", end_date: null },
          ],
          total_entries: 4,
        }),
        { status: 200 },
      ),
      "/taskInstances": () => new Response(
        JSON.stringify({
          task_instances: [
            { task_id: "compile", dag_id: "x", state: "success", duration: 10 },
            { task_id: "run", dag_id: "x", state: "failed", duration: 80 },
          ],
          total_entries: 2,
        }),
        { status: 200 },
      ),
    });
    globalThis.fetch = fn;
    const { dagHealthRollup } = await import("../src/tools/aggregations.js");
    const r = (await dagHealthRollup({ dagId: "x", recentRuns: 4, includeFailingTasks: true })) as {
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
    expect(r.lastFailureTasks).toEqual([{ taskId: "run", state: "failed", duration: 80 }]);
  });

  it("airflow-list-assets sends /api/v2/assets with uri_pattern and repeated dag_ids", async () => {
    const { fn, calls } = makeFetchMock({
      "/auth/token": () => new Response(JSON.stringify({ access_token: tokenResponse() }), { status: 200 }),
      "/api/v2/assets": () =>
        new Response(
          JSON.stringify({
            assets: [
              {
                id: 7,
                uri: "s3://my-bucket/orders",
                producing_tasks: [{ dag_id: "ingest", task_id: "write_orders" }],
                consuming_dags: [{ dag_id: "transform" }, { dag_id: "metrics" }],
                updated_at: "2026-05-30T00:00:00Z",
              },
            ],
            total_entries: 1,
          }),
          { status: 200 },
        ),
    });
    globalThis.fetch = fn;
    const { airflowListAssets } = await import("../src/tools/assets.js");
    const result = (await airflowListAssets({
      uriPattern: "s3://my-bucket/",
      dagIds: ["ingest", "transform"],
      limit: 50,
      offset: 0,
    })) as { totalEntries: number; assets: Array<{ uri: string; producingTasks: unknown[]; consumingDags: string[] }> };

    const assetsCall = calls.find((c) => c.url.includes("/api/v2/assets"));
    expect(assetsCall?.url).toContain("uri_pattern=s3%3A%2F%2Fmy-bucket%2F");
    expect(assetsCall?.url).toContain("dag_ids=ingest");
    expect(assetsCall?.url).toContain("dag_ids=transform");

    expect(result.totalEntries).toBe(1);
    expect(result.assets[0]!.uri).toBe("s3://my-bucket/orders");
    expect(result.assets[0]!.consumingDags).toEqual(["transform", "metrics"]);
  });

  it("airflow-list-asset-events orders by -timestamp by default and forwards filter args", async () => {
    const { fn, calls } = makeFetchMock({
      "/auth/token": () => new Response(JSON.stringify({ access_token: tokenResponse() }), { status: 200 }),
      "/api/v2/assets/events": () =>
        new Response(
          JSON.stringify({
            asset_events: [
              {
                id: 1,
                asset_id: 7,
                asset_uri: "s3://my-bucket/orders",
                timestamp: "2026-05-29T22:00:00Z",
                source_dag_id: "ingest",
                source_run_id: "manual__2026-05-29T22:00",
                source_task_id: "write_orders",
                created_dagruns: [{ dag_id: "transform", dag_run_id: "scheduled__2026-05-29T22:05" }],
              },
            ],
            total_entries: 1,
          }),
          { status: 200 },
        ),
    });
    globalThis.fetch = fn;
    const { airflowListAssetEvents } = await import("../src/tools/assets.js");
    const result = (await airflowListAssetEvents({
      assetId: 7,
      timestampGte: "2026-05-29T00:00:00Z",
      limit: 50,
      offset: 0,
      orderBy: "-timestamp",
    })) as { events: Array<{ assetUri: string; triggeredDagRuns: Array<{ dagId: string }> }> };

    const ev = calls.find((c) => c.url.includes("/api/v2/assets/events"));
    expect(ev?.url).toContain("asset_id=7");
    expect(ev?.url).toContain("timestamp_gte=2026-05-29T00%3A00%3A00Z");
    expect(ev?.url).toContain("order_by=-timestamp");
    expect(result.events[0]!.triggeredDagRuns[0]!.dagId).toBe("transform");
  });

  it("auth/token failure surfaces structured AirflowApiError", async () => {
    const { fn } = makeFetchMock({
      "/auth/token": () => new Response(JSON.stringify({ detail: "Invalid credentials" }), { status: 401 }),
    });
    globalThis.fetch = fn;
    const { airflowListDags } = await import("../src/tools/dags.js");
    await expect(airflowListDags({ onlyActive: true, limit: 10 })).rejects.toThrow(/auth\/token/);
  });

  it("concurrent calls on a cold cache mint exactly one JWT (in-flight coalesce)", async () => {
    let tokenMints = 0;
    const { fn } = makeFetchMock({
      "/auth/token": () => {
        tokenMints += 1;
        return new Response(JSON.stringify({ access_token: tokenResponse() }), { status: 200 });
      },
      "/api/v2/dags": () =>
        new Response(JSON.stringify({ dags: [], total_entries: 0 }), { status: 200 }),
    });
    globalThis.fetch = fn;
    const { airflowListDags } = await import("../src/tools/dags.js");
    await Promise.all([
      airflowListDags({ onlyActive: true, limit: 10 }),
      airflowListDags({ onlyActive: true, limit: 10 }),
      airflowListDags({ onlyActive: true, limit: 10 }),
      airflowListDags({ onlyActive: true, limit: 10 }),
      airflowListDags({ onlyActive: true, limit: 10 }),
    ]);
    expect(tokenMints).toBe(1);
  });

  it("AIRFLOW_BEARER_TOKEN bypasses /auth/token and sends the env token verbatim", async () => {
    let mintCalled = false;
    const { fn, calls } = makeFetchMock({
      "/auth/token": () => {
        mintCalled = true;
        return new Response(JSON.stringify({ access_token: tokenResponse() }), { status: 200 });
      },
      "/api/v2/dags": () =>
        new Response(JSON.stringify({ dags: [], total_entries: 0 }), { status: 200 }),
    });
    globalThis.fetch = fn;
    process.env.AIRFLOW_BEARER_TOKEN = "external-idp-token-abc";
    try {
      vi.resetModules();
      const mod = await import("../src/clients/airflow.js");
      mod._resetTokenCacheForTest();
      const { airflowListDags } = await import("../src/tools/dags.js");
      await airflowListDags({ onlyActive: true, limit: 10 });

      expect(mintCalled).toBe(false);
      const dagsCall = calls.find((c) => c.url.includes("/api/v2/dags"));
      expect(dagsCall?.headers.get("Authorization")).toBe("Bearer external-idp-token-abc");
    } finally {
      delete process.env.AIRFLOW_BEARER_TOKEN;
    }
  });

  it("validateConfig accepts AIRFLOW_BEARER_TOKEN alone (no username/password warn)", async () => {
    delete process.env.AIRFLOW_USERNAME;
    delete process.env.AIRFLOW_PASSWORD;
    process.env.AIRFLOW_BEARER_TOKEN = "x";
    try {
      vi.resetModules();
      const { validateConfig } = await import("../src/config.js");
      const writes: string[] = [];
      const origWrite = process.stderr.write.bind(process.stderr);
      process.stderr.write = ((chunk: string | Uint8Array) => {
        writes.push(typeof chunk === "string" ? chunk : Buffer.from(chunk).toString());
        return true;
      }) as typeof process.stderr.write;
      try {
        expect(() => validateConfig()).not.toThrow();
      } finally {
        process.stderr.write = origWrite;
      }
      expect(writes.join("")).not.toMatch(/WARN/);
    } finally {
      delete process.env.AIRFLOW_BEARER_TOKEN;
      process.env.AIRFLOW_USERNAME = "admin";
      process.env.AIRFLOW_PASSWORD = "test-pw";
    }
  });

  it("token refresh after failed mint is not poisoned (in-flight cleared on error)", async () => {
    let tokenAttempts = 0;
    const { fn } = makeFetchMock({
      "/auth/token": () => {
        tokenAttempts += 1;
        if (tokenAttempts === 1) {
          return new Response(JSON.stringify({ detail: "boom" }), { status: 500 });
        }
        return new Response(JSON.stringify({ access_token: tokenResponse() }), { status: 200 });
      },
      "/api/v2/dags": () =>
        new Response(JSON.stringify({ dags: [], total_entries: 0 }), { status: 200 }),
    });
    globalThis.fetch = fn;
    const { airflowListDags } = await import("../src/tools/dags.js");
    await expect(airflowListDags({ onlyActive: true, limit: 10 })).rejects.toThrow();
    // Second call must be allowed to retry — not blocked by stale in-flight promise.
    await airflowListDags({ onlyActive: true, limit: 10 });
    expect(tokenAttempts).toBe(2);
  });

  it("airflow-list-import-errors GETs /api/v2/importErrors newest-first and maps fields", async () => {
    const { fn, calls } = makeFetchMock({
      "/auth/token": () => new Response(JSON.stringify({ access_token: tokenResponse() }), { status: 200 }),
      "/api/v2/importErrors": () => new Response(
        JSON.stringify({
          import_errors: [
            {
              import_error_id: 3,
              filename: "dags/broken.py",
              bundle_name: "dags-folder",
              timestamp: "2026-06-18T05:00:00Z",
              stack_trace: "ModuleNotFoundError: no module named 'foo'",
            },
          ],
          total_entries: 1,
        }),
        { status: 200 },
      ),
    });
    globalThis.fetch = fn;
    const { airflowListImportErrors } = await import("../src/tools/admin.js");
    const r = (await airflowListImportErrors({ limit: 50 })) as {
      totalEntries: number;
      importErrors: { filename: string; bundle: string; stackTrace: string }[];
    };
    const call = calls.find((c) => c.url.includes("/api/v2/importErrors"));
    expect(call?.url).toContain("order_by=-timestamp");
    expect(call?.method).toBe("GET");
    expect(r.totalEntries).toBe(1);
    expect(r.importErrors[0]!.filename).toBe("dags/broken.py");
    expect(r.importErrors[0]!.bundle).toBe("dags-folder");
    expect(r.importErrors[0]!.stackTrace).toContain("ModuleNotFoundError");
  });

  it("airflow-list-dag-warnings forwards dag_id + warning_type filters", async () => {
    const { fn, calls } = makeFetchMock({
      "/auth/token": () => new Response(JSON.stringify({ access_token: tokenResponse() }), { status: 200 }),
      "/api/v2/dagWarnings": () => new Response(
        JSON.stringify({
          dag_warnings: [
            { dag_id: "dbt_daily", warning_type: "non-existent pool", message: "Pool 'x' does not exist", timestamp: "2026-06-18T05:00:00Z" },
          ],
          total_entries: 1,
        }),
        { status: 200 },
      ),
    });
    globalThis.fetch = fn;
    const { airflowListDagWarnings } = await import("../src/tools/admin.js");
    const r = (await airflowListDagWarnings({ dagId: "dbt_daily", warningType: "non-existent pool", limit: 50 })) as {
      dagWarnings: { dagId: string; warningType: string; message: string }[];
    };
    const call = calls.find((c) => c.url.includes("/api/v2/dagWarnings"));
    expect(call?.url).toContain("dag_id=dbt_daily");
    expect(call?.url).toContain("warning_type=non-existent+pool");
    expect(r.dagWarnings[0]!.warningType).toBe("non-existent pool");
    expect(r.dagWarnings[0]!.message).toContain("does not exist");
  });

  it("airflow-list-pools maps slot utilization fields", async () => {
    const { fn, calls } = makeFetchMock({
      "/auth/token": () => new Response(JSON.stringify({ access_token: tokenResponse() }), { status: 200 }),
      "/api/v2/pools": () => new Response(
        JSON.stringify({
          pools: [
            { name: "default_pool", slots: 128, occupied_slots: 5, running_slots: 3, queued_slots: 2, open_slots: 123, description: "Default pool", include_deferred: false },
          ],
          total_entries: 1,
        }),
        { status: 200 },
      ),
    });
    globalThis.fetch = fn;
    const { airflowListPools } = await import("../src/tools/admin.js");
    const r = (await airflowListPools({ limit: 50 })) as {
      pools: { name: string; slots: number; openSlots: number; queuedSlots: number }[];
    };
    const call = calls.find((c) => c.url.includes("/api/v2/pools"));
    expect(call?.method).toBe("GET");
    expect(r.pools[0]!.name).toBe("default_pool");
    expect(r.pools[0]!.slots).toBe(128);
    expect(r.pools[0]!.openSlots).toBe(123);
    expect(r.pools[0]!.queuedSlots).toBe(2);
  });
});
