#!/usr/bin/env node
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));
const serverPath = join(here, "..", "dist", "index.js");

if (!process.env.AIRFLOW_API_URL) {
  console.error("AIRFLOW_API_URL is required for the smoke test");
  process.exit(1);
}

const proc = spawn(process.execPath, [serverPath], {
  stdio: ["pipe", "pipe", "inherit"],
  env: process.env,
});

let buffer = "";
const pending = new Map();
proc.stdout.on("data", (c) => {
  buffer += c.toString();
  let nl;
  while ((nl = buffer.indexOf("\n")) !== -1) {
    const line = buffer.slice(0, nl).trim();
    buffer = buffer.slice(nl + 1);
    if (!line) continue;
    try {
      const m = JSON.parse(line);
      const cb = pending.get(m.id);
      if (cb) { pending.delete(m.id); cb(m); }
    } catch {}
  }
});

let nextId = 1;
const send = (method, params) => new Promise((resolve, reject) => {
  const id = nextId++;
  pending.set(id, (m) => m.error ? reject(new Error(JSON.stringify(m.error))) : resolve(m.result));
  proc.stdin.write(JSON.stringify({ jsonrpc: "2.0", id, method, params }) + "\n");
});
const notify = (method, params) => proc.stdin.write(JSON.stringify({ jsonrpc: "2.0", method, params }) + "\n");

(async () => {
  await send("initialize", { protocolVersion: "2024-11-05", capabilities: {}, clientInfo: { name: "smoke", version: "0" } });
  notify("notifications/initialized", {});
  const tools = await send("tools/list", {});
  console.log(`tools/list returned ${tools.tools.length} tools`);
  try {
    const r = await send("tools/call", { name: "airflow-list-dags", arguments: { onlyActive: true, limit: 5 } });
    console.log("airflow-list-dags OK:", r.content?.[0]?.text?.slice(0, 200) + "...");
  } catch (e) {
    console.warn("airflow-list-dags failed (acceptable if no tunnel):", e.message);
  }
  proc.kill();
  process.exit(0);
})().catch((e) => { console.error("FAIL:", e); proc.kill(); process.exit(1); });
