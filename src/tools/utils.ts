import { createWrapToolHandler } from "@us-all/mcp-toolkit";
import { config } from "../config.js";

export class WriteBlockedError extends Error {
  constructor() {
    super("Write operations are disabled. Set AIRFLOW_ALLOW_WRITE=true to enable.");
    this.name = "WriteBlockedError";
  }
}

export function assertWriteAllowed(): void {
  if (!config.allowWrite) {
    throw new WriteBlockedError();
  }
}

export class AirflowApiError extends Error {
  status: number;
  body: unknown;
  constructor(status: number, body: unknown, message: string) {
    super(message);
    this.name = "AirflowApiError";
    this.status = status;
    this.body = body;
  }
}

export const wrapToolHandler = createWrapToolHandler({
  redactionPatterns: [
    /AIRFLOW_PASSWORD/i,
    /Authorization:\s*Basic\s+[A-Za-z0-9+/=]+/i,
  ],
  errorExtractors: [
    {
      match: (error) => error instanceof WriteBlockedError,
      extract: (error) => ({
        kind: "passthrough",
        text: (error as WriteBlockedError).message,
      }),
    },
    {
      match: (error) => error instanceof AirflowApiError,
      extract: (error) => {
        const err = error as AirflowApiError;
        return {
          kind: "structured",
          data: { message: err.message, status: err.status, details: err.body },
        };
      },
    },
  ],
});
