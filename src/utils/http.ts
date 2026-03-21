import { IncomingMessage, ServerResponse } from "node:http";
import { AppError, ValidationError } from "./errors";

const JSON_HEADERS = { "content-type": "application/json" };
const MAX_BODY_SIZE_BYTES = 1024 * 1024;

export function sendJson(
  response: ServerResponse,
  statusCode: number,
  payload: unknown,
  headers: Record<string, string> = {},
): void {
  response.writeHead(statusCode, { ...JSON_HEADERS, ...headers });
  response.end(JSON.stringify(payload));
}

export async function readJsonBody(request: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  let size = 0;

  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buffer.length;

    if (size > MAX_BODY_SIZE_BYTES) {
      throw new ValidationError("Request body exceeds the 1MB limit.");
    }

    chunks.push(buffer);
  }

  if (chunks.length === 0) {
    throw new ValidationError("Request body is required.");
  }

  const rawBody = Buffer.concat(chunks).toString("utf8");

  try {
    return JSON.parse(rawBody);
  } catch {
    throw new ValidationError("Request body must be valid JSON.");
  }
}

export function sendError(response: ServerResponse, error: unknown): void {
  if (error instanceof AppError) {
    sendJson(response, error.statusCode, {
      error: {
        code: error.code,
        message: error.message,
        details: error.details,
      },
    });
    return;
  }

  const message = error instanceof Error ? error.message : "Internal server error";
  sendJson(response, 500, {
    error: {
      code: "INTERNAL_ERROR",
      message,
    },
  });
}

export function parseResourceId(pathname: string, collectionPath: string): string | null {
  if (!pathname.startsWith(`${collectionPath}/`)) {
    return null;
  }

  const resourceId = pathname.slice(collectionPath.length + 1);
  return resourceId.length > 0 && !resourceId.includes("/") ? resourceId : null;
}
