import { Request } from "express";
import { JsonValue } from "../../utils/json.js";
import { ValidationError } from "../../utils/errors.js";

export function requireJsonBody(request: Request): JsonValue {
  const contentLength = request.headers["content-length"];
  const hasTransferEncoding = request.headers["transfer-encoding"] !== undefined;

  if (contentLength === "0" || (contentLength === undefined && !hasTransferEncoding)) {
    throw new ValidationError("Request body is required.");
  }

  return request.body as JsonValue;
}
