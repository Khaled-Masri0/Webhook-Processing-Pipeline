import { ActionType } from "@prisma/client";
import { ValidationError } from "./errors";
import { PipelineInput, PipelineSubscriberInput } from "../services/pipeline-service";

const VALID_ACTION_TYPES = new Set<ActionType>(["TRANSFORM", "FILTER", "ENRICH"]);
const VALID_SOURCE_PATH = /^\/[a-zA-Z0-9/_-]+$/;

export function parsePipelineInput(payload: unknown): PipelineInput {
  const input = asObject(payload, "Request body must be a JSON object.");
  const subscribers = parseSubscribers(input.subscribers);

  return {
    name: requireString(input.name, "name"),
    sourcePath: normalizeSourcePath(requireString(input.sourcePath, "sourcePath")),
    actionType: parseActionType(input.actionType),
    actionConfig: parseActionConfig(input.actionConfig),
    active: parseOptionalBoolean(input.active, true),
    subscribers,
  };
}

function parseSubscribers(value: unknown): PipelineSubscriberInput[] {
  if (!Array.isArray(value) || value.length === 0) {
    throw new ValidationError("subscribers must be a non-empty array.");
  }

  const subscribers = value.map((entry, index) => {
    const subscriber = asObject(entry, `subscribers[${index}] must be an object.`);
    return {
      url: parseSubscriberUrl(requireString(subscriber.url, `subscribers[${index}].url`)),
      active: parseOptionalBoolean(subscriber.active, true),
    };
  });

  const uniqueUrls = new Set(subscribers.map((subscriber) => subscriber.url));
  if (uniqueUrls.size !== subscribers.length) {
    throw new ValidationError("subscribers must not contain duplicate URLs.");
  }

  return subscribers;
}

function parseActionType(value: unknown): ActionType {
  if (typeof value !== "string" || !VALID_ACTION_TYPES.has(value as ActionType)) {
    throw new ValidationError("actionType must be one of TRANSFORM, FILTER, or ENRICH.");
  }

  return value as ActionType;
}

function parseActionConfig(value: unknown): Record<string, unknown> {
  if (value === undefined) {
    return {};
  }

  return asObject(value, "actionConfig must be a JSON object.");
}

function parseSubscriberUrl(value: string): string {
  let parsedUrl: URL;

  try {
    parsedUrl = new URL(value);
  } catch {
    throw new ValidationError("Subscriber URLs must be valid absolute URLs.");
  }

  if (parsedUrl.protocol !== "http:" && parsedUrl.protocol !== "https:") {
    throw new ValidationError("Subscriber URLs must use http or https.");
  }

  return parsedUrl.toString();
}

function normalizeSourcePath(value: string): string {
  const trimmedValue = value.trim();

  if (!VALID_SOURCE_PATH.test(trimmedValue)) {
    throw new ValidationError(
      "sourcePath must start with / and contain only letters, numbers, /, _, or -.",
    );
  }

  if (trimmedValue === "/") {
    throw new ValidationError("sourcePath must not be the root path.");
  }

  return trimmedValue.endsWith("/") ? trimmedValue.slice(0, -1) : trimmedValue;
}

function requireString(value: unknown, fieldName: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new ValidationError(`${fieldName} must be a non-empty string.`);
  }

  return value.trim();
}

function parseOptionalBoolean(value: unknown, defaultValue: boolean): boolean {
  if (value === undefined) {
    return defaultValue;
  }

  if (typeof value !== "boolean") {
    throw new ValidationError("Boolean fields must be true or false.");
  }

  return value;
}

function asObject(value: unknown, message: string): Record<string, unknown> {
  if (!isPlainObject(value)) {
    throw new ValidationError(message);
  }

  return value;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
