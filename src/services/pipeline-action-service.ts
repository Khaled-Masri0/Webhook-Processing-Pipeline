import { ActionType } from "@prisma/client";
import { ValidationError } from "../utils/errors";
import { isJsonObject, isJsonValue, JsonObject, JsonPrimitive, JsonValue } from "../utils/json";

const VALID_PATH = /^[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+)*$/;
const VALID_OUTPUT_FIELD = /^[a-zA-Z0-9_-]+$/;
const VALID_FILTER_OPERATORS = new Set<FilterOperator>([
  "exists",
  "equals",
  "notEquals",
  "gt",
  "gte",
  "lt",
  "lte",
]);

export interface TransformActionConfig {
  fields: Record<string, string>;
}

export type FilterOperator = "exists" | "equals" | "notEquals" | "gt" | "gte" | "lt" | "lte";

export interface FilterCondition {
  path: string;
  operator: FilterOperator;
  value?: JsonPrimitive;
}

export interface FilterActionConfig {
  match: "all" | "any";
  conditions: FilterCondition[];
}

export interface EnrichActionConfig {
  add: JsonObject;
}

export type PipelineActionConfig = TransformActionConfig | FilterActionConfig | EnrichActionConfig;

export interface PipelineActionExecutionInput {
  actionType: ActionType;
  actionConfig: unknown;
  payload: JsonValue;
}

export interface PipelineActionExecutionResult {
  status: "SUCCESS" | "FILTERED_OUT";
  result: JsonValue | null;
}

export function parsePipelineActionConfig(
  actionType: "TRANSFORM",
  actionConfig: unknown,
): TransformActionConfig;
export function parsePipelineActionConfig(
  actionType: "FILTER",
  actionConfig: unknown,
): FilterActionConfig;
export function parsePipelineActionConfig(
  actionType: "ENRICH",
  actionConfig: unknown,
): EnrichActionConfig;
export function parsePipelineActionConfig(
  actionType: ActionType,
  actionConfig: unknown,
): PipelineActionConfig;
export function parsePipelineActionConfig(
  actionType: ActionType,
  actionConfig: unknown,
): PipelineActionConfig {
  const input = asObject(actionConfig, "actionConfig must be a JSON object.");

  switch (actionType) {
    case "TRANSFORM":
      return parseTransformActionConfig(input);
    case "FILTER":
      return parseFilterActionConfig(input);
    case "ENRICH":
      return parseEnrichActionConfig(input);
  }
}

export function executePipelineAction(
  input: PipelineActionExecutionInput,
): PipelineActionExecutionResult {
  switch (input.actionType) {
    case "TRANSFORM": {
      const actionConfig = parsePipelineActionConfig("TRANSFORM", input.actionConfig);

      return {
        status: "SUCCESS",
        result: executeTransformAction(actionConfig, input.payload),
      };
    }
    case "FILTER": {
      const actionConfig = parsePipelineActionConfig("FILTER", input.actionConfig);
      return executeFilterAction(actionConfig, input.payload);
    }
    case "ENRICH": {
      const actionConfig = parsePipelineActionConfig("ENRICH", input.actionConfig);

      return {
        status: "SUCCESS",
        result: executeEnrichAction(actionConfig, input.payload),
      };
    }
  }
}

function parseTransformActionConfig(actionConfig: JsonObject): TransformActionConfig {
  const fields = asObject(
    actionConfig.fields,
    "TRANSFORM actionConfig.fields must be a non-empty object.",
  );
  const entries = Object.entries(fields);

  if (entries.length === 0) {
    throw new ValidationError("TRANSFORM actionConfig.fields must define at least one output field.");
  }

  return {
    fields: Object.fromEntries(
      entries.map(([outputField, sourcePath]) => {
        if (!VALID_OUTPUT_FIELD.test(outputField)) {
          throw new ValidationError(
            "TRANSFORM actionConfig.fields keys must use letters, numbers, underscores, or dashes.",
          );
        }

        return [outputField, requirePathString(sourcePath, "TRANSFORM actionConfig.fields")];
      }),
    ),
  };
}

function parseFilterActionConfig(actionConfig: JsonObject): FilterActionConfig {
  const match = parseFilterMatch(actionConfig.match);
  const rawConditions = actionConfig.conditions;

  if (!Array.isArray(rawConditions) || rawConditions.length === 0) {
    throw new ValidationError("FILTER actionConfig.conditions must be a non-empty array.");
  }

  return {
    match,
    conditions: rawConditions.map((condition, index) => parseFilterCondition(condition, index)),
  };
}

function parseEnrichActionConfig(actionConfig: JsonObject): EnrichActionConfig {
  const add = asObject(actionConfig.add, "ENRICH actionConfig.add must be a non-empty object.");
  const entries = Object.entries(add);

  if (entries.length === 0) {
    throw new ValidationError("ENRICH actionConfig.add must define at least one field.");
  }

  for (const [key, value] of entries) {
    if (!VALID_OUTPUT_FIELD.test(key)) {
      throw new ValidationError(
        "ENRICH actionConfig.add keys must use letters, numbers, underscores, or dashes.",
      );
    }

    if (!isJsonValue(value)) {
      throw new ValidationError("ENRICH actionConfig.add values must be valid JSON values.");
    }
  }

  return { add };
}

function parseFilterMatch(value: unknown): "all" | "any" {
  if (value === undefined) {
    return "all";
  }

  if (value !== "all" && value !== "any") {
    throw new ValidationError('FILTER actionConfig.match must be either "all" or "any".');
  }

  return value;
}

function parseFilterCondition(value: unknown, index: number): FilterCondition {
  const condition = asObject(value, `FILTER actionConfig.conditions[${index}] must be an object.`);
  const operator = parseFilterOperator(condition.operator, index);
  const path = requirePathString(condition.path, `FILTER actionConfig.conditions[${index}].path`);

  switch (operator) {
    case "exists":
      if ("value" in condition) {
        throw new ValidationError(
          `FILTER actionConfig.conditions[${index}].value is not allowed for the exists operator.`,
        );
      }

      return { path, operator };
    case "equals":
    case "notEquals":
      return {
        path,
        operator,
        value: requirePrimitiveValue(condition, index),
      };
    case "gt":
    case "gte":
    case "lt":
    case "lte":
      return {
        path,
        operator,
        value: requireNumericValue(condition, index),
      };
  }
}

function parseFilterOperator(value: unknown, index: number): FilterOperator {
  if (typeof value !== "string" || !VALID_FILTER_OPERATORS.has(value as FilterOperator)) {
    throw new ValidationError(
      `FILTER actionConfig.conditions[${index}].operator must be one of exists, equals, notEquals, gt, gte, lt, or lte.`,
    );
  }

  return value as FilterOperator;
}

function requirePrimitiveValue(condition: JsonObject, index: number): JsonPrimitive {
  if (!("value" in condition) || !isJsonPrimitive(condition.value)) {
    throw new ValidationError(
      `FILTER actionConfig.conditions[${index}].value must be a string, number, boolean, or null.`,
    );
  }

  return condition.value;
}

function requireNumericValue(condition: JsonObject, index: number): number {
  if (!("value" in condition) || typeof condition.value !== "number") {
    throw new ValidationError(
      `FILTER actionConfig.conditions[${index}].value must be a number for numeric operators.`,
    );
  }

  return condition.value;
}

function executeTransformAction(actionConfig: TransformActionConfig, payload: JsonValue): JsonObject {
  if (!isJsonObject(payload)) {
    throw new ValidationError("TRANSFORM payload must be a JSON object.");
  }

  return Object.fromEntries(
    Object.entries(actionConfig.fields).map(([outputField, sourcePath]) => {
      const value = getPathValue(payload, sourcePath);
      return [outputField, value ?? null];
    }),
  );
}

function executeFilterAction(
  actionConfig: FilterActionConfig,
  payload: JsonValue,
): PipelineActionExecutionResult {
  const matches =
    actionConfig.match === "any"
      ? actionConfig.conditions.some((condition) => evaluateFilterCondition(condition, payload))
      : actionConfig.conditions.every((condition) => evaluateFilterCondition(condition, payload));

  return {
    status: matches ? "SUCCESS" : "FILTERED_OUT",
    result: matches ? payload : null,
  };
}

function executeEnrichAction(actionConfig: EnrichActionConfig, payload: JsonValue): JsonObject {
  if (!isJsonObject(payload)) {
    throw new ValidationError("ENRICH payload must be a JSON object.");
  }

  return {
    ...payload,
    ...actionConfig.add,
  };
}

function evaluateFilterCondition(condition: FilterCondition, payload: JsonValue): boolean {
  const value = getPathValue(payload, condition.path);

  switch (condition.operator) {
    case "exists":
      return value !== undefined && value !== null;
    case "equals":
      return isJsonPrimitive(value) && value === condition.value;
    case "notEquals":
      return !isJsonPrimitive(value) || value !== condition.value;
    case "gt":
      return typeof value === "number" && value > getNumericConditionValue(condition);
    case "gte":
      return typeof value === "number" && value >= getNumericConditionValue(condition);
    case "lt":
      return typeof value === "number" && value < getNumericConditionValue(condition);
    case "lte":
      return typeof value === "number" && value <= getNumericConditionValue(condition);
  }
}

function getNumericConditionValue(condition: FilterCondition): number {
  if (typeof condition.value !== "number") {
    throw new ValidationError("Numeric filter conditions must use a numeric value.");
  }

  return condition.value;
}

function getPathValue(payload: JsonValue, path: string): JsonValue | undefined {
  let current: JsonValue | undefined = payload;

  for (const segment of path.split(".")) {
    if (!isJsonObject(current) || !(segment in current)) {
      return undefined;
    }

    current = current[segment];
  }

  return current;
}

function requirePathString(value: unknown, fieldName: string): string {
  if (typeof value !== "string" || !VALID_PATH.test(value)) {
    throw new ValidationError(
      `${fieldName} values must be dot-separated paths using only letters, numbers, underscores, or dashes.`,
    );
  }

  return value;
}

function isJsonPrimitive(value: unknown): value is JsonPrimitive {
  return (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  );
}

function asObject(value: unknown, message: string): JsonObject {
  if (!isJsonObject(value)) {
    throw new ValidationError(message);
  }

  return value;
}
