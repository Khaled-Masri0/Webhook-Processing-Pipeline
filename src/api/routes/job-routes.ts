import { DeliveryStatus, JobStatus } from "@prisma/client";
import { Request, Router } from "express";
import { ApiDependencies } from "../app";
import {
  DeliveryHistoryQuery,
  JobHistoryQuery,
  PaginationInput,
} from "../../services/job-query-service";
import { ValidationError } from "../../utils/errors";
import { asyncRoute } from "../utils/async-route";

const DEFAULT_PAGE = 1;
const DEFAULT_PAGE_SIZE = 20;
const MAX_PAGE_SIZE = 100;

export function createJobRoutes(dependencies: ApiDependencies): Router {
  const router = Router();

  router.get(
    "/",
    asyncRoute(async (request, response) => {
      const jobs = await dependencies.jobQueryService.listJobs(parseJobHistoryQuery(request));

      response.status(200).json({
        data: jobs.items,
        meta: buildPaginationMeta(jobs),
      });
    }),
  );

  router.get(
    "/:id/deliveries",
    asyncRoute(async (request, response) => {
      const deliveries = await dependencies.jobQueryService.listJobDeliveries(
        getJobId(request),
        parseDeliveryHistoryQuery(request),
      );

      response.status(200).json({
        data: deliveries.items,
        meta: buildPaginationMeta(deliveries),
      });
    }),
  );

  router.get(
    "/:id",
    asyncRoute(async (request, response) => {
      const job = await dependencies.jobQueryService.getJob(getJobId(request));

      response.status(200).json({ data: job });
    }),
  );

  return router;
}

function getJobId(request: Request): string {
  const jobId = request.params.id;

  if (typeof jobId !== "string" || jobId.length === 0) {
    throw new ValidationError("Job id path parameter is required.");
  }

  return jobId;
}

function parseJobHistoryQuery(request: Request): JobHistoryQuery {
  const createdAfter = parseOptionalDate(request.query.createdAfter, "createdAfter");
  const createdBefore = parseOptionalDate(request.query.createdBefore, "createdBefore");

  if (
    createdAfter !== undefined &&
    createdBefore !== undefined &&
    createdAfter.getTime() > createdBefore.getTime()
  ) {
    throw new ValidationError("createdAfter must be less than or equal to createdBefore.");
  }

  return {
    ...parsePagination(request),
    pipelineId: parseOptionalString(request.query.pipelineId, "pipelineId"),
    status: parseOptionalEnum<JobStatus>(request.query.status, "status", Object.values(JobStatus)),
    createdAfter,
    createdBefore,
  };
}

function parseDeliveryHistoryQuery(request: Request): DeliveryHistoryQuery {
  return {
    ...parsePagination(request),
    status: parseOptionalEnum<DeliveryStatus>(
      request.query.status,
      "status",
      Object.values(DeliveryStatus),
    ),
  };
}

function parsePagination(request: Request): PaginationInput {
  const page = parsePositiveInteger(request.query.page, "page", DEFAULT_PAGE);
  const pageSize = parsePositiveInteger(request.query.pageSize, "pageSize", DEFAULT_PAGE_SIZE);

  if (pageSize > MAX_PAGE_SIZE) {
    throw new ValidationError(`pageSize must be less than or equal to ${MAX_PAGE_SIZE}.`);
  }

  return {
    page,
    pageSize,
  };
}

function parsePositiveInteger(
  value: unknown,
  fieldName: string,
  defaultValue: number,
): number {
  if (value === undefined) {
    return defaultValue;
  }

  const stringValue = parseOptionalString(value, fieldName);

  if (stringValue === undefined) {
    return defaultValue;
  }

  if (!/^\d+$/.test(stringValue)) {
    throw new ValidationError(`${fieldName} must be a positive integer.`);
  }

  const parsedValue = Number.parseInt(stringValue, 10);

  if (parsedValue < 1) {
    throw new ValidationError(`${fieldName} must be greater than or equal to 1.`);
  }

  return parsedValue;
}

function parseOptionalDate(value: unknown, fieldName: string): Date | undefined {
  const stringValue = parseOptionalString(value, fieldName);

  if (stringValue === undefined) {
    return undefined;
  }

  const parsedDate = new Date(stringValue);

  if (Number.isNaN(parsedDate.getTime())) {
    throw new ValidationError(`${fieldName} must be a valid ISO-8601 date string.`);
  }

  return parsedDate;
}

function parseOptionalEnum<T extends string>(
  value: unknown,
  fieldName: string,
  supportedValues: readonly T[],
): T | undefined {
  const stringValue = parseOptionalString(value, fieldName);

  if (stringValue === undefined) {
    return undefined;
  }

  if (!supportedValues.includes(stringValue as T)) {
    throw new ValidationError(
      `${fieldName} must be one of: ${supportedValues.join(", ")}.`,
    );
  }

  return stringValue as T;
}

function parseOptionalString(value: unknown, fieldName: string): string | undefined {
  if (value === undefined) {
    return undefined;
  }

  if (Array.isArray(value)) {
    throw new ValidationError(`${fieldName} must be provided only once.`);
  }

  if (typeof value !== "string") {
    throw new ValidationError(`${fieldName} must be a string.`);
  }

  if (value.length === 0) {
    throw new ValidationError(`${fieldName} cannot be empty.`);
  }

  return value;
}

function buildPaginationMeta<T>(result: {
  items: T[];
  page: number;
  pageSize: number;
  totalItems: number;
  totalPages: number;
}) {
  return {
    page: result.page,
    pageSize: result.pageSize,
    totalItems: result.totalItems,
    totalPages: result.totalPages,
  };
}
