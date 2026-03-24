import { NextFunction, Request, Response } from "express";
import { AppError, ValidationError } from "../../utils/errors.js";

interface BodyParserError extends Error {
  type?: string;
}

function normalizeError(error: unknown): AppError | Error {
  if (error instanceof AppError) {
    return error;
  }

  const bodyParserError = error as BodyParserError;

  if (bodyParserError.type === "entity.parse.failed") {
    return new ValidationError("Request body must be valid JSON.");
  }

  if (bodyParserError.type === "entity.too.large") {
    return new ValidationError("Request body exceeds the 1MB limit.");
  }

  return error instanceof Error ? error : new Error("Internal server error");
}

export function notFoundHandler(_request: Request, response: Response): void {
  response.status(404).json({
    error: {
      code: "NOT_FOUND",
      message: "Route not found.",
    },
  });
}

export function errorHandler(
  error: unknown,
  _request: Request,
  response: Response,
  _next: NextFunction,
): void {
  const normalizedError = normalizeError(error);

  if (normalizedError instanceof AppError) {
    response.status(normalizedError.statusCode).json({
      error: {
        code: normalizedError.code,
        message: normalizedError.message,
        details: normalizedError.details,
      },
    });
    return;
  }

  response.status(500).json({
    error: {
      code: "INTERNAL_ERROR",
      message: normalizedError.message,
    },
  });
}
