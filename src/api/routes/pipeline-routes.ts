import { Router } from "express";
import { ApiDependencies } from "../app.js";
import { requireJsonBody } from "../utils/request-body.js";
import { asyncRoute } from "../utils/async-route.js";
import { ValidationError } from "../../utils/errors.js";
import { parsePipelineInput } from "../../utils/pipeline-validation.js";

export function createPipelineRoutes(dependencies: ApiDependencies): Router {
  const router = Router();

  router.get(
    "/",
    asyncRoute(async (_request, response) => {
      const pipelines = await dependencies.pipelineService.listPipelines();

      response.status(200).json({ data: pipelines });
    }),
  );

  router.post(
    "/",
    asyncRoute(async (request, response) => {
      const payload = requireJsonBody(request);
      const pipeline = await dependencies.pipelineService.createPipeline(parsePipelineInput(payload));

      response.status(201).json({ data: pipeline });
    }),
  );

  router.get(
    "/:id",
    asyncRoute(async (request, response) => {
      const pipeline = await dependencies.pipelineService.getPipeline(getPipelineId(request));

      response.status(200).json({ data: pipeline });
    }),
  );

  router.put(
    "/:id",
    asyncRoute(async (request, response) => {
      const payload = requireJsonBody(request);
      const pipeline = await dependencies.pipelineService.updatePipeline(
        getPipelineId(request),
        parsePipelineInput(payload),
      );

      response.status(200).json({ data: pipeline });
    }),
  );

  router.delete(
    "/:id",
    asyncRoute(async (request, response) => {
      const pipeline = await dependencies.pipelineService.deletePipeline(getPipelineId(request));

      response.status(200).json({ data: pipeline });
    }),
  );

  return router;
}

function getPipelineId(request: { params: { id?: string | string[] } }): string {
  const pipelineId = request.params.id;

  if (typeof pipelineId !== "string") {
    throw new ValidationError("Pipeline id path parameter is required.");
  }

  return pipelineId;
}
