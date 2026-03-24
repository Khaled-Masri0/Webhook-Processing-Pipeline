import { Router } from "express";
import { ApiDependencies } from "../app.js";
import { requireJsonBody } from "../utils/request-body.js";
import { asyncRoute } from "../utils/async-route.js";

export function createWebhookRoutes(dependencies: ApiDependencies): Router {
  const router = Router();

  router.post(
    /.*/,
    asyncRoute(async (request, response) => {
      const payload = requireJsonBody(request);
      const queuedJob = await dependencies.webhookService.enqueueWebhook(request.path, payload);

      response.status(202).json({ data: queuedJob });
    }),
  );

  return router;
}
