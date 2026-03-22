import { Router } from "express";
import { ApiDependencies } from "../app";
import { asyncRoute } from "../utils/async-route";

export function createHealthRoutes(dependencies: ApiDependencies): Router {
  const router = Router();

  router.get(
    "/",
    asyncRoute(async (_request, response) => {
      await dependencies.healthcheck();

      response.status(200).json({
        data: {
          status: "ok",
          database: "up",
        },
      });
    }),
  );

  return router;
}
