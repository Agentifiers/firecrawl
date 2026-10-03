import express from "express";
import { livenessController } from "../controllers/v0/liveness";
import { readinessController } from "../controllers/v0/readiness";

// Deployments.ai: the deprecated v0 scrape/crawl/search/keyAuth routes are
// removed (hosted-only, Redis-backed); only the health probes remain.
export const v0Router = express.Router();

// Health/Probe routes
v0Router.get("/v0/health/liveness", livenessController);
v0Router.get("/v0/health/readiness", readinessController);
