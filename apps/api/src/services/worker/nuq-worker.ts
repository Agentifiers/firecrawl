// Deployments.ai modification: runs the shared NuQ consumer loop on DynamoDB.
import "dotenv/config";
import "../sentry";
import { setSentryServiceTag } from "../sentry";
import { nuqHealthCheck, nuqShutdown, scrapeQueue } from "./nuq";
import { runNuqWorker } from "./nuq-worker-runner";

(async () => {
  setSentryServiceTag("nuq-worker");

  await runNuqWorker({
    serviceName: "nuq-worker",
    queue: scrapeQueue,
    healthCheck: nuqHealthCheck,
    shutdown: nuqShutdown,
  });
})();
