// Deployments.ai modification: cache metrics removed with the Redis index cache.
import { Counter } from "prom-client";

export const indexLookupCounter = new Counter({
  name: "firecrawl_index_lookup_total",
  help: "Index URL->id lookups by outcome (cache layer vs DB vs miss)",
  labelNames: ["outcome"],
});
