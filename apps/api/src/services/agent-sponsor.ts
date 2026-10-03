// Deployments.ai modification: the Redis status cache (and the unused,
// unexported sponsor helpers) were removed with Redis; the status is read
// from the database on each authenticated request (database auth only).
import { eq } from "drizzle-orm";
import { logger } from "../lib/logger";
import { dbRr } from "../db/connection";
import * as schema from "../db/schema";

type AgentSponsorStatus = {
  status: "pending" | "verified" | "blocked";
  verification_deadline: string;
  email: string;
};

/**
 * Look up agent sponsor status by api_key_id.
 */
export async function getAgentSponsorStatus({
  apiKeyId,
}: {
  apiKeyId: number;
}): Promise<AgentSponsorStatus | null> {
  try {
    const [data] = await dbRr
      .select({
        status: schema.agent_sponsors.status,
        verification_deadline: schema.agent_sponsors.verification_deadline,
        email: schema.agent_sponsors.email,
      })
      .from(schema.agent_sponsors)
      .where(eq(schema.agent_sponsors.api_key_id, apiKeyId))
      .limit(1);

    if (!data) return null;

    return {
      status: data.status as AgentSponsorStatus["status"],
      verification_deadline: data.verification_deadline!,
      email: data.email!,
    };
  } catch (err) {
    logger.error("Failed to look up agent sponsor status", {
      apiKeyId,
      error: err,
    });
    return null;
  }
}
