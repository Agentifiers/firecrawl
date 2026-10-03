// Deployments.ai modification: Redis was removed, so database authentication
// (USE_DB_AUTHENTICATION=true, hosted only) no longer caches ACUC chunks or
// OAuth introspection results, takes redlock locks, or applies the
// rate-limiter-flexible per-minute limits, and the keyless free tier is gone
// (a request without an API key is unauthorized). See MODIFICATIONS.md.
import { isValidUuid } from "../lib/owner-id";
import { config } from "../config";
import { logger } from "../lib/logger";
import { parseApi } from "../lib/parseApi";
import { withAuth } from "../lib/withAuth";
import { getAgentSponsorStatus } from "../services/agent-sponsor";
import { checkIpRestriction } from "../lib/ip-restriction";
import { checkKeyEndpointRestriction } from "../lib/key-restriction";
import { db, dbRr } from "../db/connection";
import {
  authCreditUsageChunk,
  authCreditUsageChunkFromTeam,
  AuthCreditUsageChunkRow,
} from "../db/rpc";
import { AuthResponse, RateLimiterMode } from "../types";
import { AuthCreditUsageChunk, AuthCreditUsageChunkFromTeam } from "./v1/types";

function normalizedApiIsUuid(potentialUuid: string): boolean {
  // Check if the string is a valid UUID
  return isValidUuid(potentialUuid);
}

const mockPreviewACUC: (
  team_id: string,
  is_extract: boolean,
) => AuthCreditUsageChunk = (team_id, is_extract) => ({
  api_key: "preview",
  api_key_id: 0,
  team_id,
  org_id: "preview",
  rate_limits: {
    crawl: 2,
    scrape: 10,
    extract: 10,
    search: 5,
    map: 5,
    preview: 5,
    crawlStatus: 500,
    extractStatus: 500,
    extractAgentPreview: 1,
    scrapeAgentPreview: 5,
  },
  plan_priority: {
    bucketLimit: 25,
    planModifier: 0.1,
  },
  concurrency: is_extract ? 200 : 2,
  flags: null,
  is_extract,
});

const mockACUC: () => AuthCreditUsageChunk = () => ({
  api_key: "bypass",
  api_key_id: 0,
  team_id: "bypass",
  org_id: "bypass",
  rate_limits: {
    crawl: 99999999,
    scrape: 99999999,
    extract: 99999999,
    search: 99999999,
    map: 99999999,
    preview: 99999999,
    crawlStatus: 99999999,
    extractStatus: 99999999,
    extractAgentPreview: 99999999,
    scrapeAgentPreview: 99999999,
  },
  plan_priority: {
    bucketLimit: 25,
    planModifier: 0.1,
  },
  concurrency: 99999999,
  flags: null,
  is_extract: false,
});

/**
 * Introspection response from the OAuth token endpoint.
 */
interface OAuthIntrospectionResponse {
  active: boolean;
  api_key: string;
  scope: string;
  client_id: string;
  team_id: string;
  exp: number;
}

/**
 * Resolve an OAuth access token (fco_…) to the underlying API key via
 * the introspection endpoint.
 */
async function resolveOAuthToken(
  token: string,
): Promise<OAuthIntrospectionResponse | null> {
  const introspectUrl = config.OAUTH_INTROSPECT_URL;
  const introspectSecret = config.OAUTH_INTROSPECT_SECRET;

  if (!introspectUrl || !introspectSecret) {
    logger.warn(
      "OAuth introspection not configured (OAUTH_INTROSPECT_URL / OAUTH_INTROSPECT_SECRET)",
    );
    return null;
  }

  try {
    const response = await fetch(introspectUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${introspectSecret}`,
      },
      body: JSON.stringify({ token }),
    });

    if (!response.ok) {
      logger.error("OAuth introspection request failed", {
        status: response.status,
      });
      return null;
    }

    const data = (await response.json()) as OAuthIntrospectionResponse;
    return data.active ? data : null;
  } catch (error) {
    logger.error("OAuth introspection error", { error });
    return null;
  }
}

async function getACUC(
  api_key: string,
  mode?: RateLimiterMode,
): Promise<AuthCreditUsageChunk | null> {
  let isExtract =
    mode === RateLimiterMode.Extract ||
    mode === RateLimiterMode.ExtractStatus ||
    mode === RateLimiterMode.ExtractAgentPreview;

  if (api_key === config.PREVIEW_TOKEN) {
    const acuc = mockPreviewACUC(api_key, isExtract);
    acuc.is_extract = isExtract;
    return acuc;
  }

  if (config.USE_DB_AUTHENTICATION !== true) {
    const acuc = mockACUC();
    acuc.is_extract = isExtract;
    return acuc;
  }

  {
    let data: AuthCreditUsageChunkRow[] = [];
    let retries = 0;
    const maxRetries = 5;
    while (retries < maxRetries) {
      const database = Math.random() > 2 / 3 ? dbRr : db;
      try {
        data = await authCreditUsageChunk(database, api_key);
        break;
      } catch (error) {
        logger.warn(
          `Failed to retrieve authentication and credit usage data after ${retries}, trying again...`,
          { error },
        );
        retries++;
        if (retries === maxRetries) {
          throw new Error(
            "Failed to retrieve authentication and credit usage data after 3 attempts: " +
              JSON.stringify(error),
          );
        }

        // Wait for a short time before retrying
        await new Promise(resolve => setTimeout(resolve, 200));
      }
    }

    const chunk: AuthCreditUsageChunk | null =
      data.length === 0
        ? null
        : data[0].team_id === null
          ? null
          : (data[0] as any);

    if (chunk) {
      chunk.is_extract = isExtract;
    }

    return chunk;
  }
}

export async function getACUCTeam(
  team_id: string,
  mode?: RateLimiterMode,
): Promise<AuthCreditUsageChunkFromTeam | null> {
  let isExtract =
    mode === RateLimiterMode.Extract ||
    mode === RateLimiterMode.ExtractStatus ||
    mode === RateLimiterMode.ExtractAgentPreview;

  if (team_id.startsWith("preview")) {
    const acuc = mockPreviewACUC(team_id, isExtract);
    return acuc;
  }

  if (config.USE_DB_AUTHENTICATION !== true) {
    const acuc = mockACUC();
    acuc.is_extract = isExtract;
    return acuc;
  }

  {
    let data: AuthCreditUsageChunkRow[] = [];
    let retries = 0;
    const maxRetries = 5;

    while (retries < maxRetries) {
      const database = Math.random() > 2 / 3 ? dbRr : db;
      try {
        data = await authCreditUsageChunkFromTeam(database, team_id);
        break;
      } catch (error) {
        logger.warn(
          `Failed to retrieve authentication and credit usage data after ${retries}, trying again...`,
          { error },
        );
        retries++;
        if (retries === maxRetries) {
          throw new Error(
            "Failed to retrieve authentication and credit usage data after 3 attempts: " +
              JSON.stringify(error),
          );
        }

        // Wait for a short time before retrying
        await new Promise(resolve => setTimeout(resolve, 200));
      }
    }

    const chunk: AuthCreditUsageChunk | null =
      data.length === 0
        ? null
        : data[0].team_id === null
          ? null
          : (data[0] as any);

    return chunk ? { ...chunk, is_extract: isExtract } : null;
  }
}

export async function authenticateUser(
  req,
  res,
  mode?: RateLimiterMode,
): Promise<AuthResponse> {
  const bypassChunk = mockACUC();
  bypassChunk.is_extract =
    mode === RateLimiterMode.Extract ||
    mode === RateLimiterMode.ExtractStatus ||
    mode === RateLimiterMode.ExtractAgentPreview;

  return withAuth(supaAuthenticateUser, {
    success: true,
    chunk: bypassChunk,
    team_id: bypassChunk.team_id,
    org_id: null,
  })(req, res, mode);
}

async function supaAuthenticateUser(
  req,
  res,
  mode?: RateLimiterMode,
): Promise<AuthResponse> {
  const authHeader =
    req.headers.authorization ??
    (req.headers["sec-websocket-protocol"]
      ? `Bearer ${req.headers["sec-websocket-protocol"]}`
      : null);
  if (!authHeader) {
    return { success: false, error: "Unauthorized", status: 401 };
  }
  const token = authHeader.split(" ")[1]; // Extract the token from "Bearer <token>"
  if (!token) {
    return {
      success: false,
      error: "Unauthorized: Token missing",
      status: 401,
    };
  }

  const incomingIP = (req.headers["x-preview-ip"] ||
    req.headers["x-forwarded-for"] ||
    req.socket.remoteAddress) as string;
  const iptoken = incomingIP + token;

  let subscriptionData: { team_id: string } | null = null;
  let normalizedApi: string;

  let teamId: string | null = null;
  let chunk: AuthCreditUsageChunk | null = null;
  if (token == "this_is_just_a_preview_token") {
    throw new Error(
      "Unauthenticated Playground calls are temporarily disabled due to abuse. Please sign up.",
    );
  }
  if (token == config.PREVIEW_TOKEN) {
    teamId = `preview_${iptoken}`;
  } else if (token.startsWith("fco_")) {
    // OAuth access token — resolve via introspection endpoint
    const introspection = await resolveOAuthToken(token);
    if (!introspection) {
      return {
        success: false,
        error: "Unauthorized: Invalid or expired OAuth token",
        status: 401,
      };
    }

    // Use the resolved fc- API key to get the normal ACUC chunk
    const resolvedApi = parseApi(introspection.api_key);
    chunk = await getACUC(resolvedApi, RateLimiterMode.Scrape);

    if (chunk === null) {
      return {
        success: false,
        error: "Unauthorized: Invalid token",
        status: 401,
      };
    }

    teamId = chunk.team_id;

    subscriptionData = {
      team_id: teamId,
    };
  } else {
    normalizedApi = parseApi(token);
    if (!normalizedApiIsUuid(normalizedApi)) {
      return {
        success: false,
        error: "Unauthorized: Invalid token",
        status: 401,
      };
    }

    chunk = await getACUC(normalizedApi, RateLimiterMode.Scrape);

    if (chunk === null) {
      return {
        success: false,
        error: "Unauthorized: Invalid token",
        status: 401,
      };
    }

    teamId = chunk.team_id;

    subscriptionData = {
      team_id: teamId,
    };
  }

  if (chunk?.flags?.ipRestriction) {
    const ipCheck = await checkIpRestriction(
      req.ip ?? req.socket?.remoteAddress,
      chunk.team_id,
      chunk.flags,
    );
    if (!ipCheck.allowed) {
      return {
        success: false,
        error: ipCheck.error,
        status: ipCheck.status,
      };
    }
  }

  if (chunk?.flags?.keyRestriction) {
    // Enforced here rather than in route middleware so every authenticated
    // surface (v0/v1/v2, websocket status) goes through the same gate.
    const endpointCheck = await checkKeyEndpointRestriction(
      req.originalUrl ?? req.url ?? "",
      chunk.api_key_id,
      chunk.flags,
    );
    if (!endpointCheck.allowed) {
      return {
        success: false,
        error: endpointCheck.error,
        status: endpointCheck.status,
      };
    }
  }

  if (
    token === config.PREVIEW_TOKEN &&
    (mode === RateLimiterMode.Scrape ||
      mode === RateLimiterMode.Preview ||
      mode === RateLimiterMode.Map ||
      mode === RateLimiterMode.Crawl ||
      mode === RateLimiterMode.CrawlStatus ||
      mode === RateLimiterMode.Extract ||
      mode === RateLimiterMode.Search ||
      mode === RateLimiterMode.Research)
  ) {
    return {
      success: true,
      team_id: `preview_${iptoken}`,
      org_id: null,
      chunk: null,
    };
    // check the origin of the request and make sure its from firecrawl.dev
    // const origin = req.headers.origin;
    // if (origin && origin.includes("firecrawl.dev")){
    //   return { success: true, team_id: "preview" };
    // }
    // if(config.ENV !== "production") {
    //   return { success: true, team_id: "preview" };
    // }

    // return { success: false, error: "Unauthorized: Invalid token", status: 401 };
  }

  // Check if this is an agent-provisioned key and attach sponsor status
  if (chunk && chunk.api_key_id) {
    try {
      const sponsorStatus = await getAgentSponsorStatus({
        apiKeyId: chunk.api_key_id,
      });
      if (sponsorStatus) {
        chunk._agentSponsor = {
          status: sponsorStatus.status,
          verification_deadline: sponsorStatus.verification_deadline,
          email: sponsorStatus.email,
        };
      }
    } catch (err) {
      logger.warn("Failed to check agent sponsor status", {
        error: err,
        api_key_id: chunk.api_key_id,
      });
    }
  }

  return {
    success: true,
    team_id: teamId ?? undefined,
    org_id: chunk?.org_id ?? null,
    chunk,
  };
}
