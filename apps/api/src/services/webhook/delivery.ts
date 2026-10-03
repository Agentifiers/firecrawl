import undici from "undici";
import { config } from "../../config";
import { createHmac } from "crypto";
import { logger as _logger, logger } from "../../lib/logger";
import {
  getSecureDispatcherNoCookies,
  isIPPrivate,
} from "../../scraper/scrapeURL/engines/utils/safeFetch";
import type {
  WebhookConfig,
  WebhookEvent,
  WebhookEventDataMap,
} from "./types";
import { randomUUID } from "crypto";

type WebhookSendResult = {
  attempted: boolean;
  delivered?: boolean;
  queued?: boolean;
  skipped?: boolean;
  statusCode?: number;
};

export function webhookEventMatchesFilter(
  configuredEvents: string[] | undefined,
  event: WebhookEvent,
): boolean {
  if (!configuredEvents?.length) {
    return true;
  }

  const legacySubType = event.split(".")[1];
  const namespaceSuffix = event.split(".").slice(1).join(".");
  return (
    configuredEvents.includes(event) ||
    configuredEvents.includes(legacySubType) ||
    configuredEvents.includes(namespaceSuffix)
  );
}

export class WebhookSender {
  private config: WebhookConfig;
  private secret?: string;
  private context: { teamId: string; jobId: string; v0: boolean };
  private logger: any;

  constructor(
    config: WebhookConfig,
    secret: string | undefined,
    context: { teamId: string; jobId: string; v0: boolean },
  ) {
    this.config = config;
    this.secret = secret;
    this.context = context;
    this.logger = _logger.child({
      module: "webhook-sender",
      teamId: context.teamId,
      jobId: context.jobId,
      isV0: context.v0,
    });
  }

  async send<T extends WebhookEvent>(
    event: T,
    data: WebhookEventDataMap[T],
  ): Promise<WebhookSendResult> {
    if (!this.shouldSendEvent(event)) {
      return { attempted: false, skipped: true };
    }

    const payload = {
      success: data.success,
      type: event,
      [this.context.v0 ? "jobId" : "id"]: this.context.jobId,
      webhookId: randomUUID(), // Unique ID for this webhook delivery (used for e.g. retries)
      data: "data" in data ? data.data : [],
      error: "error" in data ? data.error : undefined,
      metadata: this.config.metadata || undefined,
    };

    const delivery = this.deliver(
      payload,
      (data as any)?.scrapeId ?? undefined,
    );

    if (data.awaitWebhook) {
      return { attempted: true, ...(await delivery) };
    }

    delivery.catch(() => {});
    // Deployments.ai: the RabbitMQ webhook queue is removed; delivery is always direct.
    return {
      attempted: true,
      delivered: false,
      queued: false,
    };
  }

  private shouldSendEvent(event: WebhookEvent): boolean {
    if (config.DISABLE_WEBHOOK_DELIVERY) {
      return false;
    }

    return webhookEventMatchesFilter(this.config.events, event);
  }

  private async deliver(
    payload: any,
    scrapeId?: string,
  ): Promise<Omit<WebhookSendResult, "attempted">> {
    const webhookHost = new URL(this.config.url).hostname;
    if (isIPPrivate(webhookHost) && config.ALLOW_LOCAL_WEBHOOKS !== true) {
      this.logger.warn("Aborting webhook call to private IP address", {
        webhookUrl: this.config.url,
      });
      return { delivered: false, skipped: true };
    }

    const payloadString = JSON.stringify(payload);
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      ...this.config.headers,
    };

    if (this.secret) {
      const hmac = createHmac("sha256", this.secret);
      hmac.update(payloadString);
      headers["X-Firecrawl-Signature"] = `sha256=${hmac.digest("hex")}`;
    }

    const abortController = new AbortController();
    const timeoutHandle = setTimeout(
      () => {
        if (abortController) {
          abortController.abort();
        }
      },
      this.context.v0 ? 30000 : 10000,
    );

    try {
      const res = await undici.fetch(this.config.url, {
        method: "POST",
        headers,
        body: payloadString,
        dispatcher: getSecureDispatcherNoCookies(),
        signal: abortController.signal,
      });

      if (!res.ok) {
        throw new Error(`Unexpected response status: ${res.status}`);
      }

      await logWebhook({
        success: res.status >= 200 && res.status < 300,
        teamId: this.context.teamId,
        crawlId: this.context.jobId, // this is legacy naming, we should rename it to jobId at some point
        scrapeId,
        url: this.config.url,
        event: payload.type,
        statusCode: res.status,
      });
      return { delivered: true, queued: false, statusCode: res.status };
    } catch (error) {
      this.logger.error("Failed to send webhook", {
        error,
        webhookUrl: this.config.url,
      });

      await logWebhook({
        success: false,
        teamId: this.context.teamId,
        crawlId: this.context.jobId, // same as above
        scrapeId,
        url: this.config.url,
        event: payload.type,
        error:
          error instanceof Error
            ? error.message
            : typeof error === "string"
              ? error
              : undefined,
        statusCode:
          typeof (error as any)?.status === "number"
            ? (error as any).status
            : undefined,
      });

      throw error;
    } finally {
      if (timeoutHandle) clearTimeout(timeoutHandle);
    }
  }

  get metadata(): Record<string, string> {
    return this.config.metadata || {};
  }
}

// Deployments.ai: upstream buffered webhook_logs rows in a Redis list drained by
// the hosted index worker into Postgres. Self-host keeps only the structured log.
async function logWebhook(data: {
  success: boolean;
  error?: string;
  teamId: string;
  crawlId: string;
  scrapeId?: string;
  url: string;
  statusCode?: number;
  event: WebhookEvent;
}): Promise<void> {
  logger.info("Webhook delivery attempted", {
    module: "webhook-sender",
    success: data.success,
    error: data.error,
    teamId: data.teamId,
    crawlId: data.crawlId,
    scrapeId: data.scrapeId,
    webhookUrl: data.url,
    statusCode: data.statusCode,
    event: data.event,
  });
}
