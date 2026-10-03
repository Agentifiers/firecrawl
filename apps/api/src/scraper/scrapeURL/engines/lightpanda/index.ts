import { lookup } from "dns/promises";
import { STATUS_CODES } from "http";
import IPAddr from "ipaddr.js";
import {
  chromium,
  type Browser,
  type BrowserContext,
  type Page,
  type Request as PlaywrightRequest,
  type Response as PlaywrightResponse,
  type Route,
} from "playwright-core";
import { getInnerJson } from "@mendable/firecrawl-rs";
import { config } from "../../../../config";
import type { EngineScrapeResult } from "..";
import type { Meta } from "../..";
import { EngineError } from "../../error";

// Request handling mirrors upstream apps/playwright-service-ts (api.ts) at the
// vendored commit, driving a Lightpanda browser over CDP instead of a local
// Chromium. Every failure surfaces as EngineError so the waterfall moves on to
// the next engine (Chromium via the Playwright service, then fetch).

const DEFAULT_NAVIGATION_TIMEOUT_MS = 15000; // upstream service default
const MAX_CONNECT_TIMEOUT_MS = 10000;
const VIEWPORT = { width: 1280, height: 800 };

// Status codes that usually mean the site rejected Lightpanda's fixed
// User-Agent; Chromium gets a chance unless the caller forced Lightpanda.
const BOT_WALL_STATUS_CODES = [401, 403, 429];

const AD_SERVING_DOMAINS = [
  "doubleclick.net",
  "adservice.google.com",
  "googlesyndication.com",
  "googletagservices.com",
  "googletagmanager.com",
  "google-analytics.com",
  "adsystem.com",
  "adservice.com",
  "adnxs.com",
  "ads-twitter.com",
  "facebook.net",
  "fbcdn.net",
  "amazon-adsystem.com",
];

class InsecureTargetError extends Error {
  constructor(
    public readonly blockedUrl: string,
    reason: string,
  ) {
    super(`Blocked insecure target URL "${blockedUrl}": ${reason}`);
    this.name = "InsecureTargetError";
  }
}

async function isInternalHost(hostname: string): Promise<boolean> {
  const host = hostname.toLowerCase().replace(/\.$/, "");
  if (!host) return true;

  let addresses: string[];
  if (IPAddr.isValid(host)) {
    addresses = [host];
  } else {
    try {
      addresses = (await lookup(host, { all: true })).map(a => a.address);
    } catch {
      return true;
    }
  }
  return (
    addresses.length === 0 ||
    addresses.some(a => IPAddr.parse(a).range() !== "unicast")
  );
}

async function assertSafeTargetUrl(urlString: string): Promise<void> {
  let parsedUrl: URL;
  try {
    parsedUrl = new URL(urlString);
  } catch {
    throw new InsecureTargetError(urlString, "URL is invalid");
  }
  if (parsedUrl.protocol !== "http:" && parsedUrl.protocol !== "https:") {
    throw new InsecureTargetError(
      urlString,
      `unsupported protocol "${parsedUrl.protocol}"`,
    );
  }
  if (
    config.ALLOW_LOCAL_WEBHOOKS !== true &&
    (await isInternalHost(parsedUrl.hostname))
  ) {
    throw new InsecureTargetError(
      urlString,
      "resolves to a private/internal address",
    );
  }
}

type SeedCookie = {
  name: string;
  value: string;
  url?: string;
  domain?: string;
  path?: string;
};

// A Cookie header sent through setExtraHTTPHeaders is dropped on redirect
// hops, so it seeds the context's cookie jar instead, scoped to the last two
// host labels so redirects across sibling subdomains stay authenticated.
function cookiesFromHeader(cookieHeader: string, url: string): SeedCookie[] {
  let cookieDomain: string | undefined;
  try {
    const host = new URL(url).hostname;
    const labels = host.split(".");
    cookieDomain = labels.length > 2 ? labels.slice(-2).join(".") : host;
  } catch {
    cookieDomain = undefined;
  }
  return cookieHeader
    .split(";")
    .map(pair => pair.trim())
    .filter(Boolean)
    .map((pair): SeedCookie | null => {
      const eq = pair.indexOf("=");
      if (eq === -1) return null;
      const name = pair.slice(0, eq).trim();
      const value = pair.slice(eq + 1).trim();
      return cookieDomain
        ? { name, value, domain: `.${cookieDomain}`, path: "/" }
        : { name, value, url };
    })
    .filter((c): c is SeedCookie => c !== null);
}

type NavigationSecurityState = { blockedNavigationRequestUrl: string | null };

async function routeRequest(
  route: Route,
  request: PlaywrightRequest,
  securityState: NavigationSecurityState,
  meta: Meta,
): Promise<void> {
  const requestUrlString = request.url();
  try {
    await assertSafeTargetUrl(requestUrlString);
  } catch (error) {
    if (request.isNavigationRequest()) {
      securityState.blockedNavigationRequestUrl = requestUrlString;
    }
    meta.logger.warn("Lightpanda blocked request", {
      url: requestUrlString,
      error,
    });
    return route.abort("blockedbyclient");
  }

  const hostname = new URL(requestUrlString).hostname.toLowerCase();
  if (AD_SERVING_DOMAINS.some(domain => hostname.includes(domain))) {
    return route.abort();
  }
  return route.continue();
}

async function scrapePage(
  meta: Meta,
  page: Page,
  url: string,
  timeout: number,
  securityState: NavigationSecurityState,
): Promise<{ content: string; status: number; contentType?: string }> {
  let response: PlaywrightResponse | null;
  try {
    response = await page.goto(url, { waitUntil: "load", timeout });
  } catch (error) {
    if (securityState.blockedNavigationRequestUrl) {
      throw new InsecureTargetError(
        securityState.blockedNavigationRequestUrl,
        "navigation to private/internal resource is not allowed",
      );
    }
    throw error;
  }

  if (response === null) {
    throw new EngineError("Lightpanda navigation returned no response");
  }

  const waitFor = meta.options.waitFor ?? 0;
  if (waitFor > 0) {
    await page.waitForTimeout(waitFor);
  }

  let content = await page.content();
  const contentType: string | undefined = response.headers()["content-type"];
  const lowerContentType = contentType?.toLowerCase();
  if (
    lowerContentType &&
    (lowerContentType.includes("application/json") ||
      lowerContentType.includes("text/plain"))
  ) {
    try {
      content = (await response.body()).toString("utf8");
    } catch (error) {
      meta.logger.debug(
        "Lightpanda could not read the response body; using page content",
        { error },
      );
    }
  }

  return { content, status: response.status(), contentType };
}

export async function scrapeURLWithLightpanda(
  meta: Meta,
): Promise<EngineScrapeResult> {
  const cdpUrl = config.LIGHTPANDA_CDP_URL;
  if (!cdpUrl) {
    throw new EngineError("LIGHTPANDA_CDP_URL is not configured");
  }

  const url = meta.rewrittenUrl ?? meta.url;
  const remaining = meta.abort.scrapeTimeout();
  if (remaining !== undefined && remaining <= 0) {
    meta.abort.throwIfAborted();
    throw new EngineError("No time left for a Lightpanda scrape");
  }
  const timeout = remaining ?? DEFAULT_NAVIGATION_TIMEOUT_MS;

  const signal = meta.abort.asSignal();
  let browser: Browser | null = null;
  let context: BrowserContext | null = null;
  // Disconnecting from CDP rejects the in-flight navigation.
  const onAbort = () => {
    void browser?.close().catch(() => {});
  };
  signal.addEventListener("abort", onAbort, { once: true });

  try {
    try {
      await assertSafeTargetUrl(url);
    } catch (error) {
      if (!(error instanceof InsecureTargetError)) throw error;
      return await finish(
        meta,
        url,
        { content: "", status: 403 },
        error.message,
      );
    }

    browser = await chromium.connectOverCDP(cdpUrl, {
      timeout: Math.min(timeout, MAX_CONNECT_TIMEOUT_MS),
    });
    meta.abort.throwIfAborted();

    context = await browser.newContext({
      viewport: VIEWPORT,
      ignoreHTTPSErrors: meta.options.skipTlsVerification,
      serviceWorkers: "block",
    });
    const securityState: NavigationSecurityState = {
      blockedNavigationRequestUrl: null,
    };
    await context.route("**/*", (route, request) =>
      routeRequest(route, request, securityState, meta).catch(error => {
        meta.logger.debug("Lightpanda route handler failed", { error });
      }),
    );

    const page = await context.newPage();
    const headers = meta.options.headers;
    if (headers) {
      const cookieHeader = Object.entries(headers).find(
        ([k]) => k.toLowerCase() === "cookie",
      )?.[1];
      if (cookieHeader) {
        const cookies = cookiesFromHeader(cookieHeader, url);
        if (cookies.length > 0) {
          try {
            await context.addCookies(cookies);
          } catch (error) {
            meta.logger.warn(
              "Lightpanda failed to seed cookies from Cookie header",
              { error },
            );
          }
        }
      }
      const extraHeaders = Object.fromEntries(
        Object.entries(headers).filter(([k]) => k.toLowerCase() !== "cookie"),
      );
      if (Object.keys(extraHeaders).length > 0) {
        await page.setExtraHTTPHeaders(extraHeaders);
      }
    }

    let result: { content: string; status: number; contentType?: string };
    try {
      result = await scrapePage(meta, page, url, timeout, securityState);
    } catch (error) {
      if (!(error instanceof InsecureTargetError)) throw error;
      return await finish(
        meta,
        url,
        { content: "", status: 403 },
        error.message,
      );
    }
    // Upstream reports a page error for every status from 300 up.
    const pageError =
      result.status >= 300
        ? (STATUS_CODES[result.status] ?? "Unknown Error")
        : undefined;
    return await finish(meta, url, result, pageError);
  } catch (error) {
    meta.abort.throwIfAborted();
    if (error instanceof EngineError) throw error;
    const message = error instanceof Error ? error.message : String(error);
    throw new EngineError(`Lightpanda scrape failed: ${message}`, {
      cause: error,
    });
  } finally {
    signal.removeEventListener("abort", onAbort);
    if (context) {
      await context.close().catch(error => {
        meta.logger.debug("Failed to close Lightpanda context", { error });
      });
    }
    if (browser) {
      await browser.close().catch(error => {
        meta.logger.debug("Failed to close Lightpanda CDP connection", {
          error,
        });
      });
    }
  }
}

async function finish(
  meta: Meta,
  url: string,
  result: { content: string; status: number; contentType?: string },
  pageError: string | undefined,
): Promise<EngineScrapeResult> {
  if (
    BOT_WALL_STATUS_CODES.includes(result.status) &&
    meta.options.renderer !== "lightpanda"
  ) {
    throw new EngineError(
      `Lightpanda received HTTP ${result.status}; deferring to the next engine`,
    );
  }

  let html = result.content;
  if (result.contentType?.includes("application/json")) {
    html = await getInnerJson(html);
  }

  return {
    url,
    html,
    statusCode: result.status,
    error: pageError,
    contentType: result.contentType,

    proxyUsed: "basic",
  };
}

export function lightpandaMaxReasonableTime(meta: Meta): number {
  return (meta.options.waitFor ?? 0) + 15000;
}
