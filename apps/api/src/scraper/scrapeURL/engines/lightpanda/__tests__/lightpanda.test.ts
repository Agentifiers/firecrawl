import type { Meta } from "../../..";
import { AbortManager } from "../../../lib/abortManager";

type Renderer = "auto" | "lightpanda" | "chromium";

const PAGE_HTML = "<html><body><h1>Access denied</h1></body></html>";

// A CDP session stand-in: playwright-core's connectOverCDP resolves to this
// browser, so the engine runs without a live Lightpanda.
function fakeBrowser(status: number) {
  const response = {
    status: () => status,
    headers: () => ({ "content-type": "text/html; charset=utf-8" }),
    body: vi.fn(async () => Buffer.from(PAGE_HTML)),
  };
  const page = {
    goto: vi.fn(async () => response),
    waitForTimeout: vi.fn(async () => {}),
    content: vi.fn(async () => PAGE_HTML),
    setExtraHTTPHeaders: vi.fn(async () => {}),
  };
  const context = {
    route: vi.fn(async () => {}),
    addCookies: vi.fn(async () => {}),
    newPage: vi.fn(async () => page),
    close: vi.fn(async () => {}),
  };
  const browser = {
    newContext: vi.fn(async () => context),
    close: vi.fn(async () => {}),
  };
  return { browser, context, page };
}

const logger = {
  debug: vi.fn(),
  info: vi.fn(),
  warn: vi.fn(),
  error: vi.fn(),
};

// The engine reads LIGHTPANDA_CDP_URL from config at module load, so each test
// stubs the environment and dynamically imports a fresh module graph (a static
// import would freeze the first configuration and the unmocked transport).
async function loadEngine(connectOverCDP: (...args: unknown[]) => unknown) {
  vi.resetModules();
  vi.stubEnv("LIGHTPANDA_CDP_URL", "ws://lightpanda.test:9222");
  vi.doMock("playwright-core", () => ({ chromium: { connectOverCDP } }));
  vi.doMock("@mendable/firecrawl-rs", () => ({
    getInnerJson: vi.fn(async (html: string) => html),
  }));

  const { scrapeURLWithLightpanda } = await import("../index.js");
  const { EngineError } = await import("../../../error.js");

  const scrape = (renderer: Renderer) =>
    scrapeURLWithLightpanda({
      id: "test",
      // An IP literal keeps the private-address guard off DNS.
      url: "http://1.1.1.1/",
      options: {
        renderer,
        waitFor: 0,
        skipTlsVerification: true,
        headers: undefined,
      },
      internalOptions: { teamId: "test", orgId: null },
      logger,
      abort: new AbortManager(),
      featureFlags: new Set(),
      mock: null,
    } as unknown as Meta);

  return { scrape, EngineError };
}

describe("Lightpanda engine", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
    vi.doUnmock("playwright-core");
    vi.doUnmock("@mendable/firecrawl-rs");
  });

  it.each([401, 403, 429])(
    "defers HTTP %i to the next engine for auto",
    async status => {
      const { browser, context } = fakeBrowser(status);
      const { scrape, EngineError } = await loadEngine(
        vi.fn(async () => browser),
      );

      await expect(scrape("auto")).rejects.toBeInstanceOf(EngineError);
      expect(context.close).toHaveBeenCalledTimes(1);
      expect(browser.close).toHaveBeenCalledTimes(1);
    },
  );

  it.each([401, 403, 429])(
    "returns HTTP %i as-is when lightpanda is forced",
    async status => {
      const { browser, context } = fakeBrowser(status);
      const { scrape } = await loadEngine(vi.fn(async () => browser));

      const result = await scrape("lightpanda");

      expect(result).toMatchObject({
        url: "http://1.1.1.1/",
        html: PAGE_HTML,
        statusCode: status,
        contentType: "text/html; charset=utf-8",
        proxyUsed: "basic",
      });
      expect(result.error).toEqual(expect.any(String));
      expect(context.close).toHaveBeenCalledTimes(1);
      expect(browser.close).toHaveBeenCalledTimes(1);
    },
  );

  it("returns a successful page for auto", async () => {
    const { browser } = fakeBrowser(200);
    const { scrape } = await loadEngine(vi.fn(async () => browser));

    const result = await scrape("auto");

    expect(result.statusCode).toBe(200);
    expect(result.html).toBe(PAGE_HTML);
    expect(result.error).toBeUndefined();
  });

  it("turns a CDP connection failure into an EngineError", async () => {
    const { scrape, EngineError } = await loadEngine(
      vi.fn(async () => {
        throw new Error("connect ECONNREFUSED 127.0.0.1:9222");
      }),
    );

    await expect(scrape("auto")).rejects.toBeInstanceOf(EngineError);
  });
});
