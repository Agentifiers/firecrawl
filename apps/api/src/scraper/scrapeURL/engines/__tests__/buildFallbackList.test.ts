import type { Meta } from "../..";
import type { Engine } from "..";
import type * as ExchangeModule from "../../../../lib/exchange";

type Renderer = "auto" | "lightpanda" | "chromium";

const logger = {
  debug: vi.fn(),
  info: vi.fn(),
  warn: vi.fn(),
  error: vi.fn(),
};

// Engine availability is decided from config when the module loads, so the
// suite stubs the environment and then dynamically imports a fresh module
// graph once (a static import would freeze an unstubbed configuration, and
// re-importing per test re-registers the module's Prometheus metrics).
type FallbackList = (
  renderer: Renderer,
  forceEngine?: Engine[],
) => Promise<Engine[]>;

async function loadFallbackList(): Promise<FallbackList> {
  vi.resetModules();
  vi.stubEnv("LIGHTPANDA_CDP_URL", "ws://lightpanda.test:9222");
  vi.stubEnv(
    "PLAYWRIGHT_MICROSERVICE_URL",
    "http://playwright.test:3000/scrape",
  );
  vi.stubEnv("FIRE_ENGINE_BETA_URL", "");
  vi.stubEnv("INDEX_DATABASE_URL", "");
  vi.stubEnv("XAI_API_KEY", "");
  vi.stubEnv("USE_DB_AUTHENTICATION", "false");
  vi.doMock("../../../../lib/exchange", async importOriginal => ({
    ...(await importOriginal<typeof ExchangeModule>()),
    canUseExchangeForRequest: vi.fn(async () => false),
  }));

  const { buildFallbackList } = await import("../index.js");
  const { scrapeOptions } = await import("../../../../controllers/v2/types.js");

  return async (
    renderer: Renderer,
    forceEngine?: Engine[],
  ): Promise<Engine[]> => {
    const meta = {
      id: "test",
      url: "https://example.com/",
      options: scrapeOptions.parse({ renderer }),
      internalOptions: { teamId: "test", orgId: null, forceEngine },
      logger,
      featureFlags: new Set(),
      mock: null,
    } as unknown as Meta;
    return (await buildFallbackList(meta)).map(x => x.engine);
  };
}

describe("buildFallbackList renderer selection", () => {
  let fallbackList: FallbackList;

  beforeAll(async () => {
    fallbackList = await loadFallbackList();
  });

  afterAll(() => {
    vi.unstubAllEnvs();
    vi.doUnmock("../../../../lib/exchange");
  });

  it("tries Lightpanda, then Chromium, then fetch for auto", async () => {
    expect(await fallbackList("auto")).toEqual([
      "lightpanda",
      "playwright",
      "fetch",
    ]);
  });

  it("skips Lightpanda for chromium", async () => {
    expect(await fallbackList("chromium")).toEqual(["playwright", "fetch"]);
  });

  it("skips Chromium for lightpanda", async () => {
    expect(await fallbackList("lightpanda")).toEqual(["lightpanda", "fetch"]);
  });

  it("uses a forced engine list as given", async () => {
    expect(await fallbackList("chromium", ["lightpanda", "fetch"])).toEqual([
      "lightpanda",
      "fetch",
    ]);
  });
});
