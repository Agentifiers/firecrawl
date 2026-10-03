vi.mock("uuid", () => ({
  v7: vi.fn(() => "job-1"),
}));

import { scrapeSearchResults } from "./scrape";
import { processJobInternal } from "../services/worker/scrape-worker";

vi.mock("../services/worker/scrape-worker", () => ({
  processJobInternal: vi.fn().mockResolvedValue({
    markdown: "body",
    metadata: { creditsUsed: 1, statusCode: 200, proxyUsed: "basic" },
  }),
}));

describe("scrapeSearchResults", () => {
  it("preserves billing metadata on spawned scrape jobs", async () => {
    await scrapeSearchResults(
      [
        {
          url: "https://example.com",
          title: "Example",
          description: "Desc",
        },
      ],
      {
        teamId: "team-1",
        origin: "api",
        timeout: 60_000,
        scrapeOptions: {} as any,
        apiKeyId: 123,
        requestId: "req-1",
        billing: { endpoint: "agent" },
      },
      { debug: vi.fn(), info: vi.fn(), error: vi.fn() } as any,
      null as any,
    );

    expect(processJobInternal).toHaveBeenCalledWith(
      expect.objectContaining({
        // Deployments.ai: the base priority (Redis job priority was removed).
        priority: 10,
        data: expect.objectContaining({
          billing: { endpoint: "agent" },
        }),
      }),
    );
  });
});
