import { vi } from "vitest";

// vi.mock is hoisted above the file's static imports, so any value a factory
// reads at build time must be created in vi.hoisted(). Deployments.ai: the
// batch queue is in-process (it was a Redis list), so tests enqueue through
// queueBillingOperation.
const {
  captureException,
  logger,
  withAuth,
  trackCredits,
  refundCredits,
  billTeam7,
} = vi.hoisted(() => {
  const logger: any = {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    child: vi.fn(() => logger),
  };
  return {
    captureException: vi.fn(),
    logger,
    withAuth: vi.fn((fn: any) => fn),
    trackCredits: vi.fn<(args: any) => Promise<boolean>>(),
    refundCredits: vi.fn<(args: any) => Promise<void>>(),
    billTeam7: vi.fn<(params: any) => Promise<{ api_key: string }[]>>(),
  };
});

vi.mock("@sentry/node", () => ({
  captureException,
}));

vi.mock("../../../lib/logger", () => ({
  logger,
}));

vi.mock("../../../lib/withAuth", () => ({
  withAuth,
}));

vi.mock("../../autumn/autumn.service", () => ({
  autumnService: {
    trackCredits,
    refundCredits,
  },
  featureIdForBillingEndpoint: (endpoint?: string) =>
    endpoint === "search" ? "SEARCH_CREDITS" : "CREDITS",
}));

vi.mock("../../../db/rpc", () => ({
  billTeam7,
}));

import { processBillingBatch, queueBillingOperation } from "../batch_billing";

async function enqueue(
  overrides: { team_id?: string; autumnTrackInRequest?: boolean } = {},
) {
  await queueBillingOperation(
    overrides.team_id ?? "team-1",
    10,
    123,
    { endpoint: "extract" },
    false,
    overrides.autumnTrackInRequest ?? false,
  );
}

beforeEach(async () => {
  vi.clearAllMocks();
  billTeam7.mockResolvedValue([]);
  trackCredits.mockResolvedValue(true);
  refundCredits.mockResolvedValue(undefined);
  // Drain anything a previous test left behind.
  await processBillingBatch();
  vi.clearAllMocks();
  billTeam7.mockResolvedValue([]);
});

describe("processBillingBatch", () => {
  it("commits the ledger but never re-tracks usage to Autumn", async () => {
    // Even when an op was not request-tracked, the batch must not track usage
    // to Autumn — request-time tracking is the single source, so re-tracking
    // here would double-count. The batch only commits the ledger.
    await enqueue();

    await processBillingBatch();

    expect(billTeam7).toHaveBeenCalled();
    expect(trackCredits).not.toHaveBeenCalled();
    expect(captureException).not.toHaveBeenCalled();
  });

  it("does not re-track even when the op was already tracked at request time", async () => {
    await enqueue({ autumnTrackInRequest: true });

    await processBillingBatch();

    expect(billTeam7).toHaveBeenCalled();
    expect(trackCredits).not.toHaveBeenCalled();
  });

  it("refunds request-tracked credits when billing returns success false", async () => {
    await enqueue({ autumnTrackInRequest: true });
    billTeam7.mockRejectedValueOnce(new Error("db failed"));

    await processBillingBatch();

    expect(refundCredits).toHaveBeenCalledWith({
      teamId: "team-1",
      value: 10,
      properties: {
        source: "processBillingBatch",
        endpoint: "extract",
        apiKeyId: 123,
      },
      featureId: "CREDITS",
    });
    expect(captureException).toHaveBeenCalled();
  });

  it("captures exceptions and refunds when billing throws", async () => {
    await enqueue({ autumnTrackInRequest: true });
    billTeam7.mockRejectedValueOnce(new Error("rpc exploded"));

    await processBillingBatch();

    expect(refundCredits).toHaveBeenCalledWith({
      teamId: "team-1",
      value: 10,
      properties: {
        source: "processBillingBatch",
        endpoint: "extract",
        apiKeyId: 123,
      },
      featureId: "CREDITS",
    });
    expect(captureException).toHaveBeenCalled();
  });

  it("continues processing later groups when an Autumn refund fails", async () => {
    await enqueue({ team_id: "team-1", autumnTrackInRequest: true });
    await enqueue({ team_id: "team-2", autumnTrackInRequest: true });
    billTeam7
      .mockRejectedValueOnce(new Error("db failed"))
      .mockResolvedValueOnce([]);
    refundCredits.mockRejectedValueOnce(new Error("refund failed"));

    await processBillingBatch();

    expect(refundCredits).toHaveBeenCalledWith({
      teamId: "team-1",
      value: 10,
      properties: {
        source: "processBillingBatch",
        endpoint: "extract",
        apiKeyId: 123,
      },
      featureId: "CREDITS",
    });
    expect(billTeam7).toHaveBeenCalledTimes(2);
    // The batch never tracks usage to Autumn, regardless of the request-time flag.
    expect(trackCredits).not.toHaveBeenCalled();
    expect(captureException).toHaveBeenCalled();
  });
});
