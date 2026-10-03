// Deployments.ai modification: NuQ queue invariants against DynamoDB Local
// (see src/services/dynamo/test-table.ts for the container command).
import { randomBytes, randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { QueryCommand } from "@aws-sdk/lib-dynamodb";
import { logger } from "../../../lib/logger";
import { dynamo, tableName } from "../../dynamo/client";
import { useTestTable } from "../../dynamo/test-table";
import { consumeJobs } from "./consumer";
import { crawlGroup } from "./groups";
import { jobPk } from "./keys";
import { NuQ, STALL_TIMEOUT_MS } from "./queue";
import { sweepOnce } from "./sweeper";

type Data = { mode: string; blob?: string; n?: number };

const newQueue = (name: string, maxStalls = 3) =>
  new NuQ<Data, unknown>(`${name}-${randomUUID()}`, { maxStalls });

beforeAll(async () => {
  await useTestTable();
});

/** Claims until a few consecutive polls come back empty. */
async function claimAll(queue: NuQ<Data, unknown>, workers: number) {
  const claims: { id: string; lock: string }[] = [];
  await Promise.all(
    Array.from({ length: workers }, async () => {
      // DynamoDB Local updates indexes synchronously, so empty polls mean
      // the queue is drained.
      for (let misses = 0; misses < 3; ) {
        const job = await queue.getJobToProcess();
        if (!job) {
          misses++;
          continue;
        }
        misses = 0;
        claims.push({ id: job.id, lock: job.lock! });
      }
    }),
  );
  return claims;
}

describe("NuQ claims", () => {
  it("claims every job exactly once under concurrent workers", async () => {
    const queue = newQueue("race");
    const ids = Array.from({ length: 40 }, () => randomUUID());
    await queue.addJobs(ids.map(id => ({ id, data: { mode: "single_urls" }, options: {} })));

    const claims = await claimAll(queue, 10);

    expect(claims).toHaveLength(ids.length);
    expect(new Set(claims.map(claim => claim.id))).toEqual(new Set(ids));
    const jobs = await queue.getJobs(ids);
    expect(jobs.every(job => job.status === "active" && job.lock)).toBe(true);
  });

  it("dequeues by priority, then creation time", async () => {
    const queue = newQueue("order");
    await queue.addJob("low", { mode: "x" }, { priority: 20 });
    await queue.addJob("high", { mode: "x" }, { priority: -5 });
    await queue.addJob("mid-1", { mode: "x" }, { priority: 10 });
    await queue.addJob("mid-2", { mode: "x" }, { priority: 10 });

    const claimed = (await claimAll(queue, 1)).map(claim => claim.id);
    expect(claimed).toEqual(["high", "mid-1", "mid-2", "low"]);
  });

  it("rejects duplicate ids, or skips them with addJobIfNotExists", async () => {
    const queue = newQueue("dup");
    await queue.addJob("same", { mode: "x" }, {});
    await expect(queue.addJob("same", { mode: "x" }, {})).rejects.toThrow();
    expect(await queue.addJobIfNotExists("same", { mode: "x" }, {})).toBeNull();
  });

  it("settles only under the current lock, exactly once", async () => {
    const queue = newQueue("settle");
    await queue.addJob("job", { mode: "x" }, {});
    const [claim] = await claimAll(queue, 1);
    expect(await queue.jobFinish("job", "not-the-lock", { ok: true })).toBe(false);
    const outcomes = await Promise.all([
      queue.jobFinish("job", claim.lock, { ok: true }),
      queue.jobFinish("job", claim.lock, { ok: true }),
      queue.jobFail("job", claim.lock, "late"),
    ]);
    expect(outcomes.filter(Boolean)).toHaveLength(1);
    const job = await queue.getJob("job");
    expect(job?.status === "completed" || job?.status === "failed").toBe(true);
  });

  it("processes every job exactly once across concurrent consumer loops", async () => {
    const queue = newQueue("loops");
    const ids = Array.from({ length: 60 }, () => randomUUID());
    await queue.addJobs(ids.map(id => ({ id, data: { mode: "single_urls" }, options: {} })));

    const runs = new Map<string, number>();
    let inFlight = 0;
    let maxInFlight = 0;
    const consumer = {
      queue,
      logger,
      // Two replicas of eight loops each, all racing for the same jobs.
      concurrency: 8,
      isShuttingDown: () => runs.size === ids.length && inFlight === 0,
      process: async (job: { id: string }) => {
        runs.set(job.id, (runs.get(job.id) ?? 0) + 1);
        maxInFlight = Math.max(maxInFlight, ++inFlight);
        await sleep(25);
        inFlight--;
        return { ok: true };
      },
    };
    await Promise.all([consumeJobs(consumer), consumeJobs(consumer)]);

    expect([...runs.keys()].sort()).toEqual([...ids].sort());
    expect([...runs.values()].every(count => count === 1)).toBe(true);
    expect(maxInFlight).toBeGreaterThan(1);
    const jobs = await queue.getJobs(ids);
    expect(jobs.every(job => job.status === "completed")).toBe(true);
  });

  it("rejects a non-positive consumer concurrency", async () => {
    await expect(
      consumeJobs({
        queue: newQueue("bad-loops"),
        logger,
        concurrency: 0,
        isShuttingDown: () => true,
        process: async () => null,
      }),
    ).rejects.toThrow("positive integer");
  });
});

describe("NuQ stalled locks", () => {
  it("reaps a stalled job exactly once under concurrent sweepers and lets another worker claim it", async () => {
    const queue = newQueue("stall");
    await queue.addJob("stalled", { mode: "x" }, {});
    const first = await queue.getJobToProcess();
    expect(first?.id).toBe("stalled");

    // The worker "crashed": its lock is never renewed.
    const later = Date.now() + STALL_TIMEOUT_MS + 1_000;
    const results = await Promise.all(
      Array.from({ length: 8 }, () => sweepOnce({ queues: [queue], now: later })),
    );
    expect(results.reduce((sum, stats) => sum + stats.requeued, 0)).toBe(1);

    const requeued = await queue.getJob("stalled");
    expect(requeued?.status).toBe("queued");
    expect(requeued?.lock).toBeUndefined();

    const second = await queue.getJobToProcess();
    expect(second?.id).toBe("stalled");
    expect(second?.lock).not.toBe(first?.lock);
    // The crashed worker's late completion is rejected; the new owner wins.
    expect(await queue.jobFinish("stalled", first!.lock!, "stale")).toBe(false);
    expect(await queue.renewLock("stalled", first!.lock!)).toBe(false);
    expect(await queue.jobFinish("stalled", second!.lock!, "fresh")).toBe(true);
    expect((await queue.getJob("stalled"))?.returnvalue).toBe("fresh");
  });

  it("does not reap a job whose lock is renewed", async () => {
    const queue = newQueue("renew");
    await queue.addJob("renewed", { mode: "x" }, {});
    const job = await queue.getJobToProcess();
    expect(await queue.renewLock(job!.id, job!.lock!)).toBe(true);
    const stats = await sweepOnce({ queues: [queue], now: Date.now() + 1_000 });
    expect(stats.requeued).toBe(0);
    expect((await queue.getJob("renewed"))?.status).toBe("active");
  });

  it("fails a job after maxStalls and runs the stall hook once", async () => {
    const queue = newQueue("fragile", 1);
    await queue.addJob("doomed", { mode: "x" }, {});
    await queue.getJobToProcess();
    const hook = vi.fn(async () => {});
    const later = Date.now() + STALL_TIMEOUT_MS + 1_000;
    const results = await Promise.all(
      Array.from({ length: 5 }, () =>
        sweepOnce({
          queues: [queue],
          now: later,
          onStallFailure: { [queue.queueName]: hook },
        }),
      ),
    );
    expect(results.reduce((sum, stats) => sum + stats.failed, 0)).toBe(1);
    expect(hook).toHaveBeenCalledTimes(1);
    expect(hook).toHaveBeenCalledWith("doomed", expect.anything());
    const job = await queue.getJob("doomed");
    expect(job?.status).toBe("failed");
    expect(job?.failedReason).toBe("Job stalled too many times");
  });
});

describe("NuQ payloads", () => {
  it("chunks job data and results larger than 300 KB", async () => {
    const blob = randomBytes(600 * 1024).toString("base64");
    const queue = newQueue("big");
    await queue.addJob("big", { mode: "x", blob }, {});

    const chunks = await dynamo().send(
      new QueryCommand({
        TableName: tableName(),
        KeyConditionExpression: "pk = :pk AND begins_with(sk, :data)",
        ExpressionAttributeValues: {
          ":pk": jobPk(queue.queueName, "big"),
          ":data": "data#",
        },
      }),
    );
    expect(chunks.Items!.length).toBeGreaterThan(1);

    const claimed = await queue.getJobToProcess();
    expect(claimed?.data.blob).toBe(blob);
    const waiting = queue.waitForJob("big", 10_000);
    expect(await queue.jobFinish("big", claimed!.lock!, { blob })).toBe(true);
    expect(await waiting).toEqual({ blob });
  });

  it("waitForJob rejects with the failure reason or on timeout", async () => {
    const queue = newQueue("wait");
    await queue.addJob("will-fail", { mode: "x" }, {});
    const job = await queue.getJobToProcess();
    const waiting = queue.waitForJob("will-fail", 10_000);
    await queue.jobFail("will-fail", job!.lock!, "boom");
    await expect(waiting).rejects.toThrow("boom");
    await expect(queue.waitForJob("never-enqueued", 300)).rejects.toThrow("Timed out");
  });
});

describe("NuQ crawl listing", () => {
  it("lists completed single_urls jobs in finish order with offsets", async () => {
    const queue = newQueue("list");
    const groupId = randomUUID();
    await crawlGroup.addGroup(groupId, "owner");
    const ids = Array.from({ length: 7 }, (_, n) => `job-${n}`);
    await queue.addJobs(
      ids.map((id, n) => ({
        id,
        data: { mode: "single_urls", n },
        options: { groupId, ownerId: "owner" },
      })),
    );
    await queue.addJob("kickoff", { mode: "kickoff" }, { groupId, ownerId: "owner" });
    const locks = new Map(
      (await claimAll(queue, 1)).map(claim => [claim.id, claim.lock]),
    );

    // Finish in reverse creation order, one simulated second apart, so the
    // expected order does not depend on clock resolution. One job fails and
    // is not listed.
    const finishOrder = [...ids].reverse();
    let clock = Date.now();
    const now = vi.spyOn(Date, "now").mockImplementation(() => clock);
    try {
      for (const id of finishOrder) {
        clock += 1_000;
        if (id === "job-3") await queue.jobFail(id, locks.get(id)!, "nope");
        else await queue.jobFinish(id, locks.get(id)!, { id });
      }
    } finally {
      now.mockRestore();
    }
    await queue.jobFinish("kickoff", locks.get("kickoff")!, null);

    const expected = finishOrder.filter(id => id !== "job-3");
    const all = await queue.getCrawlJobsForListing(groupId, 100, 0);
    expect(all.map(job => job.id)).toEqual(expected);
    const page = await queue.getCrawlJobsForListing(groupId, 2, 3);
    expect(page.map(job => job.id)).toEqual(expected.slice(3, 5));

    expect(await queue.getGroupNumericStats(groupId)).toEqual({
      queued: 0,
      active: 0,
      backlog: 0,
      completed: 6,
      failed: 1,
    });
    expect((await queue.getGroupAnyJob(groupId, "owner"))?.groupId).toBe(groupId);
    expect(await queue.getGroupAnyJob(groupId, "someone-else")).toBeNull();
  });
});
