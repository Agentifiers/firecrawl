// Deployments.ai modification: crawl group invariants of the DynamoDB NuQ
// (concurrency gating, delay, backlog promotion, completion) against
// DynamoDB Local (see src/services/dynamo/test-table.ts).
import { randomUUID } from "node:crypto";
import { QueryCommand, TransactWriteCommand } from "@aws-sdk/lib-dynamodb";
import { dynamo, tableName } from "../../dynamo/client";
import { useTestTable } from "../../dynamo/test-table";
import {
  CRAWL_FINISHED_QUEUE,
  crawlGroup,
  groupUpdate,
  readGroup,
  SCRAPE_QUEUE,
  tryCompleteGroup,
} from "./groups";
import { groupIndexPk, jobKey } from "./keys";
import { NuQ } from "./queue";
import { sweepOnce } from "./sweeper";

type Data = { mode: string; n?: number };

let scrape: NuQ<Data, unknown>;

beforeAll(async () => {
  await useTestTable();
  scrape = new NuQ<Data, unknown>(SCRAPE_QUEUE, { maxStalls: 9 });
});

const pages = (groupId: string, count: number, extra: object = {}) =>
  Array.from({ length: count }, (_, n) => ({
    id: `${groupId}-${n}`,
    data: { mode: "single_urls", n },
    options: { groupId, ownerId: "owner", ...extra },
  }));

async function crawlFinishedJobs(groupId: string) {
  const result = await dynamo().send(
    new QueryCommand({
      TableName: tableName(),
      IndexName: "gsi2",
      KeyConditionExpression: "gsi2pk = :pk",
      ExpressionAttributeValues: { ":pk": groupIndexPk(groupId) },
    }),
  );
  return (result.Items ?? []).filter(item =>
    String(item.pk).startsWith(`job#${CRAWL_FINISHED_QUEUE}#`),
  );
}

/** Claims every claimable scrape job; all must belong to `groupId`. */
async function claimGroupJobs(groupId: string) {
  const claimed: { id: string; lock: string }[] = [];
  for (let job = await scrape.getJobToProcess(); job; job = await scrape.getJobToProcess()) {
    expect(job.groupId).toBe(groupId);
    claimed.push({ id: job.id, lock: job.lock! });
  }
  return claimed;
}

/** Runs `fn` with Date.now() under test control. */
async function withClock<T>(fn: (advance: (ms: number) => void) => Promise<T>) {
  let clock = Date.now();
  const now = vi.spyOn(Date, "now").mockImplementation(() => clock);
  try {
    return await fn(ms => {
      clock += ms;
    });
  } finally {
    now.mockRestore();
  }
}

describe("crawl group gating", () => {
  it("never runs more than maxConcurrency jobs and drains the backlog", async () => {
    const groupId = randomUUID();
    await crawlGroup.addGroup(groupId, "owner", undefined, { maxConcurrency: 2 });
    const jobs = await scrape.addJobs(pages(groupId, 6));
    expect(jobs.map(job => job.status)).toEqual([
      "queued",
      "queued",
      "backlog",
      "backlog",
      "backlog",
      "backlog",
    ]);
    expect(await scrape.getGroupNumericStats(groupId)).toMatchObject({
      queued: 2,
      backlog: 4,
    });

    // Four workers race; a job counts as running from its claim until just
    // before it settles (strictly inside the window it holds a slot).
    let running = 0;
    let peak = 0;
    const started: string[] = [];
    await Promise.all(
      Array.from({ length: 4 }, async () => {
        for (let idle = 0; idle < 20; ) {
          const job = await scrape.getJobToProcess();
          if (!job) {
            idle++;
            continue;
          }
          idle = 0;
          running++;
          peak = Math.max(peak, running);
          started.push(job.id);
          await scrape.renewLock(job.id, job.lock!);
          running--;
          await scrape.jobFinish(job.id, job.lock!, { id: job.id });
        }
      }),
    );

    expect(peak).toBeLessThanOrEqual(2);
    expect(started.sort()).toEqual(jobs.map(job => job.id).sort());
    expect(await readGroup(groupId)).toMatchObject({
      status: "completed",
      slotsUsed: 0,
      pending: 0,
    });
    expect(await crawlFinishedJobs(groupId)).toHaveLength(1);
  });

  it("lets bypassGroupLimit jobs start immediately", async () => {
    const groupId = randomUUID();
    await crawlGroup.addGroup(groupId, "owner", undefined, { maxConcurrency: 1 });
    await scrape.addJobs(pages(groupId, 2));
    const kickoff = await scrape.addJob(
      `${groupId}-kickoff`,
      { mode: "kickoff" },
      { groupId, ownerId: "owner", bypassGroupLimit: true },
    );
    expect(kickoff.status).toBe("queued");
    const claimed = await claimGroupJobs(groupId);
    expect(claimed.map(claim => claim.id).sort()).toEqual(
      [`${groupId}-0`, `${groupId}-kickoff`].sort(),
    );
    for (const claim of claimed) await scrape.jobFinish(claim.id, claim.lock, null);
    expect((await claimGroupJobs(groupId)).map(claim => claim.id)).toEqual([
      `${groupId}-1`,
    ]);
  });

  it("spaces job starts of a delayed group", async () => {
    const groupId = randomUUID();
    await crawlGroup.addGroup(groupId, "owner", undefined, { delaySeconds: 2 });
    await scrape.addJobs(pages(groupId, 2));
    const [first] = await claimGroupJobs(groupId);
    expect(first.id).toBe(`${groupId}-0`);

    await withClock(async advance => {
      await scrape.jobFinish(first.id, first.lock, null);
      expect((await scrape.getJob(`${groupId}-1`))?.status).toBe("queued");
      // Promoted, but not claimable until the delay has elapsed.
      expect(await scrape.getJobToProcess()).toBeNull();
      advance(2_100);
      expect((await scrape.getJobToProcess())?.id).toBe(`${groupId}-1`);
    });
  });

  it("drops backlogged jobs past their deadline and still completes the group", async () => {
    const groupId = randomUUID();
    await crawlGroup.addGroup(groupId, "owner", undefined, { maxConcurrency: 1 });
    await scrape.addJobs(pages(groupId, 3, { backlogTimeoutMs: 1_000 }));
    const [holder] = await claimGroupJobs(groupId);

    await withClock(async advance => {
      advance(5_000);
      await scrape.jobFinish(holder.id, holder.lock, null);
    });

    expect(await scrape.getJob(`${groupId}-1`)).toBeNull();
    expect(await scrape.getJob(`${groupId}-2`)).toBeNull();
    expect(await readGroup(groupId)).toMatchObject({ status: "completed", pending: 0 });
    expect(await crawlFinishedJobs(groupId)).toHaveLength(1);
  });

  it("sweeper promotes a backlog whose slot release was never followed up", async () => {
    const groupId = randomUUID();
    await crawlGroup.addGroup(groupId, "owner", undefined, { maxConcurrency: 1 });
    await scrape.addJobs(pages(groupId, 2));
    const [holder] = await claimGroupJobs(groupId);
    // A worker died right after its job left the group: the slot is
    // released, but nobody promoted the backlog.
    await dynamo().send(
      new TransactWriteCommand({
        TransactItems: [
          { Delete: { TableName: tableName(), Key: jobKey(SCRAPE_QUEUE, holder.id) } },
          groupUpdate(groupId, { pending: -1, pendingSingle: -1, slotsUsed: -1 }),
        ],
      }),
    );
    const stats = await sweepOnce({ queues: [scrape] });
    expect(stats.promoted).toBe(1);
    expect((await claimGroupJobs(groupId)).map(claim => claim.id)).toEqual([
      `${groupId}-1`,
    ]);
  });
});

describe("crawl group completion", () => {
  it("completes exactly once when the last jobs finish concurrently", async () => {
    const groupId = randomUUID();
    await crawlGroup.addGroup(groupId, "owner");
    await scrape.addJobs(pages(groupId, 8));
    const claimed = await claimGroupJobs(groupId);
    expect(claimed).toHaveLength(8);

    await Promise.all([
      ...claimed.map(claim => scrape.jobFinish(claim.id, claim.lock, null)),
      ...Array.from({ length: 4 }, () => tryCompleteGroup(groupId)),
      sweepOnce({ queues: [scrape] }),
    ]);
    await Promise.all(Array.from({ length: 4 }, () => tryCompleteGroup(groupId)));
    await sweepOnce({ queues: [scrape] });

    expect(await readGroup(groupId)).toMatchObject({ status: "completed", pending: 0 });
    expect(await crawlFinishedJobs(groupId)).toHaveLength(1);
  });

  it("does not complete a group while a job is pending", async () => {
    const groupId = randomUUID();
    await crawlGroup.addGroup(groupId, "owner");
    await scrape.addJobs(pages(groupId, 2));
    const [first, second] = await claimGroupJobs(groupId);
    await scrape.jobFinish(first.id, first.lock, null);
    expect(await tryCompleteGroup(groupId)).toBe(false);
    expect((await readGroup(groupId))?.status).toBe("active");
    await scrape.jobFail(second.id, second.lock, "boom");
    expect((await readGroup(groupId))?.status).toBe("completed");
  });

  it("cancelGroup drops the backlog and lets the group finish", async () => {
    const groupId = randomUUID();
    await crawlGroup.addGroup(groupId, "owner", undefined, { maxConcurrency: 1 });
    await scrape.addJobs(pages(groupId, 4));
    const [holder] = await claimGroupJobs(groupId);
    expect(await crawlGroup.cancelGroup(groupId)).toBe(true);
    expect(await readGroup(groupId)).toMatchObject({ status: "active", pending: 1 });
    await scrape.jobFinish(holder.id, holder.lock, null);
    expect((await readGroup(groupId))?.status).toBe("completed");
    expect(await crawlFinishedJobs(groupId)).toHaveLength(1);
    const ongoing = await crawlGroup.getOngoingByOwner("owner");
    expect(ongoing.map(group => group.id)).not.toContain(groupId);
  });

  it("deletes an expired completed group with its jobs", async () => {
    const groupId = randomUUID();
    await crawlGroup.addGroup(groupId, "owner", 1_000);
    await scrape.addJobs(pages(groupId, 2));
    for (const claim of await claimGroupJobs(groupId)) {
      await scrape.jobFinish(claim.id, claim.lock, null);
    }
    expect((await readGroup(groupId))?.status).toBe("completed");

    // Drain the crawl-finished job so its partition is also in the group.
    await withClock(async advance => {
      advance(10_000);
      for (let pass = 0; pass < 5 && (await readGroup(groupId)); pass++) {
        await sweepOnce({ queues: [scrape] });
      }
    });
    expect(await readGroup(groupId)).toBeNull();
    expect(await scrape.getJob(`${groupId}-0`)).toBeNull();
    expect(await crawlFinishedJobs(groupId)).toHaveLength(0);
  });
});
