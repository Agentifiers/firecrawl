// Deployments.ai modification: the single DynamoDB table used by this fork.
// Terraform (infrastructure/modules/web-tools-runtime) creates the same shape
// in AWS; local runs and tests create it with `ensureTable`.
import {
  CreateTableCommand,
  DescribeTableCommand,
  DynamoDBClient,
  ResourceInUseException,
  UpdateTimeToLiveCommand,
} from "@aws-sdk/client-dynamodb";
import { setTimeout as sleep } from "node:timers/promises";

const keyAttributes = ["pk", "sk", "gsi1pk", "gsi1sk", "gsi2pk", "gsi2sk", "gsi3pk", "gsi3sk"];

/**
 * Index projections:
 * - gsi1 (queue index): queued/active/backlog jobs per queue; projects the
 *   claim-relevant attributes so dispatch and reaping need no table reads.
 * - gsi2 (group index): crawl group membership and completed-job listing.
 * - gsi3 (sweep index): active and expiring crawl groups.
 */
export const firecrawlTableIndexes = [
  {
    IndexName: "gsi1",
    KeySchema: [
      { AttributeName: "gsi1pk", KeyType: "HASH" as const },
      { AttributeName: "gsi1sk", KeyType: "RANGE" as const },
    ],
    Projection: {
      ProjectionType: "INCLUDE" as const,
      NonKeyAttributes: ["id", "lock", "lockedAt", "notBefore", "stalls", "readySortKey"],
    },
  },
  {
    IndexName: "gsi2",
    KeySchema: [
      { AttributeName: "gsi2pk", KeyType: "HASH" as const },
      { AttributeName: "gsi2sk", KeyType: "RANGE" as const },
    ],
    Projection: {
      ProjectionType: "INCLUDE" as const,
      NonKeyAttributes: ["id", "status", "mode", "ownerId"],
    },
  },
  {
    IndexName: "gsi3",
    KeySchema: [
      { AttributeName: "gsi3pk", KeyType: "HASH" as const },
      { AttributeName: "gsi3sk", KeyType: "RANGE" as const },
    ],
    Projection: { ProjectionType: "KEYS_ONLY" as const },
  },
];

/** Creates the table (on-demand, TTL on `ttl`) when it does not exist. */
export async function ensureTable(client: DynamoDBClient, tableName: string) {
  try {
    await client.send(
      new CreateTableCommand({
        TableName: tableName,
        BillingMode: "PAY_PER_REQUEST",
        AttributeDefinitions: keyAttributes.map(name => ({
          AttributeName: name,
          AttributeType: "S",
        })),
        KeySchema: [
          { AttributeName: "pk", KeyType: "HASH" },
          { AttributeName: "sk", KeyType: "RANGE" },
        ],
        GlobalSecondaryIndexes: firecrawlTableIndexes,
      }),
    );
  } catch (error) {
    if (!(error instanceof ResourceInUseException)) throw error;
  }
  for (let attempt = 0; attempt < 60; attempt++) {
    const table = await client.send(
      new DescribeTableCommand({ TableName: tableName }),
    );
    if (table.Table?.TableStatus === "ACTIVE") break;
    await sleep(500);
  }
  await client
    .send(
      new UpdateTimeToLiveCommand({
        TableName: tableName,
        TimeToLiveSpecification: { AttributeName: "ttl", Enabled: true },
      }),
    )
    .catch(error => {
      // Re-enabling an already enabled TTL is rejected; anything else is real.
      if (!String(error?.message ?? "").includes("already enabled")) throw error;
    });
}
