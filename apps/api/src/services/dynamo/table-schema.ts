// Deployments.ai modification: the single DynamoDB table used by this fork.
// `dynamodb-table.json` is the only description of the table. The runtime image
// ships it at /app/dynamodb-table.json; infrastructure reads it from the exact
// image it deploys, and local runs and tests create the table with
// `ensureTable`, which builds every call from the same file.
//
// Index purposes:
// - gsi1 (queue index): queued/active/backlog jobs per queue; projects the
//   claim-relevant attributes so dispatch and reaping need no table reads.
// - gsi2 (group index): crawl group membership and completed-job listing.
// - gsi3 (sweep index): active and expiring crawl groups.
import {
  CreateTableCommand,
  type CreateTableCommandInput,
  DescribeTableCommand,
  DynamoDBClient,
  type KeySchemaElement,
  ResourceInUseException,
  UpdateTimeToLiveCommand,
  type UpdateTimeToLiveCommandInput,
} from "@aws-sdk/client-dynamodb";
import { setTimeout as sleep } from "node:timers/promises";
import tableDefinitionJson from "./dynamodb-table.json";

/** Version 1 of the table definition contract in `dynamodb-table.json`. */
export interface FirecrawlTableDefinition {
  version: 1;
  billing_mode: "PAY_PER_REQUEST";
  hash_key: string;
  range_key?: string;
  attributes: { name: string; type: "S" | "N" | "B" }[];
  ttl_attribute: string;
  global_secondary_indexes: {
    name: string;
    hash_key: string;
    range_key?: string;
    projection_type: "ALL" | "KEYS_ONLY" | "INCLUDE";
    non_key_attributes?: string[];
  }[];
}

export const firecrawlTableDefinition =
  tableDefinitionJson as FirecrawlTableDefinition;

function keySchema(hashKey: string, rangeKey?: string): KeySchemaElement[] {
  return [
    { AttributeName: hashKey, KeyType: "HASH" },
    ...(rangeKey
      ? [{ AttributeName: rangeKey, KeyType: "RANGE" as const }]
      : []),
  ];
}

function createTableInput(tableName: string): CreateTableCommandInput {
  const table = firecrawlTableDefinition;
  return {
    TableName: tableName,
    BillingMode: table.billing_mode,
    AttributeDefinitions: table.attributes.map(attribute => ({
      AttributeName: attribute.name,
      AttributeType: attribute.type,
    })),
    KeySchema: keySchema(table.hash_key, table.range_key),
    GlobalSecondaryIndexes: table.global_secondary_indexes.map(index => ({
      IndexName: index.name,
      KeySchema: keySchema(index.hash_key, index.range_key),
      Projection: {
        ProjectionType: index.projection_type,
        ...(index.non_key_attributes
          ? { NonKeyAttributes: index.non_key_attributes }
          : {}),
      },
    })),
  };
}

function timeToLiveInput(tableName: string): UpdateTimeToLiveCommandInput {
  return {
    TableName: tableName,
    TimeToLiveSpecification: {
      AttributeName: firecrawlTableDefinition.ttl_attribute,
      Enabled: true,
    },
  };
}

/** Creates the table described by `dynamodb-table.json` when it does not exist. */
export async function ensureTable(client: DynamoDBClient, tableName: string) {
  try {
    await client.send(new CreateTableCommand(createTableInput(tableName)));
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
    .send(new UpdateTimeToLiveCommand(timeToLiveInput(tableName)))
    .catch(error => {
      // Re-enabling an already enabled TTL is rejected; anything else is real.
      if (!String(error?.message ?? "").includes("already enabled"))
        throw error;
    });
}
