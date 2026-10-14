// Deployments.ai modification: proves `ensureTable` creates exactly the table
// described by `dynamodb-table.json` and that the runtime image ships that file
// at the stable path infrastructure reads it from.
import {
  CreateTableCommand,
  DescribeTableCommand,
  type DynamoDBClient,
  UpdateTimeToLiveCommand,
} from "@aws-sdk/client-dynamodb";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { ensureTable } from "./table-schema";

const apiRoot = join(__dirname, "../../..");
const definition = JSON.parse(
  readFileSync(join(__dirname, "dynamodb-table.json"), "utf8"),
);

function recordingClient() {
  const sent: object[] = [];
  const client = {
    send: async (command: object) => {
      sent.push(command);
      return command instanceof DescribeTableCommand
        ? { Table: { TableStatus: "ACTIVE" } }
        : {};
    },
  } as unknown as DynamoDBClient;
  return { client, sent };
}

function keySchema(hashKey: string, rangeKey?: string) {
  return [
    { AttributeName: hashKey, KeyType: "HASH" },
    ...(rangeKey ? [{ AttributeName: rangeKey, KeyType: "RANGE" }] : []),
  ];
}

describe("Firecrawl DynamoDB table definition", () => {
  it("is version 1 of the contract and declares every key attribute exactly once", () => {
    expect(definition.version).toBe(1);
    expect(definition.billing_mode).toBe("PAY_PER_REQUEST");
    const declared = definition.attributes.map(attribute => attribute.name);
    const keys = [definition.hash_key, definition.range_key];
    for (const index of definition.global_secondary_indexes) {
      keys.push(index.hash_key, index.range_key);
      expect(["ALL", "KEYS_ONLY", "INCLUDE"]).toContain(index.projection_type);
      expect(Boolean(index.non_key_attributes?.length)).toBe(
        index.projection_type === "INCLUDE",
      );
    }
    expect([...declared].sort()).toEqual(keys.filter(Boolean).sort());
    expect(new Set(declared).size).toBe(declared.length);
  });

  it("ensureTable sends CreateTable and TTL inputs built from the JSON alone", async () => {
    const { client, sent } = recordingClient();
    await ensureTable(client, "firecrawl-table");

    expect(sent.map(command => command.constructor)).toEqual([
      CreateTableCommand,
      DescribeTableCommand,
      UpdateTimeToLiveCommand,
    ]);
    expect((sent[0] as CreateTableCommand).input).toEqual({
      TableName: "firecrawl-table",
      BillingMode: definition.billing_mode,
      AttributeDefinitions: definition.attributes.map(attribute => ({
        AttributeName: attribute.name,
        AttributeType: attribute.type,
      })),
      KeySchema: keySchema(definition.hash_key, definition.range_key),
      GlobalSecondaryIndexes: definition.global_secondary_indexes.map(
        index => ({
          IndexName: index.name,
          KeySchema: keySchema(index.hash_key, index.range_key),
          Projection: {
            ProjectionType: index.projection_type,
            ...(index.non_key_attributes
              ? { NonKeyAttributes: index.non_key_attributes }
              : {}),
          },
        }),
      ),
    });
    expect((sent[2] as UpdateTimeToLiveCommand).input).toEqual({
      TableName: "firecrawl-table",
      TimeToLiveSpecification: {
        AttributeName: definition.ttl_attribute,
        Enabled: true,
      },
    });
  });

  it("is copied into the runtime image at /app/dynamodb-table.json", () => {
    const dockerfile = readFileSync(join(apiRoot, "Dockerfile"), "utf8");
    const runtime = dockerfile.slice(dockerfile.indexOf("AS runtime"));
    expect(runtime).toContain(
      "COPY apps/api/src/services/dynamo/dynamodb-table.json ./dynamodb-table.json\n",
    );
  });
});
