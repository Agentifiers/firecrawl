// Deployments.ai modification: test helper. Points the shared DynamoDB client
// at DynamoDB Local (FIRECRAWL_TEST_DYNAMODB_ENDPOINT, default
// http://127.0.0.1:18000) and creates an isolated table for one test file.
//
//   docker run -d -p 127.0.0.1:18000:8000 amazon/dynamodb-local \
//     -jar DynamoDBLocal.jar -inMemory -sharedDb
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { randomUUID } from "node:crypto";
import { config } from "../../config";
import { ensureTable } from "./table-schema";

export async function useTestTable(): Promise<string> {
  const endpoint =
    process.env.FIRECRAWL_TEST_DYNAMODB_ENDPOINT ?? "http://127.0.0.1:18000";
  process.env.AWS_REGION ??= "us-east-1";
  process.env.AWS_ACCESS_KEY_ID ??= "local";
  process.env.AWS_SECRET_ACCESS_KEY ??= "local";
  const table = `firecrawl-test-${randomUUID()}`;
  config.FIRECRAWL_DYNAMODB_ENDPOINT = endpoint;
  config.FIRECRAWL_DYNAMODB_TABLE = table;
  await ensureTable(new DynamoDBClient({ endpoint }), table);
  return table;
}
