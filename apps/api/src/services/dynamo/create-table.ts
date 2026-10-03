// Deployments.ai modification: creates the Firecrawl table for local stacks
// (the Compose `dynamodb-init` service) from FIRECRAWL_DYNAMODB_TABLE and
// FIRECRAWL_DYNAMODB_ENDPOINT. AWS deployments create the table with
// Terraform, so this refuses to run without an endpoint override.
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { config } from "../../config";
import { tableName } from "./client";
import { ensureTable } from "./table-schema";

async function main() {
  const endpoint = config.FIRECRAWL_DYNAMODB_ENDPOINT;
  if (!endpoint) {
    throw new Error(
      "FIRECRAWL_DYNAMODB_ENDPOINT is required; AWS tables are created by Terraform.",
    );
  }
  const table = tableName();
  await ensureTable(new DynamoDBClient({ endpoint }), table);
  console.log(`DynamoDB table ${table} is ready at ${endpoint}`);
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
