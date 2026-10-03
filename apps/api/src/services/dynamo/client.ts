// Deployments.ai modification: shared DynamoDB access for the queue and the
// crawl/extract/research/llms.txt state stores that replace Postgres and Redis.
// See MODIFICATIONS.md and src/services/worker/nuq/keys.ts for the data model.
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { setTimeout as sleep } from "node:timers/promises";
import { config } from "../../config";

let documentClient: DynamoDBDocumentClient | null = null;

export function dynamo(): DynamoDBDocumentClient {
  if (!documentClient) {
    documentClient = DynamoDBDocumentClient.from(
      new DynamoDBClient({
        ...(config.FIRECRAWL_DYNAMODB_ENDPOINT
          ? { endpoint: config.FIRECRAWL_DYNAMODB_ENDPOINT }
          : {}),
        maxAttempts: 6,
      }),
      {
        marshallOptions: {
          convertClassInstanceToMap: false,
          removeUndefinedValues: true,
        },
      },
    );
  }
  return documentClient;
}

export function tableName(): string {
  const name = config.FIRECRAWL_DYNAMODB_TABLE;
  if (!name) {
    throw new Error("FIRECRAWL_DYNAMODB_TABLE is required.");
  }
  return name;
}

/** DynamoDB TTL attribute value: whole epoch seconds. */
export function ttlAt(epochMs: number): number {
  return Math.ceil(epochMs / 1000);
}

/**
 * True when an item carrying a `ttl` attribute is logically expired. DynamoDB
 * deletes expired items asynchronously (often hours later), so every read of
 * an expiring item must check this before returning it.
 */
export function isLogicallyExpired(
  item: { ttl?: number } | undefined,
  now = Date.now(),
): boolean {
  return typeof item?.ttl === "number" && item.ttl * 1000 <= now;
}

function errorName(error: unknown): string | undefined {
  return error && typeof error === "object" && "name" in error
    ? String(error.name)
    : undefined;
}

export function isConditionalCheckFailed(error: unknown): boolean {
  return errorName(error) === "ConditionalCheckFailedException";
}

/**
 * Cancellation reason codes of a TransactWriteItems call, in request order,
 * or null when the error is not a cancelled transaction.
 */
export function transactionCancellationCodes(error: unknown): string[] | null {
  if (errorName(error) !== "TransactionCanceledException") return null;
  const reasons =
    error && typeof error === "object" && "CancellationReasons" in error
      ? error.CancellationReasons
      : undefined;
  if (!Array.isArray(reasons)) return [];
  return reasons.map(reason =>
    reason && typeof reason === "object" && "Code" in reason
      ? String(reason.Code)
      : "None",
  );
}

function isRetryableTransactionConflict(error: unknown): boolean {
  const codes = transactionCancellationCodes(error);
  if (codes) {
    return (
      codes.includes("TransactionConflict") &&
      !codes.includes("ConditionalCheckFailed")
    );
  }
  return errorName(error) === "TransactionInProgressException";
}

/**
 * Retries an operation that cancels only because a concurrent transaction
 * touched the same item (for example two workers finishing jobs of the same
 * crawl group). Condition failures are returned to the caller unchanged.
 */
export async function retryTransactionConflicts<T>(
  operation: () => Promise<T>,
  attempts = 10,
): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await operation();
    } catch (error) {
      if (attempt >= attempts || !isRetryableTransactionConflict(error)) {
        throw error;
      }
      await sleep(
        5 + Math.floor(Math.random() * Math.min(25 * 2 ** attempt, 500)),
      );
    }
  }
}

/** Fixed-width, lexicographically sortable non-negative integer. */
export function sortableNumber(value: number, width = 15): string {
  return Math.max(0, Math.floor(value)).toString().padStart(width, "0");
}
