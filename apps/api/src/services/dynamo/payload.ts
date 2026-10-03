// Deployments.ai modification: JSON payload storage for DynamoDB items.
//
// Job data, scrape results, and status documents are stored gzip-compressed.
// Payloads that still exceed the inline limit are split into chunk items that
// share the owner's partition key, so one strongly consistent Query reads
// them back. Chunk sort keys embed a caller-chosen reference (for example the
// job lock that produced a result), so a rewrite never mixes chunks of two
// versions and a lost conditional update leaves only orphan chunks behind,
// which their TTL removes.
import { gunzipSync, gzipSync } from "node:zlib";
import {
  BatchWriteCommand,
  QueryCommand,
  type QueryCommandOutput,
} from "@aws-sdk/lib-dynamodb";
import { setTimeout as sleep } from "node:timers/promises";
import { dynamo, tableName } from "./client";

/** Compressed payloads up to this size stay on the owning item. */
const INLINE_LIMIT_BYTES = 300 * 1024;
/** Chunk size; keeps each chunk item well below DynamoDB's 400 KB limit. */
const CHUNK_BYTES = 350 * 1024;

/**
 * Stored form of a payload attribute: inline gzip bytes, or a pointer to
 * `count` chunk items under `<owner pk>, <chunkSortKeyPrefix>#<ref>#<n>`.
 */
export type StoredPayload =
  | Uint8Array
  | { readonly chunked: { readonly count: number; readonly ref: string } };

export type PackedPayload = {
  readonly attribute: StoredPayload;
  readonly chunks: readonly Buffer[];
};

export function packPayload(value: unknown, ref: string): PackedPayload {
  const compressed = gzipSync(Buffer.from(JSON.stringify(value), "utf8"));
  if (compressed.length <= INLINE_LIMIT_BYTES) {
    return { attribute: compressed, chunks: [] };
  }
  const chunks: Buffer[] = [];
  for (let offset = 0; offset < compressed.length; offset += CHUNK_BYTES) {
    chunks.push(compressed.subarray(offset, offset + CHUNK_BYTES));
  }
  return { attribute: { chunked: { count: chunks.length, ref } }, chunks };
}

export function chunkSortKey(prefix: string, ref: string, index: number) {
  return `${prefix}#${ref}#${index.toString().padStart(5, "0")}`;
}

async function batchWrite(requests: Record<string, unknown>[]) {
  for (let offset = 0; offset < requests.length; offset += 25) {
    let pending = requests.slice(offset, offset + 25);
    for (let attempt = 0; pending.length > 0; attempt++) {
      if (attempt >= 10) {
        throw new Error("DynamoDB left payload chunk writes unprocessed.");
      }
      const result = await dynamo().send(
        new BatchWriteCommand({ RequestItems: { [tableName()]: pending } }),
      );
      pending = (result.UnprocessedItems?.[tableName()] ?? []) as Record<
        string,
        unknown
      >[];
      if (pending.length > 0) await sleep(20 * 2 ** attempt);
    }
  }
}

/** Writes the chunk items of a packed payload. Inline payloads write nothing. */
export async function writePayloadChunks(options: {
  readonly pk: string;
  readonly prefix: string;
  readonly packed: PackedPayload;
  readonly ttl?: number;
}) {
  const { attribute, chunks } = options.packed;
  if (chunks.length === 0 || attribute instanceof Uint8Array) return;
  await batchWrite(
    chunks.map((data, index) => ({
      PutRequest: {
        Item: {
          pk: options.pk,
          sk: chunkSortKey(options.prefix, attribute.chunked.ref, index),
          data,
          ...(options.ttl !== undefined ? { ttl: options.ttl } : {}),
        },
      },
    })),
  );
}

function isChunked(
  value: unknown,
): value is { chunked: { count: number; ref: string } } {
  return (
    !!value &&
    typeof value === "object" &&
    "chunked" in value &&
    !!value.chunked &&
    typeof value.chunked === "object" &&
    "ref" in value.chunked &&
    typeof value.chunked.ref === "string" &&
    "count" in value.chunked &&
    typeof value.chunked.count === "number"
  );
}

async function readChunks(pk: string, prefix: string, ref: string) {
  const parts: Buffer[] = [];
  let exclusiveStartKey: QueryCommandOutput["LastEvaluatedKey"];
  do {
    const page = await dynamo().send(
      new QueryCommand({
        TableName: tableName(),
        KeyConditionExpression: "pk = :pk AND begins_with(sk, :prefix)",
        ExpressionAttributeValues: { ":pk": pk, ":prefix": `${prefix}#${ref}#` },
        ConsistentRead: true,
        ExclusiveStartKey: exclusiveStartKey,
      }),
    );
    for (const item of page.Items ?? []) {
      parts.push(Buffer.from(item.data as Uint8Array));
    }
    exclusiveStartKey = page.LastEvaluatedKey;
  } while (exclusiveStartKey);
  return parts;
}

/**
 * Decodes a stored payload attribute. Returns undefined for an absent
 * attribute; throws when chunk items are missing.
 */
export async function readPayload<T>(options: {
  readonly pk: string;
  readonly prefix: string;
  readonly stored: unknown;
}): Promise<T | undefined> {
  const { stored } = options;
  if (stored === undefined || stored === null) return undefined;
  let compressed: Buffer;
  if (stored instanceof Uint8Array) {
    compressed = Buffer.from(stored);
  } else if (isChunked(stored)) {
    const parts = await readChunks(options.pk, options.prefix, stored.chunked.ref);
    if (parts.length !== stored.chunked.count) {
      throw new Error(
        `Payload ${options.pk}/${options.prefix} has ${parts.length} of ${stored.chunked.count} chunks.`,
      );
    }
    compressed = Buffer.concat(parts);
  } else {
    throw new Error(`Payload ${options.pk}/${options.prefix} is malformed.`);
  }
  return JSON.parse(gunzipSync(compressed).toString("utf8")) as T;
}

/** Sort keys of the chunk items a stored payload points at. */
export function payloadChunkSortKeys(prefix: string, stored: unknown): string[] {
  if (!isChunked(stored)) return [];
  return Array.from({ length: stored.chunked.count }, (_, index) =>
    chunkSortKey(prefix, stored.chunked.ref, index),
  );
}
