// Deployments.ai modification: versioned JSON status documents in DynamoDB,
// replacing the Redis `SET key json EX ttl` documents of extract, deep
// research, and llms.txt jobs. Item `pk = <kind>#<id>`, `sk = <name>`; the
// document is gzip JSON in `doc` (chunked under `doc#<version>#<n>` when
// large). Updates are read-merge-write under a conditional check on
// `version`, so concurrent updates (for example research activities appended
// by parallel steps) are never lost, unlike upstream's racy GET + SET. Every
// write extends the TTL, as SET EX did.
import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { GetCommand, PutCommand } from "@aws-sdk/lib-dynamodb";
import {
  dynamo,
  isConditionalCheckFailed,
  isLogicallyExpired,
  tableName,
  ttlAt,
} from "../services/dynamo/client";
import {
  packPayload,
  payloadChunkSortKeys,
  readPayload,
  writePayloadChunks,
} from "../services/dynamo/payload";
import { deleteKeys } from "./crawl-dynamo";

const DOC_CHUNKS = "doc";
const UPDATE_ATTEMPTS = 25;

export type StatusDocumentStore<T> = {
  save(id: string, doc: T): Promise<void>;
  get(id: string): Promise<T | null>;
  /** Applies `merge` to the current document; no-op when it is absent. */
  update(id: string, merge: (current: T) => T): Promise<void>;
  /** When the document expires (now when it is absent), at second precision. */
  expiry(id: string): Promise<Date>;
};

type StoredItem = { doc?: unknown; version?: string; ttl?: number };

export function statusDocumentStore<T>(
  kind: string,
  name: string,
  ttlSeconds: number,
): StatusDocumentStore<T> {
  const key = (id: string) => ({ pk: `${kind}#${id}`, sk: name });

  async function readItem(id: string): Promise<StoredItem | null> {
    const result = await dynamo().send(
      new GetCommand({ TableName: tableName(), Key: key(id), ConsistentRead: true }),
    );
    const item = result.Item as StoredItem | undefined;
    return item && !isLogicallyExpired(item) ? item : null;
  }

  async function decode(id: string, item: StoredItem): Promise<T | null> {
    const doc = await readPayload<T>({
      pk: key(id).pk,
      prefix: DOC_CHUNKS,
      stored: item.doc,
    });
    return doc ?? null;
  }

  /** Writes `doc`; with `expected`, only over that version (null: absent). */
  async function write(id: string, doc: T, expected?: StoredItem | null) {
    const version = randomUUID();
    const ttl = ttlAt(Date.now() + ttlSeconds * 1000);
    const packed = packPayload(doc, version);
    await writePayloadChunks({ pk: key(id).pk, prefix: DOC_CHUNKS, packed, ttl });
    await dynamo().send(
      new PutCommand({
        TableName: tableName(),
        Item: { ...key(id), doc: packed.attribute, version, ttl },
        ...(expected
          ? {
              ConditionExpression: "version = :version",
              ExpressionAttributeValues: { ":version": expected.version },
            }
          : {}),
      }),
    );
    const stale = payloadChunkSortKeys(DOC_CHUNKS, expected?.doc);
    if (stale.length > 0) await deleteKeys(key(id).pk, stale);
  }

  return {
    async save(id, doc) {
      // Replaces any previous document, like SET.
      const previous = await readItem(id);
      await write(id, doc);
      const stale = payloadChunkSortKeys(DOC_CHUNKS, previous?.doc);
      if (stale.length > 0) await deleteKeys(key(id).pk, stale);
    },
    async get(id) {
      const item = await readItem(id);
      return item ? decode(id, item) : null;
    },
    async update(id, merge) {
      for (let attempt = 0; attempt < UPDATE_ATTEMPTS; attempt++) {
        const item = await readItem(id);
        const current = item ? await decode(id, item) : null;
        if (!item || current === null) return;
        try {
          await write(id, merge(current), item);
          return;
        } catch (error) {
          if (!isConditionalCheckFailed(error)) throw error;
          await sleep(Math.random() * 20 * (attempt + 1));
        }
      }
      throw new Error(`Concurrent updates kept conflicting on ${kind} ${id}.`);
    },
    async expiry(id) {
      const item = await readItem(id);
      const d = new Date(typeof item?.ttl === "number" ? item.ttl * 1000 : Date.now());
      d.setMilliseconds(0);
      return d;
    },
  };
}
