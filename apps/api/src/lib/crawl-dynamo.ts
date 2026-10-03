// Deployments.ai modification: DynamoDB primitives behind crawl-redis.ts and
// the crawler's sitemap link dedupe, replacing Redis sets and sorted sets.
// A set member is one item `<prefix><sha256(member)>` under the owner's
// partition (members such as URLs can exceed key limits), with the raw member
// in `member`. Counters that stand in for SCARD move in the same
// TransactWriteItems call as the member create/delete, so they never drift
// from the member items. The item layout is documented in crawl-redis.ts.
import { createHash } from "node:crypto";
import {
  DeleteCommand,
  GetCommand,
  QueryCommand,
  TransactWriteCommand,
  UpdateCommand,
  type QueryCommandOutput,
} from "@aws-sdk/lib-dynamodb";
import {
  dynamo,
  retryTransactionConflicts,
  tableName,
  transactionCancellationCodes,
} from "../services/dynamo/client";

export const CONDITIONAL_CHECK_FAILED = "ConditionalCheckFailed";

/** Members per TransactWriteItems call (the API allows 100 items). */
const TRANSACTION_MEMBERS = 50;
const TRANSACTION_ATTEMPTS = 25;

type Item = Record<string, unknown>;

export function nowSeconds(): number {
  return Math.floor(Date.now() / 1000);
}

export function memberSortKey(prefix: string, member: string): string {
  return prefix + createHash("sha256").update(member).digest("hex");
}

/** Create condition that also replaces a logically expired leftover item. */
export function absentOrExpired(now = nowSeconds()) {
  return {
    ConditionExpression: "attribute_not_exists(pk) OR #ttl <= :now",
    ExpressionAttributeNames: { "#ttl": "ttl" },
    ExpressionAttributeValues: { ":now": now },
  };
}

export async function mapLimit<T, R>(
  items: readonly T[],
  limit: number,
  fn: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
  const results = new Array<R>(items.length);
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(limit, items.length) }, async () => {
      while (next < items.length) {
        const index = next++;
        results[index] = await fn(items[index], index);
      }
    }),
  );
  return results;
}

export async function transactWrite(items: Item[]): Promise<void> {
  await retryTransactionConflicts(
    () => dynamo().send(new TransactWriteCommand({ TransactItems: items })),
    TRANSACTION_ATTEMPTS,
  );
}

/** Builds `SET`/`ADD`/`REMOVE` clauses with aliased attribute names. */
export function updateExpression(changes: {
  set?: Record<string, unknown>;
  add?: Record<string, number>;
  remove?: string[];
}) {
  const names: Record<string, string> = {};
  const values: Record<string, unknown> = {};
  const clauses: string[] = [];
  let index = 0;
  const alias = (attribute: string) => {
    const name = `#u${index}`;
    names[name] = attribute;
    return { name, value: `:u${index++}` };
  };
  const set = Object.entries(changes.set ?? {}).map(([attribute, value]) => {
    const a = alias(attribute);
    values[a.value] = value;
    return `${a.name} = ${a.value}`;
  });
  const add = Object.entries(changes.add ?? {}).map(([attribute, value]) => {
    const a = alias(attribute);
    values[a.value] = value;
    return `${a.name} ${a.value}`;
  });
  const remove = (changes.remove ?? []).map(attribute => alias(attribute).name);
  if (set.length) clauses.push("SET " + set.join(", "));
  if (add.length) clauses.push("ADD " + add.join(", "));
  if (remove.length) clauses.push("REMOVE " + remove.join(", "));
  return {
    UpdateExpression: clauses.join(" "),
    ExpressionAttributeNames: names,
    ...(Object.keys(values).length ? { ExpressionAttributeValues: values } : {}),
  };
}

export async function updateItem(
  key: { pk: string; sk: string },
  changes: Parameters<typeof updateExpression>[0],
  returnOld = false,
): Promise<Item | undefined> {
  const result = await dynamo().send(
    new UpdateCommand({
      TableName: tableName(),
      Key: key,
      ...updateExpression(changes),
      ...(returnOld ? { ReturnValues: "UPDATED_OLD" as const } : {}),
    }),
  );
  return result.Attributes;
}

export async function getItem(key: {
  pk: string;
  sk: string;
}): Promise<Item | undefined> {
  const result = await dynamo().send(
    new GetCommand({ TableName: tableName(), Key: key, ConsistentRead: true }),
  );
  return result.Item;
}

/** A counter on an item of the same partition (for example crawl meta). */
export type CounterTarget = { sk: string; attribute: string; ttl: number };

export function counterDelta(pk: string, counter: CounterTarget, delta: number) {
  return {
    Update: {
      TableName: tableName(),
      Key: { pk, sk: counter.sk },
      ...updateExpression({
        set: { ttl: counter.ttl },
        add: { [counter.attribute]: delta },
      }),
    },
  };
}

/**
 * SADD. Returns, per input position, whether that member was newly added;
 * a member repeated in the input counts at its first position only, like a
 * pipelined SADD. Members are written in transactions of up to 50, each
 * incrementing the optional counter by the number of items it creates.
 */
export async function addMembers(options: {
  pk: string;
  prefix: string;
  members: readonly string[];
  ttl: number;
  counter?: CounterTarget;
  attributes?: Record<string, unknown>;
}): Promise<boolean[]> {
  const { pk, prefix, ttl, counter } = options;
  const added = options.members.map(() => false);
  const firstIndex = new Map<string, number>();
  options.members.forEach((member, index) => {
    if (!firstIndex.has(member)) firstIndex.set(member, index);
  });
  const distinct = [...firstIndex.keys()];
  const chunks: string[][] = [];
  for (let offset = 0; offset < distinct.length; offset += TRANSACTION_MEMBERS) {
    chunks.push(distinct.slice(offset, offset + TRANSACTION_MEMBERS));
  }
  // Chunks sharing a counter item would only conflict with each other.
  await mapLimit(chunks, counter ? 1 : 4, async chunk => {
    let pending = chunk;
    while (pending.length > 0) {
      const puts = pending.map(member => ({
        Put: {
          TableName: tableName(),
          Item: {
            ...options.attributes,
            pk,
            sk: memberSortKey(prefix, member),
            member,
            ttl,
          },
          ...absentOrExpired(),
        },
      }));
      try {
        await transactWrite(
          counter ? [...puts, counterDelta(pk, counter, pending.length)] : puts,
        );
        for (const member of pending) added[firstIndex.get(member)!] = true;
        return;
      } catch (error) {
        const codes = transactionCancellationCodes(error);
        const existing = new Set(
          pending.filter((_, i) => codes?.[i] === CONDITIONAL_CHECK_FAILED),
        );
        if (existing.size === 0) throw error;
        pending = pending.filter(member => !existing.has(member));
      }
    }
  });
  return added;
}

/** SREM of one member; returns whether an item was deleted. */
export async function removeMember(options: {
  pk: string;
  prefix: string;
  member: string;
  counter?: CounterTarget;
}): Promise<boolean> {
  const { pk, counter } = options;
  const remove = {
    Delete: {
      TableName: tableName(),
      Key: { pk, sk: memberSortKey(options.prefix, options.member) },
      ConditionExpression: "attribute_exists(pk)",
    },
  };
  try {
    await transactWrite(counter ? [remove, counterDelta(pk, counter, -1)] : [remove]);
    return true;
  } catch (error) {
    if (transactionCancellationCodes(error)?.[0] === CONDITIONAL_CHECK_FAILED) {
      return false;
    }
    throw error;
  }
}

/** Upper bound for a `BETWEEN` range covering every key under `prefix`. */
export function prefixEnd(prefix: string): string {
  return prefix + "\uffff";
}

/**
 * Strongly consistent single-partition Query of `from..to` (inclusive),
 * skipping logically expired items. Stops once `stopAfter` live items were
 * collected.
 */
export async function queryLive(options: {
  pk: string;
  from: string;
  to: string;
  forward?: boolean;
  stopAfter?: number;
  count?: false;
}): Promise<Item[]>;
export async function queryLive(options: {
  pk: string;
  from: string;
  to: string;
  count: true;
}): Promise<number>;
export async function queryLive(options: {
  pk: string;
  from: string;
  to: string;
  forward?: boolean;
  stopAfter?: number;
  count?: boolean;
}): Promise<Item[] | number> {
  const items: Item[] = [];
  let total = 0;
  let exclusiveStartKey: QueryCommandOutput["LastEvaluatedKey"];
  do {
    const remaining =
      options.stopAfter === undefined ? undefined : options.stopAfter - items.length;
    const page = await dynamo().send(
      new QueryCommand({
        TableName: tableName(),
        KeyConditionExpression: "pk = :pk AND sk BETWEEN :from AND :to",
        FilterExpression: "attribute_not_exists(#ttl) OR #ttl > :now",
        ExpressionAttributeNames: { "#ttl": "ttl" },
        ExpressionAttributeValues: {
          ":pk": options.pk,
          ":from": options.from,
          ":to": options.to,
          ":now": nowSeconds(),
        },
        ScanIndexForward: options.forward ?? true,
        ConsistentRead: true,
        ExclusiveStartKey: exclusiveStartKey,
        ...(options.count ? { Select: "COUNT" as const } : {}),
        ...(remaining !== undefined ? { Limit: Math.max(remaining, 25) } : {}),
      }),
    );
    total += page.Count ?? 0;
    items.push(...(page.Items ?? []));
    exclusiveStartKey = page.LastEvaluatedKey;
  } while (
    exclusiveStartKey &&
    (options.stopAfter === undefined || items.length < options.stopAfter)
  );
  if (options.count) return total;
  return options.stopAfter === undefined ? items : items.slice(0, options.stopAfter);
}

/** SMEMBERS. */
export async function listMembers(pk: string, prefix: string): Promise<string[]> {
  const items = await queryLive({ pk, from: prefix, to: prefixEnd(prefix) });
  return items.map(item => item.member as string);
}

export async function deleteKeys(pk: string, sortKeys: readonly string[]) {
  await mapLimit(sortKeys, 8, sk =>
    dynamo().send(new DeleteCommand({ TableName: tableName(), Key: { pk, sk } })),
  );
}

/** Deletes every item (expired or not) whose sort key starts with `prefix`. */
export async function deletePrefix(pk: string, prefix: string) {
  let exclusiveStartKey: QueryCommandOutput["LastEvaluatedKey"];
  do {
    const page = await dynamo().send(
      new QueryCommand({
        TableName: tableName(),
        KeyConditionExpression: "pk = :pk AND begins_with(sk, :prefix)",
        ExpressionAttributeValues: { ":pk": pk, ":prefix": prefix },
        ProjectionExpression: "pk, sk",
        ConsistentRead: true,
        Limit: 100,
        ExclusiveStartKey: exclusiveStartKey,
      }),
    );
    await deleteKeys(
      pk,
      (page.Items ?? []).map(item => item.sk as string),
    );
    exclusiveStartKey = page.LastEvaluatedKey;
  } while (exclusiveStartKey);
}
