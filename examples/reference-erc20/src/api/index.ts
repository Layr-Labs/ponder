import schema from "ponder:schema";
import { Hono } from "hono";
import { replaceBigInts } from "ponder";
import {
  createHistoricalIndexingStore,
  createRealtimeIndexingStore,
} from "ponder/experimental_unsafe_stores";

const app = new Hono();

const historicalIndexingStore = createHistoricalIndexingStore({
  // @ts-expect-error
  common: globalThis.COMMON,
  // @ts-expect-error
  database: globalThis.PONDER_DATABASE,
  schemaBuild: { schema },
  isDatabaseEmpty: true,
});

const realtimeIndexingStore = createRealtimeIndexingStore({
  // @ts-expect-error
  common: globalThis.COMMON,
  // @ts-expect-error
  database: globalThis.PONDER_DATABASE,
  schemaBuild: { schema },
});

app.get("/insert", async (c) => {
  const row = await realtimeIndexingStore
    .insert(schema.account)
    .values({ address: "0x123", balance: 0n, isOwner: false })
    .onConflictDoUpdate((row) => ({
      balance: row.balance - 1n,
    }));

  return c.json(replaceBigInts(row, (value) => value.toString()));
});

export default app;
