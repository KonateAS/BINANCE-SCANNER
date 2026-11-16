import json, logging
from .config import DATABASE_URL
log=logging.getLogger(__name__)
class Store:
    def __init__(self): self.pool=None
    async def connect(self):
        try:
            import asyncpg
            self.pool=await asyncpg.create_pool(DATABASE_URL,min_size=1,max_size=4,timeout=3)
            async with self.pool.acquire() as c: await c.execute("CREATE TABLE IF NOT EXISTS signals(id BIGSERIAL PRIMARY KEY,symbol TEXT NOT NULL,timeframe TEXT NOT NULL,candle_ms BIGINT NOT NULL,payload JSONB NOT NULL,saved_at TIMESTAMPTZ DEFAULT now(),UNIQUE(symbol,timeframe,candle_ms))")
        except Exception as e: log.warning("PostgreSQL unavailable; live data remains in memory: %s",e); self.pool=None
    async def save(self,symbol,tf,result):
        if not self.pool or not result or not result["confirmed"]:return False
        try:
            async with self.pool.acquire() as c: await c.execute("INSERT INTO signals(symbol,timeframe,candle_ms,payload) VALUES($1,$2,$3,$4::jsonb) ON CONFLICT(symbol,timeframe,candle_ms) DO UPDATE SET payload=EXCLUDED.payload,saved_at=now()",symbol,tf,result["timestamp"],json.dumps(result))
            return True
        except Exception:
            log.exception("Signal persistence failed")
            return False

    async def invalidate(self, symbol, tf, timestamp):
        """Mark a corrected-away signal invalid while retaining its history."""
        if not self.pool:
            return False
        try:
            async with self.pool.acquire() as connection:
                await connection.execute(
                    "UPDATE signals SET payload=jsonb_set(jsonb_set(payload,'{confirmed}','false'::jsonb,true),'{invalidated}','true'::jsonb,true),saved_at=now() WHERE symbol=$1 AND timeframe=$2 AND candle_ms=$3",
                    symbol, tf, timestamp)
            return True
        except Exception:
            log.exception("Signal invalidation failed")
            return False
