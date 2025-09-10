# redis_service.py (file mới)
import time, json
from typing import Optional, Dict, Any, List
import redis.asyncio as redis

class RedisService:
    """
    - call:{call_id}      -> HASH   (metadata cuộc gọi)
    - camp:{cid}:retry    -> ZSET   (các call_id cần retry, score = epoch khi đến hạn)
    - camp:{cid}:done     -> SET    (lead_id đã thành công -> bỏ qua)
    """
    def __init__(self, redis_url: str):
        self._url = redis_url
        self._r: Optional[redis.Redis] = None

    async def connect(self):
        self._r = redis.from_url(self._url, decode_responses=True)

    async def close(self):
        if self._r:
            await self._r.aclose()
            self._r = None

    # ----- LEAD SUCCESS -----
    async def mark_lead_success(self, campaign_id: int, lead_id: int):
        assert self._r is not None
        await self._r.sadd(f"camp:{campaign_id}:done", str(lead_id))

    async def is_lead_success(self, campaign_id: int, lead_id: int) -> bool:
        assert self._r is not None
        return bool(await self._r.sismember(f"camp:{campaign_id}:done", str(lead_id)))

    # ----- CALL PAYLOAD & RETRY -----
    async def save_failure_and_schedule_retry(
        self,
        campaign_id: int,
        call_id: str,
        payload: Dict[str, Any],    # {lead_id, phone, attempt, max_attempts, retry_interval_s, ...}
        delay_seconds: int,
    ):
        assert self._r is not None
        hkey = f"call:{call_id}"
        zkey = f"camp:{campaign_id}:retry"
        retry_at = int(time.time()) + int(delay_seconds)

        mapping = {}
        for k, v in payload.items():
            if isinstance(v, (dict, list)):
                mapping[k] = json.dumps(v)
            else:
                mapping[k] = str(v)

        async with self._r.pipeline(transaction=True) as p:
            await p.hset(hkey, mapping=mapping)
            await p.zadd(zkey, {call_id: retry_at})
            await p.execute()

    async def save_success_and_finalize(self, call_id: str):
        assert self._r is not None
        await self._r.delete(f"call:{call_id}")

    # ----- IN-PROGRESS SET (avoid duplicate sends before result) -----
    async def mark_inprogress(self, campaign_id: int, lead_id: int):
        assert self._r is not None
        await self._r.sadd(f"camp:{campaign_id}:inprogress", str(lead_id))

    async def clear_inprogress(self, campaign_id: int, lead_id: int):
        assert self._r is not None
        await self._r.srem(f"camp:{campaign_id}:inprogress", str(lead_id))

    async def is_inprogress(self, campaign_id: int, lead_id: int) -> bool:
        assert self._r is not None
        return bool(await self._r.sismember(f"camp:{campaign_id}:inprogress", str(lead_id)))

    # ----- PHONE-LEVEL DEDUP (per-campaign only) -----
    async def mark_phone_success(self, campaign_id: int, phone: str):
        assert self._r is not None
        await self._r.sadd(f"camp:{campaign_id}:done_phone", str(phone))

    async def is_phone_success(self, campaign_id: int, phone: str) -> bool:
        assert self._r is not None
        return bool(await self._r.sismember(f"camp:{campaign_id}:done_phone", str(phone)))

    async def mark_phone_inprogress(self, campaign_id: int, phone: str):
        assert self._r is not None
        await self._r.sadd(f"camp:{campaign_id}:inprog_phone", str(phone))

    async def clear_phone_inprogress(self, campaign_id: int, phone: str):
        assert self._r is not None
        await self._r.srem(f"camp:{campaign_id}:inprog_phone", str(phone))

    async def is_phone_inprogress(self, campaign_id: int, phone: str) -> bool:
        assert self._r is not None
        return bool(await self._r.sismember(f"camp:{campaign_id}:inprog_phone", str(phone)))

    _POP_DUE_LUA = """
    local zkey = KEYS[1]
    local now  = tonumber(ARGV[1])
    local limit = tonumber(ARGV[2])
    local res = {}
    for i=1,limit,1 do
        local ids = redis.call('ZRANGEBYSCORE', zkey, '-inf', now, 'LIMIT', 0, 1)
        if (ids == nil) or (#ids == 0) then break end
        local id = ids[1]
        redis.call('ZREM', zkey, id)
        table.insert(res, id)
    end
    return res
    """

    async def claim_due_retries(self, campaign_id: int, limit: int = 10) -> List[str]:
        assert self._r is not None
        zkey = f"camp:{campaign_id}:retry"
        now_ts = int(time.time())
        return await self._r.eval(self._POP_DUE_LUA, 1, zkey, now_ts, limit)

    async def get_call_payload(self, call_id: str) -> Dict[str, Any]:
        assert self._r is not None
        data = await self._r.hgetall(f"call:{call_id}")
        def _maybe(v: str):
            try: return json.loads(v)
            except Exception: return v
        return {k: _maybe(v) for k, v in data.items()}
