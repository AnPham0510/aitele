# redis_service.py (file mới)
import time, json
import logging
from typing import Optional, Dict, Any, List
import redis.asyncio as redis

logger = logging.getLogger(__name__)

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

    async def mark_lead_success(self, campaign_id: str, lead_id: str):
        assert self._r is not None
        await self._r.sadd(f"camp:{campaign_id}:done", str(lead_id))

    async def is_lead_success(self, campaign_id: str, lead_id: str) -> bool:
        assert self._r is not None
        return bool(await self._r.sismember(f"camp:{campaign_id}:done", str(lead_id)))

    async def save_failure_and_schedule_retry(
        self,
        campaign_id: str,
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
            elif hasattr(v, '__str__') and not isinstance(v, (str, int, float, bool)):
                mapping[k] = str(v)
            else:
                mapping[k] = str(v)

        async with self._r.pipeline(transaction=True) as p:
            await p.hset(hkey, mapping=mapping)
            await p.zadd(zkey, {call_id: retry_at})
            await p.execute()

    async def save_success_and_finalize(self, call_id: str):
        assert self._r is not None
        await self._r.delete(f"call:{call_id}")

    async def mark_inprogress(self, campaign_id: str, lead_id: str):
        assert self._r is not None
        await self._r.sadd(f"camp:{campaign_id}:inprogress", str(lead_id))

    async def clear_inprogress(self, campaign_id: str, lead_id: str):
        assert self._r is not None
        await self._r.srem(f"camp:{campaign_id}:inprogress", str(lead_id))

    async def is_inprogress(self, campaign_id: str, lead_id: str) -> bool:
        assert self._r is not None
        return bool(await self._r.sismember(f"camp:{campaign_id}:inprogress", str(lead_id)))

    async def mark_phone_success(self, campaign_id: str, phone: str):
        assert self._r is not None
        await self._r.sadd(f"camp:{campaign_id}:done_phone", str(phone))

    async def is_phone_success(self, campaign_id: str, phone: str) -> bool:
        assert self._r is not None
        return bool(await self._r.sismember(f"camp:{campaign_id}:done_phone", str(phone)))

    async def mark_phone_inprogress(self, campaign_id: str, phone: str):
        assert self._r is not None
        await self._r.sadd(f"camp:{campaign_id}:inprog_phone", str(phone))

    async def clear_phone_inprogress(self, campaign_id: str, phone: str):
        assert self._r is not None
        await self._r.srem(f"camp:{campaign_id}:inprog_phone", str(phone))

    async def is_phone_inprogress(self, campaign_id: str, phone: str) -> bool:
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

    async def claim_due_retries(self, campaign_id: str, limit: int = 10) -> List[str]:
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

    async def remove_retry(self, campaign_id: str, call_id: str):
        assert self._r is not None
        try:
            await self._r.zrem(f"camp:{campaign_id}:retry", call_id)
        except Exception:
            pass

    # ----- CALL REQUEST QUEUE (for Call Agent) -----
    async def send_call_request(self, call_request: Dict[str, Any]):
        """Gửi call request cho Call Agent"""
        assert self._r is not None
        await self._r.lpush("call_requests", json.dumps(call_request))
        logger.info(f"Sent call request: {call_request.get('callId')}")

    async def get_call_requests(self, timeout: int = 1) -> List[Dict[str, Any]]:
        """Lấy call requests từ queue (cho Call Agent)"""
        assert self._r is not None
        requests = []
        for _ in range(10):  # Lấy tối đa 10 requests
            result = await self._r.brpop("call_requests", timeout=timeout)
            if result is None:
                break
            try:
                request = json.loads(result[1])
                requests.append(request)
            except Exception as e:
                logger.error(f"Failed to parse call request: {e}")
        return requests

    # ----- CALLBACK HANDLING -----
    async def send_call_callback(self, callback_data: Dict[str, Any]):
        """Gửi callback từ Call Agent về Scheduler"""
        assert self._r is not None
        await self._r.lpush("call_callbacks", json.dumps(callback_data))
        logger.info(f"Sent callback: {callback_data.get('callId')}")

    async def get_call_callbacks(self, timeout: int = 1) -> List[Dict[str, Any]]:
        """Lấy callbacks từ queue (cho Scheduler)"""
        assert self._r is not None
        callbacks = []
        for _ in range(10):  # Lấy tối đa 10 callbacks
            result = await self._r.brpop("call_callbacks", timeout=timeout)
            if result is None:
                break
            try:
                callback = json.loads(result[1])
                callbacks.append(callback)
            except Exception as e:
                logger.error(f"Failed to parse callback: {e}")
        return callbacks