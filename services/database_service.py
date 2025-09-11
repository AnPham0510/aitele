import asyncpg
import logging
from datetime import datetime, timedelta
from typing import List, Optional
from models.campaign import Campaign
from models.lead import Lead

logger = logging.getLogger(__name__)

class DatabaseService:
    """Service layer cho database operations"""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool = None
        
    async def connect(self):
        """Kết nối database"""
        self.pool = await asyncpg.create_pool(self.database_url)
        logger.info("Connected to database")
        
    async def disconnect(self):
        """Ngắt kết nối database"""
        if self.pool:
            await self.pool.close()
            logger.info("Disconnected from database")
            
    async def get_running_campaigns(self) -> List[Campaign]:
        """Lấy danh sách campaigns có status running"""
        query = """
        SELECT c.id, c.tenant_id, c.name, c.status, c.start_time, c.end_time, c.script_id, c.call_interval,
               c.description, c.voice_id, c.email, c.max_call_time, c.time_of_day, c.max_callback, c.callback_conditions
        FROM public.campaigns c
        WHERE c.status = 'running'
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)
            
        return [Campaign(**dict(row)) for row in rows]
    
    async def get_stopped_campaigns(self) -> List[Campaign]:
        """Lấy danh sách campaigns đã dừng hoặc tạm dừng"""
        query = """
        SELECT c.id, c.tenant_id, c.name, c.status, c.start_time, c.end_time, c.script_id, c.call_interval,
               c.description, c.voice_id, c.email, c.max_call_time, c.time_of_day, c.max_callback, c.callback_conditions
        FROM public.campaigns c
        WHERE c.status IN ('paused', 'ended')
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)
            
        return [Campaign(**dict(row)) for row in rows]
    
    async def get_pending_leads_for_campaign(self, campaign_id: str) -> List[Lead]:
        """Lấy danh sách leads chưa được gọi cho campaign"""
        query = """
        SELECT c.id, c.phone_number, c.name, c.tenant_id, c.campaign_id
        FROM public.customers c
        WHERE c.campaign_id = $1
        ORDER BY c.created_at
        LIMIT 50
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, campaign_id)
        leads: List[Lead] = []
        for row in rows:
            data = dict(row)
            leads.append(
                Lead(
                    id=str(data.get("id")),
                    phone_number=str(data.get("phone_number")),
                    name=data.get("name"),
                    tenant_id=str(data.get("tenant_id")) if data.get("tenant_id") is not None else None,
                    campaign_id=str(data.get("campaign_id")) if data.get("campaign_id") is not None else None,
                )
            )
        return leads
    
    async def get_campaign_by_id(self, campaign_id: str) -> Optional[Campaign]:
        """Lấy thông tin campaign theo id"""
        query = """
        SELECT c.id, c.tenant_id, c.name, c.status, c.start_time, c.end_time, c.script_id, c.call_interval,
               c.description, c.voice_id, c.email, c.max_call_time, c.time_of_day, c.max_callback, c.callback_conditions
        FROM public.campaigns c
        WHERE c.id = $1
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, campaign_id)
        if row is None:
            return None
        return Campaign(**dict(row))