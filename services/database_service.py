import asyncpg
import logging
from datetime import datetime, timedelta
from typing import List, Optional
from models.campaign import Campaign
from models.lead import Lead
from models.call_session import CallSession

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
            
        # Chuẩn hóa kiểu dữ liệu để tránh lệch kiểu (UUID vs str) gây lỗi rate-limit
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
    
    async def get_retry_calls_for_campaign(self, campaign_id: str, retry_interval: int) -> List[CallSession]:
        logger.info("Retry calls disabled (call_sessions not in use)")
        return []
    
    async def create_call_session(self, call_id: str, campaign: Campaign, lead: Lead, retry_count: int):
        logger.info(f"[NO-CS] create_call_session: call_id={call_id} lead={lead.phone_number} campaign={campaign.name}")
            
    async def update_call_retry_count(self, call_session_id: str, retry_count: int):
        logger.info(f"[NO-CS] update_call_retry_count: id={call_session_id} retry={retry_count}")
