import asyncio
import logging
from datetime import datetime
import uuid
from models.campaign import Campaign
from models.config import Config
from services.database_service import DatabaseService
from typing import Optional
from services.campaign_service import CampaignService

logger = logging.getLogger(__name__)

class CampaignController:
    """Controller cho xử lý campaign - quản lý logic nghiệp vụ của một campaign"""
    
    def __init__(self, campaign: Campaign, db_service: DatabaseService, campaign_service: CampaignService, 
                 config: Config):
        self.campaign = campaign
        self.db_service = db_service
        self.campaign_service = campaign_service
        self.config = config
        
        self.is_running = False
        self.is_stopped = False
        self._finished = False
        
        # Rate limiting
        self.last_call_time = {}  # lead_id -> timestamp
        self.processed_leads = 0
        # Per-campaign pacing: last call at campaign level
        self.last_campaign_call_at: Optional[datetime] = None
        
    async def start(self):
        """Bắt đầu controller cho campaign"""
        self.is_running = True
        logger.info(f"Campaign controller started for {self.campaign.name}")
        
        while self.is_running and not self.is_stopped:
            try:
                # Nếu ngoài khung giờ campaign -> chờ ngắn rồi kiểm tra lại
                if not self.campaign_service.is_within_time_of_day(self.campaign, datetime.now()):
                    await asyncio.sleep(30)
                    continue

                interval = getattr(self.campaign, "call_interval", None)
                if isinstance(interval, int) and interval > 0 and self.last_campaign_call_at is not None:
                    elapsed = (datetime.now() - self.last_campaign_call_at).total_seconds()
                    if elapsed < interval:
                        await asyncio.sleep(max(0.5, interval - elapsed))
                        continue

                # Thực hiện 1 lần xử lý: tạo 1 cuộc gọi nếu có lead phù hợp
                made_call = await self._process_campaign_once()

                if made_call:
                    # Nếu vừa tạo cuộc gọi, cập nhật mốc thời gian gọi cuối
                    self.last_campaign_call_at = datetime.now()
                    # Nếu có interval, vòng lặp trên sẽ tự chờ đủ interval ở iteration kế tiếp
                    continue
                else:
                    # Không có lead phù hợp -> chờ ngắn
                    await asyncio.sleep(5)
                    continue

            except Exception as e:
                logger.error(f"Error in campaign controller {self.campaign.name}: {e}")
                await asyncio.sleep(10)
                
        self._finished = True
        logger.info(f"Campaign controller finished for {self.campaign.name}")
        
    async def stop(self):
        """Dừng controller"""
        self.is_stopped = True
        self.is_running = False
        
    def is_finished(self) -> bool:
        """Kiểm tra controller đã hoàn thành chưa"""
        return self._finished
        
    async def _process_campaign_once(self) -> bool:
        """Thực hiện một lần xử lý: cố gắng tạo 1 cuộc gọi (new hoặc retry).
        Trả về True nếu đã tạo cuộc gọi, False nếu không có gì để làm.
        """
        retry_calls = await self.db_service.get_retry_calls_for_campaign(
            self.campaign.id,
            self.config.DEFAULT_RETRY_INTERVAL
        )
        for call_session in retry_calls:
            if self.campaign_service.should_retry_call(call_session, self.campaign):
                await self._retry_call(call_session)
                return True

        # Nếu không có retry phù hợp, thử new leads
        pending_leads = await self.db_service.get_pending_leads_for_campaign(self.campaign.id)
        for lead in pending_leads:
            if self._should_make_call(lead):
                await self._create_call(lead)
                return True

        return False
                
    def _should_make_call(self, lead) -> bool:
        """Kiểm tra có nên tạo cuộc gọi không"""
            
        now = datetime.now()
        
        # Respect campaign time windows
        if not self.campaign_service.is_within_time_of_day(self.campaign, now):
            return False
            
        # Rate limiting - không gọi quá nhanh cho cùng 1 lead
        if lead.id in self.last_call_time:
            time_diff = now - self.last_call_time[lead.id]
            if time_diff.total_seconds() < 60:  # Ít nhất 1 phút giữa các cuộc gọi
                return False
                
        return True

    # Bỏ cơ chế break theo interval; pacing được xử lý trong vòng lặp start()
        
    async def _create_call(self, lead):
        """Tạo cuộc gọi mới"""
        call_id = str(uuid.uuid4())
        
        message = self.campaign_service.create_call_message(call_id, self.campaign, lead)

        # Cập nhật database
        await self.db_service.create_call_session(
            call_id=call_id,
            campaign=self.campaign,
            lead=lead,
            retry_count=0
        )
        
        # Cập nhật thời gian gọi cuối
        self.last_call_time[lead.id] = datetime.now()
        self.processed_leads += 1
        
        logger.info(f"Created call {call_id} for lead {lead.phone_number}")
        
    async def _retry_call(self, call_session):
        """Retry cuộc gọi"""
        new_call_id = str(uuid.uuid4())
        
        # Lấy lead info từ call_session
        lead = type('Lead', (), {
            'id': call_session.lead_id,
            'phone_number': call_session.lead_phone_number,
            'get_display_name': lambda: f"Lead {call_session.lead_phone_number}"
        })()
        
        message = self.campaign_service.create_call_message(
            new_call_id, 
            self.campaign, 
            lead, 
            is_retry=True, 
            original_call_id=call_session.id
        )
        
        # Cập nhật retry count
        await self.db_service.update_call_retry_count(
            call_session.id, 
            call_session.retry_count + 1
        )
        
        logger.info(f"Retried call {call_session.id} -> {new_call_id} (attempt {call_session.retry_count + 1})")
        
    def get_status(self) -> dict:
        """Lấy trạng thái của campaign controller"""
        return {
            "campaign_id": self.campaign.id,
            "campaign_code": self.campaign.name,
            "is_running": self.is_running,
            "is_stopped": self.is_stopped,
            "is_finished": self._finished,
            "processed_leads": self.processed_leads,
            "last_call_times": len(self.last_call_time)
        }
