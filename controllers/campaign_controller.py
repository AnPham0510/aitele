import asyncio
import logging
from datetime import datetime
import uuid
from models.campaign import Campaign
from models.config import Config
from services.database_service import DatabaseService
from typing import Optional
from services.campaign_service import CampaignService
import uuid, time
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
        self.redis = None
        # Local guard to prevent duplicate sends even if Redis check fails
        self._inprogress_local: set[str] = set()
        
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
        if self.redis is not None:
            logger.info("run redis")
            due_ids = await self.redis.claim_due_retries(self.campaign.id, limit=10)
            for call_id in due_ids:
                payload = await self.redis.get_call_payload(call_id)
                # lead_id có thể là UUID/string -> giữ nguyên dạng chuỗi để tương thích Redis
                lead_id = str(payload.get("lead_id"))
                phone   = payload["phone"]
                attempt = int(payload.get("attempt", 0))
                max_attempts = int(payload.get("max_attempts", 3))
                retry_interval_s = int(payload.get("retry_interval_s", 300))

                outcome = await self._dial(phone)
                if outcome == "SUCCESS":
                    await self.redis.mark_lead_success(self.campaign.id, lead_id)
                    await self.redis.save_success_and_finalize(call_id)
                else:
                    if attempt + 1 < max_attempts:
                        payload["attempt"] = attempt + 1
                        await self.redis.save_failure_and_schedule_retry(
                            self.campaign.id, call_id, payload, delay_seconds=retry_interval_s
                        )
                    else:
                        # hết lượt -> bỏ, không gọi nữa
                        await self.redis.save_success_and_finalize(call_id)

        # Nếu không có retry phù hợp, thử new leads
        pending_leads = await self.db_service.get_pending_leads_for_campaign(self.campaign.id)
        for lead in pending_leads:
            if await self._should_make_call(lead):
                await self._create_call(lead)
                return True

        return False
                
    async  def _should_make_call(self, lead) -> bool:
        """Kiểm tra có nên tạo cuộc gọi không"""
            
        now = datetime.now()
        if self.redis is not None:
            # Nếu lead đã thành công -> bỏ
            done = await self.redis.is_lead_success(self.campaign.id, lead.id)
            if done:
                logger.info(f"[SKIP] lead {lead.id} already SUCCESS in Redis")
                return False
            # Dedup theo phone
            try:
                if await self.redis.is_phone_success(self.campaign.id, lead.phone_number):
                    logger.info(f"[SKIP] phone {lead.phone_number} already SUCCESS in Redis")
                    return False
            except Exception:
                pass
            # Nếu lead đang chờ kết quả (đã gửi message đi) -> bỏ
            inprog = await self.redis.is_inprogress(self.campaign.id, lead.id)
            if inprog:
                return False
            try:
                if await self.redis.is_phone_inprogress(self.campaign.id, lead.phone_number):
                    return False
            except Exception:
                pass
        # Local in-progress fallback
        if str(lead.id) in self._inprogress_local:
            logger.info(f"[SKIP] lead {lead.id} IN-PROGRESS (local cache)")
            return False
        # Respect campaign time windows
        if not self.campaign_service.is_within_time_of_day(self.campaign, now):
            logger.info(f"[SKIP] lead {lead.id} outside time window")
            return False
            
        # Rate limiting - không gọi quá nhanh cho cùng 1 lead
        if lead.id in self.last_call_time:
            time_diff = now - self.last_call_time[lead.id]
            if time_diff.total_seconds() < 60:  # Ít nhất 1 phút giữa các cuộc gọi
                logger.info(f"[SKIP] lead {lead.id} rate-limited {time_diff.total_seconds():.1f}s < 60s")
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
        # Đánh dấu lead đang xử lý để tránh gửi trùng trước khi có callback kết quả
        try:
            if self.redis is not None:
                await self.redis.mark_inprogress(self.campaign.id, lead.id)
                await self.redis.mark_phone_inprogress(self.campaign.id, lead.phone_number)
                logger.info(f"[INPROG] mark lead {lead.id} and phone {lead.phone_number} as in-progress (campaign)")
            # Always mark locally
            self._inprogress_local.add(str(lead.id))
        except Exception:
            self._inprogress_local.add(str(lead.id))
        
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
    def attach_redis(self, redis_service):
        self.redis = redis_service

    async def _call_lead_once(self, lead):
        call_id = str(uuid.uuid4())
        phone   = lead.phone_number
        outcome = await self._dial(phone)         # 'SUCCESS' | 'FAILED' | 'NO_ANSWER' | 'BUSY'
        max_attempts     = self.campaign.max_callback or 3
        retry_interval_s = self.campaign.max_call_time or 300

        if self.redis is None:
            # Nếu chưa attach redis, fallback không retry
            return

        if outcome == "SUCCESS":
            # Đánh dấu lead đã xong trong chiến dịch -> lần sau bỏ qua
            await self.redis.mark_lead_success(self.campaign.id, lead.id)
            # (không cần lưu call hash nữa)
            return

        payload = {
            "campaign_id": self.campaign.id,
            "lead_id": lead.id,
            "phone": phone,
            "attempt": 0,
            "max_attempts": max_attempts,
            "retry_interval_s": retry_interval_s,
            "call_id": call_id,
            "last_outcome": outcome,
        }
        await self.redis.save_failure_and_schedule_retry(
            self.campaign.id, call_id, payload, delay_seconds=retry_interval_s
        )

    async def _dial(self, phone: str) -> str:
        """Stub gọi ra hệ thống gọi điện.
        Trả về một trong các trạng thái: 'SUCCESS' | 'FAILED' | 'NO_ANSWER' | 'BUSY'.
        Hiện tại để an toàn, mặc định trả 'FAILED' để kích hoạt cơ chế retry.
        Tích hợp thật: thay bằng call HTTP/gRPC tới hệ thống gọi và map kết quả.
        """
        try:
            await asyncio.sleep(0)  # yield control
            return "FAILED"
        except Exception:
            return "FAILED"