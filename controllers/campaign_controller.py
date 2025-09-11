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
        
        self.last_call_time = {}
        self.processed_leads = 0
        self.last_campaign_call_at: Optional[datetime] = None
        self.redis = None        
    async def start(self):
        """Bắt đầu controller cho campaign"""
        self.is_running = True
        logger.info(f"Campaign controller started for {self.campaign.name}")
        
        while self.is_running and not self.is_stopped:
            try:
                try:
                    latest = await self.db_service.get_campaign_by_id(str(self.campaign.id))
                    if latest is None:
                        logger.info(f"Campaign {self.campaign.name} not found, stopping controller")
                        break
                    self.campaign = latest
                except Exception:
                    pass

                # Nếu ngoài khung giờ campaign -> kết thúc controller để scheduler dọn dẹp và sẽ khởi tạo lại khi vào khung giờ
                if not self.campaign_service.is_within_time_of_day(self.campaign, datetime.now()):
                    logger.info(f"Campaign {self.campaign.name} outside time window, stopping controller")
                    break

                interval = getattr(self.campaign, "call_interval", None)
                if isinstance(interval, int) and interval > 0 and self.last_campaign_call_at is not None:
                    elapsed = (datetime.now() - self.last_campaign_call_at).total_seconds()
                    if elapsed < interval:
                        wait_time = max(0.5, interval - elapsed)
                        logger.debug(f"Campaign {self.campaign.name} waiting {wait_time:.1f}s for next call")
                        await asyncio.sleep(wait_time)
                        continue

                # Thực hiện 1 lần xử lý: tạo 1 cuộc gọi nếu có lead phù hợp
                made_call = await self._process_campaign_once()

                if made_call:
                    # Nếu vừa tạo cuộc gọi, cập nhật mốc thời gian gọi cuối
                    self.last_campaign_call_at = datetime.now()
                    logger.debug(f"Campaign {self.campaign.name} made a call, waiting for next interval")
                    continue
                else:
                    logger.debug(f"Campaign {self.campaign.name} no leads to process, waiting...")
                    await asyncio.sleep(5)
                    continue

            except Exception as e:
                logger.error(f"Error in campaign controller {self.campaign.name}: {e}", exc_info=True)
                await asyncio.sleep(10)
                
        self._finished = True
        logger.info(f"Campaign controller finished for {self.campaign.name}")
        
    async def stop(self):
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
            due_ids = await self.redis.claim_due_retries(str(self.campaign.id), limit=10)
            for call_id in due_ids:
                payload = await self.redis.get_call_payload(call_id)
                lead_id = str(payload.get("lead_id"))
                phone   = payload["phone"]
                attempt = int(payload.get("attempt", 0))
                max_attempts = int(payload.get("max_attempts", 3))
                retry_interval_s = int(payload.get("retry_interval_s", 300))
                try:
                    if await self.redis.is_lead_success(str(self.campaign.id), str(lead_id)):
                        try:
                            await self.redis.save_success_and_finalize(call_id)
                            await self.redis.remove_retry(str(self.campaign.id), call_id)
                        except Exception:
                            pass
                        logger.info(f"[RETRY-SKIP] lead {lead_id} already SUCCESS, cleaned {call_id}")
                        continue
                except Exception:
                    pass

                try:
                    if await self.redis.is_phone_success(str(self.campaign.id), phone):
                        try:
                            await self.redis.save_success_and_finalize(call_id)
                            await self.redis.remove_retry(str(self.campaign.id), call_id)
                        except Exception:
                            pass
                        logger.info(f"[RETRY-SKIP] phone {phone} already SUCCESS, cleaned {call_id}")
                        continue
                except Exception:
                    pass

                # Gửi retry request cho Call Agent
                retry_request = {
                    "callId": call_id,
                    "tenantId": str(self.campaign.tenant_id) if self.campaign.tenant_id else None,
                    "campaignId": str(self.campaign.id) if self.campaign.id else None,
                    "campaignCode": self.campaign.name,
                    "scriptId": str(self.campaign.script_id) if self.campaign.script_id else None,
                    "leadId": str(lead_id) if lead_id else None,
                    "leadPhoneNumber": phone,
                    "isRetry": True,
                    "attempt": attempt,
                    "maxAttempts": max_attempts,
                    "timestamp": datetime.now().isoformat()
                }
                
                if self.redis is not None:
                    await self.redis.send_call_request(retry_request)
                    logger.info(f"Sent retry request {call_id} for lead {phone} to Call Agent")
                    # Đánh dấu in-progress cho lead/phone khi gửi retry để tránh gửi trùng
                    try:
                        await self.redis.mark_inprogress(str(self.campaign.id), str(lead_id))
                        await self.redis.mark_phone_inprogress(str(self.campaign.id), phone)
                        logger.info(f"[INPROG] (retry) mark lead {lead_id} and phone {phone} as in-progress")
                    except Exception:
                        pass
                else:
                    logger.warning(f"Redis not available, cannot send retry request for {call_id}")

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
            # Nếu lead đã thành công -> bỏ qua
            done = await self.redis.is_lead_success(str(self.campaign.id), str(lead.id))
            if done:
                logger.info(f"[SKIP] lead {lead.id} already SUCCESS in Redis")
                return False
            try:
                if await self.redis.is_phone_success(str(self.campaign.id), lead.phone_number):
                    logger.info(f"[SKIP] phone {lead.phone_number} already SUCCESS in Redis")
                    return False
            except Exception:
                pass
            # Nếu lead đang chờ kết quả (đã gửi message đi) -> bỏ qua
            inprog = await self.redis.is_inprogress(str(self.campaign.id), str(lead.id))
            if inprog:
                return False
            try:
                if await self.redis.is_phone_inprogress(str(self.campaign.id), lead.phone_number):
                    return False
            except Exception:
                pass
        if not self.campaign_service.is_within_time_of_day(self.campaign, now):
            logger.info(f"[SKIP] lead {lead.id} outside time window")
            return False
            
        if lead.id in self.last_call_time:
            time_diff = now - self.last_call_time[lead.id]
            if time_diff.total_seconds() < 60:
                logger.info(f"[SKIP] lead {lead.id} rate-limited {time_diff.total_seconds():.1f}s < 60s")
                return False
                
        return True
        
    async def _create_call(self, lead):
        """Tạo cuộc gọi mới - gửi request cho Call Agent"""
        call_id = str(uuid.uuid4())
        
        # Tạo call request message
        call_request = self.campaign_service.create_call_message(call_id, self.campaign, lead)
        
        # Gửi call request cho Call Agent qua Redis
        if self.redis is not None:
            await self.redis.send_call_request(call_request)
            logger.info(f"Sent call request {call_id} for lead {lead.phone_number} to Call Agent")
        else:
            logger.warning(f"Redis not available, cannot send call request for {call_id}")
        
        # Cập nhật thời gian gọi cuối
        self.last_call_time[lead.id] = datetime.now()
        self.processed_leads += 1

        # Đánh dấu lead đang xử lý để tránh gửi trùng trước khi có callback kết quả
        try:
            if self.redis is not None:
                await self.redis.mark_inprogress(str(self.campaign.id), str(lead.id))
                await self.redis.mark_phone_inprogress(str(self.campaign.id), lead.phone_number)
                logger.info(f"[INPROG] mark lead {lead.id} and phone {lead.phone_number} as in-progress")
        except Exception:
            pass
        
        logger.info(f"Created call request {call_id} for lead {lead.phone_number}")
        
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