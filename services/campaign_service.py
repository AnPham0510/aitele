import logging
import json
from datetime import datetime, timezone
from typing import List, Dict, Any
from models.campaign import Campaign
from models.lead import Lead
from models.call_session import CallSession
from models.config import Config

logger = logging.getLogger(__name__)

class CampaignService:
    """Service layer cho campaign business logic"""
    
    def __init__(self, config: Config):
        self.config = config
        
    def filter_active_campaigns(self, campaigns: List[Campaign]) -> List[Campaign]:
        """Lọc campaigns active trong thời gian campaign hoạt động"""
        active_campaigns = []
        
        for campaign in campaigns:
            if not campaign.is_active():
                continue
            # Safe time validity check (normalize timezone)
            if not self._is_time_valid_safe(campaign.start_time, campaign.end_time):
                continue
            # Time windows theo campaign.time_of_day (nếu có)
            if not self.is_within_time_of_day(campaign, datetime.now()):
                continue
            active_campaigns.append(campaign)
        
        return active_campaigns

    @staticmethod
    def _is_time_valid_safe(start_time: datetime | None, end_time: datetime | None) -> bool:
        def to_aware_utc(dt: datetime | None) -> datetime | None:
            if dt is None:
                return None
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)

        now_utc = datetime.now(timezone.utc)
        start_utc = to_aware_utc(start_time)
        end_utc = to_aware_utc(end_time)
        start_ok = (start_utc is None) or (start_utc <= now_utc)
        end_ok = (end_utc is None) or (end_utc > now_utc)
        return start_ok and end_ok
    
    def filter_stopped_campaigns(self, campaigns: List[Campaign]) -> List[Campaign]:
        """Lọc campaigns đã dừng hoặc tạm dừng"""
        stopped_campaigns = []
        
        for campaign in campaigns:
            if campaign.status == 'paused':
                stopped_campaigns.append(campaign)
                
        return stopped_campaigns

    
    def should_retry_call(self, call_session: CallSession, campaign: Campaign) -> bool:
        """Kiểm tra có nên retry cuộc gọi theo cấu hình campaign.
        - Số lần tối đa: campaign.max_callback (fallback: config.MAX_RETRY_ATTEMPTS)
        - Khoảng cách tối thiểu: campaign.max_call_time (giây) (fallback: config.DEFAULT_RETRY_INTERVAL)
        """
        max_attempts = campaign.max_callback if isinstance(campaign.max_callback, int) and campaign.max_callback >= 0 else self.config.MAX_RETRY_ATTEMPTS
        min_interval = campaign.max_call_time if isinstance(campaign.max_call_time, int) and campaign.max_call_time is not None else self.config.DEFAULT_RETRY_INTERVAL

        if not call_session.can_retry(max_attempts):
            return False

        if call_session.end_time:
            time_since_last_call = datetime.now() - call_session.end_time
            if time_since_last_call.total_seconds() < (min_interval or 0):
                return False

        return True
    
    def create_call_message(self, call_id: str, campaign: Campaign, lead: Lead, 
                          is_retry: bool = False, original_call_id: str = None) -> Dict[str, Any]:
        """Tạo message cho cuộc gọi"""
        message = {
            "callId": call_id,
            "tenantId": campaign.tenant_id,
            "campaignId": campaign.id,
            "campaignCode": campaign.name,
            "scriptId": campaign.script_id,
            "leadId": lead.id,
            "leadPhoneNumber": lead.phone_number,
            "leadName": lead.get_display_name(),
            "timestamp": datetime.now().isoformat()
        }
        
        if is_retry:
            message.update({
                "isRetry": True,
                "originalCallId": original_call_id
            })
            
        return message

    # --- Time of day helpers ---
    def is_within_time_of_day(self, campaign: Campaign, now: datetime) -> bool:
        """Kiểm tra thời điểm now có nằm trong các khung giờ campaign cho phép không.

        time_of_day format: [{"fromHour":8,"fromMinute":8,"toHour":8,"toMinute":8}]
        - Nếu không có cấu hình hoặc cấu hình rỗng/không hợp lệ: cho phép (trả True)
        - Hỗ trợ khung giờ qua đêm (from > to)
        """
        windows = self._parse_time_windows(campaign.time_of_day)
        if not windows:
            return True

        now_minutes = now.hour * 60 + now.minute

        for w in windows:
            start = w.get("fromHour", 0) * 60 + w.get("fromMinute", 0)
            end = w.get("toHour", 23) * 60 + w.get("toMinute", 59)

            if start == end:
                # Zero-length window -> skip
                continue

            if start < end:
                # Normal window: [start, end)
                if start <= now_minutes < end:
                    return True
            # else:
            #     # Overnight window: [start, 1440) or [0, end)
            #     if now_minutes >= start or now_minutes < end:
            #         return True

        return False

    def _parse_time_windows(self, time_of_day_field: Any) -> List[Dict[str, int]]:
        """Parse time_of_day JSON to list of windows with ints. Tolerant to bad data."""
        try:
            if not time_of_day_field:
                return []
            if isinstance(time_of_day_field, str):
                time_of_day = json.loads(time_of_day_field)
            else:
                time_of_day = time_of_day_field
            if not isinstance(time_of_day, list):
                return []
            result: List[Dict[str, int]] = []
            for item in time_of_day:
                if not isinstance(item, dict):
                    continue
                fh = int(item.get("fromHour", 0))
                fm = int(item.get("fromMinute", 0))
                th = int(item.get("toHour", 23))
                tm = int(item.get("toMinute", 59))
                fh = max(0, min(23, fh))
                th = max(0, min(23, th))
                fm = max(0, min(59, fm))
                tm = max(0, min(59, tm))
                result.append({
                    "fromHour": fh,
                    "fromMinute": fm,
                    "toHour": th,
                    "toMinute": tm,
                })
            return result
        except Exception:
            logger.warning("Invalid time_of_day format; ignoring windows")
            return []
