import logging
import json
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
from models.campaign import Campaign
from models.lead import Lead
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
            #Check start time, end time
            if not self._is_time_valid_safe(campaign.start_time, campaign.end_time):
                logger.info(f"[TIME] Skip campaign {getattr(campaign, 'name', campaign.id)}: outside start/end window")
                continue
            #Check time of day
            if not self.is_within_time_of_day(campaign, datetime.now()):
                logger.info(f"[TIME] Skip campaign {getattr(campaign, 'name', campaign.id)}: outside time-of-day window")
                continue
            active_campaigns.append(campaign)
        
        return active_campaigns

    @staticmethod
    def _is_time_valid_safe(start_time: datetime | None, end_time: datetime | None) -> bool:
        """Kiểm tra thời gian campaign có hợp lệ không (xử lý timezone UTC+7)."""
        def to_utc7_aware(dt: datetime | None) -> datetime | None:
            if dt is None:
                return None
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone(timedelta(hours=7)))
            return dt.astimezone(timezone(timedelta(hours=7)))

        # Current time in UTC+7
        now_utc7 = datetime.now(timezone(timedelta(hours=7)))
        start_utc7 = to_utc7_aware(start_time)
        end_utc7 = to_utc7_aware(end_time)
        start_ok = (start_utc7 is None) or (start_utc7 <= now_utc7)
        end_ok = (end_utc7 is None) or (end_utc7 > now_utc7)
        return start_ok and end_ok
    
    def create_call_message(self, call_id: str, campaign: Campaign, lead: Lead, 
                          is_retry: bool = False, original_call_id: str = None) -> Dict[str, Any]:
        """Tạo message cho cuộc gọi"""
        message = {
            "callId": call_id,
            "tenantId": str(campaign.tenant_id) if campaign.tenant_id else None,
            "campaignId": str(campaign.id) if campaign.id else None,
            "campaignCode": campaign.name,
            "scriptId": str(campaign.script_id) if campaign.script_id else None,
            "leadId": str(lead.id) if lead.id else None,
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

    def is_within_time_of_day(self, campaign: Campaign, now: datetime) -> bool:
        """Kiểm tra thời điểm now có nằm trong các khung giờ campaign cho phép không.

        time_of_day format: [{"fromHour":8,"fromMinute":8,"toHour":8,"toMinute":8}]
        - Nếu không có cấu hình hoặc cấu hình rỗng/không hợp lệ: cho phép (trả True)
        """
        windows = self._parse_time_windows(campaign.time_of_day)
        if not windows:
            return True

        # Convert to UTC+7 timezone for comparison
        if now.tzinfo is None:
            now_utc7 = now.replace(tzinfo=timezone(timedelta(hours=7)))
        else:
            now_utc7 = now.astimezone(timezone(timedelta(hours=7)))
        
        now_minutes = now_utc7.hour * 60 + now_utc7.minute

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
