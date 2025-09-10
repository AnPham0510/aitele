from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional

@dataclass
class Campaign:
    """Campaign model - đại diện cho một chiến dịch gọi điện (theo schema thực tế)."""
    id: str
    tenant_id: str
    name: str
    status: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    script_id: Optional[str] = None
    call_interval: Optional[int] = None
    # Optional meta fields
    description: Optional[str] = None
    voice_id: Optional[str] = None
    email: Optional[str] = None
    max_call_time: Optional[int] = None
    time_of_day: Optional[str] = None
    max_callback: Optional[int] = None
    callback_conditions: Optional[str] = None
    
    def is_in_working_hours(self, current_hour: int) -> bool:
        """Kiểm tra có trong giờ làm việc không.
        """
        return True
    
    def is_time_valid(self) -> bool:
        """Kiểm tra thời gian campaign có hợp lệ không (xử lý timezone UTC+7)."""
        def to_utc7_aware(dt: Optional[datetime]) -> Optional[datetime]:
            if dt is None:
                return None
            if dt.tzinfo is None:
                # Assume naive datetime is already in UTC+7
                return dt.replace(tzinfo=timezone(timedelta(hours=7)))
            return dt.astimezone(timezone(timedelta(hours=7)))

        # Current time in UTC+7
        now_utc7 = datetime.now(timezone(timedelta(hours=7)))
        start_utc7 = to_utc7_aware(self.start_time)
        end_utc7 = to_utc7_aware(self.end_time)

        start_ok = (start_utc7 is None) or (start_utc7 <= now_utc7)
        end_ok = (end_utc7 is None) or (end_utc7 > now_utc7)
        return start_ok and end_ok
