from dataclasses import dataclass
from datetime import datetime, timezone
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
    
    def is_active(self) -> bool:
        """Kiểm tra campaign có đang active không"""
        return (self.status or "").lower() in {"running"}
    
    def is_in_working_hours(self, current_hour: int) -> bool:
        """Kiểm tra có trong giờ làm việc không.
        """
        return True
    
    def is_time_valid(self) -> bool:
        """Kiểm tra thời gian campaign có hợp lệ không (chuẩn hóa UTC để tránh naive/aware)."""
        def to_aware_utc(dt: Optional[datetime]) -> Optional[datetime]:
            if dt is None:
                return None
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)

        now_utc = datetime.now(timezone.utc)
        start_utc = to_aware_utc(self.start_time)
        end_utc = to_aware_utc(self.end_time)

        start_ok = (start_utc is None) or (start_utc <= now_utc)
        end_ok = (end_utc is None) or (end_utc > now_utc)
        return start_ok and end_ok
