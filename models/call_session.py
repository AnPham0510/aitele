from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class CallSession:
    """CallSession model - đại diện cho một phiên gọi điện"""
    id: str
    tenant_id: str
    tenant_code: str
    campaign_id: str
    script_id: str
    lead_id: str
    lead_phone_number: str
    call_status: str
    disposition: Optional[str] = None
    retry_count: int = 0
    end_time: Optional[datetime] = None
    start_time: Optional[datetime] = None
    
    def is_retryable(self) -> bool:
        """Kiểm tra có thể retry cuộc gọi không"""
        return self.call_status in ['FAILED', 'NO_ANSWER', 'BUSY']
    
    def can_retry(self, max_retries: int = 3) -> bool:
        """Kiểm tra có thể retry dựa trên số lần retry"""
        return self.retry_count < max_retries and self.is_retryable()
    
    def get_duration(self) -> Optional[float]:
        """Tính thời lượng cuộc gọi (giây)"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
