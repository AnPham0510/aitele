from dataclasses import dataclass
from typing import Optional

@dataclass
class Lead:
    """Bảng public.customers."""
    id: str
    phone_number: str
    name: Optional[str] = None
    tenant_id: Optional[str] = None
    campaign_id: Optional[str] = None
    
    def get_display_name(self) -> str:
        return self.name or f"Lead {self.phone_number}"
