import os
from dataclasses import dataclass

@dataclass
class Config:
    """Configuration model - quản lý cấu hình hệ thống"""
    # Database config
    def _build_db_url() -> str:
        # Ưu tiên DATABASE_URL nếu có
        db_url = os.getenv("DATABASE_URL")
        if db_url:
            return db_url
        # Hỗ trợ tự dựng từ POSTGRES_*
        host = os.getenv("POSTGRES_HOST")
        if host:
            user = os.getenv("POSTGRES_USER", "postgres")
            password = os.getenv("POSTGRES_PASSWORD", "")
            db = os.getenv("POSTGRES_DB", "postgres")
            port = os.getenv("POSTGRES_PORT", "5432")
            return f"postgresql://{user}:{password}@{host}:{port}/{db}"
        # Mặc định local
        return "postgresql://user:pass@localhost:5432/callbot"

    DATABASE_URL: str = _build_db_url()
    
    # Scheduler config
    CHECK_INTERVAL: int = int(os.getenv("CHECK_INTERVAL", "60"))  # seconds
    MAX_CONCURRENT_CAMPAIGNS: int = int(os.getenv("MAX_CONCURRENT_CAMPAIGNS", "10"))
    
    # Call scheduling config
    WORKING_HOURS_START: int = int(os.getenv("WORKING_HOURS_START", "7"))  # 7AM
    WORKING_HOURS_END: int = int(os.getenv("WORKING_HOURS_END", "21"))     # 9PM
    
    # Retry config
    DEFAULT_RETRY_INTERVAL: int = int(os.getenv("DEFAULT_RETRY_INTERVAL", "300"))  # 5 minutes
    MAX_RETRY_ATTEMPTS: int = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))
    