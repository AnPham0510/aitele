import logging
from datetime import datetime
from typing import Dict, Any
from controllers.scheduler_controller import SchedulerController

logger = logging.getLogger(__name__)

class SchedulerView:
    """View layer cho scheduler - xử lý hiển thị và logging"""
    
    def __init__(self, scheduler_controller: SchedulerController):
        self.scheduler_controller = scheduler_controller
        
    def display_startup_message(self):
        """Hiển thị thông báo khởi động"""
        logger.info("=" * 60)
        logger.info("🚀 AI Callbot Scheduler Server")
        logger.info("=" * 60)
        
    def display_cycle_start(self, cycle_number: int):
        """Hiển thị bắt đầu chu kỳ"""
        logger.info(f"🔄 Scheduler Cycle #{cycle_number}")
        logger.info("-" * 40)
        
    def display_cycle_stats(self, stats: Dict[str, Any]):
        """Hiển thị thống kê chu kỳ"""
        logger.info("📊 Cycle Statistics:")
        logger.info(f"   • Active Controllers: {stats.get('active_controllers', 0)}")
        logger.info(f"   • Processed Campaigns: {stats.get('processed_campaigns', 0)}")
        logger.info(f"   • Check Interval: {stats.get('config', {}).get('check_interval', 60)}s")
        
    def display_campaign_start(self, campaign_code: str, leads_count: int):
        """Hiển thị bắt đầu campaign"""
        logger.info(f"🎯 Starting Campaign: {campaign_code}")
        logger.info(f"   • Leads to process: {leads_count}")
        
    def display_campaign_stop(self, campaign_code: str, status: str):
        """Hiển thị dừng campaign"""
        logger.info(f"⏹️  Stopping Campaign: {campaign_code}")
        logger.info(f"   • Status: {status}")
        
    def display_call_created(self, call_id: str, phone_number: str, campaign_code: str):
        """Hiển thị tạo cuộc gọi"""
        logger.info(f"📞 Call Created: {call_id}")
        logger.info(f"   • Phone: {phone_number}")
        logger.info(f"   • Campaign: {campaign_code}")
        
    def display_call_retry(self, original_id: str, new_id: str, attempt: int):
        """Hiển thị retry cuộc gọi"""
        logger.info(f"🔄 Call Retry: {original_id} -> {new_id}")
        logger.info(f"   • Attempt: {attempt}")
        
    def display_error(self, error: str, context: str = ""):
        """Hiển thị lỗi"""
        logger.error(f"❌ Error: {error}")
        if context:
            logger.error(f"   • Context: {context}")
            
    def display_shutdown(self):
        """Hiển thị thông báo shutdown"""
        logger.info("=" * 60)
        logger.info("🛑 Scheduler Server Shutdown")
        logger.info("=" * 60)
        
    def display_controller_status(self, controller_stats: list):
        """Hiển thị trạng thái các controllers"""
        if controller_stats:
            logger.info("🎮 Active Controllers:")
            for stat in controller_stats:
                logger.info(f"   • {stat['campaign_code']}: {stat['processed_leads']} leads processed")
        else:
            logger.info("🎮 No active controllers")
            
    def format_message(self, level: str, message: str, **kwargs) -> str:
        """Format message với timestamp và level"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted = f"[{timestamp}] {level.upper()}: {message}"
        
        if kwargs:
            details = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
            formatted += f" | {details}"
            
        return formatted
