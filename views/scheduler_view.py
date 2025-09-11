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
        logger.info("Scheduler Server Starting")
        
    def display_cycle_start(self, cycle_number: int):
        """Hiển thị bắt đầu chu kỳ"""
        logger.info(f"Scheduler Cycle #{cycle_number}")
        logger.info("-" * 40)
        
    def display_cycle_stats(self, stats: Dict[str, Any]):
        """Hiển thị thống kê chu kỳ"""
        logger.info("Cycle Statistics:")
        logger.info(f"   • Active Controllers: {stats.get('active_controllers', 0)}")
        logger.info(f"   • Processed Campaigns: {stats.get('processed_campaigns', 0)}")
        logger.info(f"   • Check Interval: {stats.get('config', {}).get('check_interval', 60)}s")
        
    def display_error(self, error: str, context: str = ""):
        """Hiển thị lỗi"""
        logger.error(f"Error: {error}")
        if context:
            logger.error(f"   • Context: {context}")
            
    def display_shutdown(self):
        """Hiển thị thông báo shutdown"""
        logger.info(" Scheduler Server Shutdown")
        
    def display_controller_status(self, controller_stats: list):
        """Hiển thị trạng thái các controllers"""
        if controller_stats:
            logger.info("Active Controllers:")
            for stat in controller_stats:
                logger.info(f"   • {stat['campaign_code']}: {stat['processed_leads']} leads processed")
        else:
            logger.info("No active controllers")