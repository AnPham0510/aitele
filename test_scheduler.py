"""
Test script để kiểm tra logic của Scheduler Server
"""
import asyncio
import logging
from datetime import datetime, timedelta
from models.config import Config
from controllers.scheduler_controller import SchedulerController

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_scheduler_logic():
    """Test logic của scheduler"""
    logger.info("🧪 Testing Scheduler Logic...")
    
    try:
        # Tạo config
        config = Config()
        logger.info(f"Config loaded: CHECK_INTERVAL={config.CHECK_INTERVAL}s")
        
        # Tạo scheduler controller
        scheduler = SchedulerController(config)
        
        # Initialize
        await scheduler.initialize()
        logger.info("✅ Scheduler initialized successfully")
        
        # Test một cycle
        logger.info("🔄 Running test cycle...")
        await scheduler.run_cycle()
        
        # Check status
        status = scheduler.get_status()
        logger.info(f"📊 Scheduler status: {status}")
        
        # Cleanup
        await scheduler.cleanup()
        logger.info("✅ Scheduler cleaned up successfully")
        
        logger.info("🎉 All tests passed!")
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(test_scheduler_logic())
