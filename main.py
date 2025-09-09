import asyncio
import logging
import signal
import sys
from datetime import datetime

from models.config import Config
from controllers.scheduler_controller import SchedulerController
from views.scheduler_view import SchedulerView

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SchedulerServer:
    """Main server"""
    
    def __init__(self):
        self.config = Config()
        self.scheduler_controller = SchedulerController(self.config)
        self.scheduler_view = SchedulerView(self.scheduler_controller)
        
        self.running = False
        
    async def start(self):
        # Display startup message
        self.scheduler_view.display_startup_message()
        
        # Đăng ký signal handlers để graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.running = True
        
        # Initialize controller
        await self.scheduler_controller.initialize()
        
        logger.info(f"🚀 Scheduler started - checking every {self.config.CHECK_INTERVAL} seconds")
        logger.info(f"📊 Max concurrent campaigns: {self.config.MAX_CONCURRENT_CAMPAIGNS}")
        
        # Main loop
        cycle_count = 0
        while self.running:
            try:
                cycle_count += 1
                self.scheduler_view.display_cycle_start(cycle_count)
                
                # Run scheduler cycle
                await self.scheduler_controller.run_cycle()
                
                # Display cycle statistics
                status = self.scheduler_controller.get_status()
                self.scheduler_view.display_cycle_stats(status)
                
                # Display controller status
                controller_stats = [
                    controller.get_status() 
                    for controller in self.scheduler_controller.active_controllers.values()
                ]
                self.scheduler_view.display_controller_status(controller_stats)
                
                await asyncio.sleep(self.config.CHECK_INTERVAL)
                
            except Exception as e:
                self.scheduler_view.display_error(str(e), "Main loop")
                await asyncio.sleep(5)  # Chờ 5s trước khi retry
                
    def _signal_handler(self, signum, frame):
        """Xử lý signal để shutdown gracefully"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
        
    async def stop(self):
        """Dừng server"""
        self.scheduler_view.display_shutdown()
        self.running = False
        await self.scheduler_controller.cleanup()
        logger.info("Scheduler server stopped")

def main():
    server = SchedulerServer()
    
    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        logger.info("Shutdown initiated by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()