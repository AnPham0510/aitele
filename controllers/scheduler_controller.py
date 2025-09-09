import asyncio
import threading
import logging
from typing import Dict, Set
from models.config import Config
from services.database_service import DatabaseService
from services.campaign_service import CampaignService
from controllers.campaign_controller import CampaignController

logger = logging.getLogger(__name__)

class SchedulerController:
    """Controller chính cho scheduler - điều phối toàn bộ hệ thống"""
    
    def __init__(self, config: Config):
        self.config = config
        self.db_service = DatabaseService(config.DATABASE_URL)
        self.campaign_service = CampaignService(config)
        
        # Track active campaign controllers
        self.active_controllers: Dict[str, CampaignController] = {}
        self.processed_campaigns: Set[str] = set()
        # Threading resources per campaign
        self.controller_threads: Dict[str, threading.Thread] = {}
        self.controller_loops: Dict[str, asyncio.AbstractEventLoop] = {}
        self.controller_db_services: Dict[str, DatabaseService] = {}
        
    async def initialize(self):
        """Khởi tạo các services"""
        await self.db_service.connect()
        logger.info("Scheduler controller initialized")
        
    async def cleanup(self):
        """Dọn dẹp resources"""
        # Stop all campaign controllers
        for controller in self.active_controllers.values():
            await controller.stop()
            
        await self.db_service.disconnect()
        logger.info("Scheduler controller cleaned up")
        
    async def run_cycle(self):
        """Chạy một chu kỳ kiểm tra và xử lý"""
        logger.debug("Starting scheduler cycle...")
        
        try:
            # 1. Kiểm tra campaigns active cần xử lý
            await self._process_active_campaigns()
            
            # 2. Kiểm tra campaigns cần dừng/tạm dừng
            await self._process_stopped_campaigns()
            
            # 3. Cleanup finished controllers
            await self._cleanup_finished_controllers()
            
        except Exception as e:
            logger.error(f"Error in scheduler cycle: {e}")
            
    async def _process_active_campaigns(self):
        """Xử lý các campaigns đang active"""
        # Lấy danh sách campaigns từ database
        campaigns = await self.db_service.get_active_campaigns_in_working_hours()
        logger.info(f"[DEBUG] fetched_active_campaigns={len(campaigns)}")
        
        # Filter campaigns active (no global working hours)
        active_campaigns = self.campaign_service.filter_active_campaigns(campaigns)
        logger.info(f"[DEBUG] filtered_active_campaigns={len(active_campaigns)}")
        
        for campaign in active_campaigns:
            # Kiểm tra xem campaign đã có controller chưa
            if campaign.id not in self.active_controllers:
                # Kiểm tra có leads cần gọi không
                pending_leads = await self.db_service.get_pending_leads_for_campaign(campaign.id)
                logger.info(f"[DEBUG] campaign_id={campaign.id} name={campaign.name} pending_leads={len(pending_leads)}")
                
                if pending_leads:
                    logger.info(f"Starting controller for campaign {campaign.name} with {len(pending_leads)} leads")

                    # Create per-campaign db service (isolated pool per thread)
                    per_db = DatabaseService(self.config.DATABASE_URL)

                    # Create controller object (will be run in its own event loop in a thread)
                    controller = CampaignController(
                        campaign=campaign,
                        db_service=per_db,
                        campaign_service=self.campaign_service,
                        config=self.config
                    )

                    self.active_controllers[campaign.id] = controller
                    self.controller_db_services[campaign.id] = per_db

                    # Start controller in a dedicated thread
                    t = threading.Thread(target=self._run_controller_thread, args=(campaign.id,), daemon=True)
                    self.controller_threads[campaign.id] = t
                    t.start()
                    
    async def _process_stopped_campaigns(self):
        """Xử lý các campaigns cần dừng hoặc tạm dừng"""
        # Lấy campaigns đã ended hoặc paused
        campaigns = await self.db_service.get_stopped_campaigns()
        stopped_campaigns = self.campaign_service.filter_stopped_campaigns(campaigns)
        
        for campaign in stopped_campaigns:
            if campaign.id in self.active_controllers:
                logger.info(f"Stopping controller for campaign {campaign.name} (status: {campaign.status})")   

                # Stop controller via its own loop
                controller = self.active_controllers[campaign.id]
                loop = self.controller_loops.get(campaign.id)
                if loop is not None:
                    try:
                        fut = asyncio.run_coroutine_threadsafe(controller.stop(), loop)
                        fut.result(timeout=5)
                    except Exception as e:
                        logger.warning(f"Failed to stop controller gracefully for {campaign.id}: {e}")

                # Do not remove here; wait for cleanup when finished
                
    async def _cleanup_finished_controllers(self):
        """Dọn dẹp các controllers đã hoàn thành"""
        finished_controllers = []
        
        for campaign_id, controller in self.active_controllers.items():
            if controller.is_finished():
                finished_controllers.append(campaign_id)
                
        for campaign_id in finished_controllers:
            logger.info(f"Cleaning up finished controller for campaign {campaign_id}")
            # Clean maps
            self.active_controllers.pop(campaign_id, None)
            self.controller_loops.pop(campaign_id, None)
            # Ensure db service is disconnected (thread entry should have closed already)
            self.controller_db_services.pop(campaign_id, None)
            # Thread is daemon; remove reference
            self.controller_threads.pop(campaign_id, None)
            
    def get_status(self) -> Dict[str, any]:
        """Lấy trạng thái của scheduler"""
        return {
            "active_controllers": len(self.active_controllers),
            "processed_campaigns": len(self.processed_campaigns),
            "config": {
                "check_interval": self.config.CHECK_INTERVAL,
                "max_concurrent_campaigns": self.config.MAX_CONCURRENT_CAMPAIGNS
            }
        }

    def _run_controller_thread(self, campaign_id: str):
        """Thread entry: create loop, connect DB, run controller until completion, then disconnect."""
        controller = self.active_controllers.get(campaign_id)
        if controller is None:
            return
        loop = asyncio.new_event_loop()
        self.controller_loops[campaign_id] = loop
        try:
            asyncio.set_event_loop(loop)
            db = self.controller_db_services.get(campaign_id)
            if db is None:
                return
            loop.run_until_complete(db.connect())
            loop.run_until_complete(controller.start())
        except Exception as e:
            logger.error(f"Controller thread crashed for campaign {campaign_id}: {e}")
        finally:
            try:
                db = self.controller_db_services.get(campaign_id)
                if db is not None:
                    loop.run_until_complete(db.disconnect())
            except Exception:
                pass
            finally:
                try:
                    loop.stop()
                except Exception:
                    pass
                loop.close()
