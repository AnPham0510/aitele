import asyncio
import threading
import logging
from typing import Dict, Set
from models.config import Config
from services.database_service import DatabaseService
from services.campaign_service import CampaignService
from controllers.campaign_controller import CampaignController
from services.redis_service import RedisService
logger = logging.getLogger(__name__)

class SchedulerController:
    """Controller chính cho scheduler - điều phối toàn bộ hệ thống"""
    
    def __init__(self, config: Config):
        self.config = config
        self.db_service = DatabaseService(config.DATABASE_URL)
        self.campaign_service = CampaignService(config)
        self.redis_service = RedisService(config.REDIS_URL)
        
        # Track active campaign controllers
        self.active_controllers: Dict[str, CampaignController] = {}
        self.processed_campaigns: Set[str] = set()
        # Threading resources per campaign
        self.controller_threads: Dict[str, threading.Thread] = {}
        self.controller_loops: Dict[str, asyncio.AbstractEventLoop] = {}
        self.controller_db_services: Dict[str, DatabaseService] = {}
        self.controller_redis_services: Dict[str, RedisService] = {}
        # Background callback listener task
        self._callback_task: asyncio.Task | None = None
        
    async def initialize(self):
        """Khởi tạo các services"""
        await self.db_service.connect()
        await self.redis_service.connect()
        logger.info("Scheduler controller initialized")
        # Start background callback listener (independent from campaign threads)
        try:
            if self._callback_task is None or self._callback_task.done():
                self._callback_task = asyncio.create_task(self._callback_listener())
                logger.info("Callback listener started")
        except Exception as e:
            logger.warning(f"Failed to start callback listener: {e}")
        
    async def cleanup(self):
        """Dọn dẹp resources"""
        # Stop background callback listener first
        if self._callback_task is not None:
            try:
                self._callback_task.cancel()
                try:
                    await self._callback_task
                except asyncio.CancelledError:
                    pass
            except Exception as e:
                logger.warning(f"Error stopping callback listener: {e}")
            finally:
                self._callback_task = None
        # Stop all campaign controllers
        for controller in self.active_controllers.values():
            await controller.stop()
            
        await self.db_service.disconnect()
        await self.redis_service.close()
        logger.info("Scheduler controller cleaned up")
        
    async def run_cycle(self):
        """Chạy một chu kỳ kiểm tra và xử lý"""
        logger.debug("Starting scheduler cycle...")
        
        try:
            # 1. Khởi tạo/duy trì controllers cho các campaigns đang active trước
            await self._process_active_campaigns()
            
            # 2. Kiểm tra campaigns cần dừng/tạm dừng
            await self._process_stopped_campaigns()
            
            # 3. Cleanup finished controllers
            await self._cleanup_finished_controllers()
            
        except Exception as e:
            logger.error(f"Error in scheduler cycle: {e}")

    async def _callback_listener(self):
        """Background loop to consume callbacks independently of campaign threads."""
        logger.debug("Callback listener loop started")
        try:
            while True:
                try:
                    callbacks = await self.redis_service.get_call_callbacks(timeout=1)
                    for callback in callbacks:
                        await self._handle_call_callback(callback)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Error in callback listener: {e}")
                    await asyncio.sleep(1)
        finally:
            logger.debug("Callback listener loop stopped")
    
    async def _process_call_callbacks(self):
        """Xử lý callbacks từ Call Agent"""
        try:
            # Lấy callbacks từ Redis queue
            callbacks = await self.redis_service.get_call_callbacks()
            
            if callbacks:
                logger.info(f"Processing {len(callbacks)} callbacks from Call Agent")
                
            for callback in callbacks:
                await self._handle_call_callback(callback)
                
        except Exception as e:
            logger.error(f"Error processing call callbacks: {e}", exc_info=True)
    
    async def _handle_call_callback(self, callback_data: dict):
        """Xử lý một callback từ Call Agent"""
        try:
            call_id = callback_data.get("callId")
            status = callback_data.get("status")
            campaign_id = str(callback_data.get("campaignId", ""))
            lead_id = str(callback_data.get("leadId", ""))
            
            logger.info(f"Received callback for call {call_id}: {status}")
            
            # Tìm campaign controller tương ứng
            controller = self.active_controllers.get(campaign_id)
            if not controller:
                logger.warning(f"No active controller found for campaign {campaign_id}")
                try:
                    if status == "SUCCESS":
                        if self.redis_service:
                            await self.redis_service.mark_lead_success(campaign_id, lead_id)
                            await self.redis_service.mark_phone_success(campaign_id, callback_data.get("leadPhoneNumber", ""))

                            try:
                                await self.redis_service.save_success_and_finalize(call_id)
                                await self.redis_service.remove_retry(campaign_id, call_id)
                            except Exception:
                                pass
                    else:
                        attempt = int(callback_data.get("attempt", 0))
                        max_attempts = int(callback_data.get("maxAttempts", 3))
                        if attempt + 1 < max_attempts and self.redis_service:
                            retry_interval = int(callback_data.get("retryInterval", 300))
                            payload = {
                                "campaign_id": campaign_id,
                                "lead_id": lead_id,
                                "phone": callback_data.get("leadPhoneNumber", ""),
                                "attempt": attempt + 1,
                                "max_attempts": max_attempts,
                                "retry_interval_s": retry_interval,
                                "call_id": call_id,
                                "last_outcome": status,
                            }
                            await self.redis_service.save_failure_and_schedule_retry(
                                campaign_id, call_id, payload, delay_seconds=retry_interval
                            )
                    # Clear in-progress nếu có
                    if self.redis_service:
                        await self.redis_service.clear_inprogress(campaign_id, lead_id)
                        try:
                            await self.redis_service.clear_phone_inprogress(campaign_id, callback_data.get("leadPhoneNumber", ""))
                        except Exception:
                            pass
                except Exception as e:
                    logger.error(f"Fallback Redis update failed for call {call_id}: {e}")
                finally:
                    return
            
            if status == "SUCCESS":
                if controller.redis:
                    await controller.redis.mark_lead_success(campaign_id, lead_id)
                    await controller.redis.mark_phone_success(campaign_id, callback_data.get("leadPhoneNumber", ""))
                    try:
                        await controller.redis.save_success_and_finalize(call_id)
                        await controller.redis.remove_retry(campaign_id, call_id)
                    except Exception:
                        pass
                logger.info(f"Lead {lead_id} marked as SUCCESS")
                
            else:
                # Xử lý retry nếu cần
                attempt = callback_data.get("attempt", 0)
                max_attempts = callback_data.get("maxAttempts", 3)
                
                if attempt + 1 < max_attempts:
                    # Lên lịch retry
                    retry_interval = callback_data.get("retryInterval", 300)
                    payload = {
                        "campaign_id": campaign_id,
                        "lead_id": lead_id,
                        "phone": callback_data.get("leadPhoneNumber", ""),
                        "attempt": attempt + 1,
                        "max_attempts": max_attempts,
                        "retry_interval_s": retry_interval,
                        "call_id": call_id,
                        "last_outcome": status,
                    }
                    
                    if controller.redis:
                        await controller.redis.save_failure_and_schedule_retry(
                            campaign_id, call_id, payload, delay_seconds=retry_interval
                        )
                    logger.info(f"Scheduled retry for call {call_id} (attempt {attempt + 1})")
                else:
                    # Hết lượt retry
                    logger.info(f"Call {call_id} exceeded max attempts, marking as failed")
            
            # Clear in-progress status
            if controller.redis:
                await controller.redis.clear_inprogress(campaign_id, lead_id)
                await controller.redis.clear_phone_inprogress(campaign_id, callback_data.get("leadPhoneNumber", ""))
                
        except Exception as e:
            logger.error(f"Error handling callback for call {callback_data.get('callId', 'unknown')}: {e}")
            
    async def _process_active_campaigns(self):
        """Xử lý các campaigns đang active"""
        # Lấy danh sách campaigns đang ở trạng thái running từ database
        campaigns = await self.db_service.get_running_campaigns()
        logger.info(f"[DEBUG] Số các chiến dịch đang running là: {len(campaigns)}")
        
        # Lấy danh sách campaings thỏa mãn thời gian thực hiện cuộc gọi
        active_campaigns = self.campaign_service.filter_active_campaigns(campaigns)
        logger.info(f"[DEBUG] filtered_active_campaigns={len(active_campaigns)}")
        
        # Cleanup dead controllers first
        await self._cleanup_dead_controllers()
        
        # Check thread limit
        current_thread_count = len(self.active_controllers)
        max_threads = self.config.MAX_CONCURRENT_CAMPAIGNS
        
        if current_thread_count >= max_threads:
            logger.warning(f"Thread limit reached: {current_thread_count}/{max_threads}. Skipping new campaigns.")
            return
        
        available_slots = max_threads - current_thread_count
        logger.info(f"Available thread slots: {available_slots}/{max_threads}")
        
        for campaign in active_campaigns[:available_slots]:  # Limit by available slots
            # Kiểm tra xem campaign đã có controller chưa
            if campaign.id not in self.active_controllers:
                # Kiểm tra có leads cần gọi không
                pending_leads = await self.db_service.get_pending_leads_for_campaign(campaign.id)
                logger.info(f"[DEBUG] campaign_id={campaign.id} name={campaign.name} pending_leads={len(pending_leads)}")
                
                if pending_leads:
                    logger.info(f"Starting controller for campaign {campaign.name} with {len(pending_leads)} leads")

                    # Create per-campaign db service (isolated pool per thread)
                    per_db = DatabaseService(self.config.DATABASE_URL)
                    
                    # Create per-campaign redis service (isolated connection per thread)
                    per_redis = RedisService(self.config.REDIS_URL)

                    # Create controller object
                    controller = CampaignController(
                        campaign=campaign,
                        db_service=per_db,
                        campaign_service=self.campaign_service,
                        config=self.config
                    )

                    self.active_controllers[campaign.id] = controller
                    self.controller_db_services[campaign.id] = per_db
                    self.controller_redis_services[campaign.id] = per_redis

                    # Start controller in a dedicated thread
                    t = threading.Thread(target=self._run_controller_thread, args=(campaign.id,), daemon=True)
                    self.controller_threads[campaign.id] = t
                    t.start()
                    
    async def _process_stopped_campaigns(self):
        """Xử lý các campaigns cần dừng hoặc tạm dừng"""
        # Lấy campaigns đã ended hoặc paused
        stopped_campaigns = await self.db_service.get_stopped_campaigns()
        
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
                        logger.warning(f"Failed to stop controller for {campaign.id}: {e}")

                # Do not remove here; wait for cleanup when finished
                
    async def _cleanup_dead_controllers(self):
        """Dọn dẹp các controllers đã chết hoặc hoàn thành"""
        dead_controllers = []
        
        for campaign_id, thread in self.controller_threads.items():
            if not thread.is_alive():
                dead_controllers.append(campaign_id)
                logger.warning(f"Dead thread detected for campaign {campaign_id}")
        
        for campaign_id in dead_controllers:
            logger.info(f"Cleaning up dead controller for campaign {campaign_id}")
            # Clean maps
            self.active_controllers.pop(campaign_id, None)
            self.controller_loops.pop(campaign_id, None)
            self.controller_db_services.pop(campaign_id, None)
            self.controller_threads.pop(campaign_id, None)
            self.processed_campaigns.discard(campaign_id)
    
    async def _cleanup_finished_controllers(self):
        """Dọn dẹp các controllers đã hoàn thành"""
        finished_controllers = []
        
        for campaign_id, controller in self.active_controllers.items():
            if controller.is_finished():
                finished_controllers.append(campaign_id)
        
        for campaign_id in finished_controllers:
            logger.info(f"Cleaning up finished controller for campaign {campaign_id}")
            
            # Stop controller nếu chưa dừng
            controller = self.active_controllers.get(campaign_id)
            if controller and not controller.is_stopped:
                await controller.stop()
            
            # Clean maps
            self.active_controllers.pop(campaign_id, None)
            self.controller_loops.pop(campaign_id, None)
            
            # Disconnect db service
            db_service = self.controller_db_services.pop(campaign_id, None)
            if db_service:
                try:
                    await db_service.disconnect()
                except Exception as e:
                    logger.warning(f"Error disconnecting db service for campaign {campaign_id}: {e}")
            
            # Disconnect redis service
            redis_service = self.controller_redis_services.pop(campaign_id, None)
            if redis_service:
                try:
                    await redis_service.close()
                except Exception as e:
                    logger.warning(f"Error disconnecting redis service for campaign {campaign_id}: {e}")
            
            # Thread is daemon; remove reference
            self.controller_threads.pop(campaign_id, None)
            self.processed_campaigns.discard(campaign_id)
            
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
            # KẾT NỐI DB
            loop.run_until_complete(db.connect())

            # KẾT NỐI REDIS (trong thread này) VÀ GẮN VÀO CONTROLLER
            redis_svc = self.controller_redis_services.get(campaign_id)
            if redis_svc is None:
                logger.error(f"No Redis service found for campaign {campaign_id}")
                return
                
            loop.run_until_complete(redis_svc.connect())
            controller.attach_redis(redis_svc)

            # CHẠY CONTROLLER
            loop.run_until_complete(controller.start())
        except Exception as e:
            logger.error(f"Controller thread crashed for campaign {campaign_id}: {e}")
        finally:
            try:
                db = self.controller_db_services.get(campaign_id)
                if db is not None:
                    loop.run_until_complete(db.disconnect())
                
                redis_svc = self.controller_redis_services.get(campaign_id)
                if redis_svc is not None:
                    loop.run_until_complete(redis_svc.close())
                    
            except Exception as e:
                logger.warning(f"Error cleaning up resources for campaign {campaign_id}: {e}")
            finally:
                try:
                    loop.stop()
                except Exception:
                    pass
                loop.close()
