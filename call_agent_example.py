"""
Call Agent Example - Minh họa cách Call Agent tương tác với Scheduler
"""
import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Dict, Any

import redis.asyncio as redis

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CallAgent:
    """Call Agent - Thực hiện cuộc gọi thực tế"""
    
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.redis_client = None
        
    async def connect(self):
        """Kết nối Redis"""
        self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
        logger.info("Connected to Redis")
        
    async def close(self):
        """Đóng kết nối"""
        if self.redis_client:
            await self.redis_client.aclose()
        logger.info("Disconnected")
        
    async def start(self):
        """Bắt đầu Call Agent"""
        logger.info("🚀 Call Agent started")
        
        while True:
            try:
                # Lấy call requests từ Redis queue
                requests = await self.get_call_requests()
                
                for request in requests:
                    await self.process_call_request(request)
                    
                # Nếu không có request, chờ một chút
                if not requests:
                    await asyncio.sleep(1)
                    
            except Exception as e:
                logger.error(f"Error in Call Agent: {e}")
                await asyncio.sleep(5)
                
    async def get_call_requests(self) -> list:
        """Lấy call requests từ Redis queue"""
        try:
            # Lấy từ Redis queue
            result = await self.redis_client.brpop("call_requests", timeout=1)
            if result is None:
                return []
                
            request = json.loads(result[1])
            logger.info(f"Received call request: {request.get('callId')}")
            return [request]
            
        except Exception as e:
            logger.error(f"Error getting call requests: {e}")
            return []
            
    async def process_call_request(self, request: Dict[str, Any]):
        """Xử lý một call request"""
        try:
            call_id = request.get("callId")
            phone = request.get("leadPhoneNumber")
            campaign_id = request.get("campaignId")
            lead_id = request.get("leadId")
            is_retry = request.get("isRetry", False)
            attempt = request.get("attempt", 0)
            
            logger.info(f"Processing call {call_id} for {phone} (attempt {attempt})")
            
            # Simulate cuộc gọi thực tế
            outcome = await self.simulate_call(phone)
            
            # Gửi callback về Scheduler
            await self.send_callback({
                "callId": call_id,
                "campaignId": campaign_id,
                "leadId": lead_id,
                "leadPhoneNumber": phone,
                "status": outcome,
                "attempt": attempt,
                "maxAttempts": request.get("maxAttempts", 3),
                "retryInterval": request.get("retryInterval", 300),
                "duration": 15,  # Simulate call duration
                "timestamp": datetime.now().isoformat()
            })
            
            logger.info(f"Call {call_id} completed with status: {outcome}")
            
        except Exception as e:
            logger.error(f"Error processing call request: {e}")
            
    async def simulate_call(self, phone: str) -> str:
        """Simulate cuộc gọi thực tế"""
        # Simulate call duration
        await asyncio.sleep(2)
        
        # Simulate different outcomes
        import random
        outcomes = ["SUCCESS", "NO_ANSWER", "BUSY", "FAILED"]
        weights = [0.7, 0.1, 0.1, 0.1]  # 30% success, 40% no answer, etc.
        
        return random.choices(outcomes, weights=weights)[0]
        
    async def send_callback(self, callback_data: Dict[str, Any]):
        """Gửi callback về Scheduler qua Redis queue"""
        try:
            # Gửi callback vào Redis queue
            await self.redis_client.lpush("call_callbacks", json.dumps(callback_data))
            logger.info(f"Callback sent successfully for call {callback_data['callId']}")
                
        except Exception as e:
            logger.error(f"Error sending callback: {e}")

async def main():
    """Main function"""
    # Cấu hình
    REDIS_URL = "redis://localhost:6379/0"
    
    # Tạo Call Agent
    call_agent = CallAgent(REDIS_URL)
    
    try:
        # Kết nối và bắt đầu
        await call_agent.connect()
        await call_agent.start()
        
    except KeyboardInterrupt:
        logger.info("Shutting down Call Agent...")
    finally:
        await call_agent.close()

if __name__ == "__main__":
    asyncio.run(main())
