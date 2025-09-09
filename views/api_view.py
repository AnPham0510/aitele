import json
import logging
from typing import Dict, Any, List
from datetime import datetime

logger = logging.getLogger(__name__)

class APIView:
    """View layer cho API responses - xử lý format dữ liệu trả về"""
    
    @staticmethod
    def success_response(data: Any = None, message: str = "Success") -> Dict[str, Any]:
        """Tạo response thành công"""
        response = {
            "success": True,
            "message": message,
            "timestamp": datetime.now().isoformat()
        }
        
        if data is not None:
            response["data"] = data
            
        return response
    
    @staticmethod
    def error_response(error: str, code: int = 400, details: Any = None) -> Dict[str, Any]:
        """Tạo response lỗi"""
        response = {
            "success": False,
            "error": error,
            "code": code,
            "timestamp": datetime.now().isoformat()
        }
        
        if details is not None:
            response["details"] = details
            
        return response
    
    @staticmethod
    def format_campaign_data(campaign) -> Dict[str, Any]:
        """Format dữ liệu campaign"""
        return {
            "id": campaign.id,
            "code": campaign.code,
            "tenant_id": campaign.tenant_id,
            "tenant_code": campaign.tenant_code,
            "status": campaign.status,
            "start_time": campaign.start_time.isoformat() if campaign.start_time else None,
            "end_time": campaign.end_time.isoformat() if campaign.end_time else None,
            "working_hours": {
                "start": campaign.working_hours_start,
                "end": campaign.working_hours_end
            }
        }
    
    @staticmethod
    def format_lead_data(lead) -> Dict[str, Any]:
        """Format dữ liệu lead"""
        return {
            "id": lead.id,
            "phone_number": lead.phone_number,
            "name": lead.name,
            "display_name": lead.get_display_name(),
            "gender": lead.gender,
            "province": lead.province,
            "district": lead.district,
            "status": lead.status
        }
    
    @staticmethod
    def format_call_session_data(call_session) -> Dict[str, Any]:
        """Format dữ liệu call session"""
        return {
            "id": call_session.id,
            "campaign_id": call_session.campaign_id,
            "lead_id": call_session.lead_id,
            "lead_phone_number": call_session.lead_phone_number,
            "call_status": call_session.call_status,
            "disposition": call_session.disposition,
            "retry_count": call_session.retry_count,
            "start_time": call_session.start_time.isoformat() if call_session.start_time else None,
            "end_time": call_session.end_time.isoformat() if call_session.end_time else None,
            "duration": call_session.get_duration()
        }
    
    @staticmethod
    def format_scheduler_status(status: Dict[str, Any]) -> Dict[str, Any]:
        """Format trạng thái scheduler"""
        return {
            "active_controllers": status.get("active_controllers", 0),
            "processed_campaigns": status.get("processed_campaigns", 0),
            "configuration": status.get("config", {}),
            "uptime": datetime.now().isoformat()
        }
    
    @staticmethod
    def format_controller_status(controller_stats: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format trạng thái các controllers"""
        return [
            {
                "campaign_id": stat["campaign_id"],
                "campaign_code": stat["campaign_code"],
                "is_running": stat["is_running"],
                "is_stopped": stat["is_stopped"],
                "is_finished": stat["is_finished"],
                "processed_leads": stat["processed_leads"],
                "last_call_times": stat["last_call_times"]
            }
            for stat in controller_stats
        ]
    
    @staticmethod
    def format_error_details(error: Exception, context: str = "") -> Dict[str, Any]:
        """Format chi tiết lỗi"""
        return {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "context": context,
            "timestamp": datetime.now().isoformat()
        }
