import json
import logging
import os
import io
import traceback
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any
import requests
import asyncio
from dotenv import load_dotenv
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    Request,
)

from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, RedirectResponse
from fastapi.security import OAuth2PasswordRequestForm
from fastapi import HTTPException, Response
from rich import print
from src.api.base_models import (
    UserLogin,
    UserRegister,
    UserOut,
    LoginResponse,
    UpdateUserProfileRequest,
    Assistant_Payload,
    PromptCustomizationUpdate,
    UpdateAgentRequest,
    CreateAgentRequest,
    ResetPasswordRequest,
    ForgotPasswordRequest,
    ContactFormRequest,
    ToggleAgentStatusRequest,
    BusinessDetailsRequest,
    UpdateAdminStatusRequest
)
from src.utils.db import PGDB 
from src.utils.mail_management import Send_Mail
from src.utils.jwt_utils import create_access_token
from src.utils.utils import (
    get_current_user, 
    require_admin,
    add_call_event, 
    fetch_and_store_transcript, 
    fetch_and_store_recording, 
    calculate_duration, 
    check_if_answered, 
    hetzner_storage,
    generate_presigned_url,
    get_s3_client,
    serialize_agent_data
)
from fastapi import File, UploadFile, Form

load_dotenv()

router = APIRouter()
mail_obj = Send_Mail()
db = PGDB()
load_dotenv(override=True)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
HETZNER_BUCKET_NAME = os.getenv("HETZNER_BUCKET_NAME")

# ==================== HELPER ====================
def error_response(message, status_code=400):
    return JSONResponse(
        status_code=status_code,
        content={"error": message}
    )

def add_presigned_urls_to_agent(agent: dict) -> dict:
    """
    Add presigned URLs to agent data (avatar).
    Modifies agent dict in-place.
    """
    if agent.get("avatar_url"):
        # avatar_url contains the OBJECT KEY, generate presigned URL
        agent["avatar_presigned_url"] = generate_presigned_url(
            agent["avatar_url"], 
            expiration=86400  # 24 hours
        )
    return agent

def add_presigned_urls_to_call(call: dict) -> dict:
    """
    Add presigned URLs to call data (recording, transcript).
    Modifies call dict in-place.
    """
    # Recording presigned URL
    if call.get("recording_blob"):
        call["recording_presigned_url"] = generate_presigned_url(
            call["recording_blob"],
            expiration=3600  # 1 hour
        )
    
    # Transcript presigned URL (if stored as blob)
    if call.get("transcript_blob"):
        call["transcript_presigned_url"] = generate_presigned_url(
            call["transcript_blob"],
            expiration=3600  # 1 hour
        )
    
    return call

# ==================== ADMIN USER MANAGEMENT ====================
@router.get("/admin/users")
async def get_all_users_admin(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    search: str = Query(None),
    current_user: dict = Depends(require_admin)
):
    """
    Get paginated list of all users (admin only).
    Supports search by username or email.
    
    Requires: Admin authentication
    """
    try:
        result = db.get_all_users(page=page, page_size=page_size, search=search)
        
        # Format created_at timestamps
        for user in result.get("users", []):
            if user.get("created_at"):
                user["created_at"] = user["created_at"].isoformat() if hasattr(user["created_at"], 'isoformat') else str(user["created_at"])
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "data": result
            }
        )
        
    except Exception as e:
        logging.error(f"Error fetching users: {str(e)}")
        traceback.print_exc()
        return error_response(f"Failed to fetch users: {str(e)}", status_code=500)


@router.get("/users/list")

async def get_users_list(current_user: dict = Depends(get_current_user)):
    """
    Get simplified list of all users for dropdowns.
    Available to all authenticated users.
    """
    try:
        # Use the existing DB method
        users = db.get_all_users_simple()
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "users": users
            }
        )
        
    except Exception as e:
        logging.error(f"Error fetching users list: {str(e)}")
        traceback.print_exc()
        return error_response(f"Failed to fetch users list: {str(e)}", status_code=500)


# @router.patch("/admin/users/{user_id}/admin-status")
# async def update_user_admin_status_endpoint(
#     user_id: int,
#     request: UpdateAdminStatusRequest,
#     current_user: dict = Depends(require_admin)
# ):
#     """
#     Promote or demote user to admin status (admin only).
#     Cannot remove own admin status.
    
#     Requires: Admin authentication
#     """
#     try:
#         admin_id = current_user.get("id")
        
#         # Update user admin status
#         updated_user = db.update_user_admin_status(
#             user_id=user_id,
#             is_admin=request.is_admin,
#             admin_id=admin_id
#         )
        
#         # Format created_at
#         if updated_user.get("created_at"):
#             updated_user["created_at"] = updated_user["created_at"].isoformat() if hasattr(updated_user["created_at"], 'isoformat') else str(updated_user["created_at"])
        
#         action = "promoted to admin" if request.is_admin else "demoted from admin"
        
#         return JSONResponse(
#             status_code=200,
#             content={
#                 "success": True,
#                 "message": f"User {action} successfully",
#                 "user": updated_user
#             }
#         )
        
#     except ValueError as ve:
#         # Validation errors (self-demotion, user not found, etc.)
#         return error_response(str(ve), status_code=400)
#     except Exception as e:
#         logging.error(f"Error updating user admin status: {str(e)}")
#         traceback.print_exc()
#         return error_response(f"Failed to update admin status: {str(e)}", status_code=500)


# # ==================== CALL STATUS ====================
# @router.get("/call-status/{call_id}")
# async def get_call_status(call_id: str):
#     """Optimized status check with proper connection handling"""
#     try:
#         conn = db.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 cursor.execute("""
#                     SELECT status, created_at, ended_at, duration, started_at
#                     FROM call_history 
#                     WHERE call_id = %s
#                 """, (call_id,))
#                 row = cursor.fetchone()
#         finally:
#             db.release_connection(conn)
        
#         if not row:
#             return JSONResponse(
#                 status_code=404,
#                 content={"status": "not_found", "is_final": True}
#             )
        
#         current_status, created_at, ended_at, duration, started_at = row
        
#         # Normalize status
#         if current_status not in {"initialized", "dialing", "connected", "completed", "unanswered"}:
#             STATUS_MAP = {
#                 "initiated": "initialized",
#                 "in_progress": "connected",
#                 "failed": "unanswered",
#                 "not_attended": "unanswered"
#             }
#             current_status = STATUS_MAP.get(current_status, "initialized")
        
#         # Calculate elapsed time
#         time_elapsed = 0
#         if created_at:
#             if created_at.tzinfo is None:
#                 created_at = created_at.replace(tzinfo=timezone.utc)
#             time_elapsed = (datetime.now(timezone.utc) - created_at).total_seconds()
        
#         is_final = current_status in {"completed", "unanswered"}
        
#         response = {
#             "status": current_status,
#             "message": {
#                 "initialized": "Initializing...",
#                 "dialing": "Dialing...",
#                 "connected": "Call in progress",
#                 "completed": "Call completed",
#                 "unanswered": "Call not answered"
#             }.get(current_status, current_status),
#             "time_elapsed": round(time_elapsed, 1),
#             "is_final": is_final
#         }
        
#         if is_final and duration:
#             response["duration"] = round(duration, 1)
        
#         if started_at:
#             response["started_at"] = started_at.isoformat()
#         if ended_at:
#             response["ended_at"] = ended_at.isoformat()
        
#         return JSONResponse(response)
#     except Exception as e:
#         logging.error(f"get_call_status error: {e}")
#         return JSONResponse(
#             {"status": "error", "message": str(e), "is_final": True},
#             status_code=500
#         )

# # ==================== CALL HISTORY ====================
# @router.get("/call-history")
# async def get_user_call_history(
#     page: int = Query(1, ge=1),
#     page_size: int = Query(10, le=100),
#     user=Depends(get_current_user)
# ):
#     """Get call history for all agents belonging to the logged-in admin"""
#     try:
#         history = db.get_call_history_by_admin(user["id"], page, page_size)

#         calls = []
#         for call in history.get("calls", []):
#             call_data = {**call}
            
#             # Format timestamps
#             for field in ["created_at", "started_at", "ended_at"]:
#                 if call.get(field):
#                     call_data[field] = call[field].isoformat() if hasattr(call[field], 'isoformat') else str(call[field])
            
#             # Calculate display duration if not available
#             if not call_data.get("duration") and call.get("started_at") and call.get("ended_at"):
#                 try:
#                     start = call["started_at"] if isinstance(call["started_at"], datetime) else datetime.fromisoformat(str(call["started_at"]))
#                     end = call["ended_at"] if isinstance(call["ended_at"], datetime) else datetime.fromisoformat(str(call["ended_at"]))
#                     call_data["duration"] = round((end - start).total_seconds(), 1)
#                 except:
#                     call_data["duration"] = 0
            
#             # Parse transcript from JSONB
#             transcript_text = None
#             if call.get("transcript"):
#                 try:
#                     tr = call["transcript"]
#                     if isinstance(tr, str):
#                         tr = json.loads(tr)
#                     if isinstance(tr, list):
#                         lines = []
#                         for msg in tr:
#                             if msg.get("type") == "message":
#                                 speaker = "Assistant" if msg.get("role") == "assistant" else "User"
#                                 text = " ".join(msg.get("content", [])) if isinstance(msg.get("content"), list) else str(msg.get("content"))
#                                 lines.append(f"{speaker}: {text}")
#                         transcript_text = "\n".join(lines)
#                 except Exception as e:
#                     logging.warning(f"Transcript parse error for {call.get('id')}: {e}")
            
#             call_data["transcript_text"] = transcript_text
#             call_data["has_recording"] = bool(call.get("recording_blob"))
            
#             # ADD PRESIGNED URLS
#             call_data = add_presigned_urls_to_call(call_data)
            
#             calls.append(call_data)

#         pagination = history.get("pagination") or {
#             "page": history.get("page", page),
#             "page_size": history.get("page_size", page_size),
#             "total": history.get("total", len(calls)),
#             "completed_calls": history.get("completed_calls", 0),
#             "not_completed_calls": history.get("not_completed_calls", 0),
#         }

#         return JSONResponse(content=jsonable_encoder({
#             "user_id": user["id"],
#             "pagination": pagination,
#             "calls": calls
#         }))

#     except Exception as e:
#         logging.error(f"Error fetching history: {e}")
#         traceback.print_exc()
#         raise HTTPException(status_code=500, detail=str(e))

# ==================== AGENT EVENT REPORTING ====================
@router.post("/agent/report-event")
async def receive_agent_event(request: Request):
    """Receive status updates from inbound agent"""
    try:
        data = await request.json()
        
        call_id = data.get("call_id")
        status = data.get("status")
        agent_id = data.get("agent_id")
        timestamp = data.get("timestamp")
        
        if not call_id or not status:
            return JSONResponse({"error": "Missing data"}, status_code=400)
        
        if status not in {"initialized", "dialing", "connected", "unanswered", "completed"}:
            return JSONResponse({"error": "Invalid status"}, status_code=400)
        
        updates = {"status": status}
        now = datetime.now(timezone.utc)
        
        if status == "connected":
            conn = db.get_connection()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT started_at FROM call_history WHERE call_id = %s",
                        (call_id,)
                    )
                    row = cursor.fetchone()
                    if row and not row[0]:
                        updates["started_at"] = now
            finally:
                db.release_connection(conn)
        
        if status == "unanswered":
            updates["ended_at"] = now
            updates["duration"] = 0
        
        db.update_call_history(call_id, updates)
        
        return JSONResponse({"success": True})
    except Exception as e:
        logging.error(f"report-event error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/agent/update-call-started")
async def update_call_started(request: Request):
    """Update call with started_at timestamp and caller info"""
    try:
        data = await request.json()
        call_id = data.get("call_id")
        caller_number = data.get("caller_number")
        started_at = data.get("started_at")
        
        updates = {}
        if caller_number:
            updates["caller_number"] = caller_number
        if started_at:
            updates["started_at"] = datetime.fromisoformat(started_at)
        
        if updates:
            db.update_call_history(call_id, updates)
        
        return JSONResponse({"success": True})
    except Exception as e:
        logging.error(f"update-call-started error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/agent/update-call-recording")
async def update_call_recording(request: Request):
    """Update call with recording blob path"""
    try:
        data = await request.json()
        call_id = data.get("call_id")
        recording_blob = data.get("recording_blob")
        recording_url = data.get("recording_url")
        
        updates = {}
        if recording_blob:
            updates["recording_blob"] = recording_blob
        if recording_url:
            updates["recording_url"] = recording_url
        
        if updates:
            db.update_call_history(call_id, updates)
        
        return JSONResponse({"success": True})
    except Exception as e:
        logging.error(f"update-call-recording error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/agent/save-call-data")
async def save_call_data(request: Request):
    """
    Save transcript, recording metadata, and ACCURATE call duration after call ends.
    
    This receives:
    - transcript_blob: Path in Hetzner bucket
    - recording_blob: Path in Hetzner bucket
    - call_duration_seconds: ACCURATE duration from agent (SIP participant join → leave)
    - agent_id: To update used_minutes
    
    Backend will:
    1. Store paths + duration in DB
    2. Update agent's used_minutes (accumulative)
    3. Download transcript after 5s delay → Store JSONB in DB
    """
    try:
        data = await request.json()
        
        call_id = data.get("call_id")
        agent_id = data.get("agent_id")
        transcript_blob = data.get("transcript_blob")
        recording_blob = data.get("recording_blob")
        transcript_url = data.get("transcript_url")
        recording_url = data.get("recording_url")
        call_duration_seconds = data.get("call_duration_seconds")
        sip_joined_at = data.get("sip_joined_at")
        sip_left_at = data.get("sip_left_at")
        
        if not call_id:
            return error_response("Missing call_id", 400)
        
        # Update call history with ACCURATE duration
        updates = {}
        
        if transcript_blob:
            updates["transcript_blob"] = transcript_blob
        if recording_blob:
            updates["recording_blob"] = recording_blob
        if transcript_url:
            updates["transcript_url"] = transcript_url
        if recording_url:
            updates["recording_url"] = recording_url
        
        # Store accurate duration from agent
        if call_duration_seconds is not None:
            updates["duration"] = float(call_duration_seconds)
            logging.info(
                f" Call {call_id}: Duration = {call_duration_seconds:.2f}s "
                f"({call_duration_seconds / 60:.2f} min)"
            )
        
        # Store SIP participant timestamps
        if sip_joined_at:
            try:
                updates["started_at"] = datetime.fromisoformat(sip_joined_at)
            except:
                pass
        
        if sip_left_at:
            try:
                updates["ended_at"] = datetime.fromisoformat(sip_left_at)
            except:
                pass
        
        # Mark call as completed
        updates["status"] = "completed"
        
        if updates:
            db.update_call_history(call_id, updates)
            logging.info(f" Call history updated for {call_id}")
        
        # Update agent's used_minutes (ACCUMULATIVE)
        if agent_id and call_duration_seconds and call_duration_seconds > 0:
            duration_minutes = call_duration_seconds / 60
            
            try:
                # Get current usage
                minutes_check = db.check_agent_minutes_available(agent_id)
                old_used = minutes_check["used_minutes"]
                
                # Update (will add to existing)
                db.update_agent_used_minutes(agent_id, duration_minutes)
                
                # Verify update
                new_check = db.check_agent_minutes_available(agent_id)
                new_used = new_check["used_minutes"]
                
                logging.info(
                    f" Agent {agent_id} minutes updated: "
                    f"{old_used:.2f} → {new_used:.2f} min "
                    f"(+{duration_minutes:.2f} min from call {call_id})"
                )
                
                # Warn if approaching limit
                if new_check["remaining_minutes"] < 60:  # Less than 1 hour
                    logging.warning(
                        f" Agent {agent_id} low on minutes: "
                        f"{new_check['remaining_minutes']:.1f} min remaining"
                    )
                
            except Exception as e:
                logging.error(f" Failed to update agent minutes: {e}")
                traceback.print_exc()
                # Don't fail the entire request if minutes update fails
        
        # DELAYED transcript download & DB storage (5s)
        if transcript_blob:
            async def delayed_transcript():
                await asyncio.sleep(5)
                logging.info(f" Downloading transcript for {call_id}")
                await fetch_and_store_transcript(call_id, None, transcript_blob)
            asyncio.create_task(delayed_transcript())
        
        return JSONResponse({
            "success": True,
            "message": "Call data saved successfully",
            "duration_minutes": round(call_duration_seconds / 60, 2) if call_duration_seconds else None
        })
        
    except Exception as e:
        logging.error(f"save_call_data error: {e}")
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

# ==================== AGENT MANAGEMENT ====================
@router.get("/agents/{agent_id}/calls")
async def get_agent_call_history(
    agent_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(10, le=100),
    user=Depends(get_current_user)  # Any authenticated user can now enter
):
    """Get call history for a specific agent (Global Authenticated Access)"""
    try:
        agent = db.get_agent_by_id(agent_id)
        
        if not agent:
            raise HTTPException(status_code=404, detail="Agent not found")
        
        history = db.get_call_history_by_agent(agent_id, page, page_size)
        
        calls = []
        for call in history.get("calls", []):
            call_data = {**call}
            
            # Format timestamps
            for field in ["created_at", "started_at", "ended_at"]:
                if call.get(field):
                    if isinstance(call[field], datetime):
                        call_data[field] = call[field].isoformat()
                    else:
                        call_data[field] = str(call[field])
            
            # Calculate duration if missing
            if not call_data.get("duration") and call.get("started_at") and call.get("ended_at"):
                try:
                    start = call["started_at"] if isinstance(call["started_at"], datetime) else datetime.fromisoformat(str(call["started_at"]))
                    end = call["ended_at"] if isinstance(call["ended_at"], datetime) else datetime.fromisoformat(str(call["ended_at"]))
                    call_data["duration"] = round((end - start).total_seconds(), 1)
                except:
                    call_data["duration"] = 0
            
            # Parse transcript logic remains the same
            transcript_text = None
            if call.get("transcript"):
                try:
                    tr = call["transcript"]
                    if isinstance(tr, str):
                        tr = json.loads(tr)
                    if isinstance(tr, list):
                        lines = []
                        for msg in tr:
                            if msg.get("type") == "message":
                                speaker = "Assistant" if msg.get("role") == "assistant" else "User"
                                content = msg.get("content", "")
                                text = " ".join(content) if isinstance(content, list) else str(content)
                                lines.append(f"{speaker}: {text}")
                        transcript_text = "\n".join(lines)
                except Exception as e:
                    logging.warning(f"Transcript parse error: {e}")
            
            call_data["transcript_text"] = transcript_text
            call_data["has_recording"] = bool(call.get("recording_blob"))
            
            # Presigned URLs for recordings/assets
            call_data = add_presigned_urls_to_call(call_data)
            
            calls.append(call_data)
        
        return JSONResponse(content=jsonable_encoder({
            "success": True,
            "agent_id": agent_id,
            "agent_name": agent.get("agent_name"),
            "phone_number": agent.get("phone_number"),
            "pagination": {
                "page": history.get("page", 1),
                "page_size": history.get("page_size", 10),
                "total": history.get("total", 0),
                "completed_calls": history.get("completed_calls", 0),
                "not_completed_calls": history.get("not_completed_calls", 0)
            },
            "calls": calls
        }))
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error fetching agent calls: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/agent/config/{phone_number}")
async def get_agent_config(phone_number: str):
    """
    Fetch agent configuration by phone number.
    Checks minutes availability - blocks if exhausted.
    Does NOT send minutes info to agent (security)
    """
    try:
        agent = db.get_agent_by_phone(phone_number)
        if not agent:
            raise HTTPException(status_code=404, detail="Agent not found")
        
        # Check if agent has available minutes
        agent_id = agent.get("agent_id")
        minutes_check = db.check_agent_minutes_available(agent_id)
        
        # Block if no minutes remaining
        if not minutes_check["available"]:
            logging.warning(
                f" Agent {agent_id} ({phone_number}): "
                f"No minutes remaining ({minutes_check['used_minutes']}/{minutes_check['allowed_minutes']})"
            )
            raise HTTPException(
                status_code=403,
                detail="Agent minutes exhausted"
            )
        
        # Minutes available - send config WITHOUT minutes info
        logging.info(
            f" Agent {agent_id} config sent - "
            f"{minutes_check['remaining_minutes']:.1f} minutes remaining"
        )
        agent = serialize_agent_data(dict(agent))

        return JSONResponse({
            "success": True,
            "agent": agent
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error fetching agent config: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/agent/new-call")
async def new_call(
    phone_number: str = Query(...), 
    call_id: str = Query(...),
    caller_number: str = Query(None)
):
    """
    Initialize call history record with caller information.
    NOW: Stores caller_number (customer's phone) in database.
    """
    try:
        agent = db.get_agent_by_phone(phone_number)
        if not agent:
            raise HTTPException(status_code=404, detail="Agent not found")
        
        # Clean and validate caller_number
        cleaned_caller = None
        if caller_number and caller_number != 'unknown':
            # Remove any SIP formatting
            cleaned_caller = caller_number.strip()
            if '@' in cleaned_caller:
                cleaned_caller = cleaned_caller.split('@')[0].replace('sip:', '')
            
            logging.info(f" Call from {cleaned_caller} to agent {agent['agent_id']}")
        else:
            logging.info(f" Call from unknown number to agent {agent['agent_id']}")
        
        # INSERT call history with caller_number
        db.insert_call_history(
            agent_id=agent["agent_id"],
            call_id=call_id,
            status="initialized",
            caller_number=cleaned_caller
        )
        
        return JSONResponse({
            "success": True,
            "message": "Call history initialized",
            "agent_id": agent["agent_id"],
            "caller_number": cleaned_caller
        })
    except Exception as e:
        logging.error(f"Error initializing call: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

    
@router.get("/analytics")
async def get_dashboard_analytics(current_user: dict = Depends(get_current_user)):
    """
    Get overall dashboard analytics.
    Now includes minutes info for top agents.
    """
    try:
        user_id = current_user["id"]
        analytics = db.get_admin_dashboard_analytics(user_id)
        
        # ADD MINUTES INFO TO TOP AGENTS
        for agent in analytics.get("top_agents", []):
            # Add presigned URLs
            if agent.get("avatar_url"):
                agent["avatar_presigned_url"] = generate_presigned_url(agent["avatar_url"], expiration=86400)
            
            agent = serialize_agent_data(agent)
            
            # Add minutes info
            agent_id = agent.get("id")
            minutes_check = db.check_agent_minutes_available(agent_id)
            agent["minutes_info"] = {
                "allowed_minutes": minutes_check["allowed_minutes"],
                "used_minutes": minutes_check["used_minutes"],
                "remaining_minutes": minutes_check["remaining_minutes"],
                "can_accept_calls": minutes_check["available"]
            }
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "data": analytics
            }
        )
    except Exception as e:
        logging.error(f"Error fetching dashboard analytics: {e}")
        return error_response("Failed to fetch analytics", 500)
    

@router.get("/agents")
async def get_all_agents(
    page: int = Query(1, ge=1),
    page_size: int = Query(5, ge=1, le=100),
    current_user: dict = Depends(get_current_user)
):
    """
    Get paginated list of all agents with call statistics.
    Now includes minutes info for each agent.
    """
    try:
        user_id = current_user["id"]
        result = db.get_agents_with_call_stats(user_id, page, page_size)
        
        # NEW: Add minutes info to each agent
        for agent in result.get("agents", []):
            # Add presigned URLs
            add_presigned_urls_to_agent(agent)
            agent= serialize_agent_data(agent)
            
            # Add minutes info
            agent_id = agent.get("id")
            minutes_check = db.check_agent_minutes_available(agent_id)
            agent["minutes_info"] = {
                "allowed_minutes": minutes_check["allowed_minutes"],
                "used_minutes": minutes_check["used_minutes"],
                "remaining_minutes": minutes_check["remaining_minutes"],
                "percentage_used": round(
                    (minutes_check["used_minutes"] / minutes_check["allowed_minutes"] * 100) 
                    if minutes_check["allowed_minutes"] > 0 else 0, 
                    1
                ),
                "can_accept_calls": minutes_check["available"]
            }
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "data": result
            }
        )
    except Exception as e:
        logging.error(f"Error fetching agents: {e}")
        return error_response("Failed to fetch agents", 500)

@router.get("/agents/{agent_id}")
async def get_agent_detail(
    agent_id: int,
    calls_page: int = Query(1, ge=1),
    calls_page_size: int = Query(10, ge=1, le=100),
    current_user: dict = Depends(get_current_user)
):
    """
    Get detailed agent information with paginated call history.
     Now includes minutes usage info.
    """
    try:
        user_id = current_user["id"]
        agent_detail = db.get_agent_detail_with_calls(
            agent_id, user_id, calls_page, calls_page_size
        )
        
        if not agent_detail:
            return error_response("Agent not found", 404)
        
        #  NEW: Add minutes info
        minutes_check = db.check_agent_minutes_available(agent_id)
        agent_detail["minutes_info"] = {
            "allowed_minutes": minutes_check["allowed_minutes"],
            "used_minutes": minutes_check["used_minutes"],
            "remaining_minutes": minutes_check["remaining_minutes"],
            "percentage_used": round(
                (minutes_check["used_minutes"] / minutes_check["allowed_minutes"] * 100) 
                if minutes_check["allowed_minutes"] > 0 else 0, 
                1
            ),
            "can_accept_calls": minutes_check["available"]
        }
        
        # Add presigned URL to agent
        add_presigned_urls_to_agent(agent_detail)
        agent_detail = serialize_agent_data(agent_detail)
        
        # Add presigned URLs to calls
        for call in agent_detail.get("calls", {}).get("data", []):
            add_presigned_urls_to_call(call)
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "data": agent_detail
            }
        )
    except Exception as e:
        logging.error(f"Error fetching agent detail: {e}")
        return error_response("Failed to fetch agent details", 500)
    

@router.post("/agents")
async def create_agent(
    agent_name: str = Form(...),
    phone_number: str = Form(...),
    system_prompt: str = Form(...),
    voice_type: str = Form(None),  # Optional - can be empty
    language: str = Form("en"),
    industry: str = Form(None),
    owner_name: str = Form(None),
    owner_email: str = Form(None),  # NEW
    business_hours_start: str = Form(None),  # NEW (format: "09:00")
    business_hours_end: str = Form(None),    # NEW (format: "17:00")
    allowed_minutes: int = Form(0),          # NEW
    user_id: int = Form(None),               # NEW: Assign to user
    avatar: UploadFile = File(None),
    current_user: dict = Depends(require_admin)  # Admin only - users cannot create agents
):
    """
    Create a new agent with optional avatar image.
    ADMIN ONLY - Users cannot create agents.
    Includes owner_email, business_hours, and allowed_minutes.
    user_id: Select a user from dropdown to assign this agent to them.
    voice_type is optional - defaults to 'female' if not provided.
    """
    try:
        admin_id = current_user["id"]
        assign_to_user_id = user_id  # user_id from form (dropdown selection)
        
        # Set default voice_type if not provided
        if not voice_type:
            voice_type = "female"
        
        # Check if phone number already exists
        existing = db.get_agent_by_phone(phone_number)
        if existing:
            return error_response("Phone number already in use", 400)
        
        # Validate business hours format if provided
        if business_hours_start or business_hours_end:
            if not (business_hours_start and business_hours_end):
                return error_response(
                    "Both business_hours_start and business_hours_end must be provided", 
                    400
                )
            
            # Simple HH:MM validation
            import re
            time_pattern = r'^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$'
            if not re.match(time_pattern, business_hours_start):
                return error_response("Invalid business_hours_start format. Use HH:MM", 400)
            if not re.match(time_pattern, business_hours_end):
                return error_response("Invalid business_hours_end format. Use HH:MM", 400)
        
        # Validate user_id if provided (user selected from dropdown)
        if assign_to_user_id:
            user = db.get_user_by_id(assign_to_user_id)
            if not user:
                return error_response(f"User with ID {assign_to_user_id} not found", 404)
        
        # Validate allowed_minutes
        if allowed_minutes < 0:
            return error_response("allowed_minutes cannot be negative", 400)
        
        # Upload avatar if provided
        avatar_key = None
        if avatar and avatar.filename:
            allowed_extensions = {'jpg', 'jpeg', 'png', 'gif', 'webp'}
            file_extension = avatar.filename.split('.')[-1].lower()
            
            if file_extension not in allowed_extensions:
                return error_response(
                    f"Invalid file type. Allowed: {', '.join(allowed_extensions)}", 
                    400
                )
            
            content = await avatar.read()
            if len(content) > 5 * 1024 * 1024:
                return error_response("File too large. Maximum size: 5MB", 400)
            
            try:
                avatar_key = hetzner_storage.upload_avatar(content, file_extension)
                logging.info(f" Avatar uploaded with key: {avatar_key}")
            except Exception as e:
                logging.error(f" Avatar upload failed: {e}")
                return error_response("Failed to upload avatar", 500)
        
        # Create agent data
        agent_data = {
            "agent_name": agent_name,
            "phone_number": phone_number,
            "system_prompt": system_prompt,
            "voice_type": voice_type,
            "language": language,
            "industry": industry,
            "owner_name": owner_name,
            "owner_email": owner_email,
            "avatar_url": avatar_key,
            "business_hours_start": business_hours_start,
            "business_hours_end": business_hours_end,
            "allowed_minutes": allowed_minutes,
            "user_id": assign_to_user_id,  # User selected from dropdown
            "admin_id": admin_id  # Admin creating the agent
        }
        
        # Save to database
        agent = db.create_agent_with_voice_type(agent_data)
        
        # Update user's agent_id if user was assigned
        if assign_to_user_id:
            try:
                db.update_user_agent_id(assign_to_user_id, agent['agent_id'])
                logging.info(f" Linked User {assign_to_user_id} to Agent {agent['agent_id']}")
            except Exception as link_error:
                # Don't fail agent creation if linking fails
                logging.error(f" Failed to link user to agent: {link_error}")
        
        # FIX: Serialize time objects before JSON response
        agent = serialize_agent_data(agent)
        
        # Add presigned URL for response
        add_presigned_urls_to_agent(agent)
        
        # Send email notification to business owner if owner_email is provided
        if owner_email:
            try:
                logging.info(f" Sending agent creation email to {owner_email}")
                email_sent = await mail_obj.send_agent_created_email(
                    owner_email=owner_email,
                    owner_name=owner_name or "Valued Customer",
                    agent_name=agent_name,
                    phone_number=phone_number,
                    voice_type=voice_type,
                    language=language,
                    industry=industry
                )
                
                if email_sent:
                    logging.info(f" Agent creation email sent successfully to {owner_email}")
                else:
                    logging.warning(f" Failed to send agent creation email to {owner_email}")
            except Exception as email_error:
                # Don't fail the entire agent creation if email fails
                logging.error(f" Error sending agent creation email: {email_error}")
                traceback.print_exc()
        
        # Rename 'id' to 'agent_id' for consistency in response
        if 'id' in agent:
            agent['agent_id'] = agent.pop('id')
        
        return JSONResponse(
            status_code=201,
            content={
                "success": True,
                "message": "Agent created successfully",
                "data": agent
            }
        )

        
        
    except ValueError as e:
        return error_response(str(e), 400)
    except Exception as e:
        logging.error(f"Error creating agent: {e}")
        traceback.print_exc()
        return error_response("Failed to create agent", 500)

@router.put("/agents/{agent_id}")
async def update_agent(
    agent_id: int,
    agent_name: str = Form(None),
    phone_number: str = Form(None),
    system_prompt: str = Form(None),
    voice_type: str = Form(None),
    language: str = Form(None),
    industry: str = Form(None),
    owner_name: str = Form(None),
    owner_email: str = Form(None),
    business_hours_start: str = Form(None),
    business_hours_end: str = Form(None),
    allowed_minutes: int = Form(None),
    user_id: int = Form(None),  # For admin to update user assignment
    avatar: UploadFile = File(None),
    current_user: dict = Depends(get_current_user)
):
    """
    Update agent details with optional new avatar.
    
    Role-Based Restrictions:
    - ADMIN: Full access to all fields
    - USER: Can edit agent_name, system_prompt, voice_type, language, industry, 
            owner_name, owner_email, avatar. CANNOT edit phone_number, business_hours, 
            allowed_minutes, or user_id.
    """
    try:
        current_user_id = current_user["id"]
        is_admin = current_user.get("is_admin", False)
        
        # Get existing agent
        existing_agent = db.get_agent_by_id(agent_id)
        if not existing_agent:
            return error_response("Agent not found", 404)
        
        # Authorization check based on role
        if is_admin:
            # Admin: Must be the admin who created the agent
            if existing_agent["admin_id"] != current_user_id:
                logging.error(f"Unauthorized agent update attempt. User {current_user_id} (Admin) tried to update agent {agent_id} owned by {existing_agent['admin_id']}")
                return error_response("Agent not found or unauthorized", 404)
        else:
            # User: Agent must be assigned to this user
            if existing_agent.get("user_id") != current_user_id:
                logging.error(f"Unauthorized agent update attempt. User {current_user_id} (User) tried to update agent {agent_id} assigned to {existing_agent.get('user_id')}")
                return error_response("Agent not found or unauthorized", 404)
        
        # Build updates dict with role-based restrictions
        updates = {}
        
        # Fields that ALL users can update
        if agent_name is not None:
            updates["agent_name"] = agent_name
        if system_prompt is not None:
            updates["system_prompt"] = system_prompt
        if voice_type is not None:
            updates["voice_type"] = voice_type
        if language is not None:
            updates["language"] = language
        if industry is not None:
            updates["industry"] = industry
        if owner_name is not None:
            updates["owner_name"] = owner_name
        if owner_email is not None:
            updates["owner_email"] = owner_email
        
        # ADMIN-ONLY fields: phone_number, business_hours, allowed_minutes, user_id
        if is_admin:
            if phone_number is not None:
                updates["phone_number"] = phone_number
            if business_hours_start is not None:
                updates["business_hours_start"] = business_hours_start
            if business_hours_end is not None:
                updates["business_hours_end"] = business_hours_end
            if allowed_minutes is not None:
                if allowed_minutes < 0:
                    return error_response("allowed_minutes cannot be negative", 400)
                updates["allowed_minutes"] = allowed_minutes
            if user_id is not None:
                # Validate user exists
                user = db.get_user_by_id(user_id)
                if not user:
                    return error_response(f"User with ID {user_id} not found", 404)
                updates["user_id"] = user_id
        else:
            # Non-admin user trying to update restricted fields - return error
            if phone_number is not None:
                return error_response("You don't have permission to update phone number", 403)
            if business_hours_start is not None or business_hours_end is not None:
                return error_response("You don't have permission to update business hours", 403)
            if allowed_minutes is not None:
                return error_response("You don't have permission to update allowed minutes", 403)
            if user_id is not None:
                return error_response("You don't have permission to change user assignment", 403)
        
        # Validate business hours format if provided (admin only)
        if "business_hours_start" in updates:
            import re
            time_pattern = r'^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$'
            if not re.match(time_pattern, updates["business_hours_start"]):
                return error_response("Invalid business_hours_start format. Use HH:MM", 400)

        if "business_hours_end" in updates:
            import re
            time_pattern = r'^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$'
            if not re.match(time_pattern, updates["business_hours_end"]):
                return error_response("Invalid business_hours_end format. Use HH:MM", 400)
        
        # Handle avatar upload
        if avatar and avatar.filename:
            allowed_extensions = {'jpg', 'jpeg', 'png', 'gif', 'webp'}
            file_extension = avatar.filename.split('.')[-1].lower()
            
            if file_extension not in allowed_extensions:
                return error_response(
                    f"Invalid file type. Allowed: {', '.join(allowed_extensions)}", 
                    400
                )
            
            content = await avatar.read()
            if len(content) > 5 * 1024 * 1024:
                return error_response("File too large. Maximum size: 5MB", 400)
            
            try:
                new_avatar_key = hetzner_storage.upload_avatar(content, file_extension)
                
                # Delete old avatar if exists
                old_avatar_key = existing_agent.get("avatar_url")
                if old_avatar_key:
                    hetzner_storage.delete_avatar(old_avatar_key)
                
                updates["avatar_url"] = new_avatar_key
                logging.info(f" Avatar updated: {new_avatar_key}")
                
            except Exception as e:
                logging.error(f" Avatar upload failed: {e}")
                return error_response("Failed to upload avatar", 500)
        
        if not updates:
            return error_response("No fields to update", 400)
        
        # Update agent - use admin_id for DB update
        admin_id = existing_agent["admin_id"]
        result = db.update_agent_with_voice_type(agent_id, admin_id, updates)
        
        if not result:
            return error_response("Update failed", 500)
        
        # Serialize time objects before JSON response
        result = serialize_agent_data(result)
        
        # Add presigned URL
        add_presigned_urls_to_agent(result)
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": "Agent updated successfully",
                "data": result
            }
        )
        
    except ValueError as e:
        return error_response(str(e), 400)
    except Exception as e:
        logging.error(f"Error updating agent: {e}")
        traceback.print_exc()
        return error_response("Failed to update agent", 500)

@router.delete("/agents/{agent_id}")
async def delete_agent(
    agent_id: int,
    current_user: dict = Depends(get_current_user)
):
    """Delete (deactivate) an agent and optionally delete avatar"""
    try:
        user_id = current_user["id"]
        
        # Get agent details to find avatar
        agent = db.get_agent_by_id(agent_id)
        if not agent or agent["admin_id"] != user_id:
            return error_response("Agent not found or unauthorized", 404)
        
        # Delete from database (soft delete)
        success = db.delete_agent(agent_id, user_id)
        
        if not success:
            return error_response("Delete failed", 500)
        
        # Delete avatar from Hetzner if exists
        avatar_key = agent.get("avatar_url")
        if avatar_key:
            try:
                hetzner_storage.delete_avatar(avatar_key)
                logging.info(f" Avatar deleted for agent {agent_id}")
            except Exception as e:
                logging.warning(f" Could not delete avatar: {e}")
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": "Agent deleted successfully"
            }
        )
        
    except Exception as e:
        logging.error(f"Error deleting agent: {e}")
        traceback.print_exc()
        return error_response("Failed to delete agent", 500)

# @router.post("/agents/toggle-status")
# async def toggle_agent_status(
#     request: ToggleAgentStatusRequest,
#     current_user: dict = Depends(get_current_user)
# ):
#     """
#     Toggle agent active/inactive status for the current user.
#     When user presses active/inactive button, this updates their personal
#     activation status for the agent.
#     """
#     try:
#         user_id = current_user["id"]
#         agent_id = request.agent_id
#         is_active = request.is_active
        
#         # Admin Override: Allow setting status for another user
#         if request.user_id:
#             is_admin = current_user.get("is_admin") or current_user.get("role") == "admin"
#             if is_admin:
#                 logging.info(f"Admin {current_user['id']} toggling agent {agent_id} status for User {request.user_id}")
#                 user_id = request.user_id
#             else:
#                 return error_response("Unauthorized: Only admins can specify user_id", 403)
        
#         # Toggle the status in the database
#         result = db.toggle_agent_status_for_user(user_id, agent_id, is_active)
        
#         return JSONResponse(
#             status_code=200,
#             content={
#                 "success": True,
#                 "message": f"Agent {'activated' if is_active else 'deactivated'} successfully",
#                 "data": result
#             }
#         )
        
#     except ValueError as e:
#         return error_response(str(e), 404)
#     except Exception as e:
#         logging.error(f"Error toggling agent status: {e}")
#         traceback.print_exc()
#         return error_response("Failed to toggle agent status", 500)




# @router.post("/agents/toggle-status")
# async def toggle_agent_status(
#     request: ToggleAgentStatusRequest,
#     current_user: dict = Depends(get_current_user)
# ):
#     """
#     Admin-only: Activate or deactivate an agent globally.
#     """
#     try:
#         # 🔐 Admin check (strict)
#         is_admin = current_user.get("is_admin") or current_user.get("role") == "admin"
#         if not is_admin:
#             return error_response("Unauthorized: Only admins can toggle agent status", 403)

#         admin_id = current_user["id"]
#         agent_id = request.agent_id
#         is_active = request.is_active

#         # Toggle agent status (global)
#         result = db.toggle_agent_status_admin(
#             admin_id=admin_id,
#             agent_id=agent_id,
#             is_active=is_active
#         )

#         return JSONResponse(
#             status_code=200,
#             content={
#                 "success": True,
#                 "message": f"Agent {'activated' if is_active else 'deactivated'} successfully",
#                 "data": result
#             }
#         )

#     except ValueError as e:
#         return error_response(str(e), 404)
#     except Exception as e:
#         logging.error(f"Error toggling agent status: {e}")
#         traceback.print_exc()
#         return error_response("Failed to toggle agent status", 500)


# @router.get("/agents/{agent_id}/status")
# async def get_agent_status(
#     agent_id: int,
#     current_user: dict = Depends(get_current_user)
# ):
#     """
#     Get the activation status of a specific agent for the current user.
#     """
#     try:
#         user_id = current_user["id"]
        
#         # Get the status
#         status = db.get_agent_status_for_user(user_id, agent_id)
        
#         return JSONResponse(
#             status_code=200,
#             content={
#                 "success": True,
#                 "data": {
#                     "agent_id": agent_id,
#                     **status
#                 }
#             }
#         )
        
#     except Exception as e:
#         logging.error(f"Error getting agent status: {e}")
#         traceback.print_exc()
#         return error_response("Failed to get agent status", 500)

# @router.get("/agents/statuses/all")
# async def get_all_agent_statuses(
#     current_user: dict = Depends(get_current_user)
# ):
#     """
#     Get activation status of all agents for the current user.
#     """
#     try:
#         user_id = current_user["id"]
        
#         # Get all agent statuses
#         statuses = db.get_all_agent_statuses_for_user(user_id)
        
#         return JSONResponse(
#             status_code=200,
#             content={
#                 "success": True,
#                 "data": statuses
#             }
#         )
        
#     except Exception as e:
#         logging.error(f"Error getting all agent statuses: {e}")
#         traceback.print_exc()
#         return error_response("Failed to get agent statuses", 500)


@router.post("/livekit-webhook")
async def livekit_webhook(request: Request):
    """Handle LiveKit events for call lifecycle"""
    try:
        data = await request.json()
        event = data.get("event")
        room = data.get("room", {})
        call_id = room.get("name")

        # Extract call_id from egress events
        if not call_id:
            egress_info = data.get("egress_info", {}) or data.get("egressInfo", {})
            call_id = egress_info.get("room_name") or egress_info.get("roomName")
            if not call_id:
                return JSONResponse({"message": "No call_id"})

        # Always log event
        add_call_event(call_id, event, data)
        
        # Ignore non-critical events
        if event in ["room_started", "participant_joined", "egress_started", 
                     "egress_updated", "track_published", "track_unpublished"]:
            return JSONResponse({"message": f"{event} logged"})

        # Handle room end WITHOUT calculating duration
        # Duration will come from agent via save-call-data endpoint
        if event in ["room_finished", "participant_left"]:
            await asyncio.sleep(0.5)
            
            conn = db.get_connection()
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT status, events_log, agent_id, duration
                        FROM call_history WHERE call_id = %s
                    """, (call_id,))
                    row = cursor.fetchone()
            finally:
                db.release_connection(conn)

            if not row:
                return JSONResponse({"message": "Call not found"})

            current_status, events_log, agent_id, existing_duration = row
            
            # Skip if already final
            if current_status in {"completed", "unanswered"}:
                logging.info(f" Call {call_id} already finalized, skipping webhook processing")
                return JSONResponse({"message": "Already finalized"})

            answered = check_if_answered(events_log)
            final_status = "completed" if answered else "unanswered"
            
            # Don't calculate duration here - it will come from agent
            updates = {
                "status": final_status,
                "ended_at": datetime.now(timezone.utc)
            }
            
            # Only set duration to 0 for unanswered calls
            if final_status == "unanswered":
                updates["duration"] = 0
            
            db.update_call_history(call_id, updates)
            
            logging.info(
                f" Call {call_id} marked as {final_status}. "
                f"Duration will be set by agent."
            )
            
            return JSONResponse({"message": f"Call ended: {final_status}"})

        elif event == "egress_ended":
            egress_info = data.get("egress_info", {}) or data.get("egressInfo", {})
            file_results = egress_info.get("file_results", []) or egress_info.get("fileResults", [])
            
            if file_results:
                file_info = file_results[0] if isinstance(file_results, list) else file_results
                location = file_info.get("location") or file_info.get("download_url")
                
                if location:
                    db.update_call_history(call_id, {"recording_url": location})
                    return JSONResponse({"message": "Recording saved"})

        return JSONResponse({"message": f"{event} processed"})

    except Exception as e:
        logging.error(f"Webhook error: {e}")
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)
    


@router.get("/agents/by-owner/{owner_name}")
async def get_agents_by_owner(
    owner_name: str,
    current_user: dict = Depends(get_current_user)
):
    """Get all agents for the logged-in admin filtered by owner name"""
    try:
        user_id = current_user["id"]
        
        # Validate owner_name
        if not owner_name or len(owner_name.strip()) == 0:
            return error_response("Owner name cannot be empty", 400)
        
        # Get agents
        agents = db.get_agents_by_owner_name(user_id, owner_name.strip())
        
        # ADD PRESIGNED URLS
        for agent in agents:
            add_presigned_urls_to_agent(agent)
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "owner_name": owner_name,
                "count": len(agents),
                "data": agents
            }
        )
        
    except Exception as e:
        logging.error(f"Error fetching agents by owner: {e}")
        traceback.print_exc()
        return error_response("Failed to fetch agents by owner", 500)
    

@router.post("/agent/book-appointment")
async def book_appointment(request: Request):
    """
    API for LiveKit agent to book an appointment.
    Sends confirmation emails to BOTH customer AND owner.
    """
    try:
        data = await request.json()
        
        user_id = data.get("user_id")  # This is agent_id
        appointment_date = data.get("appointment_date") 
        start_time = data.get("start_time")
        end_time = data.get("end_time")
        customer_name = data.get("customer_name", "Valued Customer")
        customer_email = data.get("customer_email")
        customer_phone = data.get("customer_phone")
        title = data.get("title", "Appointment")
        description = data.get("description", "")
        organizer_name = data.get("organizer_name")
        
        if not all([user_id, appointment_date, start_time, end_time, customer_email]):
            return error_response("Missing required fields", status_code=400)
        
        # Get agent details to fetch owner_email
        agent = db.get_agent_by_id(user_id)
        if not agent:
            return error_response("Agent not found", status_code=404)
        
        owner_email = agent.get("owner_email")
        owner_name = agent.get("owner_name", "Business Owner")
        
        # Send email to CUSTOMER
        customer_email_sent = await mail_obj.send_email_with_calendar_event(
            attendee_email=customer_email,
            attendee_name=customer_name,
            appointment_date=appointment_date,
            start_time=start_time,
            end_time=end_time,
            title=title,
            description=description,
            organizer_name=organizer_name or owner_name,
            organizer_email=owner_email or customer_email
        )
        
        # Send email to OWNER (using dedicated function)
        owner_email_sent = False
        if owner_email:
            owner_email_sent = await mail_obj.send_owner_appointment_notification(
                owner_email=owner_email,
                owner_name=owner_name,
                customer_name=customer_name,
                customer_email=customer_email,
                customer_phone=customer_phone,
                appointment_date=appointment_date,
                start_time=start_time,
                end_time=end_time,
                title=title,
                description=description
            )
            
            logging.info(
                f" Appointment emails sent - "
                f"Customer: {customer_email_sent}, Owner: {owner_email_sent}"
            )
        else:
            logging.warning(f" No owner_email for agent {user_id}, skipping owner notification")
        
        return JSONResponse({
            "success": True,
            "message": "Appointment booked successfully",
            "emails_sent": {
                "customer": customer_email_sent,
                "owner": owner_email_sent
            }
        })
        
    except Exception as e:
        logging.error(f"Error booking appointment: {e}")
        traceback.print_exc()
        return error_response(f"Failed to book appointment: {str(e)}", status_code=500)
    

@router.post("/forgot-password")
async def forgot_password(request: ForgotPasswordRequest):
    """Send password reset email"""
    try:
        email = request.email.strip().lower()
        
        conn = db.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id FROM users WHERE email = %s", (email,))
                user = cursor.fetchone()
        finally:
            db.release_connection(conn)
        
        if not user:
            logging.warning(f"Password reset requested for non-existent email: {email}")
            return JSONResponse({
                "success": True,
                "message": "If that email exists, a reset link has been sent."
            })
        
        from src.utils.jwt_utils import create_password_reset_token
        reset_token = create_password_reset_token(email)
        
        frontend_url = os.getenv("FRONTEND_URL", "https://www.mrbot-ki.de")
        mail_obj = Send_Mail()  
        email_sent = await mail_obj.send_password_reset_email(email, reset_token, frontend_url)
        
        return JSONResponse({
            "success": True,
            "message": "If that email exists, a reset link has been sent."
        })
        
    except Exception as e:
        logging.error(f"Error in forgot password: {e}")
        traceback.print_exc()
        return error_response("Failed to process request", 500)

@router.post("/reset-password")
async def reset_password(request: ResetPasswordRequest):
    """Reset password using token"""
    try:
        from src.utils.jwt_utils import verify_password_reset_token
        
        email = verify_password_reset_token(request.token)
        
        if not email:
            return error_response("Invalid or expired reset token", 400)
        
        # Update password
        db.update_user_password(email, request.new_password)
        
        return JSONResponse({
            "success": True,
            "message": "Password updated successfully. You can now login."
        })
        
    except ValueError as e:
        return error_response(str(e), 400)
    except Exception as e:
        logging.error(f"Error resetting password: {e}")
        traceback.print_exc()
        return error_response("Failed to reset password", 500)
    
# @router.post("/admin/migrate-voices-from-json")
# async def migrate_voices_from_json(
#     voices_json: UploadFile = File(..., description="JSON file with voice data"),
#     current_user: dict = Depends(get_current_user)
# ):
#     """
#     MIGRATION ENDPOINT: Import voice samples from JSON export.
    
#     Expected JSON format (array of objects):
#     [
#         {
#             "voice_name": "Emma",
#             "voice_id": "56bWURjYFHyYyVf490Dp",
#             "language": "en",
#             "country_code": "US",
#             "gender": "female",
#             "audio_blob_path": "voice_samples/56bWURjYFHyYyVf490Dp.mp3",
#             "duration_seconds": null
#         },
#         ...
#     ]
    
#     This will:
#     1. Parse the JSON file
#     2. Insert all records into voice_samples table
#     3. Skip duplicates (based on voice_id)
#     """
#     import logging
#     logger = logging.getLogger(__name__)
    
#     try:
#         logger.info("📥 Starting voice migration from JSON...")
        
#         # Read and parse JSON
#         content = await voices_json.read()
#         voices_data = json.loads(content.decode('utf-8'))
        
#         if not isinstance(voices_data, list):
#             return error_response("Invalid JSON format. Expected array of voice objects.", 400)
        
#         logger.info(f"📋 Found {len(voices_data)} voice records in JSON")
        
#         # Validate required fields
#         required_fields = ['voice_name', 'voice_id', 'language', 'country_code', 'gender', 'audio_blob_path']
        
#         inserted_count = 0
#         skipped_count = 0
#         failed = []
        
#         for idx, voice in enumerate(voices_data, 1):
#             try:
#                 # Validate fields
#                 missing_fields = [field for field in required_fields if field not in voice]
#                 if missing_fields:
#                     logger.warning(f"⚠️ Record {idx}: Missing fields {missing_fields}, skipping")
#                     failed.append(f"{voice.get('voice_name', 'Unknown')}: Missing {missing_fields}")
#                     continue
                
#                 # Insert into database
#                 db.insert_voice_sample({
#                     'voice_name': voice['voice_name'],
#                     'voice_id': voice['voice_id'],
#                     'language': voice['language'],
#                     'country_code': voice['country_code'],
#                     'gender': voice['gender'],
#                     'audio_blob_path': voice['audio_blob_path'],
#                     'duration_seconds': voice.get('duration_seconds')
#                 })
                
#                 inserted_count += 1
#                 logger.info(f"✅ [{idx}/{len(voices_data)}] Inserted: {voice['voice_name']} ({voice['voice_id']})")
                
#             except Exception as e:
#                 error_msg = str(e)
#                 if 'duplicate key' in error_msg.lower() or 'unique constraint' in error_msg.lower():
#                     skipped_count += 1
#                     logger.info(f" [{idx}/{len(voices_data)}] Skipped (duplicate): {voice.get('voice_name')}")
#                 else:
#                     logger.error(f" [{idx}/{len(voices_data)}] Failed: {voice.get('voice_name')} - {e}")
#                     failed.append(f"{voice.get('voice_name', 'Unknown')}: {error_msg}")
        
#         logger.info(f" Migration complete!")
#         logger.info(f"   Inserted: {inserted_count}")
#         logger.info(f"   Skipped (duplicates): {skipped_count}")
#         logger.info(f"   Failed: {len(failed)}")
        
#         return JSONResponse({
#             "success": True,
#             "message": f"Successfully migrated {inserted_count} voice samples!",
#             "summary": {
#                 "total_records": len(voices_data),
#                 "inserted": inserted_count,
#                 "skipped_duplicates": skipped_count,
#                 "failed": len(failed)
#             },
#             "failed_records": failed if failed else None
#         })
        
#     except json.JSONDecodeError as e:
#         logger.error(f" Invalid JSON: {e}")
#         return error_response(f"Invalid JSON file: {str(e)}", 400)
#     except Exception as e:
#         logger.error(f" Migration error: {e}")
#         traceback.print_exc()
#         return error_response(f"Migration failed: {str(e)}", 500)

# @router.post("/admin/bulk-upload-voices")
# async def bulk_upload_voices(
#     voice_doc: UploadFile = File(..., description="Text document with voice metadata"),
#     audio_files: List[UploadFile] = File(..., description="Audio files (.mp3)"),
#     current_user: dict = Depends(get_current_user)
# ):
#     """
#     ONE-TIME BULK UPLOAD: Parse voice document + upload audio files + save to DB.
#     """
#     import tempfile
#     import shutil
#     import re
#     from pathlib import Path
#     import logging
#     logger = logging.getLogger(__name__)
    
#     try:
#         # ==================== STEP 1: PARSE DOCUMENT ====================
#         logger.info("📋 Step 1: Parsing voice document...")
#         doc_content = (await voice_doc.read()).decode('utf-8')
        
#         # Language mapping
#         LANGUAGE_TO_COUNTRY = {
#             "en": "US", "de": "DE", "fr": "FR",
#             "nl": "NL", "it": "IT", "es": "ES"
#         }
        
#         language_patterns = {
#             'english': 'en', 'german': 'de', 'french': 'fr',
#             'dutch': 'nl', 'italian': 'it', 'spanish': 'es'
#         }
        
#         voices = []
#         current_language = None
#         current_voice = {}
        
#         for line in doc_content.split('\n'):
#             line = line.strip()
            
#             # Skip empty lines
#             if not line:
#                 continue
            
#             # Detect language section
#             for lang_name, lang_code in language_patterns.items():
#                 if lang_name.lower() in line.lower() and ('voice' in line.lower() or 'agent' in line.lower()):
#                     current_language = lang_code
#                     logger.info(f"🌐 Found language section: {lang_name.upper()} ({lang_code})")
#                     break
            
#             # FIX: Match ANY "Name:" pattern (not just "First Name:", "Second Name:", etc.)
#             if re.match(r'^(First\s+|Second\s+|Third\s+|Fourth\s+)?Name:\s*.+', line, re.IGNORECASE):
#                 # Save previous voice if complete
#                 if current_voice and all(k in current_voice for k in ['voice_name', 'gender', 'voice_id', 'audio_filename']):
#                     voices.append(current_voice)
#                     logger.info(f"  ✓ Added: {current_voice['voice_name']}")
                
#                 # Start new voice
#                 name = re.sub(r'^(First\s+|Second\s+|Third\s+|Fourth\s+)?Name:\s*', '', line, flags=re.IGNORECASE).strip()
#                 current_voice = {
#                     'voice_name': name,
#                     'language': current_language,
#                     'country_code': LANGUAGE_TO_COUNTRY.get(current_language, 'US')
#                 }
            
#             elif line.startswith('Gender:'):
#                 gender = line.split(':', 1)[1].strip().lower()
#                 current_voice['gender'] = gender
            
#             elif line.startswith('ID:'):
#                 voice_id = line.split(':', 1)[1].strip()
#                 current_voice['voice_id'] = voice_id
            
#             elif line.startswith('Audio:'):
#                 audio_filename = line.split(':', 1)[1].strip()
#                 # Clean filename - remove extra spaces
#                 audio_filename = re.sub(r'\s+', ' ', audio_filename)
#                 current_voice['audio_filename'] = audio_filename
        
#         # Add last voice
#         if current_voice and all(k in current_voice for k in ['voice_name', 'gender', 'voice_id', 'audio_filename']):
#             voices.append(current_voice)
#             logger.info(f"  ✓ Added: {current_voice['voice_name']}")
        
#         logger.info(f"✅ Parsed {len(voices)} voices from document")
        
#         if not voices:
#             return error_response("No voices found in document", 400)
        
#         # Log parsed voices for debugging
#         for v in voices:
#             logger.info(f"  📝 {v['voice_name']} ({v['language']}) - {v['voice_id']}")
#             logger.info(f"     Audio: {v['audio_filename']}")
        
#         # ==================== STEP 2: SAVE AUDIO FILES TEMPORARILY ====================
#         logger.info(" Step 2: Saving audio files to temp directory...")
#         temp_dir = Path(tempfile.mkdtemp())
        
#         audio_file_map = {}
#         for audio_file in audio_files:
#             if not audio_file.filename.endswith('.mp3'):
#                 logger.warning(f"  Skipping non-MP3 file: {audio_file.filename}")
#                 continue
            
#             file_path = temp_dir / audio_file.filename
#             with open(file_path, 'wb') as f:
#                 content = await audio_file.read()
#                 f.write(content)
            
#             # Store both original and normalized versions
#             audio_file_map[audio_file.filename] = file_path
            
#             # Also add normalized version for fuzzy matching
#             normalized_name = audio_file.filename.lower().replace(' ', '').replace('_', '').replace('-', '')
#             audio_file_map[normalized_name] = file_path
            
#             logger.info(f"  ✓ Saved: {audio_file.filename}")
        
#         logger.info(f" Saved {len(audio_files)} audio files")
        
#         # ==================== STEP 3: MATCH VOICES WITH AUDIO FILES ====================
#         logger.info(" Step 3: Matching voices with audio files...")
#         matched_voices = []
        
#         for voice in voices:
#             expected_filename = voice['audio_filename']
#             matched = False
            
#             # Try 1: Exact match
#             if expected_filename in audio_file_map:
#                 voice['audio_path'] = audio_file_map[expected_filename]
#                 matched_voices.append(voice)
#                 logger.info(f" Exact match: {voice['voice_name']} → {expected_filename}")
#                 matched = True
#                 continue
            
#             # Try 2: Check if file exists in temp_dir
#             exact_path = temp_dir / expected_filename
#             if exact_path.exists():
#                 voice['audio_path'] = exact_path
#                 matched_voices.append(voice)
#                 logger.info(f" File system match: {voice['voice_name']} → {expected_filename}")
#                 matched = True
#                 continue
            
#             # Try 3: Normalize and fuzzy match
#             normalized_expected = expected_filename.lower().replace(' ', '').replace('_', '').replace('-', '')
            
#             if normalized_expected in audio_file_map:
#                 voice['audio_path'] = audio_file_map[normalized_expected]
#                 matched_voices.append(voice)
#                 actual_name = audio_file_map[normalized_expected].name
#                 logger.info(f" Fuzzy match: {voice['voice_name']} → {actual_name}")
#                 matched = True
#                 continue
            
#             # Try 4: Partial matching (contains voice_id)
#             voice_id = voice.get('voice_id', '').lower()
#             if voice_id:
#                 for filename, filepath in audio_file_map.items():
#                     if voice_id in filename.lower():
#                         voice['audio_path'] = filepath
#                         matched_voices.append(voice)
#                         logger.info(f" ID match: {voice['voice_name']} → {filepath.name}")
#                         matched = True
#                         break
            
#             if not matched:
#                 logger.warning(f" No audio file found for: {voice['voice_name']} (expected: {expected_filename})")
        
#         logger.info(f" Matched {len(matched_voices)}/{len(voices)} voices")
        
#         if not matched_voices:
#             shutil.rmtree(temp_dir)
#             return error_response("No audio files matched with document data", 400)
        
#         # ==================== STEP 4: UPLOAD TO HETZNER ====================
#         logger.info(" Step 4: Uploading to Hetzner...")
#         s3_client = get_s3_client()
#         bucket_name = HETZNER_BUCKET_NAME
        
#         uploaded_voices = []
#         upload_errors = []
        
#         for voice in matched_voices:
#             try:
#                 audio_path = voice['audio_path']
                
#                 # Read audio file
#                 with open(audio_path, 'rb') as f:
#                     audio_content = f.read()
                
#                 # Create blob path
#                 voice_id = voice['voice_id']
#                 blob_path = f"voice_samples/{voice_id}.mp3"
                
#                 # Upload to Hetzner
#                 logger.info(f"   Uploading {voice['voice_name']} ({len(audio_content)} bytes)...")
#                 s3_client.put_object(
#                     Bucket=bucket_name,
#                     Key=blob_path,
#                     Body=audio_content,
#                     ContentType='audio/mpeg',
#                     CacheControl='public, max-age=31536000'
#                 )
                
#                 voice['audio_blob_path'] = blob_path
#                 uploaded_voices.append(voice)
                
#                 logger.info(f" Uploaded: {voice['voice_name']} → {blob_path}")
                
#             except Exception as e:
#                 error_msg = f"Upload failed for {voice['voice_name']}: {str(e)}"
#                 logger.error(f" {error_msg}")
#                 upload_errors.append(error_msg)
#                 import traceback
#                 traceback.print_exc()
#                 continue
        
#         logger.info(f" Uploaded {len(uploaded_voices)} files to Hetzner")
        
#         if upload_errors:
#             logger.error("Upload errors:")
#             for err in upload_errors:
#                 logger.error(f"  - {err}")
        
#         # ==================== STEP 5: SAVE TO DATABASE ====================
#         logger.info(" Step 5: Saving to database...")
#         saved_count = 0
#         failed = []
        
#         for voice in uploaded_voices:
#             try:
#                 db.insert_voice_sample({
#                     'voice_name': voice['voice_name'],
#                     'voice_id': voice['voice_id'],
#                     'language': voice['language'],
#                     'country_code': voice['country_code'],
#                     'gender': voice['gender'],
#                     'audio_blob_path': voice['audio_blob_path'],
#                     'duration_seconds': None
#                 })
#                 saved_count += 1
#                 logger.info(f" Saved to DB: {voice['voice_name']}")
#             except Exception as e:
#                 logger.error(f" DB save failed for {voice['voice_name']}: {e}")
#                 failed.append(voice['voice_name'])
#                 import traceback
#                 traceback.print_exc()
        
#         # ==================== CLEANUP ====================
#         shutil.rmtree(temp_dir)
#         logger.info(" Cleaned up temp files")
        
#         # ==================== RESPONSE ====================
#         return JSONResponse({
#             "success": True,
#             "message": f" Successfully uploaded {saved_count} voice samples!",
#             "summary": {
#                 "parsed_from_document": len(voices),
#                 "matched_with_audio": len(matched_voices),
#                 "uploaded_to_hetzner": len(uploaded_voices),
#                 "saved_to_database": saved_count,
#                 "failed": len(failed)
#             },
#             "voices": [
#                 {
#                     "name": v['voice_name'],
#                     "language": v['language'],
#                     "gender": v['gender'],
#                     "voice_id": v['voice_id'],
#                     "audio_blob_path": v.get('audio_blob_path')
#                 }
#                 for v in uploaded_voices
#             ],
#             "failed_voices": failed if failed else None,
#             "upload_errors": upload_errors if upload_errors else None
#         })
        
#     except Exception as e:
#         logger.error(f" Bulk upload error: {e}")
#         import traceback
#         traceback.print_exc()
#         return error_response(f"Upload failed: {str(e)}", 500)


@router.post("/agents/{agent_id}/reset-minutes")
async def reset_agent_minutes(
    agent_id: int,
    current_user: dict = Depends(get_current_user)
):
    """
    Reset used_minutes to 0 for billing cycle reset.
    Called when admin receives payment and wants to reset the agent's quota.
    """
    try:
        user_id = current_user["id"]
        
        # Get current stats before reset
        agent = db.get_agent_by_id(agent_id)
        if not agent or agent["admin_id"] != user_id:
            return error_response("Agent not found or unauthorized", 404)
        
        old_used = agent.get("used_minutes", 0)
        allowed = agent.get("allowed_minutes", 0)
        
        # Reset minutes
        db.reset_agent_minutes(agent_id, user_id)
        
        logging.info(
            f" Agent {agent_id} minutes reset by admin {user_id}. "
            f"Was: {old_used}/{allowed} min, Now: 0/{allowed} min"
        )
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": "Agent minutes reset successfully",
                "data": {
                    "agent_id": agent_id,
                    "previous_used_minutes": float(old_used),
                    "allowed_minutes": allowed,
                    "new_used_minutes": 0,
                    "remaining_minutes": allowed
                }
            }
        )
        
    except ValueError as e:
        return error_response(str(e), 400)
    except Exception as e:
        logging.error(f"Error resetting agent minutes: {e}")
        traceback.print_exc()
        return error_response("Failed to reset minutes", 500)
    

@router.post("/contact-form")
async def submit_contact_form(request: ContactFormRequest):
    """
    PUBLIC endpoint - Handle contact form submissions from website
    Sends email to business with customer details
    No authentication required
    """
    try:
        BUSINESS_EMAIL = os.getenv("BUSINESS_EMAIL", "info@mrbot-ki.de")
        
        if not request.first_name or not request.last_name or not request.email:
            return error_response("First name, last name, and email are required", 400)
        
        # Send email to business
        mail_obj = Send_Mail()
        email_sent = await mail_obj.send_contact_form_email(
            first_name=request.first_name,
            last_name=request.last_name,
            customer_email=request.email,
            customer_message=request.message,
            recipient_email=BUSINESS_EMAIL
        )
        
        if not email_sent:
            logging.error(f"Failed to send contact form email from {request.email}")
            return error_response("Failed to send message. Please try again.", 500)
        
        logging.info(f" Contact form submitted by {request.first_name} {request.last_name} ({request.email})")
        
        return JSONResponse({
            "success": True,
            "message": "Thank you for contacting us! We'll get back to you soon."
        })
        
    except Exception as e:
        logging.error(f"Error processing contact form: {e}")
        traceback.print_exc()
        return error_response("Failed to submit contact form", 500)