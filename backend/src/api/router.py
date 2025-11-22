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
    ForgotPasswordRequest
)
from src.utils.db import PGDB 
from src.utils.mail_management import Send_Mail
from src.utils.jwt_utils import create_access_token
from src.utils.utils import (
    get_current_user, 
    add_call_event, 
    fetch_and_store_transcript, 
    fetch_and_store_recording, 
    calculate_duration, 
    check_if_answered, 
    hetzner_storage,
    generate_presigned_url,
    get_s3_client
)
from livekit import api
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

# ==================== AUTH ENDPOINTS ====================
@router.post("/register")
def register_user(user: UserRegister):
    user_dict = user.dict()
    user_dict["email"] = user_dict["email"].strip().lower()
    user_dict["username"] = user_dict["username"].strip().lower()
    user_dict['is_admin'] = True
    try:
        db.register_user(user_dict)
        return JSONResponse(status_code=201, content={"message": "You are registered successfully."})
    except ValueError as ve:
        return error_response(status_code=400, message=str(ve))
    except Exception as e:
        traceback.print_exc()
        return error_response(status_code=500, message=f"Registration failed: {str(e)}")

@router.post("/login", response_model=LoginResponse)
def login_user(user: UserLogin):
    try:
        user_dict = {
            "email": user.email,
            "password": user.password
        }
        logging.info(f"User dict: {user_dict}")
        user_dict["email"] = user_dict["email"].strip().lower()
        result = db.login_user(user_dict)
        if not result:
            return error_response("Invalid username or password", status_code=422)
        
        token = create_access_token({"sub": str(result["id"])})
        return {
            "access_token": token,
            "token_type": "bearer",
            "user": result
        }
    except ValueError as ve:
        return error_response(str(ve), status_code=422)
    except Exception as e:
        logging.error(f"Error during login: {str(e)}")
        return error_response(f"Internal server error: {str(e)}", status_code=500)

# ==================== CALL STATUS ====================
@router.get("/call-status/{call_id}")
async def get_call_status(call_id: str):
    """Optimized status check with proper connection handling"""
    try:
        conn = db.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT status, created_at, ended_at, duration, started_at
                    FROM call_history 
                    WHERE call_id = %s
                """, (call_id,))
                row = cursor.fetchone()
        finally:
            db.release_connection(conn)
        
        if not row:
            return JSONResponse(
                status_code=404,
                content={"status": "not_found", "is_final": True}
            )
        
        current_status, created_at, ended_at, duration, started_at = row
        
        # Normalize status
        if current_status not in {"initialized", "dialing", "connected", "completed", "unanswered"}:
            STATUS_MAP = {
                "initiated": "initialized",
                "in_progress": "connected",
                "failed": "unanswered",
                "not_attended": "unanswered"
            }
            current_status = STATUS_MAP.get(current_status, "initialized")
        
        # Calculate elapsed time
        time_elapsed = 0
        if created_at:
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
            time_elapsed = (datetime.now(timezone.utc) - created_at).total_seconds()
        
        is_final = current_status in {"completed", "unanswered"}
        
        response = {
            "status": current_status,
            "message": {
                "initialized": "Initializing...",
                "dialing": "Dialing...",
                "connected": "Call in progress",
                "completed": "Call completed",
                "unanswered": "Call not answered"
            }.get(current_status, current_status),
            "time_elapsed": round(time_elapsed, 1),
            "is_final": is_final
        }
        
        if is_final and duration:
            response["duration"] = round(duration, 1)
        
        if started_at:
            response["started_at"] = started_at.isoformat()
        if ended_at:
            response["ended_at"] = ended_at.isoformat()
        
        return JSONResponse(response)
    except Exception as e:
        logging.error(f"get_call_status error: {e}")
        return JSONResponse(
            {"status": "error", "message": str(e), "is_final": True},
            status_code=500
        )

# ==================== CALL HISTORY ====================
@router.get("/call-history")
async def get_user_call_history(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, le=100),
    user=Depends(get_current_user)
):
    """Get call history for all agents belonging to the logged-in admin"""
    try:
        history = db.get_call_history_by_admin(user["id"], page, page_size)

        calls = []
        for call in history.get("calls", []):
            call_data = {**call}
            
            # Format timestamps
            for field in ["created_at", "started_at", "ended_at"]:
                if call.get(field):
                    call_data[field] = call[field].isoformat() if hasattr(call[field], 'isoformat') else str(call[field])
            
            # Calculate display duration if not available
            if not call_data.get("duration") and call.get("started_at") and call.get("ended_at"):
                try:
                    start = call["started_at"] if isinstance(call["started_at"], datetime) else datetime.fromisoformat(str(call["started_at"]))
                    end = call["ended_at"] if isinstance(call["ended_at"], datetime) else datetime.fromisoformat(str(call["ended_at"]))
                    call_data["duration"] = round((end - start).total_seconds(), 1)
                except:
                    call_data["duration"] = 0
            
            # Parse transcript from JSONB
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
                                text = " ".join(msg.get("content", [])) if isinstance(msg.get("content"), list) else str(msg.get("content"))
                                lines.append(f"{speaker}: {text}")
                        transcript_text = "\n".join(lines)
                except Exception as e:
                    logging.warning(f"Transcript parse error for {call.get('id')}: {e}")
            
            call_data["transcript_text"] = transcript_text
            call_data["has_recording"] = bool(call.get("recording_blob"))
            
            # 🔥 ADD PRESIGNED URLS
            call_data = add_presigned_urls_to_call(call_data)
            
            calls.append(call_data)

        pagination = history.get("pagination") or {
            "page": history.get("page", page),
            "page_size": history.get("page_size", page_size),
            "total": history.get("total", len(calls)),
            "completed_calls": history.get("completed_calls", 0),
            "not_completed_calls": history.get("not_completed_calls", 0),
        }

        return JSONResponse(content=jsonable_encoder({
            "user_id": user["id"],
            "pagination": pagination,
            "calls": calls
        }))

    except Exception as e:
        logging.error(f"Error fetching history: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

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
    Save transcript and recording metadata after call ends.
    
    This receives:
    - transcript_blob: Path in Hetzner bucket (e.g., "transcripts/xxx.json")
    - recording_blob: Path in Hetzner bucket (e.g., "recordings/xxx.ogg")
    
    Backend will:
    1. Store paths in DB immediately
    2. Download transcript after 5s delay → Store JSONB in DB
    3. Recording stays in Hetzner (access via presigned URL)
    """
    try:
        data = await request.json()
        
        call_id = data.get("call_id")
        transcript_blob = data.get("transcript_blob")
        recording_blob = data.get("recording_blob")
        transcript_url = data.get("transcript_url")
        recording_url = data.get("recording_url")
        
        # Save metadata (paths only)
        updates = {}
        if transcript_blob:
            updates["transcript_blob"] = transcript_blob
        if recording_blob:
            updates["recording_blob"] = recording_blob
        if transcript_url:
            updates["transcript_url"] = transcript_url
        if recording_url:
            updates["recording_url"] = recording_url
        
        if updates:
            db.update_call_history(call_id, updates)
        
        # DELAYED transcript download & DB storage (5s)
        if transcript_blob:
            async def delayed_transcript():
                await asyncio.sleep(5)
                logging.info(f"?? Downloading transcript for {call_id}")
                await fetch_and_store_transcript(call_id, None, transcript_blob)
            asyncio.create_task(delayed_transcript())
        
        # Recording stays in Hetzner - no download needed!
        # Frontend will use presigned URL to stream it
        
        return JSONResponse({"success": True})
    except Exception as e:
        logging.error(f"save_call_data error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

# ==================== RECORDING & TRANSCRIPT ACCESS ====================
@router.options("/calls/{call_id}/recording/stream")
async def stream_call_recording_options(call_id: str):
    return Response(
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Range, Content-Type, Authorization, Accept",
            "Access-Control-Max-Age": "3600"
        }
    )

@router.get("/calls/{call_id}/recording/stream")
async def stream_call_recording(
    call_id: str, 
    user=Depends(get_current_user),
    request: Request = None
):
    """
    Stream recording from Hetzner bucket using presigned URL redirect.
    
    Flow:
    1. Get recording_blob path from DB
    2. Generate presigned URL (valid 1 hour)
    3. Redirect browser to presigned URL
    """
    try:
        conn = db.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT recording_blob, agent_id
                    FROM call_history
                    WHERE call_id = %s
                """, (call_id,))
                row = cursor.fetchone()
        finally:
            db.release_connection(conn)
        
        if not row or not row[0]:
            raise HTTPException(status_code=404, detail="Recording not found")
        
        recording_blob, agent_id = row
        
        # Verify ownership
        agent = db.get_agent_by_id(agent_id)
        if not agent or agent["admin_id"] != user["id"]:
            raise HTTPException(status_code=403, detail="Access denied")
        
        # 🔥 Generate presigned URL (valid for 1 hour)
        presigned_url = generate_presigned_url(recording_blob, expiration=3600)
        
        if not presigned_url:
            raise HTTPException(status_code=500, detail="Failed to generate access URL")
        
        # Redirect to presigned URL
        return RedirectResponse(url=presigned_url, status_code=302)
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error streaming recording: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/calls/{call_id}/transcript")
async def get_call_transcript(call_id: str, user=Depends(get_current_user)):
    """
    Get transcript for a specific call.
    
    Transcript is stored as JSONB in database (downloaded from Hetzner after call ends).
    """
    try:
        conn = db.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT transcript, agent_id
                    FROM call_history
                    WHERE call_id = %s
                """, (call_id,))
                row = cursor.fetchone()
        finally:
            db.release_connection(conn)
        
        if not row:
            raise HTTPException(status_code=404, detail="Call not found")
        
        transcript, agent_id = row
        
        # Verify ownership
        agent = db.get_agent_by_id(agent_id)
        if not agent or agent["admin_id"] != user["id"]:
            raise HTTPException(status_code=403, detail="Access denied")
        
        if not transcript:
            raise HTTPException(status_code=404, detail="Transcript not available yet")
        
        return JSONResponse({"transcript": transcript})
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error fetching transcript: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== AGENT MANAGEMENT ====================
@router.get("/agents/{agent_id}/calls")
async def get_agent_call_history(
    agent_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(10, le=100),
    user=Depends(get_current_user)
):
    """Get call history for a specific agent"""
    try:
        agent = db.get_agent_by_id(agent_id)
        if not agent or agent["admin_id"] != user["id"]:
            raise HTTPException(status_code=403, detail="Access denied")
        
        history = db.get_call_history_by_agent(agent_id, page, page_size)
        
        calls = []
        for call in history.get("calls", []):
            call_data = {**call}
            
            # Format timestamps
            for field in ["created_at", "started_at", "ended_at"]:
                if call.get(field):
                    call_data[field] = call[field].isoformat()
            
            # Calculate duration if missing
            if not call_data.get("duration") and call.get("started_at") and call.get("ended_at"):
                try:
                    start = call["started_at"] if isinstance(call["started_at"], datetime) else datetime.fromisoformat(str(call["started_at"]))
                    end = call["ended_at"] if isinstance(call["ended_at"], datetime) else datetime.fromisoformat(str(call["ended_at"]))
                    call_data["duration"] = round((end - start).total_seconds(), 1)
                except:
                    call_data["duration"] = 0
            
            # Parse transcript
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
                                text = " ".join(msg.get("content", [])) if isinstance(msg.get("content"), list) else str(msg.get("content"))
                                lines.append(f"{speaker}: {text}")
                        transcript_text = "\n".join(lines)
                except Exception as e:
                    logging.warning(f"Transcript parse error: {e}")
            
            call_data["transcript_text"] = transcript_text
            call_data["has_recording"] = bool(call.get("recording_blob"))
            
            # 🔥 ADD PRESIGNED URLS
            call_data = add_presigned_urls_to_call(call_data)
            
            calls.append(call_data)
        
        return JSONResponse(content=jsonable_encoder({
            "success": True,
            "agent_id": agent_id,
            "agent_name": agent["agent_name"],
            "phone_number": agent["phone_number"],
            "pagination": {
                "page": history["page"],
                "page_size": history["page_size"],
                "total": history["total"],
                "completed_calls": history["completed_calls"],
                "not_completed_calls": history["not_completed_calls"]
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
    """Fetch agent configuration by phone number"""
    try:
        agent = db.get_agent_by_phone(phone_number)
        if not agent:
            raise HTTPException(status_code=404, detail="Agent not found")
        
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
async def new_call(phone_number: str = Query(...), call_id: str = Query(...)):
    """Initialize call history record"""
    try:
        agent = db.get_agent_by_phone(phone_number)
        if not agent:
            raise HTTPException(status_code=404, detail="Agent not found")
        
        # 🔥 FIX: Actually insert the call history record
        db.insert_call_history(
            agent_id=agent["agent_id"],
            call_id=call_id,
            status="initialized"
        )
        
        return JSONResponse({
            "success": True,
            "message": "Call history initialized",
            "agent_id": agent["agent_id"]  # Return agent_id for verification
        })
    except Exception as e:
        logging.error(f"Error initializing call: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/analytics")
async def get_dashboard_analytics(current_user: dict = Depends(get_current_user)):
    """Get overall dashboard analytics"""
    try:
        user_id = current_user["id"]
        analytics = db.get_admin_dashboard_analytics(user_id)
        
        # 🔥 ADD PRESIGNED URLS TO TOP AGENTS
        for agent in analytics.get("top_agents", []):
            if agent.get("avatar_url"):
                agent["avatar_presigned_url"] = generate_presigned_url(agent["avatar_url"], expiration=86400)
        
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
    """Get paginated list of all agents with call statistics"""
    try:
        user_id = current_user["id"]
        result = db.get_agents_with_call_stats(user_id, page, page_size)
        
        # 🔥 ADD PRESIGNED URLS TO ALL AGENTS
        for agent in result.get("agents", []):
            add_presigned_urls_to_agent(agent)
        
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
    """Get detailed agent information with paginated call history"""
    try:
        user_id = current_user["id"]
        agent_detail = db.get_agent_detail_with_calls(
            agent_id, user_id, calls_page, calls_page_size
        )
        
        if not agent_detail:
            return error_response("Agent not found", 404)
        
        # 🔥 ADD PRESIGNED URL TO AGENT
        add_presigned_urls_to_agent(agent_detail)
        
        # 🔥 ADD PRESIGNED URLS TO CALLS
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
    voice_type: str = Form(...),
    language: str = Form("en"),
    industry: str = Form(None),
    owner_name: str = Form(None),
    avatar: UploadFile = File(None),
    current_user: dict = Depends(get_current_user)
):
    """Create a new agent with optional avatar image"""
    try:
        user_id = current_user["id"]
        
        # Check if phone number already exists
        existing = db.get_agent_by_phone(phone_number)
        if existing:
            return error_response("Phone number already in use", 400)
        
        # Upload avatar if provided
        avatar_key = None  # Store OBJECT KEY, not URL
        if avatar and avatar.filename:
            # Validate file
            allowed_extensions = {'jpg', 'jpeg', 'png', 'gif', 'webp'}
            file_extension = avatar.filename.split('.')[-1].lower()
            
            if file_extension not in allowed_extensions:
                return error_response(
                    f"Invalid file type. Allowed: {', '.join(allowed_extensions)}", 
                    400
                )
            
            # Validate file size (max 5MB)
            content = await avatar.read()
            if len(content) > 5 * 1024 * 1024:
                return error_response("File too large. Maximum size: 5MB", 400)
            
            # Upload to Hetzner
            try:
                avatar_key = hetzner_storage.upload_avatar(content, file_extension)
                logging.info(f"✅ Avatar uploaded with key: {avatar_key}")
            except Exception as e:
                logging.error(f"❌ Avatar upload failed: {e}")
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
            "avatar_url": avatar_key,  # Store OBJECT KEY (e.g., "avatars/uuid.jpg")
            "admin_id": user_id
        }
        
        # Save to database
        agent = db.create_agent_with_voice_type(agent_data)
        
        # Format timestamps
        if agent.get("created_at"):
            agent["created_at"] = agent["created_at"].isoformat()
        if agent.get("updated_at"):
            agent["updated_at"] = agent["updated_at"].isoformat()
        
        # 🔥 ADD PRESIGNED URL FOR RESPONSE
        add_presigned_urls_to_agent(agent)
        
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
    avatar: UploadFile = File(None),
    current_user: dict = Depends(get_current_user)
):
    """Update agent details with optional new avatar"""
    try:
        user_id = current_user["id"]
        
        # Get existing agent
        existing_agent = db.get_agent_by_id(agent_id)
        if not existing_agent or existing_agent["admin_id"] != user_id:
            return error_response("Agent not found or unauthorized", 404)
        
        # Build updates dict
        updates = {}
        if agent_name is not None:
            updates["agent_name"] = agent_name
        if phone_number is not None:
            updates["phone_number"] = phone_number
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
                # Upload new avatar
                new_avatar_key = hetzner_storage.upload_avatar(content, file_extension)
                
                # Delete old avatar if exists
                old_avatar_key = existing_agent.get("avatar_url")
                if old_avatar_key:
                    hetzner_storage.delete_avatar(old_avatar_key)
                
                updates["avatar_url"] = new_avatar_key
                logging.info(f"✅ Avatar updated: {new_avatar_key}")
                
            except Exception as e:
                logging.error(f"❌ Avatar upload failed: {e}")
                return error_response("Failed to upload avatar", 500)
        
        if not updates:
            return error_response("No fields to update", 400)
        
        # Update agent
        result = db.update_agent_with_voice_type(agent_id, user_id, updates)
        
        if not result:
            return error_response("Update failed", 500)
        
        # Format timestamps
        if result.get("created_at"):
            result["created_at"] = result["created_at"].isoformat()
        if result.get("updated_at"):
            result["updated_at"] = result["updated_at"].isoformat()
        
        # 🔥 ADD PRESIGNED URL
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
                logging.info(f"🗑️ Avatar deleted for agent {agent_id}")
            except Exception as e:
                logging.warning(f"?? Could not delete avatar: {e}")
        
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

# ==================== CALL DETAILS ====================
@router.get("/calls/{call_id}")
async def get_call_details(
    call_id: str,
    agent_id: Optional[int] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    """Get complete call details including metadata"""
    try:
        user_id = current_user["id"]
        
        call = db.get_call_by_id(call_id, agent_id)
        
        if not call:
            return error_response("Call not found", 404)
        
        # Verify ownership
        agent = db.get_agent_by_id(call["agent_id"])
        if not agent or agent["admin_id"] != user_id:
            return error_response("Unauthorized", 403)
        
        # 🔥 ADD PRESIGNED URLS
        call = add_presigned_urls_to_call(call)
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "data": call
            }
        )
    except Exception as e:
        logging.error(f"Error fetching call details: {e}")
        return error_response("Failed to fetch call details", 500)

# ==================== LIVEKIT WEBHOOK ====================
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

        # Handle room end
        if event in ["room_finished", "participant_left"]:
            await asyncio.sleep(0.5)
            
            conn = db.get_connection()
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT status, events_log, started_at, created_at
                        FROM call_history WHERE call_id = %s
                    """, (call_id,))
                    row = cursor.fetchone()
            finally:
                db.release_connection(conn)

            if not row:
                return JSONResponse({"message": "Call not found"})

            current_status, events_log, db_started_at, created_at = row
            
            # Skip if already final
            if current_status in {"completed", "unanswered"}:
                # Just update duration
                started = db_started_at or created_at
                ended = datetime.now(timezone.utc)
                duration = (ended - started).total_seconds() if started else 0
                
                db.update_call_history(call_id, {
                    "duration": max(0, duration),
                    "ended_at": ended
                })
                return JSONResponse({"message": "Duration updated"})

            # Determine final status
            answered = check_if_answered(events_log)
            final_status = "completed" if answered else "unanswered"
            
            started = db_started_at or created_at
            ended = datetime.now(timezone.utc)
            duration = (ended - started).total_seconds() if (answered and started) else 0

            db.update_call_history(call_id, {
                "status": final_status,
                "duration": max(0, duration),
                "ended_at": ended,
                "started_at": started
            })
            
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

# ==================== OWNER FILTER ====================
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
        
        # 🔥 ADD PRESIGNED URLS
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
    API for LiveKit agent to book an appointment
    """
    try:
        data = await request.json()
        
        user_id = data.get("user_id")
        appointment_date = data.get("appointment_date") 
        start_time = data.get("start_time")
        end_time = data.get("end_time")
        attendee_name = data.get("attendee_name", "Valued Customer")
        title = data.get("title", "Appointment")
        description = data.get("description", "")
        organizer_name = data.get("organizer_name")
        organizer_email = data.get("organizer_email")
        
        if not all([user_id, appointment_date, start_time, end_time, organizer_email]):
            return error_response("Missing required fields", status_code=400)
        
        has_conflict = db.check_appointment_conflict(
            user_id=user_id,
            appointment_date=appointment_date,
            start_time=start_time,
            end_time=end_time
        )
        
        if has_conflict:
            return JSONResponse(
                status_code=409,
                content={
                    "success": False,
                    "message": "Time slot already booked",
                    "conflict": True
                }
            )
        
        appointment_id = db.create_appointment(
            user_id=user_id,
            appointment_date=appointment_date,
            start_time=start_time,
            end_time=end_time,
            attendee_name=attendee_name,
            attendee_email=organizer_email,
            title=title,
            description=description
        )
        
        email_sent = await mail_obj.send_email_with_calendar_event(
            attendee_email=organizer_email,
            attendee_name=organizer_name,
            appointment_date=appointment_date,
            start_time=start_time,
            end_time=end_time,
            title=title,
            description=description,
            organizer_name=organizer_name,
            organizer_email=organizer_email
        )
        
        return JSONResponse({
            "success": True,
            "appointment_id": appointment_id,
            "email_sent": email_sent,
            "message": "Appointment booked successfully"
        })
        
    except Exception as e:
        logging.error(f"Error booking appointment: {e}")
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
        
        frontend_url = os.getenv("FRONTEND_URL", "https://munif-agent.vercel.app")
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
    


@router.post("/admin/bulk-upload-voices")
async def bulk_upload_voices(
    voice_doc: UploadFile = File(..., description="Text document with voice metadata"),
    audio_files: List[UploadFile] = File(..., description="Audio files (.mp3)"),
    current_user: dict = Depends(get_current_user)
):
    """
    ONE-TIME BULK UPLOAD: Parse voice document + upload audio files + save to DB.
    """
    import tempfile
    import shutil
    import re
    from pathlib import Path
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        # ==================== STEP 1: PARSE DOCUMENT ====================
        logger.info("📋 Step 1: Parsing voice document...")
        doc_content = (await voice_doc.read()).decode('utf-8')
        
        # Language mapping
        LANGUAGE_TO_COUNTRY = {
            "en": "US", "de": "DE", "fr": "FR",
            "nl": "NL", "it": "IT", "es": "ES"
        }
        
        language_patterns = {
            'english': 'en', 'german': 'de', 'french': 'fr',
            'dutch': 'nl', 'italian': 'it', 'spanish': 'es'
        }
        
        voices = []
        current_language = None
        current_voice = {}
        
        for line in doc_content.split('\n'):
            line = line.strip()
            
            # Skip empty lines
            if not line:
                continue
            
            # Detect language section
            for lang_name, lang_code in language_patterns.items():
                if lang_name.lower() in line.lower() and ('voice' in line.lower() or 'agent' in line.lower()):
                    current_language = lang_code
                    logger.info(f"🌐 Found language section: {lang_name.upper()} ({lang_code})")
                    break
            
            # 🔥 FIX: Match ANY "Name:" pattern (not just "First Name:", "Second Name:", etc.)
            if re.match(r'^(First\s+|Second\s+|Third\s+|Fourth\s+)?Name:\s*.+', line, re.IGNORECASE):
                # Save previous voice if complete
                if current_voice and all(k in current_voice for k in ['voice_name', 'gender', 'voice_id', 'audio_filename']):
                    voices.append(current_voice)
                    logger.info(f"  ✓ Added: {current_voice['voice_name']}")
                
                # Start new voice
                name = re.sub(r'^(First\s+|Second\s+|Third\s+|Fourth\s+)?Name:\s*', '', line, flags=re.IGNORECASE).strip()
                current_voice = {
                    'voice_name': name,
                    'language': current_language,
                    'country_code': LANGUAGE_TO_COUNTRY.get(current_language, 'US')
                }
            
            elif line.startswith('Gender:'):
                gender = line.split(':', 1)[1].strip().lower()
                current_voice['gender'] = gender
            
            elif line.startswith('ID:'):
                voice_id = line.split(':', 1)[1].strip()
                current_voice['voice_id'] = voice_id
            
            elif line.startswith('Audio:'):
                audio_filename = line.split(':', 1)[1].strip()
                # Clean filename - remove extra spaces
                audio_filename = re.sub(r'\s+', ' ', audio_filename)
                current_voice['audio_filename'] = audio_filename
        
        # Add last voice
        if current_voice and all(k in current_voice for k in ['voice_name', 'gender', 'voice_id', 'audio_filename']):
            voices.append(current_voice)
            logger.info(f"  ✓ Added: {current_voice['voice_name']}")
        
        logger.info(f"✅ Parsed {len(voices)} voices from document")
        
        if not voices:
            return error_response("No voices found in document", 400)
        
        # Log parsed voices for debugging
        for v in voices:
            logger.info(f"  📝 {v['voice_name']} ({v['language']}) - {v['voice_id']}")
            logger.info(f"     Audio: {v['audio_filename']}")
        
        # ==================== STEP 2: SAVE AUDIO FILES TEMPORARILY ====================
        logger.info("💾 Step 2: Saving audio files to temp directory...")
        temp_dir = Path(tempfile.mkdtemp())
        
        audio_file_map = {}
        for audio_file in audio_files:
            if not audio_file.filename.endswith('.mp3'):
                logger.warning(f"  ⚠️ Skipping non-MP3 file: {audio_file.filename}")
                continue
            
            file_path = temp_dir / audio_file.filename
            with open(file_path, 'wb') as f:
                content = await audio_file.read()
                f.write(content)
            
            # Store both original and normalized versions
            audio_file_map[audio_file.filename] = file_path
            
            # Also add normalized version for fuzzy matching
            normalized_name = audio_file.filename.lower().replace(' ', '').replace('_', '').replace('-', '')
            audio_file_map[normalized_name] = file_path
            
            logger.info(f"  ✓ Saved: {audio_file.filename}")
        
        logger.info(f"✅ Saved {len(audio_files)} audio files")
        
        # ==================== STEP 3: MATCH VOICES WITH AUDIO FILES ====================
        logger.info("🔗 Step 3: Matching voices with audio files...")
        matched_voices = []
        
        for voice in voices:
            expected_filename = voice['audio_filename']
            matched = False
            
            # Try 1: Exact match
            if expected_filename in audio_file_map:
                voice['audio_path'] = audio_file_map[expected_filename]
                matched_voices.append(voice)
                logger.info(f"✅ Exact match: {voice['voice_name']} → {expected_filename}")
                matched = True
                continue
            
            # Try 2: Check if file exists in temp_dir
            exact_path = temp_dir / expected_filename
            if exact_path.exists():
                voice['audio_path'] = exact_path
                matched_voices.append(voice)
                logger.info(f"✅ File system match: {voice['voice_name']} → {expected_filename}")
                matched = True
                continue
            
            # Try 3: Normalize and fuzzy match
            normalized_expected = expected_filename.lower().replace(' ', '').replace('_', '').replace('-', '')
            
            if normalized_expected in audio_file_map:
                voice['audio_path'] = audio_file_map[normalized_expected]
                matched_voices.append(voice)
                actual_name = audio_file_map[normalized_expected].name
                logger.info(f"✅ Fuzzy match: {voice['voice_name']} → {actual_name}")
                matched = True
                continue
            
            # Try 4: Partial matching (contains voice_id)
            voice_id = voice.get('voice_id', '').lower()
            if voice_id:
                for filename, filepath in audio_file_map.items():
                    if voice_id in filename.lower():
                        voice['audio_path'] = filepath
                        matched_voices.append(voice)
                        logger.info(f"✅ ID match: {voice['voice_name']} → {filepath.name}")
                        matched = True
                        break
            
            if not matched:
                logger.warning(f"⚠️ No audio file found for: {voice['voice_name']} (expected: {expected_filename})")
        
        logger.info(f"✅ Matched {len(matched_voices)}/{len(voices)} voices")
        
        if not matched_voices:
            shutil.rmtree(temp_dir)
            return error_response("No audio files matched with document data", 400)
        
        # ==================== STEP 4: UPLOAD TO HETZNER ====================
        logger.info("☁️ Step 4: Uploading to Hetzner...")
        s3_client = get_s3_client()
        bucket_name = HETZNER_BUCKET_NAME
        
        uploaded_voices = []
        upload_errors = []
        
        for voice in matched_voices:
            try:
                audio_path = voice['audio_path']
                
                # Read audio file
                with open(audio_path, 'rb') as f:
                    audio_content = f.read()
                
                # Create blob path
                voice_id = voice['voice_id']
                blob_path = f"voice_samples/{voice_id}.mp3"
                
                # Upload to Hetzner
                logger.info(f"  📤 Uploading {voice['voice_name']} ({len(audio_content)} bytes)...")
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=blob_path,
                    Body=audio_content,
                    ContentType='audio/mpeg',
                    CacheControl='public, max-age=31536000'
                )
                
                voice['audio_blob_path'] = blob_path
                uploaded_voices.append(voice)
                
                logger.info(f"✅ Uploaded: {voice['voice_name']} → {blob_path}")
                
            except Exception as e:
                error_msg = f"Upload failed for {voice['voice_name']}: {str(e)}"
                logger.error(f"❌ {error_msg}")
                upload_errors.append(error_msg)
                import traceback
                traceback.print_exc()
                continue
        
        logger.info(f"✅ Uploaded {len(uploaded_voices)} files to Hetzner")
        
        if upload_errors:
            logger.error("Upload errors:")
            for err in upload_errors:
                logger.error(f"  - {err}")
        
        # ==================== STEP 5: SAVE TO DATABASE ====================
        logger.info("💾 Step 5: Saving to database...")
        saved_count = 0
        failed = []
        
        for voice in uploaded_voices:
            try:
                db.insert_voice_sample({
                    'voice_name': voice['voice_name'],
                    'voice_id': voice['voice_id'],
                    'language': voice['language'],
                    'country_code': voice['country_code'],
                    'gender': voice['gender'],
                    'audio_blob_path': voice['audio_blob_path'],
                    'duration_seconds': None
                })
                saved_count += 1
                logger.info(f"✅ Saved to DB: {voice['voice_name']}")
            except Exception as e:
                logger.error(f"❌ DB save failed for {voice['voice_name']}: {e}")
                failed.append(voice['voice_name'])
                import traceback
                traceback.print_exc()
        
        # ==================== CLEANUP ====================
        shutil.rmtree(temp_dir)
        logger.info("🧹 Cleaned up temp files")
        
        # ==================== RESPONSE ====================
        return JSONResponse({
            "success": True,
            "message": f"✅ Successfully uploaded {saved_count} voice samples!",
            "summary": {
                "parsed_from_document": len(voices),
                "matched_with_audio": len(matched_voices),
                "uploaded_to_hetzner": len(uploaded_voices),
                "saved_to_database": saved_count,
                "failed": len(failed)
            },
            "voices": [
                {
                    "name": v['voice_name'],
                    "language": v['language'],
                    "gender": v['gender'],
                    "voice_id": v['voice_id'],
                    "audio_blob_path": v.get('audio_blob_path')
                }
                for v in uploaded_voices
            ],
            "failed_voices": failed if failed else None,
            "upload_errors": upload_errors if upload_errors else None
        })
        
    except Exception as e:
        logger.error(f"❌ Bulk upload error: {e}")
        import traceback
        traceback.print_exc()
        return error_response(f"Upload failed: {str(e)}", 500)

@router.get("/voice-samples")
async def get_voice_samples(language: Optional[str] = Query(None)):
    """
    PUBLIC endpoint - Get all voice samples with presigned URLs
    No authentication required
    """
    try:
        # Get samples from database
        if language:
            samples = db.get_voice_samples_by_language(language)
        else:
            samples = db.get_all_voice_samples()
        
        # Add presigned URLs (valid for 24 hours)
        for sample in samples:
            if sample.get("audio_blob_path"):
                sample["audio_url"] = generate_presigned_url(
                    sample["audio_blob_path"],
                    expiration=86400  # 24 hours
                )
            
            # Format timestamp
            if sample.get("created_at"):
                sample["created_at"] = sample["created_at"].isoformat()
        
        # Group by language for easier frontend consumption
        grouped = {}
        for sample in samples:
            lang = sample["language"]
            if lang not in grouped:
                grouped[lang] = []
            grouped[lang].append(sample)
        
        return JSONResponse({
            "success": True,
            "total": len(samples),
            "grouped_by_language": grouped,
            "all_samples": samples
        })
        
    except Exception as e:
        logging.error(f"Error fetching voice samples: {e}")
        traceback.print_exc()
        return error_response("Failed to fetch voice samples", 500)