import json
import logging
import os
import traceback
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, HTTPException, Query, File, UploadFile, Form
from fastapi.responses import JSONResponse

from src.api.base_models import (
    UserLogin,
    UserRegister,
    LoginResponse,
    BusinessDetailsRequest
)
from src.utils.db import PGDB
from src.utils.mail_management import Send_Mail
from src.utils.jwt_utils import create_access_token
from src.utils.utils import get_current_user, generate_presigned_url

load_dotenv()

router = APIRouter()
mail_obj = Send_Mail()
db = PGDB()

# ==================== HELPER ====================
def error_response(message, status_code=400):
    return JSONResponse(
        status_code=status_code,
        content={"error": message}
    )

# ==================== USER AUTH ENDPOINTS ====================
@router.post("/register")
def register_user(user: UserRegister):
    """
    Register a new user account.
    Email and username will be converted to lowercase.
    User will be created with is_admin = False by default.
    """
    user_dict = user.dict()
    user_dict["email"] = user_dict["email"].strip().lower()
    user_dict["username"] = user_dict["username"].strip().lower()
    user_dict['is_admin'] = False
    try:
        db.register_user(user_dict)
        return JSONResponse(status_code=201, content={"message": "You are registered successfully."})
    except ValueError as ve:
        return error_response(status_code=400, message=str(ve))
    except Exception as e:
        traceback.print_exc()
        return error_response(status_code=500, message=f"Registration failed: {str(e)}")

@router.post("/login")
def login_user(user: UserLogin):
    """
    Login with email and password.
    Returns JWT access token and user information.
    
    Includes onboarding logic:
    - If is_check = true: onboard = true (user has submitted business details, can proceed)
    - If is_check = false: onboard = false (user needs to submit business details)
    """
    try:
        logging.info(f"Login request received")
        logging.info(f"📧 Email: {user.email}")
        logging.info(f"🔑 Password length: {len(user.password) if user.password else 0}")
        
        user_dict = {
            "email": user.email,
            "password": user.password
        }
        logging.info(f"📦 User dict created: {user_dict}")
        user_dict["email"] = user_dict["email"].strip().lower()
        
        result = db.login_user(user_dict)
        if not result:
            logging.warning(f"❌ Login failed for {user_dict['email']}")
            return error_response("Invalid username or password", status_code=422)
        
        logging.info(f"✅ Login successful for {user_dict['email']}")
        
        # Check if user has submitted business details to determine onboarding status
        user_id = result["id"]
        is_check = result.get("is_check", False)
        
        # Set onboard flag based on is_check
        # onboard = true -> business details submitted (is_check = true, user can proceed)
        # onboard = false -> business details NOT submitted (is_check = false, user needs to submit)
        onboard = is_check
        
        logging.info(f"👤 User {user_id} is_check: {is_check}, onboard: {onboard}")
        
        # Format user data for JSON response
        user_data = {
            "id": result["id"],
            "username": result.get("username", ""),
            "email": result["email"],
            "first_name": result.get("first_name"),
            "last_name": result.get("last_name"),
            "created_at": result["created_at"].isoformat() if hasattr(result.get("created_at"), 'isoformat') else str(result.get("created_at")),
            "is_admin": result.get("is_admin", False),
            "role": result.get("role", "user")
        }
        
        token = create_access_token({"sub": str(result["id"])})
        
        response = {
            "access_token": token,
            "token_type": "bearer",
            "user": user_data,
            "onboard": onboard
        }
        
        logging.info(f"📤 Login response: {response}")
        
        return JSONResponse(
            status_code=200,
            content=response
        )
    except ValueError as ve:
        logging.error(f"❌ ValueError during login: {str(ve)}")
        return error_response(str(ve), status_code=422)
    except Exception as e:
        logging.error(f"❌ Error during login: {str(e)}")
        traceback.print_exc()
        return error_response(f"Internal server error: {str(e)}", status_code=500)


@router.post("/submit-business-details")
async def submit_business_details(
    details: BusinessDetailsRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Submit business details after signup.
    Sends email to admin with user's business information for review.
    User must be authenticated to submit details.
    
    After successful submission, marks user's is_check flag as true,
    which changes the onboard flag to true on next login.
    """
    try:
        # Get admin email from environment variable
        admin_email = os.getenv("ADMIN_EMAIL")
        if not admin_email:
            logging.error("ADMIN_EMAIL not configured in environment variables")
            return error_response("Admin email not configured", status_code=500)
        
        # Get user information
        user_id = current_user.get("id")
        user_email = current_user.get("email")
        user_name = current_user.get("username", "Unknown")
        
        # Send email to admin with business details
        email_sent = await mail_obj.send_business_details_to_admin(
            user_email=user_email,
            user_name=user_name,
            agent_name=details.agent_name,
            business_name=details.business_name,
            business_email=details.business_email,
            phone_number=details.phone_number,
            industry=details.industry,
            language=details.language,
            admin_email=admin_email
        )
        
        if not email_sent:
            logging.error(f"Failed to send business details email for user {user_email}")
            return error_response("Failed to send business details. Please try again.", status_code=500)
        
        # Mark business details as submitted
        db.mark_business_details_submitted(user_id)
        
        logging.info(f"✅ Business details submitted successfully for user {user_email}")
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": "Business details submitted successfully. Admin will review your request."
            }
        )
        
    except Exception as e:
        logging.error(f"Error submitting business details: {str(e)}")
        traceback.print_exc()
        return error_response(f"Failed to submit business details: {str(e)}", status_code=500)


# ==================== USER PROFILE ENDPOINT ====================
@router.get("/user-profile")
async def get_user_profile(current_user: dict = Depends(get_current_user)):
    """
    Get user profile information.
    Returns user's name, email, and phone number.
    """
    try:
        user_id = current_user.get("id")
        username = current_user.get("username", "")
        email = current_user.get("email", "")
        is_admin = current_user.get("is_admin", False)
        
        # Get phone number from agent if available
        phone_number = None
        try:
            if is_admin:
                agents = db.get_agents_by_admin(user_id)
            else:
                agents = db.get_agents_for_user(user_id)
            
            if agents and len(agents) > 0:
                # Get phone number from first agent
                phone_number = agents[0].get("phone_number")
        except Exception as e:
            logging.warning(f"⚠️ Error fetching agent phone number: {e}")
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "data": {
                    "username": username,
                    "email": email,
                    "phone_number": phone_number
                }
            }
        )
        
    except Exception as e:
        logging.error(f"Error fetching user profile: {str(e)}")
        traceback.print_exc()
        return error_response("Failed to fetch user profile", 500)


@router.get("/dashboard/overview")
async def get_dashboard_overview(current_user: dict = Depends(get_current_user)):
    """
    Get dashboard overview for user.
    Returns:
    - Agent status (ON/OFF and currently handling calls)
    - Today's activity (total calls, missed calls)
    - Plan usage (used minutes, total minutes, percentage, reset date)
    """
    try:
        user_id = current_user["id"]
        is_admin = current_user.get("is_admin", False)
        
        # Get user's agents based on role
        if is_admin:
            # Admin: Get agents they created
            agents = db.get_agents_by_admin(user_id)
        else:
            # Regular user: Get agents assigned to them
            agents = db.get_agents_for_user(user_id)
        
        if not agents or len(agents) == 0:
            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "data": {
                        "agent_status": {
                            "is_active": False,
                            "status_text": "No agent configured",
                            "currently_handling": False
                        },
                        "todays_activity": {
                            "total_calls": 0,
                            "missed_calls": 0
                        },
                        "plan_usage": {
                            "used_minutes": 0,
                            "total_minutes": 0,
                            "percentage": 0,
                            "reset_in_days": 8
                        }
                    }
                }
            )
        
        # Get first agent (primary agent)
        primary_agent = agents[0]
        agent_id = primary_agent["id"]
        
        # Get agent status
        agent_status = {
            "is_active": primary_agent.get("is_active", False),
            "status_text": "AI Agent is ON" if primary_agent.get("is_active") else "AI Agent is OFF",
            "currently_handling": False  # TODO: Check if agent is currently on a call
        }
        
        # Get today's activity
        from datetime import datetime, timezone
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Get call history for today
        call_history = db.get_call_history_by_agent(agent_id, page=1, page_size=1000)
        
        total_calls_today = 0
        missed_calls_today = 0
        
        for call in call_history.get("calls", []):
            call_time = call.get("created_at")
            if call_time and call_time >= today_start:
                total_calls_today += 1
                if call.get("status") in ["unanswered", "failed", "not_attended"]:
                    missed_calls_today += 1
        
        todays_activity = {
            "total_calls": total_calls_today,
            "missed_calls": missed_calls_today
        }
        
        # Get plan usage
        allowed_minutes = primary_agent.get("allowed_minutes", 0)
        used_minutes = primary_agent.get("used_minutes", 0)
        
        percentage = 0
        if allowed_minutes > 0:
            percentage = int((used_minutes / allowed_minutes) * 100)
        
        plan_usage = {
            "used_minutes": int(used_minutes),
            "total_minutes": int(allowed_minutes),
            "percentage": percentage,
            "reset_in_days": 8  # TODO: Calculate actual reset date
        }
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "data": {
                    "agent_status": agent_status,
                    "todays_activity": todays_activity,
                    "plan_usage": plan_usage
                }
            }
        )
        
    except Exception as e:
        logging.error(f"Error fetching dashboard overview: {e}")
        traceback.print_exc()
        return error_response("Failed to fetch dashboard data", 500)

@router.get("/dashboard/plan-usage")
async def get_plan_usage(current_user: dict = Depends(get_current_user)):
    """
    Get plan usage information for the user.
    Returns:
    - Used minutes
    - Total allowed minutes
    - Usage percentage
    """
    try:
        user_id = current_user["id"]
        is_admin = current_user.get("is_admin", False)
        
        # Get user's agents based on role
        if is_admin:
            agents = db.get_agents_by_admin(user_id)
        else:
            agents = db.get_agents_for_user(user_id)
        
        if not agents or len(agents) == 0:
            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "data": {
                        "used_minutes": 0,
                        "total_minutes": 0,
                        "percentage": 0
                    }
                }
            )
        
        # Get first agent (primary agent)
        primary_agent = agents[0]
        
        # Get plan usage
        allowed_minutes = primary_agent.get("allowed_minutes", 0)
        used_minutes = primary_agent.get("used_minutes", 0)
        
        percentage = 0
        if allowed_minutes > 0:
            percentage = int((used_minutes / allowed_minutes) * 100)
        
        plan_usage = {
            "used_minutes": int(used_minutes),
            "total_minutes": int(allowed_minutes),
            "percentage": percentage
        }
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "data": plan_usage
            }
        )
        
    except Exception as e:
        logging.error(f"Error fetching plan usage: {e}")
        traceback.print_exc()
        return error_response("Failed to fetch plan usage data", 500)

# @router.post("/dashboard/toggle-agent")
# async def toggle_agent_status(current_user: dict = Depends(get_current_user)):
#     """
#     Toggle agent ON/OFF status.
#     Toggles the is_active status of the user's primary agent.
#     """
#     try:
#         user_id = current_user["id"]
#         is_admin = current_user.get("is_admin", False)
        
#         # Get user's agents based on role
#         if is_admin:
#             agents = db.get_agents_by_admin(user_id)
#         else:
#             agents = db.get_agents_for_user(user_id)
        
#         if not agents or len(agents) == 0:
#             return error_response("No agent configured", 404)
        
#         # Toggle first agent (primary agent)
#         primary_agent = agents[0]
#         agent_id = primary_agent["id"]
#         current_status = primary_agent.get("is_active", False)
#         new_status = not current_status
        
#         # Check if enabling agent: Validate available minutes
#         if new_status:
#             minutes_check = db.check_agent_minutes_available(agent_id)
#             if not minutes_check["available"]:
#                 used = minutes_check["used_minutes"]
#                 allowed = minutes_check["allowed_minutes"]
#                 return error_response(
#                     f"Insufficient minutes. Used: {used:.1f} / {allowed}. Please upgrade plan.",
#                     status_code=400
#                 )
        
#         # Update agent status
#         db.toggle_agent_status_for_user(user_id, agent_id, new_status)
        
#         return JSONResponse(
#             status_code=200,
#             content={
#                 "success": True,
#                 "message": f"Agent turned {'ON' if new_status else 'OFF'}",
#                 "is_active": new_status
#             }
#         )
        
#     except Exception as e:
#         logging.error(f"Error toggling agent status: {e}")
#         traceback.print_exc()
#         return error_response("Failed to toggle agent status", 500)

# @router.get("/dashboard/recent-actions")
# async def get_recent_actions(
#     current_user: dict = Depends(get_current_user),
#     limit: int = 10
# ):
#     """
#     Get recent actions/activity log for user's dashboard.
#     Returns recent events like:
#     - Appointments booked
#     - Calls handled
#     - Missed calls
#     - Agent configuration updates
#     """
#     try:
#         user_id = current_user["id"]
#         is_admin = current_user.get("is_admin", False)
        
#         # Get user's agents based on role
#         if is_admin:
#             agents = db.get_agents_by_admin(user_id)
#         else:
#             agents = db.get_agents_for_user(user_id)
        
#         agent_id = None
#         if agents and len(agents) > 0:
#             agent_id = agents[0]["id"]
        
#         actions = []
        
#         # Try to get real call history if agent exists
#         if agent_id:
#             call_history = db.get_call_history_by_agent(agent_id, page=1, page_size=limit)
            
#             for call in call_history.get("calls", []):
#                 action_type = "call"
#                 icon = "phone"
                
#                 # Determine action description based on call status
#                 if call.get("status") == "completed":
#                     description = f"AI successfully handled call regarding \"{call.get('summary', 'inquiry')}\"."
#                     icon = "phone"
#                 elif call.get("status") in ["unanswered", "failed"]:
#                     caller = call.get("caller_number", "unknown number")
#                     description = f"Missed call from {caller} (Caller hung up)."
#                     icon = "warning"
#                 else:
#                     description = f"AI transferred call to 'Support Team' queue."
#                     icon = "phone"
                
#                 # Calculate time ago
#                 created_at = call.get("created_at")
#                 time_ago = "Just now"
#                 if created_at:
#                     from datetime import datetime, timezone
#                     now = datetime.now(timezone.utc)
#                     if hasattr(created_at, 'tzinfo') and created_at.tzinfo is None:
#                         created_at = created_at.replace(tzinfo=timezone.utc)
                    
#                     diff = now - created_at
#                     minutes = int(diff.total_seconds() / 60)
#                     hours = int(minutes / 60)
                    
#                     if minutes < 1:
#                         time_ago = "Just now"
#                     elif minutes < 60:
#                         time_ago = f"{minutes}m ago"
#                     elif hours < 24:
#                         time_ago = f"{hours}h ago"
#                     else:
#                         days = int(hours / 24)
#                         time_ago = f"{days}d ago"
                
#                 actions.append({
#                     "type": action_type,
#                     "icon": icon,
#                     "description": description,
#                     "time_ago": time_ago,
#                     "timestamp": created_at.isoformat() if created_at else None
#                 })
        
#         # If no real actions, return demo data
#         if len(actions) == 0:
#             demo_actions = [
#                 {
#                     "type": "call",
#                     "icon": "phone",
#                     "description": "AI successfully handled call regarding \"appointment scheduling\".",
#                     "time_ago": "5m ago",
#                     "timestamp": None
#                 },
#                 {
#                     "type": "appointment",
#                     "icon": "calendar",
#                     "description": "New appointment booked: John Smith - Consultation (Tomorrow 10:00 AM).",
#                     "time_ago": "15m ago",
#                     "timestamp": None
#                 },
#                 {
#                     "type": "call",
#                     "icon": "warning",
#                     "description": "Missed call from +1 (555) 123-4567 (Caller hung up).",
#                     "time_ago": "32m ago",
#                     "timestamp": None
#                 },
#                 {
#                     "type": "call",
#                     "icon": "phone",
#                     "description": "AI successfully handled call regarding \"pricing inquiry\".",
#                     "time_ago": "1h ago",
#                     "timestamp": None
#                 },
#                 {
#                     "type": "config",
#                     "icon": "settings",
#                     "description": "AI agent configuration updated by Admin.",
#                     "time_ago": "2h ago",
#                     "timestamp": None
#                 },
#                 {
#                     "type": "call",
#                     "icon": "phone",
#                     "description": "AI transferred call to 'Support Team' queue.",
#                     "time_ago": "3h ago",
#                     "timestamp": None
#                 },
#                 {
#                     "type": "appointment",
#                     "icon": "calendar",
#                     "description": "Appointment rescheduled: Sarah Johnson - Follow-up (Next Monday 2:00 PM).",
#                     "time_ago": "4h ago",
#                     "timestamp": None
#                 },
#                 {
#                     "type": "call",
#                     "icon": "phone",
#                     "description": "AI successfully handled call regarding \"service availability\".",
#                     "time_ago": "5h ago",
#                     "timestamp": None
#                 }
#             ]
#             return JSONResponse(
#                 status_code=200,
#                 content={
#                     "success": True,
#                     "actions": demo_actions[:limit],
#                     "is_demo": True
#                 }
#             )
        
#         # Add agent configuration update placeholder if we have real actions
#         if len(actions) > 2:
#             actions.insert(2, {
#                 "type": "config",
#                 "icon": "settings",
#                 "description": "AI agent configuration updated by Admin.",
#                 "time_ago": "2h ago",
#                 "timestamp": None
#             })
        
#         return JSONResponse(
#             status_code=200,
#             content={
#                 "success": True,
#                 "actions": actions[:limit]
#             }
#         )
        
#     except Exception as e:
#         logging.error(f"Error fetching recent actions: {e}")
#         traceback.print_exc()
#         return error_response("Failed to fetch recent actions", 500)


# @router.get("/user/my-agent")
# async def get_user_agent(
#     current_user: dict = Depends(get_current_user)
# ):
#     """
#     Get agent assigned to the current user with role-based field filtering.
    
#     Role-Based Response:
#     - ADMIN: Returns all fields
#     - USER: Returns all fields EXCEPT business_hours_start, business_hours_end, allowed_minutes.
#             phone_number is included but marked as read_only.
#     """
#     try:
#         user_id = current_user["id"]
#         is_admin = current_user.get("is_admin", False)
        
#         # Get agents based on role
#         if is_admin:
#             # Admin sees agents they created
#             agents = db.get_agents_by_admin(user_id)
#         else:
#             # User sees agents assigned to them
#             agents = db.get_agents_for_user(user_id) if hasattr(db, 'get_agents_for_user') else []
            
#             # Fallback: Check if any agents have this user_id
#             if not agents:
#                 all_agents = db.get_agents_by_admin(user_id)
#                 agents = [a for a in all_agents if a.get("user_id") == user_id]
        
#         if not agents or len(agents) == 0:
#             return JSONResponse(
#                 status_code=200,
#                 content={
#                     "success": True,
#                     "data": None,
#                     "message": "No agent assigned"
#                 }
#             )
        
#         agent = agents[0]  # Primary agent
        
#         # Build response based on role
#         if is_admin:
#             # Admin gets all fields
#             agent_data = {
#                 "id": agent.get("id"),
#                 "agent_name": agent.get("agent_name"),
#                 "phone_number": agent.get("phone_number"),
#                 "system_prompt": agent.get("system_prompt"),
#                 "voice_type": agent.get("voice_type"),
#                 "language": agent.get("language"),
#                 "industry": agent.get("industry"),
#                 "owner_name": agent.get("owner_name"),
#                 "owner_email": agent.get("owner_email"),
#                 "avatar_url": agent.get("avatar_url"),
#                 "is_active": agent.get("is_active", False),
#                 "business_hours_start": str(agent.get("business_hours_start", "")) if agent.get("business_hours_start") else None,
#                 "business_hours_end": str(agent.get("business_hours_end", "")) if agent.get("business_hours_end") else None,
#                 "allowed_minutes": agent.get("allowed_minutes", 0),
#                 "used_minutes": agent.get("used_minutes", 0),
#                 "user_id": agent.get("user_id"),
#                 "can_edit_all_fields": True
#             }
#         else:
#             # User gets limited fields - NO business_hours, NO allowed_minutes
#             # phone_number is visible but read_only
#             agent_data = {
#                 "id": agent.get("id"),
#                 "agent_name": agent.get("agent_name"),
#                 "phone_number": agent.get("phone_number"),  # Visible but read-only
#                 "phone_number_read_only": True,  # Flag to indicate read-only
#                 "system_prompt": agent.get("system_prompt"),
#                 "voice_type": agent.get("voice_type"),
#                 "language": agent.get("language"),
#                 "industry": agent.get("industry"),
#                 "owner_name": agent.get("owner_name"),
#                 "owner_email": agent.get("owner_email"),
#                 "avatar_url": agent.get("avatar_url"),
#                 "is_active": agent.get("is_active", False),
#                 # Note: business_hours and allowed_minutes are NOT included for regular users
#                 "can_edit_all_fields": False,
#                 "editable_fields": [
#                     "agent_name",
#                     "system_prompt", 
#                     "voice_type",
#                     "language",
#                     "industry",
#                     "owner_name",
#                     "owner_email",
#                     "avatar"
#                 ]
#             }
        
#         return JSONResponse(
#             status_code=200,
#             content={
#                 "success": True,
#                 "data": agent_data,
#                 "is_admin": is_admin
#             }
#         )
        
#     except Exception as e:
#         logging.error(f"Error fetching user agent: {e}")
#         traceback.print_exc()
#         return error_response("Failed to fetch agent details", 500)


@router.put("/user/my-agent")
async def update_user_agent(
    agent_name: str = Form(None),
    system_prompt: str = Form(None),
    voice_type: str = Form(None),
    language: str = Form(None),
    industry: str = Form(None),
    owner_name: str = Form(None),
    owner_email: str = Form(None),
    phone_number: str = Form(None),
    business_hours_start: str = Form(None),
    business_hours_end: str = Form(None),
    allowed_minutes: int = Form(None),
    avatar: UploadFile = File(None),
    current_user: dict = Depends(get_current_user)
):
    """
    Update agent assigned to the current user.
    
    Role-Based Restrictions:
    - ADMIN: Full access to all fields (phone_number, business_hours, allowed_minutes)
    - USER: Can edit agent_name, system_prompt, voice_type, language, industry, 
            owner_name, owner_email, avatar. CANNOT edit phone_number, business_hours, 
            allowed_minutes.
    """
    try:
        from src.utils.utils import hetzner_storage, serialize_agent_data
        from src.api.router import add_presigned_urls_to_agent
        
        user_id = current_user["id"]
        is_admin = current_user.get("is_admin", False)
        
        # Get user's agent based on role
        if is_admin:
            agents = db.get_agents_by_admin(user_id)
        else:
            agents = db.get_agents_for_user(user_id) if hasattr(db, 'get_agents_for_user') else []
            
            # Fallback: Check if any agents have this user_id
            if not agents:
                all_agents = db.get_agents_by_admin(user_id)
                agents = [a for a in all_agents if a.get("user_id") == user_id]
        
        if not agents or len(agents) == 0:
            return error_response("No agent assigned to update", 404)
        
        agent = agents[0]  # Primary agent
        agent_id = agent.get("id")
        
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
        
        # ADMIN-ONLY fields: phone_number, business_hours, allowed_minutes
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
        else:
            # Non-admin user trying to update restricted fields - return error
            if phone_number is not None:
                return error_response("You don't have permission to update phone number", 403)
            if business_hours_start is not None or business_hours_end is not None:
                return error_response("You don't have permission to update business hours", 403)
            if allowed_minutes is not None:
                return error_response("You don't have permission to update allowed minutes", 403)
        
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
                old_avatar_key = agent.get("avatar_url")
                if old_avatar_key:
                    hetzner_storage.delete_avatar(old_avatar_key)
                
                updates["avatar_url"] = new_avatar_key
                logging.info(f"✅ Avatar updated: {new_avatar_key}")
                
            except Exception as e:
                logging.error(f"❌ Avatar upload failed: {e}")
                return error_response("Failed to upload avatar", 500)
        
        if not updates:
            return error_response("No fields to update", 400)
        
        # Update agent - use admin_id from the agent record
        admin_id = agent.get("admin_id")
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
        
    except Exception as e:
        logging.error(f"Error updating user agent: {e}")
        traceback.print_exc()
        return error_response("Failed to update agent", 500)


# ==================== HELPER FOR CALL LOGS ====================
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




