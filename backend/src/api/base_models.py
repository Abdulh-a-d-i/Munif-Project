from pydantic import BaseModel, EmailStr, Field
from typing import List, Optional,Dict,Literal
from datetime import datetime


### =============== auth base model ====================

class UserRegister(BaseModel):
    username: str
    email: EmailStr
    password: str

class UserLogin(BaseModel):
    email: str  # We’ll use this to accept the username
    password: str

class UserOut(BaseModel):
    id: int
    username: str
    email: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    created_at: datetime
    is_admin: bool = False
    role: Literal['admin', 'user'] = 'user'  

class LoginResponse(BaseModel):
    access_token: str
    token_type: str
    user: UserOut
    onboard: bool  

class UpdateUserProfileRequest(BaseModel):
    # user_id: int
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    current_password: Optional[str] = None
    new_password: Optional[str] = None    


class Assistant_Payload(BaseModel):
    objective: str
    context: str
    # caller_number: str
    caller_name: str
    caller_number: str
    caller_email: str
    outbound_number : str
    language : Literal['english', 'spanish']
    voice : str
    # outbound_number : str


class CallDetailsPayload(BaseModel):
    # user_id: int
    call_id: str
    voice_name : str
    # caller_email: EmailStr

class Assistant_Payload(BaseModel):
    outbound_number: str      # Phone number to dial
    caller_name: str          # Your name/company name
    caller_email: str         # Your email (for sending calendar invites)
    caller_number: str        # Your phone number
    objective: str
    context: str
    language: str 
    voice: str 



class PromptCustomizationUpdate(BaseModel):
    system_prompt: str = Field(..., min_length=10, max_length=10000)




class CreateAgentRequest(BaseModel):
    """Request model for creating a new agent"""
    agent_name: str = Field(..., min_length=1, max_length=100)
    phone_number: str = Field(..., min_length=10, max_length=20)
    system_prompt: str = Field(..., min_length=10)
    voice_type: Optional[str] = Field(None, pattern="^(male|female)$")  # Optional - defaults to None
    language: Optional[str] = Field(default="en", max_length=10)
    industry: Optional[str] = Field(default=None, max_length=50)
    owner_name: Optional[str] = Field(default=None, max_length=100)
    owner_email: Optional[EmailStr] = Field(default=None)  # NEW
    business_hours_start: Optional[str] = Field(default=None, pattern=r'^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$')  # NEW
    business_hours_end: Optional[str] = Field(default=None, pattern=r'^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$')  # NEW
    allowed_minutes: Optional[int] = Field(default=0, ge=0)  # NEW
    user_id: Optional[int] = Field(default=None, gt=0)  # NEW: Assign agent to user
    transfer_number: Optional[str] = Field(default=None, min_length=10, max_length=20)
    
    class Config:
        json_schema_extra = {
            "example": {
                "agent_name": "Customer Support Agent",
                "phone_number": "+1234567890",
                "system_prompt": "You are a helpful customer support agent...",
                "voice_type": "female",
                "language": "en",
                "industry": "healthcare",
                "owner_name": "John Doe",
                "owner_email": "john@example.com",
                "business_hours_start": "09:00",
                "business_hours_end": "17:00",
                "allowed_minutes": 500,
                "transfer_number": "+1234567890"
            }
        }


class UpdateAgentRequest(BaseModel):
    """Request model for updating an agent (all fields optional)"""
    agent_name: Optional[str] = Field(None, min_length=1, max_length=100)
    phone_number: Optional[str] = Field(None, min_length=10, max_length=20)
    system_prompt: Optional[str] = Field(None, min_length=10)
    voice_type: Optional[str] = Field(None, pattern="^(male|female)$")
    language: Optional[str] = Field(None, max_length=10)
    industry: Optional[str] = Field(None, max_length=50)
    owner_name: Optional[str] = Field(None, max_length=100)
    owner_email: Optional[EmailStr] = Field(None)  # NEW
    business_hours_start: Optional[str] = Field(None, pattern=r'^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$')  # NEW
    business_hours_end: Optional[str] = Field(None, pattern=r'^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$')  # NEW
    allowed_minutes: Optional[int] = Field(None, ge=0)  # NEW
    user_id: Optional[int] = Field(None, gt=0)  # NEW: Update user assignment
    transfer_number: Optional[str] = Field(default=None, min_length=10, max_length=20)
    
    class Config:
        json_schema_extra = {
            "example": {
                "agent_name": "Updated Agent Name",
                "voice_type": "male",
                "industry": "retail",
                "owner_name": "Jane Smith",
                "owner_email": "jane@example.com",
                "business_hours_start": "08:00",
                "business_hours_end": "18:00",
                "allowed_minutes": 1000,
                "transfer_number": "+1234567890"
            }
        }


# NEW: Model for reset minutes request
class ResetAgentMinutesRequest(BaseModel):
    """Request model for resetting agent minutes"""
    agent_id: int = Field(..., gt=0)
    
    class Config:
        json_schema_extra = {
            "example": {
                "agent_id": 5
            }
        }

class ForgotPasswordRequest(BaseModel):
    email: EmailStr

class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str = Field(..., min_length=8)


class ContactFormRequest(BaseModel):
    first_name: str
    last_name: str
    email: EmailStr
    message: Optional[str] = None


class ToggleAgentStatusRequest(BaseModel):
    """
    Request model for toggling agent status.
    user_id is optional - if provided by Admin, acts on behalf of that user.
    """
    agent_id: int = Field(..., gt=0)
    is_active: bool = Field(...)
    user_id: Optional[int] = Field(None, gt=0)  # NEW: Allow admin to toggle for specific user
    
    class Config:
        json_schema_extra = {
            "example": {
                "agent_id": 5,
                "is_active": True,
                "user_id": 12  # Optional: For admin use
            }
        }


class BusinessDetailsRequest(BaseModel):
    """Request model for post-signup business details submission"""
    agent_name: str = Field(None, min_length=1, max_length=100)
    business_name: str = Field(None, min_length=1, max_length=100)
    business_email: str = Field(None, min_length=1)
    phone_number: str = Field(None, min_length=10, max_length=20)  
    industry: str = Field(None, min_length=1, max_length=50)
    language: str = Field(None, min_length=1, max_length=50)
    
    class Config:
        json_schema_extra = {
            "example": {
                "agent_name": "String",
                "business_name": "String",
                "business_email": "String",
                "phone_number": "String",
                "industry": "String",
                "language": "String"
            }
        }




class UpdateAdminStatusRequest(BaseModel):
    """Request model for updating user admin status"""
    is_admin: bool = Field(...)
    
    class Config:
        json_schema_extra = {
            "example": {
                "is_admin": True
            }
        }


# ==================== GOOGLE CALENDAR MODELS ====================

class GoogleAuthStatusResponse(BaseModel):
    """Response model for Google Calendar connection status"""
    connected: bool
    email: Optional[str] = None


class GoogleAuthLoginResponse(BaseModel):
    """Response model for Google OAuth login URL"""
    authorization_url: str


class GoogleEvent(BaseModel):
    """Model for a single Google Calendar event"""
    id: str
    summary: str
    date: str
    start_time: str
    end_time: str
    description: Optional[str] = None
    location: Optional[str] = None


class GoogleEventsResponse(BaseModel):
    """Response model for list of Google Calendar events"""
    events: List[GoogleEvent]


class BookAppointmentRequest(BaseModel):
    """Request model for booking an appointment via AI agent"""
    user_id: int = Field(..., gt=0)
    appointment_date: str = Field(..., pattern=r'^\d{4}-\d{2}-\d{2}$')  # YYYY-MM-DD
    start_time: str = Field(..., pattern=r'^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$')  # HH:MM
    end_time: str = Field(..., pattern=r'^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$')  # HH:MM
    attendee_email: EmailStr
    attendee_name: Optional[str] = None
    title: str = Field(..., min_length=1)
    description: Optional[str] = None
    notes: Optional[str] = None
    
    class Config:
        json_schema_extra = {
            "example": {
                "user_id": 1,
                "appointment_date": "2026-01-25",
                "start_time": "14:00",
                "end_time": "15:00",
                "attendee_email": "customer@example.com",
                "attendee_name": "John Doe",
                "title": "Consultation Call",
                "description": "Initial consultation meeting",
                "notes": "Discussed via AI agent"
            }
        }


class BookAppointmentResponse(BaseModel):
    """Response model for appointment booking"""
    success: bool
    conflict: bool = False
    event_id: Optional[str] = None
    message: str

# ==================== SUBSCRIPTION PLANS ====================
 
class SubscriptionPlanCreate(BaseModel):
    """
    Admin creates a subscription plan and assigns it to a user in one step.
    """
    name: str = Field(..., min_length=1, max_length=100)
    price: float = Field(..., ge=0)
    currency: Optional[str] = Field(default="EUR", max_length=10)
    included_minutes: int = Field(..., ge=0)
    max_agents: int = Field(default=1, ge=1)
    features: Optional[str] = Field(default=None)
    is_active: bool = Field(default=True)
    user_id: int = Field(..., gt=0, description="User to assign this plan to")
 
    class Config:
        json_schema_extra = {
            "example": {
                "name": "Business Plan",
                "price": 49.99,
                "currency": "EUR",
                "included_minutes": 500,
                "max_agents": 5,
                "features": "500 call minutes/month, Up to 5 agents, Priority support",
                "is_active": True,
                "user_id": 2
            }
        }
 
 
class SubscriptionPlanUpdate(BaseModel):
    """
    Admin updates a plan assigned to a specific user (identified by user_id).
    """
    user_id: int = Field(..., gt=0, description="User whose plan to update")
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    price: Optional[float] = Field(None, ge=0)
    currency: Optional[str] = Field(None, max_length=10)
    included_minutes: Optional[int] = Field(None, ge=0)
    max_agents: Optional[int] = Field(None, ge=1)
    features: Optional[str] = Field(None)
    is_active: Optional[bool] = Field(None)
 
    class Config:
        json_schema_extra = {
            "example": {
                "user_id": 2,
                "name": "Updated Plan Name",
                "price": 59.99,
                "included_minutes": 600,
                "max_agents": 5,
                "features":"",
                "is_active": False
            }
        }
 
 
class SubscriptionPlanOut(BaseModel):
    """
    Response model for a subscription plan (returned to users and admins).
    """
    user_id: int
    id: int
    name: str
    price: float
    currency: str
    included_minutes: int
    max_agents: int
    features: str
    created_at: datetime
    updated_at: datetime