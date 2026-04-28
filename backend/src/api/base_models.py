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
    is_admin: bool = False #*

class LoginResponse(BaseModel):
    access_token: str
    token_type: str
    user: UserOut

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
    voice_type: str = Field(..., pattern="^(male|female)$")
    language: Optional[str] = Field(default="en", max_length=10)
    industry: Optional[str] = Field(default=None, max_length=50)
    owner_name: Optional[str] = Field(default=None, max_length=100)
    owner_email: Optional[EmailStr] = Field(default=None)  # NEW
    business_hours_start: Optional[str] = Field(default=None, pattern=r'^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$')  # NEW
    business_hours_end: Optional[str] = Field(default=None, pattern=r'^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$')  # NEW
    allowed_minutes: Optional[int] = Field(default=0, ge=0)  # NEW
    
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
                "allowed_minutes": 500
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
                "allowed_minutes": 1000
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


# ==================== SUBSCRIPTION PLANS ====================
 
class SubscriptionPlanCreate(BaseModel):
    """
    Admin uses this to create a new subscription plan.
    Shown to all users on the pricing/plans page.
    """
    name: str = Field(..., min_length=1, max_length=100, description="Plan display name, e.g. 'Starter', 'Pro'")
    description: Optional[str] = Field(default=None, max_length=500)
    price: float = Field(..., ge=0, description="Plan price (0 = free)")
    currency: str = Field(default="USD", max_length=10)
    included_minutes: int = Field(..., ge=0, description="Call minutes included in this plan")
    max_agents: int = Field(default=1, ge=1, description="Maximum agents the user can create")
    features: Optional[List[str]] = Field(default=[], description="List of feature bullet points shown to users")
    is_active: bool = Field(default=True, description="Whether this plan is visible to users")
    is_popular: bool = Field(default=False, description="Mark as 'Most Popular' on pricing page")
    sort_order: int = Field(default=0, ge=0, description="Display order (lower = shown first)")
 
    class Config:
        json_schema_extra = {
            "example": {
                "name": "Pro",
                "description": "Perfect for growing businesses",
                "price": 49.99,
                "currency": "USD",
                "included_minutes": 500,
                "max_agents": 5,
                "features": ["500 call minutes/month", "Up to 5 agents", "Priority support", "Analytics dashboard"],
                "is_active": True,
                "is_popular": True,
                "sort_order": 2
            }
        }
 
 
class SubscriptionPlanUpdate(BaseModel):
    """
    Admin uses this to partially update an existing plan.
    All fields are optional — only provided fields are updated.
    """
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = Field(None, max_length=500)
    price: Optional[float] = Field(None, ge=0)
    currency: Optional[str] = Field(None, max_length=10)
    included_minutes: Optional[int] = Field(None, ge=0)
    max_agents: Optional[int] = Field(None, ge=1)
    features: Optional[List[str]] = Field(None)
    is_active: Optional[bool] = Field(None)
    is_popular: Optional[bool] = Field(None)
    sort_order: Optional[int] = Field(None, ge=0)
 
    class Config:
        json_schema_extra = {
            "example": {
                "price": 59.99,
                "included_minutes": 600,
                "is_popular": True
            }
        }
 
 
class SubscriptionPlanOut(BaseModel):
    """
    Response model for a subscription plan (returned to users and admins).
    """
    id: int
    name: str
    description: Optional[str] = None
    price: float
    currency: str
    included_minutes: int
    max_agents: int
    features: List[str] = []
    is_active: bool
    is_popular: bool
    sort_order: int
    created_at: datetime
    updated_at: datetime
 