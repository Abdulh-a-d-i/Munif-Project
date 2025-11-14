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
    
    class Config:
        json_schema_extra = {
            "example": {
                "agent_name": "Customer Support Agent",
                "phone_number": "+1234567890",
                "system_prompt": "You are a helpful customer support agent...",
                "voice_type": "female",
                "language": "en",
                "industry": "healthcare",
                "owner_name": "John Doe"
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
    
    class Config:
        json_schema_extra = {
            "example": {
                "agent_name": "Updated Agent Name",
                "voice_type": "male",
                "industry": "retail",
                "owner_name": "Jane Smith"
            }
        }