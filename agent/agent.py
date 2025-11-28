from __future__ import annotations
import asyncio
import logging
import os
import json
import base64
from typing import Any
from datetime import datetime, timedelta, timezone
import traceback
import boto3
import httpx
from dotenv import load_dotenv
# Database imports
import asyncpg
# Google Cloud imports
from google.cloud import storage
from google.oauth2 import service_account
# LiveKit imports
from livekit import rtc, api
from livekit.agents import (
    Agent,
    AgentSession,
    JobContext,
    WorkerOptions,
    cli,
    function_tool,
    RunContext,
    get_job_context,
    RoomInputOptions,
    BackgroundAudioPlayer,
    AudioConfig,
    BuiltinAudioClip
)
from livekit.plugins import deepgram, elevenlabs, openai, silero
from livekit.plugins.turn_detector.multilingual import MultilingualModel
load_dotenv(".env")
logger = logging.getLogger("inbound-agent",)
logger.setLevel(logging.INFO)
# Environment variables
BACKEND_API_URL = os.getenv("BACKEND_API_URL", "https://backend.mrbot-ki.de/api")
HETZNER_BUCKET_NAME = os.getenv("HETZNER_BUCKET_NAME")
HETZNER_ENDPOINT = os.getenv("HETZNER_ENDPOINT_URL")
HETZNER_ACCESS_KEY = os.getenv("HETZNER_ACCESS_KEY")
HETZNER_SECRET_KEY = os.getenv("HETZNER_SECRET_KEY")
AGENT_API_SECRET = os.getenv("AGENT_API_SECRET")
UPLOAD_TRANSCRIPTS = os.getenv("UPLOAD_TRANSCRIPTS", "true").lower() in ("1", "true", "yes")
UPLOAD_RECORDINGS = os.getenv("UPLOAD_RECORDINGS", "true").lower() in ("1", "true", "yes")
TURN_DETECTION_MIN_ENDPOINTING_DELAY = float(os.getenv("TURN_DETECTION_MIN_ENDPOINTING_DELAY", "0.3"))  # Reduced from 0.5
TURN_DETECTION_MIN_SILENCE_DURATION = float(os.getenv("TURN_DETECTION_MIN_SILENCE_DURATION", "0.4"))  # Reduced from 0.5

VOICE_LIBRARY = {
    # English - Female
    "Emma": "56bWURjYFHyYyVf490Dp",
    "Lauren": "14Coq6695JDX9xtLqXDE",
    "Ellie Echo": "4opnKWPbOJPB3xz3YUBh",

    # English - Male
    "Vincent C. Michaels": "n1PvBOwxb8X6m7tahp2h",
    "Pete": "i9TV8uxP1sg4AIDgzU8V",
    "Liam Bainbridge": "6J11B050yDOLPaxMFyS9",

    # German - Female
    "Lea": "M39iqBUcu1jyiwM5PfSy",
    "Rebecca Green": "ONs4CSS4LR7hEoEykuS5",
    "Laura": "zKHQdbB8oaQ7roNTiDTK",
    "Ramona": "yUy9CCX9brt8aPVvIWy3",

    # German - Male
    "Tony Saxon": "sbJf8opzqSGRyRJzCVjD",
    "Denis": "CTGK4a418btPyOX0fSX5",
    "Idrisko": "5Yk14CEtOPiDBL0eakd4",
    "Titus Trust": "2HRQZj4BjKZ0bVgy6Ikf",

    # French - Female
    "Adina": "FvmvwvObRqIHojkEGh5N",
    "Chloe": "n4xdXKggn5lFcXFYE4TA",

    # French - Male
    "Antoine": "nbiTBaMRdSobTQJDzIWm",
    "Charles Pestel": "592QbnMQfByEAOJe8maw",

    # Dutch - Female
    "Roos Dutch": "7qdUFMklKPaaAVMsBTBt",
    "Chira": "cPimkqmS0qNAJJNGxavl",

    # Dutch - Male
    "Arjen": "62klqbsYqbynbr66ypRt",
    "Thomas": "tvFp0BgJPrEXGoDhDIA4",

    # Italian - Female
    "Giulia": "CnVVMwhKmKZ6hKBAKL6Y",

    # Italian - Male
    "Marco": "2OoHspMHbpIu5oiMaqDy",
    "Andy M": "DLMxnwJE0a28JQLTMJPJ",

    # Spanish - Female
    "Clara": "PI8la2kxxgzvU6brYDdg",
    "Lumina": "x5IDPS14ZUbhosMmVFTk",
    "Isabel": "rixsIpPITphvsJd2mI03",

    # Spanish - Male
    "Juan Pablo": "5kz7Te3c1BvAWyFfkfkW",
    "Pablo": "Koms9sdpNJLacadS6g9C",
}


def get_voice_id(voice_name: str) -> str:
    """
    Get voice ID from voice name.
    Falls back to Sarah if name not found.
    """
    voice_id = VOICE_LIBRARY.get(voice_name)
    
    if voice_id:
        logger.info(f"✅ Using voice: {voice_name} ({voice_id})")
        return voice_id
    
    # Fallback to Lea
    logger.warning(f"⚠️ Voice '{voice_name}' not found, using Lea as fallback")
    return VOICE_LIBRARY["Lea"]

GREETINGS = {
    "en": "Hello! Thank you for calling. This is {agent_name}. How can I help you today?",
    "de": "Guten Tag! Vielen Dank für Ihren Anruf. Hier ist {agent_name}. Wie kann ich Ihnen heute helfen?",
    "fr": "Bonjour! Merci d'avoir appelé. C'est {agent_name}. Comment puis-je vous aider aujourd'hui?",
    "nl": "Hallo! Bedankt voor uw telefoontje. Dit is {agent_name}. Hoe kan ik u vandaag helpen?",
    "it": "Ciao! Grazie per aver chiamato. Sono {agent_name}. Come posso aiutarti oggi?",
    "es": "¡Hola! Gracias por llamar. Soy {agent_name}. ¿Cómo puedo ayudarte hoy?",
}

# 🔥 IMPROVED: Enhanced system prompt with better structure and clear instructions
BASE_SYSTEM_PROMPT_TEMPLATE = """You are {agent_name}, a professional AI voice assistant handling inbound calls for {owner_name}.

**CRITICAL RESPONSE RULES:**
1. Respond IMMEDIATELY when the customer stops speaking - DO NOT wait
2. Keep ALL responses SHORT (1-2 sentences maximum)
3. Ask ONE question at a time
4. Never use filler phrases or apologize excessively
5. Speak naturally as if having a phone conversation
6. Use {language} language ONLY

**YOUR ROLE:**
- Phone number: {phone_number}
- Industry: {industry}
- Language: {language}
- You represent: {owner_name}

**CONVERSATION FLOW:**
1. Greet warmly and ask how you can help
2. Listen carefully to the customer's needs
3. Provide information or help them book appointments
4. Confirm all details clearly
5. End politely when conversation is complete

**APPOINTMENT BOOKING PROCESS:**
When a customer wants to book an appointment, follow these steps IN ORDER:

Step 1: Ask for their preferred DATE
   Example: "What date works best for you?"

Step 2: Ask for their preferred TIME
   Example: "What time would you like? Morning or afternoon?"

Step 3: Confirm duration or ask END TIME
   Example: "This appointment will be one hour. Does that work?"

Step 4: Ask for their FULL NAME
   Example: "May I have your full name please?"

Step 5: Ask for their EMAIL and READ IT BACK
   Example: "What email address should I send the confirmation to?"
   Then: "Let me confirm - that's j dot smith at gmail dot com, correct?"

Step 6: Ask for their PHONE NUMBER
   Example: "And what's the best phone number to reach you?"

Step 7: ONLY AFTER collecting ALL information, call the book_appointment tool
   Then confirm: "Perfect! I've booked your appointment for [DATE] at [TIME]. You'll receive a confirmation email at [EMAIL] shortly."

**IMPORTANT APPOINTMENT RULES:**
- Collect information ONE piece at a time
- ALWAYS read email addresses back letter by letter (e.g., "j dot smith at gmail dot com")
- Confirm with customer before booking
- Only call book_appointment tool ONCE when you have ALL required information
- If customer says "book an appointment", guide them through the process above

**COMMUNICATION STYLE:**
- Professional but warm and friendly
- Clear and concise - no rambling
- Patient and helpful
- Natural phone conversation tone
- Respond quickly - don't make customers wait

**WHAT NOT TO DO:**
- Never mention you're an AI unless asked
- Never use bullet points or formatting
- Never make customers repeat information unnecessarily
- Never rush through collecting information
- Never forget to confirm the email address by reading it back

{context_from_backend}

Remember: Your goal is to help customers efficiently while making them feel valued and understood."""

async def _speak_status_update(ctx: RunContext, message: str, delay: float = 0.2):
    """Speak a brief status update before performing an action."""
    await asyncio.sleep(delay)
    await ctx.session.say(message, allow_interruptions=True)
    await asyncio.sleep(0.1)

def get_s3_client():
    """Initialize S3-compatible client for Hetzner Object Storage"""
    if not all([HETZNER_ENDPOINT, HETZNER_ACCESS_KEY, HETZNER_SECRET_KEY]):
        raise RuntimeError("Missing Hetzner Object Storage credentials")
    
    return boto3.client(
        's3',
        endpoint_url=HETZNER_ENDPOINT,
        aws_access_key_id=HETZNER_ACCESS_KEY,
        aws_secret_access_key=HETZNER_SECRET_KEY,
        region_name=os.getenv("HETZNER_REGION", "hel1")
    )

async def send_status_to_backend(
    call_id: str,
    status: str,
    agent_id: int = None,
    error_details: dict = None
):
    """Send status with GUARANTEED delivery"""
    payload = {
        "call_id": call_id,
        "status": status,
        "agent_id": agent_id,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
   
    if status == "failed" and error_details:
        payload["error_details"] = error_details
   
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.post(
                    f"{BACKEND_API_URL}/agent/report-event",
                    json=payload
                )
                if response.status_code == 200:
                    logger.info(f"✅ Status '{status}' sent for {call_id}")
                    return
        except Exception as e:
            if attempt == 2:
                logger.error(f"❌ Failed to send status after 3 attempts: {e}")
            else:
                await asyncio.sleep(0.5)

async def fetch_agent_config_from_backend(phone_number: str) -> dict | None:
    """
    Fetch agent configuration from backend API.
    """
    if not BACKEND_API_URL or not AGENT_API_SECRET:
        logger.error("❌ BACKEND_API_URL or AGENT_API_SECRET not configured")
        return None
    
    # Clean the phone number - remove any curly braces, whitespace, etc.
    phone_number = phone_number.strip().strip('{}').strip()
    
    try:
        logger.info(f"📞 Fetching agent config from backend for: {phone_number}")
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{BACKEND_API_URL}/agent/config/{phone_number}",
                headers={
                    "Authorization": f"Bearer {AGENT_API_SECRET}"
                }
            )
            
            if response.status_code == 404:
                logger.warning(f"⚠️ No agent found for phone: {phone_number}")
                return None
            
            if response.status_code != 200:
                logger.error(f"❌ Backend returned {response.status_code}: {response.text}")
                return None
            
            data = response.json()
            
            if not data.get("success"):
                logger.error(f"❌ Backend error: {data.get('error')}")
                return None
            
            config = data.get("agent")
            
            if not config:
                logger.error("❌ No agent data in response")
                return None
            
            logger.info(f"✅ Agent config loaded: {config['agent_name']} (ID: {config.get('agent_id', config.get('id'))})")
            logger.info(f"   Voice type: {config.get('voice_type', 'default')}")
            logger.info(f"   Language: {config.get('language', 'en')}")
            logger.info(f"   Industry: {config.get('industry', 'N/A')}")
            logger.info(f"   Owner: {config.get('owner_name', 'N/A')}")
            
            return config
            
    except httpx.TimeoutException:
        logger.error(f"⏱️ Timeout fetching agent config from backend")
        return None
    except Exception as e:
        logger.error(f"❌ Unexpected error fetching agent config: {e}")
        traceback.print_exc()
        return None


async def initialize_call_history(phone_number: str, call_id: str) -> dict | None:
    """
    Fetch dynamic/new data for the agent based on phone number.
    """
    if not BACKEND_API_URL or not AGENT_API_SECRET:
        logger.error("❌ BACKEND_API_URL or AGENT_API_SECRET not configured")
        return None
    
    # Clean the phone number
    phone_number = phone_number.strip().strip('{}').strip()
    
    try:
        logger.info(f"📊 Fetching dynamic data from backend for phone: {phone_number}")
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{BACKEND_API_URL}/agent/new-call",
                params={"phone_number": phone_number, "call_id": call_id},
                headers={
                    "Authorization": f"Bearer {AGENT_API_SECRET}"
                }
            )
            
            if response.status_code == 404:
                logger.warning(f"⚠️ No dynamic data found for phone: {phone_number}")
                return None
            
            if response.status_code != 200:
                logger.error(f"❌ Backend returned {response.status_code}: {response.text}")
                return None
            
            data = response.json()
            
            if not data.get("success"):
                logger.error(f"❌ Backend error: {data.get('error')}")
                return None
            
            dynamic_data = data.get("dynamic_data")
            
            if not dynamic_data:
                logger.info("ℹ️ No dynamic data available")
                return None
            
            logger.info(f"✅ Dynamic data loaded")
            
            return dynamic_data
            
    except httpx.TimeoutException:
        logger.error(f"⏱️ Timeout fetching dynamic data from backend")
        return None
    except Exception as e:
        logger.error(f"❌ Unexpected error fetching dynamic data: {e}")
        traceback.print_exc()
        return None

def build_complete_system_prompt(
    agent_name: str,
    phone_number: str,
    language: str,
    industry: str,
    owner_name: str,
    context_from_backend: str,
) -> str:
    """
    Build complete system prompt by combining base rules with backend context.
    """
    # Fill in the template
    complete_prompt = BASE_SYSTEM_PROMPT_TEMPLATE.format(
        agent_name=agent_name,
        phone_number=phone_number,
        language=language.upper(),
        industry=industry or "General",
        owner_name=owner_name or "the company",
        context_from_backend=context_from_backend or "",
    )
    
    return complete_prompt

class InboundAgent(Agent):
    def __init__(self, *, agent_config: dict):
        self.agent_id = agent_config.get("agent_id") or agent_config.get("id")
        self.agent_name = agent_config.get("agent_name", "AI Assistant")
        self.phone_number = agent_config.get("phone_number")
        self.industry = agent_config.get("industry")
        self.language = agent_config.get("language", "en")
        self.owner_name = agent_config.get("owner_name")
        
        # Get context from backend (this is the system_prompt from DB)
        context_from_backend = agent_config.get("system_prompt", "")
        
        # Build complete system prompt
        complete_system_prompt = build_complete_system_prompt(
            agent_name=self.agent_name,
            phone_number=self.phone_number,
            language=self.language,
            industry=self.industry,
            owner_name=self.owner_name,
            context_from_backend=context_from_backend
        )
        
        logger.info(f"🤖 Initializing agent '{self.agent_name}'")
        logger.info(f"   Language: {self.language}")
        logger.info(f"   Industry: {self.industry}")
        logger.info(f"   Complete prompt length: {len(complete_system_prompt)} chars")
        
        # Pass complete prompt to parent Agent class
        super().__init__(instructions=complete_system_prompt)
        
        self.participant: rtc.RemoteParticipant | None = None
        self.sip_call_id: str | None = None
        self.egress_id: str | None = None
        self.recording_url: str | None = None
        self.recording_blob_path: str | None = None
        self.caller_phone: str | None = None

    def set_participant(self, participant: rtc.RemoteParticipant):
        self.participant = participant

    def set_sip_call_id(self, call_id: str):
        self.sip_call_id = call_id
   
    def set_caller_phone(self, phone: str):
        self.caller_phone = phone
    
    # 🔥 FIXED: Improved appointment booking with email confirmation
    @function_tool()
    async def book_appointment(
            self,
            ctx: RunContext,
            appointment_date: str,
            start_time: str,
            end_time: str,
            customer_name: str,
            customer_email: str,
            customer_phone: str = None,
            title: str = "Appointment",
            notes: str = None
        ):
            """
            Book an appointment and send confirmation email.
            ONLY call this function when you have collected ALL required information from the customer.
            
            REQUIRED information you MUST collect before calling this:
            - appointment_date: Date in YYYY-MM-DD format (e.g., 2025-12-15)
            - start_time: Start time in HH:MM 24-hour format (e.g., 14:30)
            - end_time: End time in HH:MM 24-hour format (e.g., 15:30)
            - customer_name: Full name of the customer
            - customer_email: Email address (MUST read back to customer for confirmation)
            - customer_phone: Phone number
            
            Args:
                appointment_date: Date in YYYY-MM-DD format
                start_time: Start time in HH:MM 24-hour format
                end_time: End time in HH:MM 24-hour format
                customer_name: Full name of the person booking
                customer_email: Email to send confirmation to (MUST be confirmed with customer)
                customer_phone: Phone number of the customer
                title: Title of the appointment
                notes: Any special requests or notes
            """
            await _speak_status_update(ctx, "Perfect! Let me book that for you now...")

            try:
                logger.info(f"📅 Booking appointment: {appointment_date} {start_time}-{end_time}")
                logger.info(f"   Customer: {customer_name} ({customer_email})")

                payload = {
                    "user_id": self.agent_id,
                    "appointment_date": appointment_date,
                    "start_time": start_time,
                    "end_time": end_time,
                    "attendee_name": self.agent_name,
                    "title": title,
                    "description": notes or f"Appointment scheduled via phone call",
                    "organizer_name": self.agent_name,
                    "organizer_email": customer_email,
                    "customer_name": customer_name,
                    "customer_email": customer_email,
                    "customer_phone": customer_phone or self.caller_phone
                }

                async with httpx.AsyncClient(timeout=20.0) as client:
                    response = await client.post(
                        f"{BACKEND_API_URL}/agent/book-appointment",
                        json=payload,
                        headers={"Authorization": f"Bearer {AGENT_API_SECRET}"}
                    )

                    logger.info(f"📬 Booking response: {response.status_code}")

                    if response.status_code in (200, 201):
                        data = response.json()
                        if data.get("success"):
                            return {
                                "success": True,
                                "message": f"Your appointment is confirmed for {appointment_date} at {start_time}. "
                                        f"A confirmation email has been sent to {customer_email}."
                            }

                    # Handle errors
                    msg = response.json().get("message", "There was an issue booking the appointment.")
                    return {"success": False, "message": msg}

            except Exception as e:
                logger.error(f"❌ Error booking appointment: {e}")
                traceback.print_exc()
                return {
                    "success": False,
                    "message": "I'm having trouble with the booking system right now. "
                            "Could you please call back in a few minutes or try again?"
                }
    
    @function_tool()
    async def end_call(self, ctx: RunContext):
        """End the phone call politely and hang up. Use this when the conversation is complete."""
        logger.info("📞 Ending call...")
        try:
            await ctx.wait_for_playout()
        except:
            pass
        job_ctx = get_job_context()
        if job_ctx:
            try:
                await job_ctx.api.room.delete_room(api.DeleteRoomRequest(room=job_ctx.room.name))
                logger.info("🗑️ Room deleted")
                await send_status_to_backend(job_ctx.room.name, "completed", self.agent_id)
            except Exception as e:
                logger.warning(f"⚠️ Failed to delete room: {e}")
        
        try:
            ctx.shutdown(reason="Call ended by agent")
        except:
            pass

async def entrypoint(ctx: JobContext):
    """Entrypoint for inbound calls with voice name support."""
    logger.info("=" * 80)
    logger.info(f"📞 INBOUND CALL - Room: {ctx.room.name}")
    logger.info("=" * 80)
    
    # Connect first to get participants
    await ctx.connect()
    
    # Initialize variables
    called_number = 'unknown'
    caller_number = 'unknown'
    
    # Wait for SIP participant to join
    logger.info("⏳ Waiting for SIP participant...")
    max_wait = 5
    waited = 0
    
    while waited < max_wait:
        for participant in ctx.room.remote_participants.values():
            if hasattr(participant, 'attributes'):
                logger.info(f"📋 Participant attributes: {dict(participant.attributes)}")
                
                # Get the called number (the number being dialed - DNIS)
                called_number = participant.attributes.get('sip.trunkPhoneNumber', 'unknown')
                
                # Get the caller number (the number calling - ANI)
                caller_number = (
                    participant.attributes.get('sip.fromNumber') or
                    participant.attributes.get('sip.callerNumber') or
                    participant.attributes.get('sip.from') or
                    'unknown'
                )
                
                logger.info(f"📱 Called number (DNIS): {called_number}")
                logger.info(f"📱 Caller number (ANI): {caller_number}")
                
                if called_number != 'unknown':
                    break
        
        if called_number != 'unknown':
            break
        
        await asyncio.sleep(0.5)
        waited += 0.5
    
    # Use called_number as phone_number
    phone_number = called_number

    if not phone_number or phone_number == 'unknown':
        logger.error("❌ Missing phone_number - cannot determine agent")
        return

    logger.info(f"✅ Using phone number: {phone_number}")
    logger.info(f"✅ Caller number: {caller_number}")

    # Start parallel async tasks
    config_task = asyncio.create_task(fetch_agent_config_from_backend(phone_number))
    dynamic_task = asyncio.create_task(initialize_call_history(phone_number, ctx.room.name))
    
    # Await the config task
    agent_config = await config_task
    
    if not agent_config:
        logger.error(f"❌ No agent configured for phone number: {phone_number}")
        return
    
    # Await the dynamic data task
    dynamic_data = await dynamic_task

    # Extract configuration
    language = agent_config.get("language", "de")
    agent_id = agent_config.get("agent_id") or agent_config.get("id")
    
    # Get voice name from config
    voice_name = agent_config.get("voice_type", "Lea")
    
    # 🔥 FIXED: Get the correct voice ID
    voice_id = get_voice_id(voice_name)
    
    logger.info(f"🎤 Voice name: {voice_name}")
    logger.info(f"🎤 Voice ID: {voice_id}")
    logger.info(f"🌐 Language: {language}")
    logger.info(f"🆔 Agent ID: {agent_id}")
    
    # Create agent with complete prompt
    agent = InboundAgent(agent_config=agent_config)
    agent.set_caller_phone(caller_number)
    
    # 🔥 IMPROVED: Better turn detection
    turn_detector = MultilingualModel()
    
    # 🔥 FIXED: Use the correct voice_id instead of hardcoded one
    session = AgentSession(
        llm=openai.LLM(
            model="gpt-4o-mini",
            api_key=os.getenv("OPENAI_API_KEY"),
            temperature=0.7,  # More natural responses
        ),
        stt=deepgram.STT(
            api_key=os.getenv("DEEPGRAM_API_KEY"),
            model="nova-2-general",  # Better speech recognition
            language=language,
        ),
        tts=elevenlabs.TTS(
            api_key=os.getenv("ELEVENLABS_API_KEY"),
            model="eleven_flash_v2_5",
            voice_id=voice_id,  # 🔥 FIXED: Use configured voice_id instead of hardcoded
            optimize_streaming_latency=4,  # Reduce latency
            stability=0.5,
            similarity_boost=0.75,
        ),
        vad=silero.VAD.load(
            min_silence_duration=0.3,  # More responsive
            min_speech_duration=0.1,
        ),
        turn_detection=turn_detector,
        min_endpointing_delay=TURN_DETECTION_MIN_ENDPOINTING_DELAY,
    )
    

    async def upload_transcript():
        """Upload transcript to Hetzner Object Storage and send metadata to backend"""
        if not UPLOAD_TRANSCRIPTS:
            logger.info("📄 Transcript upload disabled")
            return
        try:
            transcript_obj = session.history.to_dict() if hasattr(session, 'history') else {"messages": []}
            transcript_json = json.dumps(transcript_obj, indent=2)
            
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            safe_phone = phone_number.replace("+", "").replace("-", "").replace(" ", "")
            blob_name = f"transcripts/{ctx.room.name}_{safe_phone}_{ts}.json"
            
            # Upload to Hetzner
            s3_client = get_s3_client()
            s3_client.put_object(
                Bucket=HETZNER_BUCKET_NAME,
                Key=blob_name,
                Body=transcript_json.encode('utf-8'),
                ContentType='application/json'
            )
            
            logger.info(f"✅ Transcript uploaded: {blob_name}")
            
            # 🔥 FIX: Send BOTH transcript AND recording data
            payload = {
                "call_id": ctx.room.name,
                "transcript_blob": blob_name,  # Transcript blob path
                # Recording was already uploaded by LiveKit, just reference it
                "recording_blob": agent.recording_blob_path if agent.recording_blob_path else None,
                "recording_url": agent.recording_url if agent.recording_url else None
            }
            
            # Remove None values
            payload = {k: v for k, v in payload.items() if v is not None}
            
            logger.info(f"📤 Sending call data to backend: {payload}")
            
            # Send to backend with retries
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    async with httpx.AsyncClient(timeout=60.0) as c:
                        response = await c.post(
                            f"{BACKEND_API_URL}/agent/save-call-data",
                            json=payload,
                            headers={"Authorization": f"Bearer {AGENT_API_SECRET}"}
                        )
                        if response.status_code == 200:
                            logger.info("✅ Call data sent to backend successfully")
                            break
                        else:
                            logger.warning(f"⚠️ Backend returned {response.status_code}: {response.text}")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(2)
                except httpx.ReadTimeout:
                    if attempt < max_retries - 1:
                        logger.warning(f"⏳ Timeout on attempt {attempt + 1}, retrying...")
                        await asyncio.sleep(2)
                    else:
                        logger.error(f"❌ Backend timeout after {max_retries} attempts")
                except Exception as e:
                    logger.error(f"❌ Backend request failed: {e}")
                    traceback.print_exc()
                    break
                    
        except Exception as e:
            logger.error(f"❌ Transcript upload failed: {e}")
            traceback.print_exc()
    ctx.add_shutdown_callback(upload_transcript)
    
    #  STATUS 1: INITIALIZED 
    await ctx.connect()
    await send_status_to_backend(ctx.room.name, "initialized", agent_id)
    
    if UPLOAD_RECORDINGS:
        try:
            safe_phone = phone_number.replace("+", "").replace("-", "").replace(" ", "")
            ts = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
            recording_filename = f"recordings/{ctx.room.name}_{safe_phone}_{ts}.ogg"
            
            agent.recording_blob_path = recording_filename
            
            logger.info(f"🎙️ Starting recording: {recording_filename}")
            
            # Extract base endpoint WITHOUT bucket subdomain
            livekit_endpoint = HETZNER_ENDPOINT.replace(f"{HETZNER_BUCKET_NAME}.", "")
            
            logger.info(f"📡 LiveKit endpoint: {livekit_endpoint}")
            logger.info(f"🪣 Using bucket: {HETZNER_BUCKET_NAME}")
            
            # Create S3Upload configuration for LiveKit
            req = api.RoomCompositeEgressRequest(
                room_name=ctx.room.name,
                audio_only=True,
                file_outputs=[
                    api.EncodedFileOutput(
                        file_type=api.EncodedFileType.OGG,
                        filepath=recording_filename,
                        s3=api.S3Upload(
                            access_key=HETZNER_ACCESS_KEY,
                            secret=HETZNER_SECRET_KEY,
                            region=os.getenv("HETZNER_REGION", "hel1"),
                            endpoint=livekit_endpoint,  # https://hel1.your-objectstorage.com
                            bucket=HETZNER_BUCKET_NAME,
                            force_path_style=True  # ✅ THIS IS THE KEY!
                        )
                    )
                ],
            )
            
            lkapi = api.LiveKitAPI(
                url=os.getenv("LIVEKIT_URL", "").replace("wss://", "https://"),
                api_key=os.getenv("LIVEKIT_API_KEY"),
                api_secret=os.getenv("LIVEKIT_API_SECRET"),
            )
            
            egress_resp = await lkapi.egress.start_room_composite_egress(req)
            agent.egress_id = egress_resp.egress_id
            
            # Build recording URL (use original endpoint with bucket)
            agent.recording_url = f"{HETZNER_ENDPOINT}/{recording_filename}"
            
            logger.info(f"✅ Recording started (egress_id: {agent.egress_id})")
            logger.info(f"📂 Recording blob path: {agent.recording_blob_path}")
            logger.info(f"🔗 Recording URL: {agent.recording_url}")
            
            await lkapi.aclose()
            
        except Exception as e:
            logger.error(f"❌ Failed to start recording: {e}")
            traceback.print_exc()
    else:
        logger.info("🔇 Recording disabled")
                
    
    try:
        #  WAIT FOR CALLER TO CONNECT 
        logger.info(f" Waiting for caller to join...")
        
        # Wait for the SIP participant (caller) to join
        participant = await ctx.wait_for_participant()
        agent.set_participant(participant)
        logger.info(f" Caller joined: {participant.identity}")
        
        #  STATUS 2: CONNECTED 
        started_at = datetime.now(timezone.utc).isoformat()
        
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(
                    f"{BACKEND_API_URL}/agent/update-call-started",
                    json={
                        "call_id": ctx.room.name,
                        "agent_id": agent_id,
                        "caller_number": caller_number,
                        "started_at": started_at
                    }
                )
                logger.info(f" Started_at timestamp set: {started_at}")
        except Exception as e:
            logger.warning(f" Could not set started_at: {e}")
        
        await send_status_to_backend(ctx.room.name, "connected", agent_id)
        
        # Start agent session
        session_task = asyncio.create_task(
            session.start(agent=agent, room=ctx.room, room_input_options=RoomInputOptions())
        )
        
        # Initial greeting based on language
        await asyncio.sleep(0.5)
        
        # Get greeting for language, default to English
        greeting_template = GREETINGS.get(language, GREETINGS["en"])
        greeting = greeting_template.format(agent_name=agent.agent_name)
        
        logger.info(f" Greeting in {language}: {greeting}")
        await session.say(greeting, allow_interruptions=True)
        await session_task
        logger.info(" Full session completed")
        
    except Exception as e:
        logger.error(f" Unexpected error: {e}")
        
        await send_status_to_backend(
            ctx.room.name,
            "failed",
            agent_id,
            error_details={
                "reason": "error",
                "error_message": str(e)
            }
        )
        
        traceback.print_exc()
        ctx.shutdown()

if __name__ == "__main__":
    cli.run_app(
        WorkerOptions(
            entrypoint_fnc=entrypoint,
            agent_name="inbound-agent",
        )
    )