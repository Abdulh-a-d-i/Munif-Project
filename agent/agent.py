from __future__ import annotations
import asyncio
import logging
import os
import json
import base64
from typing import Any
from datetime import datetime, timedelta, timezone
import traceback
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
BACKEND_API_URL = os.getenv("BACKEND_API_URL", "https://full-shrimp-deeply.ngrok-free.app/api")
AGENT_API_SECRET = os.getenv("AGENT_API_SECRET")
GOOGLE_BUCKET_NAME = os.getenv("GOOGLE_BUCKET_NAME") or os.getenv("GCS_BUCKET_NAME")
GCP_KEY_B64 = os.getenv("GCP_SERVICE_ACCOUNT_KEY_BASE64")
UPLOAD_TRANSCRIPTS = os.getenv("UPLOAD_TRANSCRIPTS", "true").lower() in ("1", "true", "yes")
UPLOAD_RECORDINGS = os.getenv("UPLOAD_RECORDINGS", "true").lower() in ("1", "true", "yes")
# Turn detection parameters
TURN_DETECTION_MIN_ENDPOINTING_DELAY = float(os.getenv("TURN_DETECTION_MIN_ENDPOINTING_DELAY", "0.5"))
TURN_DETECTION_MIN_SILENCE_DURATION = float(os.getenv("TURN_DETECTION_MIN_SILENCE_DURATION", "0.5"))

VOICE_IDS = {
    "female": {
        "german":  "v3V1d2rk6528UrLKRuy8",
        "english": "lcMyyd2HUfFzxdCaC4Ta",
    },
    "male": {
        "german":  "2HRQZj4BjKZ0bVgy6Ikf",
        "english": "i9TV8uxP1sg4AIDgzU8V",
    }
}

GREETINGS = {
    "de": "Guten Tag! Vielen Dank für Ihren Anruf. Hier ist {agent_name}. Wie kann ich Ihnen heute helfen?",
    "en": "Hello! Thank you for calling. This is {agent_name}. How can I help you today?",
    "es": "¡Hola! Gracias por llamar. Soy {agent_name}. ¿Cómo puedo ayudarte hoy?",
    "fr": "Bonjour! Merci d'avoir appelé. C'est {agent_name}. Comment puis-je vous aider aujourd'hui?",
    "it": "Ciao! Grazie per aver chiamato. Sono {agent_name}. Come posso aiutarti oggi?",
    "pt": "Olá! Obrigado por ligar. Sou {agent_name}. Como posso ajudá-lo hoje?",
}

# Base system prompt template with rules
BASE_SYSTEM_PROMPT_TEMPLATE = """You are {agent_name}, a professional AI voice assistant handling inbound calls.

**YOUR ROLE:**
You are speaking to customers who have called the phone number: {phone_number}
Language: {language}
Industry: {industry}
Owner: {owner_name}

**COMMUNICATION RULES:**
1. Keep responses brief and conversational (1-3 sentences per response)
2. Use natural, spoken language - avoid written formatting or bullet points
3. Speak in {language} language at all times
4. Be warm, professional, and helpful
5. Listen carefully and ask clarifying questions when needed
6. Never mention that you're an AI unless directly asked
7. Stay focused on the caller's needs

**CONVERSATION GUIDELINES:**
- Greet callers naturally and warmly
- Listen actively to their questions or concerns
- Provide accurate information based on your knowledge
- If you don't know something, admit it honestly
- Always be polite and patient
- End calls courteously when the conversation is complete

**INDUSTRY-SPECIFIC CONTEXT:**
{context_from_backend}

**ADDITIONAL DYNAMIC DATA:**
{dynamic_data}

Remember: You are having a natural phone conversation. Keep it simple, clear, and helpful."""

async def _speak_status_update(ctx: RunContext, message: str, delay: float = 0.3):
    """Speak a brief status update before performing an action."""
    await asyncio.sleep(delay)
    await ctx.session.say(message, allow_interruptions=True)
    await asyncio.sleep(0.2)

def get_gcs_client():
    """Initialize GCS client using base64-encoded service account JSON."""
    if not GCP_KEY_B64:
        raise RuntimeError("Missing GCP_SERVICE_ACCOUNT_KEY_BASE64 env var")
   
    try:
        decoded = base64.b64decode(GCP_KEY_B64).decode("utf-8")
        key_json = json.loads(decoded)
    except Exception as e:
        raise RuntimeError(f"Invalid base64 GCP key: {e}")
   
    for req in ("project_id", "client_email", "private_key"):
        if req not in key_json:
            raise RuntimeError(f"GCP key missing required field: {req}")
   
    credentials = service_account.Credentials.from_service_account_info(key_json)
    client = storage.Client(credentials=credentials, project=key_json.get("project_id"))
    return client

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
        logger.info(f"🔍 Fetching agent config from backend for: {phone_number}")
        
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
        logger.error(f"❌ Timeout fetching agent config from backend")
        return None
    except Exception as e:
        logger.error(f"❌ Unexpected error fetching agent config: {e}")
        traceback.print_exc()
        return None


async def fetch_dynamic_data_from_backend(phone_number: str, call_id: str) -> dict | None:
    """
    Fetch dynamic/new data for the agent based on phone number.
    """
    if not BACKEND_API_URL or not AGENT_API_SECRET:
        logger.error("❌ BACKEND_API_URL or AGENT_API_SECRET not configured")
        return None
    
    # Clean the phone number
    phone_number = phone_number.strip().strip('{}').strip()
    
    try:
        logger.info(f"🔍 Fetching dynamic data from backend for phone: {phone_number}")
        
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
            
            logger.info(f"✅ Dynamic data loaded: {json.dumps(dynamic_data)[:100]}...")
            
            return dynamic_data
            
    except httpx.TimeoutException:
        logger.error(f"❌ Timeout fetching dynamic data from backend")
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
    dynamic_data: dict = None
) -> str:
    """
    Build complete system prompt by combining base rules with backend context.
    """
    # Format dynamic data
    dynamic_data_str = ""
    if dynamic_data:
        dynamic_data_str = json.dumps(dynamic_data, indent=2)
    
    # Fill in the template
    complete_prompt = BASE_SYSTEM_PROMPT_TEMPLATE.format(
        agent_name=agent_name,
        phone_number=phone_number,
        language=language.upper(),
        industry=industry or "General",
        owner_name=owner_name or "the company",
        context_from_backend=context_from_backend or "No additional context provided.",
        dynamic_data=dynamic_data_str or "No dynamic data available."
    )
    
    return complete_prompt

class InboundAgent(Agent):
    def __init__(self, *, agent_config: dict, dynamic_data: dict = None):
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
            context_from_backend=context_from_backend,
            dynamic_data=dynamic_data
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
                logger.info("✅ Room deleted")
                await send_status_to_backend(job_ctx.room.name, "completed", self.agent_id)
            except Exception as e:
                logger.warning(f"⚠️ Failed to delete room: {e}")
        
        try:
            ctx.shutdown(reason="Call ended by agent")
        except:
            pass

async def entrypoint(ctx: JobContext):
    """Entrypoint for inbound calls with ASYNC config fetching."""
    logger.info("=" * 80)
    logger.info(f"📞 INBOUND CALL - Room: {ctx.room.name}")
    logger.info("=" * 80)
    
    # Parse metadata
    metadata_str = ctx.job.metadata or "{}"
    try:
        call_info = json.loads(metadata_str)
    except json.JSONDecodeError:
        logger.error("❌ Invalid metadata JSON")
        return
    
    # Extract and clean phone numbers
    phone_number = call_info.get("phone_number", "").strip().strip('{}').strip()
    caller_number = call_info.get("caller_number", "Unknown").strip().strip('{}').strip()
    
    logger.info(f"📱 Called number (DNIS): {phone_number}")
    logger.info(f"📱 Caller number (ANI): {caller_number}")
    
    if not phone_number:
        logger.error("❌ Missing phone_number in metadata - cannot determine agent")
        return

    # Start parallel async tasks
    config_task = asyncio.create_task(fetch_agent_config_from_backend(phone_number))
    dynamic_task = asyncio.create_task(fetch_dynamic_data_from_backend(phone_number, ctx.room.name))
    
    # Await the config task
    agent_config = await config_task
    
    if not agent_config:
        logger.error(f" No agent configured for phone number: {phone_number}")
        return
    
    # Await the dynamic data task
    dynamic_data = await dynamic_task

    # Extract configuration
    language = agent_config.get("language", "de")
    agent_id = agent_config.get("agent_id") or agent_config.get("id")
    voice_type = agent_config.get("voice_type", "").strip().lower()
    lang = agent_config.get("lang", "").strip().lower()

    voice_key = None

    if voice_type in VOICE_IDS and lang in VOICE_IDS[voice_type]:
        voice_key = VOICE_IDS[voice_type][lang]
    else:
        # fallback to a default voice
        voice_key = VOICE_IDS.get("female", {}).get("german")
    
    logger.info(f"🎤 Voice type: {voice_type}")
    logger.info(f"🌐 Language: {language}")
    logger.info(f"🤖 Agent ID: {agent_id}")
    
    # Create agent with complete prompt
    agent = InboundAgent(agent_config=agent_config, dynamic_data=dynamic_data)
    agent.set_caller_phone(caller_number)
    
    turn_detector = MultilingualModel()
    
    # Configure session with language support
    session = AgentSession(
        llm=openai.LLM(
            model="gpt-4.1-mini",
            api_key=os.getenv("OPENAI_API_KEY")
        ),
        stt=deepgram.STT(
            api_key=os.getenv("DEEPGRAM_API_KEY"),
            model="nova-3",
        ),
        tts=elevenlabs.TTS(
            api_key=os.getenv("ELEVENLABS_API_KEY"),
            model="eleven_flash_v2_5",
            voice_id=voice_key,
        ),
        vad=silero.VAD.load(min_silence_duration=0.05),
        turn_detection=turn_detector,
        min_endpointing_delay=TURN_DETECTION_MIN_ENDPOINTING_DELAY,
    )

    async def upload_transcript():
        """Upload transcript to GCS and send metadata to backend"""
        if not UPLOAD_TRANSCRIPTS:
            logger.info("⏭️ Transcript upload disabled")
            return
        try:
            # Generate transcript JSON
            transcript_obj = session.history.to_dict() if hasattr(session, 'history') else {"messages": []}
            transcript_json = json.dumps(transcript_obj, indent=2)
            
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            safe_phone = phone_number.replace("+", "").replace("-", "").replace(" ", "")
            blob_name = f"transcripts/{ctx.room.name}_{safe_phone}_{ts}.json"
            # Upload to GCS
            gcs = get_gcs_client()
            bucket = gcs.bucket(GOOGLE_BUCKET_NAME)
            blob = bucket.blob(blob_name)
            blob.upload_from_string(transcript_json, content_type="application/json")
            
            # Generate signed URL
            signed_url = blob.generate_signed_url(
                version="v4",
                expiration=timedelta(hours=24),
                method="GET"
            )
            
            logger.info(f"✅ Transcript uploaded: {blob_name}")
            # Build payload for backend
            payload = {
                "agent_id": agent.agent_id,
                "call_id": ctx.room.name,
                "caller_number": agent.caller_phone,
                "transcript_url": signed_url,
                "transcript_blob": blob_name,
                "recording_url": agent.recording_url,
                "recording_blob": agent.recording_blob_path,
                "uploaded_at": ts
            }
            
            logger.info(f"📤 Sending call data to backend:")
            logger.info(f" Agent ID: {agent.agent_id}")
            logger.info(f" Call ID: {ctx.room.name}")
            logger.info(f" Transcript blob: {blob_name}")
            logger.info(f" Recording blob: {agent.recording_blob_path}")
            
            # Send to backend with retries
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    async with httpx.AsyncClient(timeout=60.0) as c:
                        response = await c.post(
                            f"{BACKEND_API_URL}/api/agent/save-call-data",
                            json=payload
                        )
                        if response.status_code == 200:
                            logger.info("✅ Call data sent to backend")
                            break
                        else:
                            logger.warning(f"⚠️ Backend returned {response.status_code}")
                            logger.warning(f" Response: {response.text[:200]}")
                except httpx.ReadTimeout:
                    if attempt < max_retries - 1:
                        logger.warning(f"⚠️ Timeout on attempt {attempt + 1}, retrying...")
                        await asyncio.sleep(2)
                    else:
                        logger.error(f"❌ Backend timeout after {max_retries} attempts")
                except Exception as e:
                    logger.error(f"❌ Backend request failed: {e}")
                    break
        except Exception as e:
            logger.error(f"❌ Transcript upload failed: {e}")
            traceback.print_exc()
    
    ctx.add_shutdown_callback(upload_transcript)
    
    # ========== STATUS 1: INITIALIZED ==========
    await ctx.connect()
    await send_status_to_backend(ctx.room.name, "initialized", agent_id)
    
    # ============== START RECORDING ==============
    if UPLOAD_RECORDINGS:
        try:
            safe_phone = phone_number.replace("+", "").replace("-", "").replace(" ", "")
            ts = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
            recording_filename = f"recordings/{ctx.room.name}_{safe_phone}_{ts}.ogg"
            
            # Store blob path in agent
            agent.recording_blob_path = recording_filename
            
            logger.info(f"🎙️ Starting recording: {recording_filename}")
            decoded_creds = base64.b64decode(GCP_KEY_B64).decode("utf-8")
            
            req = api.RoomCompositeEgressRequest(
                room_name=ctx.room.name,
                audio_only=True,
                file_outputs=[
                    api.EncodedFileOutput(
                        file_type=api.EncodedFileType.OGG,
                        filepath=recording_filename,
                        gcp=api.GCPUpload(
                            bucket=GOOGLE_BUCKET_NAME,
                            credentials=decoded_creds
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
            
            # Build recording URL
            agent.recording_url = f"https://storage.googleapis.com/{GOOGLE_BUCKET_NAME}/{recording_filename}"
            
            logger.info(f"✅ Recording started (egress_id: {agent.egress_id})")
            logger.info(f" Recording blob path: {agent.recording_blob_path}")
            
            # Notify backend of recording blob path
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    await client.post(
                        f"{BACKEND_API_URL}/api/agent/update-call-recording",
                        json={
                            "call_id": ctx.room.name,
                            "agent_id": agent_id,
                            "recording_blob": recording_filename,
                            "recording_url": agent.recording_url
                        }
                    )
                    logger.info(f"✅ Recording blob path sent to backend")
            except Exception as e:
                logger.warning(f"⚠️ Could not send recording path to backend: {e}")
            
            await lkapi.aclose()
            
        except Exception as e:
            logger.error(f"❌ Failed to start recording: {e}")
            traceback.print_exc()
    else:
        logger.info("⏭️ Recording disabled")
    
    # Background audio
    background_audio = BackgroundAudioPlayer(
        thinking_sound=[
            AudioConfig(BuiltinAudioClip.OFFICE_AMBIENCE, volume=0.5),
            AudioConfig(BuiltinAudioClip.KEYBOARD_TYPING2, volume=0.8),
        ],
    )
    await background_audio.start(room=ctx.room, agent_session=session)
    
    try:
        # ========== WAIT FOR CALLER TO CONNECT ==========
        logger.info(f"⏳ Waiting for caller to join...")
        
        # Wait for the SIP participant (caller) to join
        participant = await ctx.wait_for_participant()
        agent.set_participant(participant)
        logger.info(f"✅ Caller joined: {participant.identity}")
        
        # ========== STATUS 2: CONNECTED ==========
        started_at = datetime.now(timezone.utc).isoformat()
        
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(
                    f"{BACKEND_API_URL}/api/agent/update-call-started",
                    json={
                        "call_id": ctx.room.name,
                        "agent_id": agent_id,
                        "caller_number": caller_number,
                        "started_at": started_at
                    }
                )
                logger.info(f"✅ Started_at timestamp set: {started_at}")
        except Exception as e:
            logger.warning(f"⚠️ Could not set started_at: {e}")
        
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
        
        logger.info(f"🗣️ Greeting in {language}: {greeting}")
        await session.say(greeting, allow_interruptions=True)
        await session_task
        logger.info("✅ Full session completed")
        
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        
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