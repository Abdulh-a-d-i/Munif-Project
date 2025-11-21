from fastapi import Depends, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError
from src.utils.jwt_utils import decode_access_token
import logging
import os 
import json
import base64
import httpx
import traceback
from datetime import datetime, timezone  

from src.utils.db import PGDB

db = PGDB()
auth_scheme = HTTPBearer()
# oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/login")

from fastapi.security import HTTPBearer,HTTPAuthorizationCredentials
auth_scheme = HTTPBearer()

import boto3
from botocore.exceptions import ClientError

def get_s3_client():
    """Initialize S3-compatible client for Hetzner Object Storage"""
    endpoint = os.getenv("HETZNER_ENDPOINT_URL")
    access_key = os.getenv("HETZNER_ACCESS_KEY")
    secret_key = os.getenv("HETZNER_SECRET_KEY")
    
    if not all([endpoint, access_key, secret_key]):
        raise RuntimeError("Missing Hetzner credentials")
    
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=os.getenv("HETZNER_REGION", "hel1")
    )

# Update _fetch_from_gcs_blob function
async def _fetch_from_s3_blob(blob_name: str) -> bytes:
    """Download file from Hetzner using blob name"""
    try:
        s3_client = get_s3_client()
        bucket_name = os.getenv("HETZNER_BUCKET_NAME")
        
        response = s3_client.get_object(Bucket=bucket_name, Key=blob_name)
        data = response['Body'].read()
        
        logging.info(f"✅ Downloaded {len(data)} bytes from Hetzner: {blob_name}")
        return data
            
    except ClientError as e:
        logging.error(f"❌ S3 download failed for {blob_name}: {e}")
        traceback.print_exc()
        return None

# Update fetch_and_store_transcript function
async def fetch_and_store_transcript(call_id: str, transcript_url: str = None, transcript_blob: str = None):
    """Download transcript from Hetzner blob"""
    try:
        transcript_data = None
        
        if transcript_blob:
            logging.info(f"📥 Downloading transcript from blob: {transcript_blob}")
            try:
                s3_client = get_s3_client()
                bucket_name = os.getenv("HETZNER_BUCKET_NAME")
                
                response = s3_client.get_object(Bucket=bucket_name, Key=transcript_blob)
                transcript_json = response['Body'].read().decode('utf-8')
                transcript_data = json.loads(transcript_json)
                logging.info(f"✅ Downloaded transcript from blob")
                
            except ClientError as e:
                logging.error(f"❌ Blob download failed: {e}")
                traceback.print_exc()
                return None
        else:
            logging.warning(f"⚠️ No transcript_blob provided for {call_id}")
            return None
        
        # Rest of function remains same...
        if transcript_data:
            has_content = False
            if isinstance(transcript_data, dict):
                items = transcript_data.get("items") or transcript_data.get("messages") or []
                has_content = len(items) > 0
            elif isinstance(transcript_data, list):
                has_content = len(transcript_data) > 0
            
            if has_content:
                db.update_call_history(call_id, {"transcript": transcript_data})
                logging.info(f"✅ Transcript stored ({len(str(transcript_data))} chars)")
            else:
                logging.warning(f"⚠️ Empty transcript for {call_id}")
                db.update_call_history(call_id, {"transcript": {"items": [], "note": "No conversation"}})
            
            return transcript_data
        
        return None
        
    except Exception as e:
        logging.error(f"❌ Error fetching transcript: {e}")
        traceback.print_exc()
        return None

# Update fetch_and_store_recording function
async def fetch_and_store_recording(call_id: str, recording_url: str = None, recording_blob_name: str = None):
    """
    Recording is already in Hetzner - we just store the blob path.
    NO DOWNLOAD NEEDED!
    """
    try:
        logging.info(f"🎵 Recording path already stored: {recording_blob_name}")
        
        # Path is already in DB from agent upload
        # We only verify it exists
        if recording_blob_name:
            s3_client = get_s3_client()
            bucket_name = os.getenv("HETZNER_BUCKET_NAME")
            
            try:
                s3_client.head_object(Bucket=bucket_name, Key=recording_blob_name)
                logging.info(f"✅ Recording exists in Hetzner: {recording_blob_name}")
            except ClientError:
                logging.warning(f"⚠️ Recording not found in bucket: {recording_blob_name}")
        
    except Exception as e:
        logging.error(f"❌ Error verifying recording: {e}")

def get_current_user(token: HTTPAuthorizationCredentials = Depends(auth_scheme)):
    # Token decode step
    try:
        logging.info(f"token: {token}")
        payload = decode_access_token(token.credentials)
        if not payload or "sub" not in payload:
            logging.warning("JWT decode failed or missing 'sub' claim.")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired token",
                headers={"WWW-Authenticate": "Bearer"},
            )
    except Exception as e:
        logging.error(f"JWT decode error: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user_id = int(payload["sub"])
    # DB lookup step
    try:
        user = db.get_user_by_id(user_id)
        if not user:
            logging.warning(f"User not found in DB for user_id: {user_id}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found.",
            )
        return user
    except Exception as e:
        logging.error(f"Database error while fetching user_id {user_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error while fetching user"
        )

def error_response(message: str, status_code: int = 400):
    return JSONResponse(
        status_code=status_code,
        content={"error": message}
    )



def is_admin(current_user=Depends(get_current_user)):
    """
    Check if the current user is an admin.
    If not, return a 403 Forbidden response.
    """
    # # Assuming current_user[5] is the admin flag (True/False)
    print(current_user)
    try:
        if current_user[5] == False:
            raise HTTPException(
                status_code=403,
                detail="You do not have permission to perform this action."
            )
    except Exception as e:
        logging.error(f"Error checking admin status for user : {e}")
        raise HTTPException(
            status_code=500,
            detail=f"{e}"
        )

    return current_user

def add_call_event(call_id: str, event_type: str, event_data: dict = None):
    """Store event in call_history.events_log (deduplicated)"""
    conn = db.get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT events_log FROM call_history WHERE call_id = %s", (call_id,))
            row = cursor.fetchone()
            if not row:
                logging.warning(f"Call {call_id} not found for event {event_type}")
                return

            events_log = row[0] or []
            if isinstance(events_log, str):
                try:
                    events_log = json.loads(events_log)
                except Exception:
                    events_log = []

            if any(ev.get("event") == event_type for ev in events_log):
                logging.info(f"Duplicate event {event_type} ignored for {call_id}")
                return

            events_log.append({
                "event": event_type,
                "timestamp": datetime.utcnow().isoformat(),
                "data": event_data or {}
            })

            cursor.execute(
                "UPDATE call_history SET events_log = %s WHERE call_id = %s",
                (json.dumps(events_log), call_id)
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Error adding call event: {e}")
    finally:
        db.release_connection(conn)  # ✅ 

import os
import asyncio
from dotenv import load_dotenv


def calculate_duration(started_at, ended_at) -> float:
    """
    Calculate call duration in seconds from timestamps.
    Handles None values, various timestamp formats, and timezone issues.
    """
    if not started_at or not ended_at:
        logging.warning(f"⚠️ Missing timestamps: start={started_at}, end={ended_at}")
        return 0
    
    try:
        # Convert to datetime objects if needed
        if isinstance(started_at, (int, float)):
            start_dt = datetime.fromtimestamp(started_at, tz=timezone.utc)
        elif isinstance(started_at, str):
            start_dt = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        elif isinstance(started_at, datetime):
            start_dt = started_at if started_at.tzinfo else started_at.replace(tzinfo=timezone.utc)
        else:
            logging.error(f"❌ Invalid started_at type: {type(started_at)}")
            return 0
        
        if isinstance(ended_at, (int, float)):
            end_dt = datetime.fromtimestamp(ended_at, tz=timezone.utc)
        elif isinstance(ended_at, str):
            end_dt = datetime.fromisoformat(ended_at.replace("Z", "+00:00"))
        elif isinstance(ended_at, datetime):
            end_dt = ended_at if ended_at.tzinfo else ended_at.replace(tzinfo=timezone.utc)
        else:
            logging.error(f"❌ Invalid ended_at type: {type(ended_at)}")
            return 0
        
        # Calculate duration
        duration = (end_dt - start_dt).total_seconds()
        
        # Sanity check
        if duration < 0:
            logging.warning(f"⚠️ Negative duration: {duration}s (end before start)")
            return 0
        
        if duration > 86400:  # More than 24 hours
            logging.warning(f"⚠️ Suspiciously long duration: {duration}s")
        
        return round(max(0, duration), 1)
        
    except Exception as e:
        logging.error(f"❌ Error calculating duration: {e}")
        logging.error(f"   started_at: {started_at} ({type(started_at)})")
        logging.error(f"   ended_at: {ended_at} ({type(ended_at)})")
        traceback.print_exc()
        return 0
    
    
def check_if_answered(events_log) -> bool:
    """
    Determine if call was actually answered by checking events_log.
    
    ⚠️ CRITICAL: We can ONLY check events_log because transcript 
    doesn't exist yet when room_ended fires!
    
    Returns True if:
    - SIP participant joined (means they picked up)
    - Recording started (egress_started means call was answered)
    """
    if not events_log:
        logging.warning("⚠️ No events_log - assuming unanswered")
        return False
    
    try:
        events = json.loads(events_log) if isinstance(events_log, str) else events_log
        
        # ✅ Check if recording started (definitive proof)
        egress_started = any(ev.get("event") == "egress_started" for ev in events)
        
        # ✅ Check if SIP participant joined (they picked up)
        sip_participant_joined = False
        for ev in events:
            if ev.get("event") == "participant_joined":
                participant = ev.get("data", {}).get("participant", {})
                identity = participant.get("identity", "")
                if identity.startswith("sip-"):
                    sip_participant_joined = True
                    break
        
        # ✅ Either condition means call was answered
        answered = egress_started or sip_participant_joined
        
        logging.info(f"📊 Answered check: egress={egress_started}, sip_joined={sip_participant_joined} → {answered}")
        
        return answered
        
    except Exception as e:
        logging.error(f"❌ Error parsing events_log: {e}")
        return False
    

def generate_presigned_url(blob_path: str, expiration: int = 3600) -> str:
    """
    Generate presigned URL for Hetzner object.
    
    Args:
        blob_path: Object key in bucket (e.g., "avatars/abc.jpg")
        expiration: URL validity in seconds (default 1 hour)
    
    Returns:
        Presigned URL string
    """
    try:
        s3_client = get_s3_client()
        bucket_name = os.getenv("HETZNER_BUCKET_NAME")
        
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': blob_path},
            ExpiresIn=expiration
        )
        
        logging.info(f"✅ Generated presigned URL (expires in {expiration}s): {blob_path}")
        return url
        
    except Exception as e:
        logging.error(f"❌ Failed to generate presigned URL: {e}")
        return None


import os
import uuid
import logging
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

class HetznerAvatarStorage:
    """
    Hetzner Object Storage Handler for Avatars
    Stores only the object key, generates presigned URLs on-demand
    """
    def __init__(self):
        self.bucket_name = os.getenv("HETZNER_BUCKET_NAME")
        self.s3_client = get_s3_client()
        logging.info(f"✅ Hetzner Avatar Storage initialized: {self.bucket_name}")
    
    def upload_avatar(self, file_content: bytes, file_extension: str) -> str:
        """
        Upload avatar and return the object key (NOT the URL).
        
        Returns:
            Object key (e.g., "avatars/uuid.jpg")
        """
        try:
            filename = f"avatars/{uuid.uuid4()}.{file_extension}"
            
            content_type_map = {
                'jpg': 'image/jpeg',
                'jpeg': 'image/jpeg',
                'png': 'image/png',
                'gif': 'image/gif',
                'webp': 'image/webp'
            }
            content_type = content_type_map.get(file_extension.lower(), 'application/octet-stream')
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=filename,
                Body=file_content,
                ContentType=content_type,
                CacheControl='public, max-age=31536000'
            )
            
            logging.info(f"✅ Uploaded avatar: {filename}")
            return filename  # Return KEY, not URL
            
        except Exception as e:
            logging.error(f"❌ Avatar upload failed: {e}")
            raise
    
    def delete_avatar(self, object_key: str) -> bool:
        """
        Delete avatar from bucket.
        
        Args:
            object_key: Object key (e.g., "avatars/uuid.jpg")
        """
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=object_key
            )
            logging.info(f"✅ Deleted avatar: {object_key}")
            return True
        except Exception as e:
            logging.error(f"❌ Delete failed: {e}")
            return False

# Replace singleton
hetzner_storage = HetznerAvatarStorage()  
