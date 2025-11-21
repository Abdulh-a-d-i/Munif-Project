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

#def get_gcs_client():
 #   """Initialize GCS client with service account"""
  #  gcp_key_b64 = os.getenv("GCS_SERVICE_ACCOUNT_KEY") or os.getenv("GCP_SERVICE_ACCOUNT_KEY_BASE64")
   # if not gcp_key_b64:
    #    raise RuntimeError("GCS_SERVICE_ACCOUNT_KEY not set")
    
   # decoded = base64.b64decode(gcp_key_b64).decode("utf-8")
    #key_json = json.loads(decoded)
    #credentials = service_account.Credentials.from_service_account_info(key_json)
    #return storage.Client(credentials=credentials, project=key_json.get("project_id"))


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











# ============================================
# ✅ HELPER FUNCTIONS
# ============================================

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
    




import os
import uuid
import logging
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

class CloudflareR2Storage:
    """
    Cloudflare R2 Storage Handler (S3-compatible)
    """
    def __init__(self):
        self.account_id = os.getenv("CLOUDFLARE_ACCOUNT_ID")
        self.access_key_id = os.getenv("CLOUDFLARE_ACCESS_KEY_ID")
        self.secret_access_key = os.getenv("CLOUDFLARE_SECRET_ACCESS_KEY")
        self.bucket_name = os.getenv("CLOUDFLARE_BUCKET_NAME")
        self.public_url = os.getenv("CLOUDFLARE_PUBLIC_URL")  
        
        if not all([self.account_id, self.access_key_id, self.secret_access_key, self.bucket_name]):
            raise ValueError("Missing Cloudflare R2 credentials in .env")
        
        # Initialize S3 client for R2
        self.s3_client = boto3.client(
            's3',
            endpoint_url=f'https://{self.account_id}.r2.cloudflarestorage.com',
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name='auto'  # R2 uses 'auto'
        )
        
        logging.info(f"✅ Cloudflare R2 initialized: {self.bucket_name}")
    
    def upload_avatar(self, file_content: bytes, file_extension: str) -> str:
        """
        Upload agent avatar to R2 bucket.
        
        Args:
            file_content: Binary image data
            file_extension: File extension (jpg, png, etc.)
        
        Returns:
            Public URL of uploaded image
        """
        try:
            # Generate unique filename
            filename = f"avatars/{uuid.uuid4()}.{file_extension}"
            
            # Determine content type
            content_type_map = {
                'jpg': 'image/jpeg',
                'jpeg': 'image/jpeg',
                'png': 'image/png',
                'gif': 'image/gif',
                'webp': 'image/webp'
            }
            content_type = content_type_map.get(file_extension.lower(), 'application/octet-stream')
            
            # Upload to R2
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=filename,
                Body=file_content,
                ContentType=content_type,
                CacheControl='public, max-age=31536000'  # Cache for 1 year
            )
            
            # Generate public URL
            if self.public_url:
                public_url = f"{self.public_url}/{filename}"
            else:
                # Fallback to account-based URL
                public_url = f"https://{self.account_id}.r2.cloudflarestorage.com/{self.bucket_name}/{filename}"
            
            logging.info(f"✅ Uploaded avatar: {public_url}")
            return public_url
            
        except ClientError as e:
            logging.error(f"❌ R2 upload failed: {e}")
            raise
        except Exception as e:
            logging.error(f"❌ Upload error: {e}")
            raise
    
    def delete_avatar(self, file_url: str) -> bool:
        """
        Delete avatar from R2 bucket.
        
        Args:
            file_url: Full URL of the file to delete
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Extract key from URL
            if self.public_url in file_url:
                key = file_url.replace(f"{self.public_url}/", "")
            else:
                # Extract from account URL
                parts = file_url.split(f"{self.bucket_name}/")
                if len(parts) == 2:
                    key = parts[1]
                else:
                    logging.warning(f"Could not parse URL: {file_url}")
                    return False
            
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=key
            )
            
            logging.info(f"✅ Deleted avatar: {key}")
            return True
            
        except ClientError as e:
            logging.error(f"❌ R2 delete failed: {e}")
            return False
        except Exception as e:
            logging.error(f"❌ Delete error: {e}")
            return False


# Singleton instance
r2_storage = CloudflareR2Storage()
