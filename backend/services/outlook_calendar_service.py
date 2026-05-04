import os
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
import requests
import msal

from dotenv import load_dotenv

load_dotenv()

MICROSOFT_CLIENT_ID = os.getenv("MICROSOFT_CLIENT_ID")
MICROSOFT_CLIENT_SECRET = os.getenv("MICROSOFT_CLIENT_SECRET")
MICROSOFT_TENANT_ID = os.getenv("MICROSOFT_TENANT_ID", "common")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class OutlookCalendarService:
    """
    Wrapper for Microsoft Graph Calendar API operations.
    Handles authentication, token refresh, and calendar operations.
    """
    
    GRAPH_API_ENDPOINT = "https://graph.microsoft.com/v1.0"
    SCOPES = ["Calendars.ReadWrite", "User.Read"]  # offline_access handled by MSAL
    
    def __init__(self, credentials_dict: Dict):
        """
        Initialize with credentials dictionary from database.
        
        Args:
            credentials_dict: Dict with access_token, refresh_token, token_expiry, scopes
        """
        self.access_token = credentials_dict.get('access_token')
        self.refresh_token = credentials_dict.get('refresh_token')
        self.token_expiry = credentials_dict.get('token_expiry')
        
        if isinstance(self.token_expiry, str):
            self.token_expiry = datetime.fromisoformat(self.token_expiry.replace('Z', '+00:00'))
        
        self.scopes = credentials_dict.get('scopes', ','.join(self.SCOPES))
        if isinstance(self.scopes, str):
            self.scopes = self.scopes.split(',')
        
        # Create MSAL confidential client for token refresh
        self.msal_app = msal.ConfidentialClientApplication(
            MICROSOFT_CLIENT_ID,
            authority=f"https://login.microsoftonline.com/{MICROSOFT_TENANT_ID}",
            client_credential=MICROSOFT_CLIENT_SECRET
        )
        
        self._refresh_if_needed()
    
    def _refresh_if_needed(self):
        """Refresh access token if expired or about to expire."""
        try:
            # Check if token is expired or will expire in next 5 minutes
            if self.token_expiry:
                if isinstance(self.token_expiry, str):
                    expiry = datetime.fromisoformat(self.token_expiry.replace('Z', '+00:00'))
                else:
                    expiry = self.token_expiry
                
                if datetime.now(timezone.utc) >= expiry - timedelta(minutes=5):
                    logger.info("Access token expired or expiring soon, refreshing...")
                    result = self.msal_app.acquire_token_by_refresh_token(
                        self.refresh_token,
                        scopes=self.SCOPES
                    )
                    
                    if "access_token" in result:
                        self.access_token = result['access_token']
                        if 'refresh_token' in result:
                            self.refresh_token = result['refresh_token']
                        
                        # Calculate new expiry
                        expires_in = result.get('expires_in', 3600)
                        self.token_expiry = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
                        logger.info("Token refreshed successfully")
                    else:
                        logger.error(f"Token refresh failed: {result.get('error_description')}")
        except Exception as e:
            logger.error(f"Error refreshing token: {e}")
    
    def _make_request(self, method: str, endpoint: str, **kwargs):
        """
        Make authenticated request to Microsoft Graph API.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (e.g., '/me/calendar/events')
            **kwargs: Additional arguments for requests
            
        Returns:
            Response JSON or None on error
        """
        self._refresh_if_needed()
        
        headers = kwargs.get('headers', {})
        headers['Authorization'] = f'Bearer {self.access_token}'
        headers['Content-Type'] = 'application/json'
        kwargs['headers'] = headers
        
        url = f"{self.GRAPH_API_ENDPOINT}{endpoint}"
        
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json() if response.content else {}
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error in Outlook Calendar API: {e}")
            logger.error(f"Response: {e.response.text if e.response else 'No response'}")
            raise
        except Exception as e:
            logger.error(f"Error making request to Outlook Calendar API: {e}")
            raise
    
    def list_events(
        self, 
        time_min: datetime, 
        time_max: datetime = None, 
        max_results: int = 100
    ) -> List[Dict]:
        """
        List events from Outlook calendar.
        
        Args:
            time_min: Minimum time (inclusive)
            time_max: Maximum time (exclusive), defaults to 1 week from time_min
            max_results: Maximum number of events to return
            
        Returns:
            List of event dictionaries
        """
        if not time_max:
            time_max = time_min + timedelta(days=7)
        
        # Ensure timezone aware
        if time_min.tzinfo is None:
            time_min = time_min.replace(tzinfo=timezone.utc)
        if time_max.tzinfo is None:
            time_max = time_max.replace(tzinfo=timezone.utc)
        
        # Convert to ISO format for Microsoft Graph
        start_str = time_min.isoformat()
        end_str = time_max.isoformat()
        
        # Build query parameters
        params = {
            '$top': max_results,
            '$filter': f"start/dateTime ge '{start_str}' and end/dateTime le '{end_str}'",
            '$orderby': 'start/dateTime'
        }
        
        try:
            result = self._make_request('GET', '/me/calendar/events', params=params)
            return result.get('value', [])
        except Exception as e:
            logger.error(f"Error listing Outlook events: {e}")
            return []
    
    def check_availability(self, start_datetime: datetime, end_datetime: datetime) -> bool:
        """
        Check if time slot is available (no overlapping events).
        
        Args:
            start_datetime: Start time
            end_datetime: End time
            
        Returns:
            True if available, False if conflicting events exist
        """
        try:
            events = self.list_events(
                time_min=start_datetime - timedelta(hours=1),
                time_max=end_datetime + timedelta(hours=1)
            )
            
            # Ensure timezone aware
            if start_datetime.tzinfo is None:
                start_datetime = start_datetime.replace(tzinfo=timezone.utc)
            if end_datetime.tzinfo is None:
                end_datetime = end_datetime.replace(tzinfo=timezone.utc)
            
            for event in events:
                # Parse event times
                event_start_str = event.get('start', {}).get('dateTime')
                event_end_str = event.get('end', {}).get('dateTime')
                
                if not event_start_str or not event_end_str:
                    continue
                
                # Parse ISO format with timezone
                event_start = datetime.fromisoformat(event_start_str.replace('Z', '+00:00'))
                event_end = datetime.fromisoformat(event_end_str.replace('Z', '+00:00'))
                
                # Check for overlap
                if (start_datetime < event_end and end_datetime > event_start):
                    logger.warning(f"Conflict found with Outlook event: {event.get('subject')} ({event_start} - {event_end})")
                    return False
            
            return True
        except Exception as e:
            logger.error(f"Error checking Outlook availability: {e}")
            return False
    
    def create_event(
        self,
        summary: str,
        start_datetime: datetime,
        end_datetime: datetime,
        description: str = '',
        location: str = '',
        attendees: List[str] = None
    ) -> Dict:
        """
        Create a new Outlook calendar event.
        
        Args:
            summary: Event title
            start_datetime: Start time
            end_datetime: End time
            description: Event description
            location: Event location
            attendees: List of attendee email addresses
            
        Returns:
            Created event dictionary with event ID
        """
        # Ensure timezone aware
        if start_datetime.tzinfo is None:
            start_datetime = start_datetime.replace(tzinfo=timezone.utc)
        if end_datetime.tzinfo is None:
            end_datetime = end_datetime.replace(tzinfo=timezone.utc)
        
        # Build attendees list
        attendees_list = []
        if attendees:
            for email in attendees:
                attendees_list.append({
                    "emailAddress": {
                        "address": email
                    },
                    "type": "required"
                })
        
        event_body = {
            "subject": summary,
            "body": {
                "contentType": "Text",
                "content": description
            },
            "start": {
                "dateTime": start_datetime.isoformat(),
                "timeZone": "UTC"
            },
            "end": {
                "dateTime": end_datetime.isoformat(),
                "timeZone": "UTC"
            },
            "location": {
                "displayName": location
            },
            "attendees": attendees_list,
            "isReminderOn": True,
            "reminderMinutesBeforeStart": 15
        }
        
        try:
            result = self._make_request('POST', '/me/calendar/events', json=event_body)
            logger.info(f"Outlook event created: {result.get('id')}")
            return result
        except Exception as e:
            logger.error(f"Error creating Outlook event: {e}")
            raise
    
    def get_updated_credentials(self) -> Dict:
        """
        Get current credentials (after potential refresh).
        
        Returns:
            Dict with access_token, refresh_token, token_expiry, scopes
        """
        return {
            'access_token': self.access_token,
            'refresh_token': self.refresh_token,
            'token_expiry': self.token_expiry,
            'scopes': ','.join(self.scopes) if isinstance(self.scopes, list) else self.scopes
        }
