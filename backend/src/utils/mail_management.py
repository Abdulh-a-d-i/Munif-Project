import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
import pytz
from dotenv import load_dotenv
import os

load_dotenv()
MAIL_SENDER = os.getenv("MAIL_SENDER")
MAIL_PASSWORD = os.getenv("MAIL_PASSWORD")
TIMEZONE = os.getenv("TIMEZONE", "CET")

class Send_Mail:
    def __init__(self):
        self.MAIL_SENDER = MAIL_SENDER
        self.EMAIL_PASSWORD = MAIL_PASSWORD
        self.TIMEZONE = TIMEZONE
        self.timeout = 30  # Increased for reliability

    async def send_email(
        self,
        to_email: str,
        subject: str,
        html_body: str,
        plain_body: str = None,
    ):
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = self.MAIL_SENDER
            msg["To"] = to_email
            if plain_body:
                msg.attach(MIMEText(plain_body, "plain"))
            msg.attach(MIMEText(html_body, "html"))

            server = smtplib.SMTP("smtp.gmail.com", 587, timeout=self.timeout)
            try:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(self.MAIL_SENDER, self.EMAIL_PASSWORD)
                server.send_message(msg)
            finally:
                server.quit()

            logging.info(f" Email sent to {to_email}")
            return True
        except Exception as e:
            logging.error(f" Error sending email: {e}")
            return False

    async def send_email_with_calendar_event(
        self,
        attendee_email: str,
        attendee_name: str,
        appointment_date: str,
        start_time: str,
        end_time: str,
        title: str,
        description: str,
        organizer_name: str,
        organizer_email: str,
    ):
        try:
            tz = pytz.timezone(self.TIMEZONE)
            start_dt = tz.localize(datetime.strptime(f"{appointment_date} {start_time}", "%Y-%m-%d %H:%M"))
            end_dt = tz.localize(datetime.strptime(f"{appointment_date} {end_time}", "%Y-%m-%d %H:%M"))
            dtstamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            dtstart = start_dt.astimezone(pytz.UTC).strftime("%Y%m%dT%H%M%SZ")
            dtend = end_dt.astimezone(pytz.UTC).strftime("%Y%m%dT%H%M%SZ")
            uid = f"{dtstamp}@{organizer_email.split('@')[1]}"
           
            ics_content = f"""BEGIN:VCALENDAR
PRODID:-//YourCompany//AI Scheduler//EN
VERSION:2.0
CALSCALE:GREGORIAN
METHOD:REQUEST
BEGIN:VEVENT
UID:{uid}
DTSTAMP:{dtstamp}
DTSTART:{dtstart}
DTEND:{dtend}
SUMMARY:{title}
DESCRIPTION:{description}
LOCATION:Online Meeting
STATUS:CONFIRMED
ORGANIZER;CN={organizer_name}:MAILTO:{organizer_email}
ATTENDEE;CN={attendee_name};RSVP=TRUE:MAILTO:{attendee_email}
BEGIN:VALARM
TRIGGER:-PT15M
ACTION:DISPLAY
DESCRIPTION:Reminder
END:VALARM
END:VEVENT
END:VCALENDAR
""".replace("\n", "\r\n")

            msg = MIMEMultipart("mixed")
            msg["Subject"] = f"Appointment Confirmation: {title}"
            msg["From"] = f"{organizer_name} <{organizer_email}>"
            msg["To"] = attendee_email

            alternative = MIMEMultipart("alternative")
            msg.attach(alternative)

            # Email body
            plain_body = (
                f"Dear {attendee_name},\n\n"
                f"Your appointment has been scheduled.\n\n"
                f" Date: {appointment_date}\n"
                f" Time: {start_time} - {end_time}\n"
                f"Best regards,\n{organizer_name}"
            )
            html_body = f"""
            <html>
                <body>
                    <p>Dear {attendee_name},</p>
                    <p>Your appointment has been scheduled.</p>
                    <ul>
                        <li><b>Date:</b> {appointment_date}</li>
                        <li><b>Time:</b> {start_time} - {end_time}</li>
                        <li><b>Notes:</b> {description or 'N/A'}</li>
                    </ul>
                    <p>You can accept or decline the meeting using your calendar buttons.</p>
                    <p>Best regards,<br>{organizer_name}</p>
                </body>
            </html>
            """
            alternative.attach(MIMEText(plain_body, "plain"))
            alternative.attach(MIMEText(html_body, "html"))

            ics_part = MIMEBase("text", "calendar", method="REQUEST", name="invite.ics")
            ics_part.set_payload(ics_content)
            encoders.encode_base64(ics_part)
            ics_part.add_header("Content-Transfer-Encoding", "base64")
            ics_part.add_header("Content-Disposition", "attachment; filename=invite.ics")
            ics_part.add_header("Content-Class", "urn:content-classes:calendarmessage")
            msg.attach(ics_part)

            server = smtplib.SMTP("smtp.gmail.com", 587, timeout=self.timeout)
            try:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(self.MAIL_SENDER, self.EMAIL_PASSWORD)
                server.send_message(msg)
            finally:
                server.quit()

            logging.info(f" Email with calendar invite sent to {attendee_email}")
            return True
        except Exception as e:
            logging.error(f" Error sending email with calendar event: {e}")
            return False

    async def send_password_reset_email(self, email: str, reset_token: str, frontend_url: str = "https://www.mrbot-ki.de"):
        """Send password reset email with token"""
        try:
            reset_link = f"{frontend_url}/reset-password?token={reset_token}"
           
            subject = "Password Reset Request"
           
            html_body = f"""
            <html>
                <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                    <h2 style="color: #333;">Password Reset Request</h2>
                    <p>You requested to reset your password. Click the button below to proceed:</p>
                   
                    <a href="{reset_link}"
                    style="display: inline-block; padding: 12px 24px; background-color: #007bff;
                            color: white; text-decoration: none; border-radius: 4px; margin: 20px 0;">
                        Reset Password
                    </a>
                   
                    <p style="color: #666; font-size: 14px;">
                        This link will expire in 1 hour for security reasons.
                    </p>
                   
                    <p style="color: #666; font-size: 14px;">
                        If you didn't request this, please ignore this email.
                    </p>
                   
                    <hr style="border: 1px solid #eee; margin: 30px 0;">
                    <p style="color: #999; font-size: 12px;">
                        If the button doesn't work, copy and paste this link:<br>
                        {reset_link}
                    </p>
                </body>
            </html>
            """
            plain_body = (
                f"You requested to reset your password.\n\n"
                f"Reset link: {reset_link}\n\n"
                f"This link will expire in 1 hour.\n"
                f"If you didn't request this, ignore this email."
            )
           
            return await self.send_email(email, subject, html_body, plain_body)
           
        except Exception as e:
            logging.error(f" Failed to send reset email: {e}")
            return False
        

    async def send_owner_appointment_notification(
        self,
        owner_email: str,
        owner_name: str,
        customer_name: str,
        customer_email: str,
        customer_phone: str,
        appointment_date: str,
        start_time: str,
        end_time: str,
        title: str,
        description: str,
    ):
        """
        Send appointment notification to business owner.
        Includes customer details for owner's reference.
        """
        try:
            tz = pytz.timezone(self.TIMEZONE)
            start_dt = tz.localize(datetime.strptime(f"{appointment_date} {start_time}", "%Y-%m-%d %H:%M"))
            end_dt = tz.localize(datetime.strptime(f"{appointment_date} {end_time}", "%Y-%m-%d %H:%M"))
            dtstamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            dtstart = start_dt.astimezone(pytz.UTC).strftime("%Y%m%dT%H%M%SZ")
            dtend = end_dt.astimezone(pytz.UTC).strftime("%Y%m%dT%H%M%SZ")
            uid = f"{dtstamp}@{owner_email.split('@')[1]}"
        
            ics_content = f"""BEGIN:VCALENDAR
    PRODID:-//YourCompany//AI Scheduler//EN
    VERSION:2.0
    CALSCALE:GREGORIAN
    METHOD:REQUEST
    BEGIN:VEVENT
    UID:{uid}
    DTSTAMP:{dtstamp}
    DTSTART:{dtstart}
    DTEND:{dtend}
    SUMMARY:New Appointment: {customer_name}
    DESCRIPTION:Customer Details:\\n\\nName: {customer_name}\\nEmail: {customer_email}\\nPhone: {customer_phone or 'N/A'}\\n\\nNotes: {description}
    LOCATION:Your Business
    STATUS:CONFIRMED
    ORGANIZER;CN={owner_name}:MAILTO:{owner_email}
    ATTENDEE;CN={owner_name};RSVP=TRUE:MAILTO:{owner_email}
    BEGIN:VALARM
    TRIGGER:-PT15M
    ACTION:DISPLAY
    DESCRIPTION:Reminder
    END:VALARM
    END:VEVENT
    END:VCALENDAR
    """.replace("\n", "\r\n")

            msg = MIMEMultipart("mixed")
            msg["Subject"] = f"New Appointment Booked: {customer_name}"
            msg["From"] = f"{owner_name} <{self.MAIL_SENDER}>"
            msg["To"] = owner_email

            alternative = MIMEMultipart("alternative")
            msg.attach(alternative)

            # Email body for owner
            plain_body = (
                f"Dear {owner_name},\n\n"
                f"A new appointment has been booked by your AI agent.\n\n"
                f"📅 Date: {appointment_date}\n"
                f"🕐 Time: {start_time} - {end_time}\n\n"
                f"👤 Customer Details:\n"
                f"   Name: {customer_name}\n"
                f"   Email: {customer_email}\n"
                f"   Phone: {customer_phone or 'N/A'}\n\n"
                f"📝 Notes: {description or 'None'}\n\n"
                f"This appointment has been added to your calendar.\n\n"
                f"Best regards,\nYour AI Assistant"
            )
            
            html_body = f"""
            <html>
                <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                    <h2 style="color: #2563eb;">New Appointment Booked 🎉</h2>
                    <p>Dear {owner_name},</p>
                    <p>A new appointment has been booked by your AI agent.</p>
                    
                    <div style="background-color: #f3f4f6; padding: 20px; border-radius: 8px; margin: 20px 0;">
                        <h3 style="margin-top: 0; color: #1f2937;">Appointment Details</h3>
                        <p><strong>📅 Date:</strong> {appointment_date}</p>
                        <p><strong>🕐 Time:</strong> {start_time} - {end_time}</p>
                        
                        <h3 style="color: #1f2937; margin-top: 20px;">Customer Information</h3>
                        <p><strong>👤 Name:</strong> {customer_name}</p>
                        <p><strong>📧 Email:</strong> {customer_email}</p>
                        <p><strong>📱 Phone:</strong> {customer_phone or 'N/A'}</p>
                        
                        {f'<p><strong>📝 Notes:</strong> {description}</p>' if description else ''}
                    </div>
                    
                    <p style="color: #6b7280; font-size: 14px;">
                        This appointment has been automatically added to your calendar.
                        The customer has also received a confirmation email.
                    </p>
                    
                    <p>Best regards,<br>Your AI Assistant</p>
                </body>
            </html>
            """
            
            alternative.attach(MIMEText(plain_body, "plain"))
            alternative.attach(MIMEText(html_body, "html"))

            ics_part = MIMEBase("text", "calendar", method="REQUEST", name="invite.ics")
            ics_part.set_payload(ics_content)
            encoders.encode_base64(ics_part)
            ics_part.add_header("Content-Transfer-Encoding", "base64")
            ics_part.add_header("Content-Disposition", "attachment; filename=invite.ics")
            ics_part.add_header("Content-Class", "urn:content-classes:calendarmessage")
            msg.attach(ics_part)

            server = smtplib.SMTP("smtp.gmail.com", 587, timeout=self.timeout)
            try:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(self.MAIL_SENDER, self.EMAIL_PASSWORD)
                server.send_message(msg)
            finally:
                server.quit()

            logging.info(f"📧 Owner notification sent to {owner_email}")
            return True
            
        except Exception as e:
            logging.error(f"❌ Error sending owner notification: {e}")
            return False


    async def send_contact_form_email(
        self,
        first_name: str,
        last_name: str,
        customer_email: str,
        customer_message: str = None,
        recipient_email: str = "info@mrbot-ki.de"
    ):
        """
        Send contact form submission to business email.
        Form fields: First Name, Last Name, Email, Message
        """
        try:
            full_name = f"{first_name} {last_name}".strip()
            subject = f"📬 Contact Form Submission - {full_name}"
            
            # Build plain text version
            plain_body = f"""
    New Contact Form Submission

    Customer Details:
    ─────────────────
    Name: {full_name}
    Email: {customer_email}

    Message:
    ─────────────────
    {customer_message or 'No message provided'}

    ─────────────────
    This email was sent from your website contact form.
    Please respond directly to: {customer_email}
    """
            
            html_body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{
                font-family: Arial, sans-serif;
                line-height: 1.6;
                color: #333;
                max-width: 600px;
                margin: 0 auto;
            }}
            .header {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 30px;
                text-align: center;
                border-radius: 8px 8px 0 0;
            }}
            .content {{
                background: #f9fafb;
                padding: 30px;
                border: 1px solid #e5e7eb;
            }}
            .section {{
                background: white;
                padding: 20px;
                margin-bottom: 20px;
                border-radius: 8px;
                border-left: 4px solid #667eea;
            }}
            .section-title {{
                color: #667eea;
                font-size: 16px;
                font-weight: bold;
                margin-bottom: 15px;
                text-transform: uppercase;
            }}
            .info-row {{
                display: flex;
                padding: 8px 0;
                border-bottom: 1px solid #f3f4f6;
            }}
            .info-row:last-child {{
                border-bottom: none;
            }}
            .info-label {{
                font-weight: bold;
                color: #6b7280;
                min-width: 100px;
            }}
            .info-value {{
                color: #1f2937;
                flex: 1;
            }}
            .message-box {{
                background: #f9fafb;
                padding: 15px;
                border-radius: 6px;
                border: 1px solid #e5e7eb;
                white-space: pre-wrap;
                word-wrap: break-word;
            }}
            .footer {{
                background: #1f2937;
                color: #9ca3af;
                padding: 20px;
                text-align: center;
                font-size: 14px;
                border-radius: 0 0 8px 8px;
            }}
            .reply-button {{
                display: inline-block;
                background: #667eea;
                color: white;
                padding: 12px 30px;
                text-decoration: none;
                border-radius: 6px;
                margin-top: 20px;
                font-weight: bold;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1 style="margin: 0;">📬 New Contact Form Submission</h1>
            <p style="margin: 10px 0 0 0; opacity: 0.9;">Someone reached out through your website</p>
        </div>
        
        <div class="content">
            <div class="section">
                <div class="section-title">👤 Customer Information</div>
                <div class="info-row">
                    <span class="info-label">Name:</span>
                    <span class="info-value">{full_name}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Email:</span>
                    <span class="info-value"><a href="mailto:{customer_email}" style="color: #667eea; text-decoration: none;">{customer_email}</a></span>
                </div>
            </div>
            
            <div class="section">
                <div class="section-title">💬 Message</div>
                <div class="message-box">
                    {customer_message or '<em>No message provided</em>'}
                </div>
            </div>
            
            <div style="text-align: center;">
                <a href="mailto:{customer_email}" class="reply-button">Reply to Customer</a>
            </div>
        </div>
        
        <div class="footer">
            <p style="margin: 0;">This email was automatically generated from your website contact form.</p>
            <p style="margin: 10px 0 0 0;">Please respond directly to <strong>{customer_email}</strong></p>
        </div>
    </body>
    </html>
    """
            
            # Send email
            success = await self.send_email(
                to_email=recipient_email,
                subject=subject,
                html_body=html_body,
                plain_body=plain_body
            )
            
            if success:
                logging.info(f"📧 Contact form email sent to {recipient_email} from {customer_email}")
            else:
                logging.error(f"❌ Failed to send contact form email from {customer_email}")
            
            return success
            
        except Exception as e:
            logging.error(f"❌ Error sending contact form email: {e}")
            return False