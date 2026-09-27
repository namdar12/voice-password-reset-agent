import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class GmailSender:
    def __init__(self):
        """Initialize the Gmail sender with credentials from environment variables."""
        self.email = os.getenv("GMAIL_USER")
        self.password = os.getenv("GMAIL_APP_PASSWORD")

        if not self.email or not self.password:
            raise ValueError("Gmail credentials not found in environment variables")

    def _create_message(self, to_email: str, code: str) -> MIMEMultipart:
        """Create the email message with the verification code."""
        message = MIMEMultipart()
        message["From"] = self.email
        message["To"] = to_email
        message["Subject"] = "Password Reset Verification Code"

        body = f"""
        Hello,

        You have requested to reset your password. Your verification code is:

        {code}

        If you did not request this password reset, please ignore this email.

        Best regards,
        Your Application Team
        """

        message.attach(MIMEText(body, "plain"))
        return message

    async def send_verification_code(self, email: str, code: str) -> bool:
        """Send a verification code to the specified email address."""
        try:
            # Create the email message
            message = self._create_message(email, code)

            # Create SMTP connection
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                server.login(self.email, self.password)
                server.send_message(message)

            return True

        except Exception as e:
            print(f"Error sending verification code: {e}")
            return False


# Create a singleton instance
_gmail_sender: Optional[GmailSender] = None


def get_gmail_sender() -> GmailSender:
    """Get or create the Gmail sender singleton instance."""
    global _gmail_sender
    if _gmail_sender is None:
        _gmail_sender = GmailSender()
    return _gmail_sender


async def send_verification_code(email: str, code: str) -> bool:
    """Send a verification code to the specified email address."""
    sender = get_gmail_sender()
    return await sender.send_verification_code(email, code)
