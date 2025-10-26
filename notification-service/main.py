from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer
import asyncio
import json
import os
import smtplib
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from dotenv import load_dotenv

load_dotenv()

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "your-email@gmail.com")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "your-app-password")

app = FastAPI(title="Notification Service", version="1.0.0")

class NotificationService:
    def __init__(self):
        self.consumer = None
        self.smtp_connection = None
    
    async def connect_smtp(self):
        """Connect to SMTP server"""
        try:
            self.smtp_connection = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
            self.smtp_connection.starttls()
            self.smtp_connection.login(SMTP_USERNAME, SMTP_PASSWORD)
            print("Connected to SMTP server")
        except Exception as e:
            print(f"Failed to connect to SMTP: {e}")
    
    async def send_email_notification(self, to_email: str, subject: str, body: str):
        """Send email notification"""
        if not self.smtp_connection:
            await self.connect_smtp()
        
        try:
            msg = MIMEMultipart()
            msg['From'] = SMTP_USERNAME
            msg['To'] = to_email
            msg['Subject'] = subject
            
            msg.attach(MIMEText(body, 'html'))
            
            self.smtp_connection.send_message(msg)
            print(f"Email sent to {to_email}")
        except Exception as e:
            print(f"Failed to send email: {e}")
    
    async def send_sms_notification(self, phone: str, message: str):
        """Send SMS notification (mock implementation)"""
        # In production, integrate with SMS gateway like Twilio, MSG91, etc.
        print(f"SMS to {phone}: {message}")
    
    async def process_notification_event(self, event: dict):
        """Process notification events from Kafka"""
        event_type = event.get("event_type")
        
        if event_type == "transaction_debit":
            await self.send_transaction_debit_notification(event)
        elif event_type == "transaction_credit":
            await self.send_transaction_credit_notification(event)
        elif event_type == "user_registered":
            await self.send_welcome_notification(event)
    
    async def send_transaction_debit_notification(self, event: dict):
        """Send debit transaction notification"""
        user_upi = event.get("user_upi")
        amount = event.get("amount")
        counterparty = event.get("counterparty")
        
        # Email content
        subject = "Transaction Alert - Debit"
        body = f"""
        <html>
            <body>
                <h2>Transaction Alert</h2>
                <p><strong>UPI ID:</strong> {user_upi}</p>
                <p><strong>Amount:</strong> ₹{amount}</p>
                <p><strong>To:</strong> {counterparty}</p>
                <p><strong>Type:</strong> Debit</p>
                <p>Thank you for using our UPI service.</p>
            </body>
        </html>
        """
        
        # Get user email from user service (mock)
        user_email = f"{user_upi.split('@')[0]}@gmail.com"
        
        await self.send_email_notification(user_email, subject, body)
        
        # Send SMS
        sms_message = f"Debit alert: ₹{amount} to {counterparty}. UPI: {user_upi}"
        await self.send_sms_notification("+91XXXXXXXXXX", sms_message)
    
    async def send_transaction_credit_notification(self, event: dict):
        """Send credit transaction notification"""
        user_upi = event.get("user_upi")
        amount = event.get("amount")
        counterparty = event.get("counterparty")
        
        # Email content
        subject = "Transaction Alert - Credit"
        body = f"""
        <html>
            <body>
                <h2>Transaction Alert</h2>
                <p><strong>UPI ID:</strong> {user_upi}</p>
                <p><strong>Amount:</strong> ₹{amount}</p>
                <p><strong>From:</strong> {counterparty}</p>
                <p><strong>Type:</strong> Credit</p>
                <p>Thank you for using our UPI service.</p>
            </body>
        </html>
        """
        
        # Get user email from user service (mock)
        user_email = f"{user_upi.split('@')[0]}@gmail.com"
        
        await self.send_email_notification(user_email, subject, body)
        
        # Send SMS
        sms_message = f"Credit alert: ₹{amount} from {counterparty}. UPI: {user_upi}"
        await self.send_sms_notification("+91XXXXXXXXXX", sms_message)
    
    async def send_welcome_notification(self, event: dict):
        """Send welcome notification for new user registration"""
        user_email = event.get("email")
        upi_id = event.get("upi_id")
        
        subject = "Welcome to UPI Payment Service"
        body = f"""
        <html>
            <body>
                <h2>Welcome to UPI Payment Service!</h2>
                <p>Your account has been successfully created.</p>
                <p><strong>UPI ID:</strong> {upi_id}</p>
                <p>You can now start sending and receiving payments using your UPI ID.</p>
                <p>Thank you for choosing our service!</p>
            </body>
        </html>
        """
        
        await self.send_email_notification(user_email, subject, body)
    
    async def start_consuming(self):
        """Start consuming messages from Kafka"""
        self.consumer = AIOKafkaConsumer(
            'notification_events',
            'user_events',
            bootstrap_servers=KAFKA_BROKER,
            group_id="notification_service"
        )
        
        await self.consumer.start()
        await self.connect_smtp()
        
        try:
            async for msg in self.consumer:
                try:
                    event = json.loads(msg.value.decode())
                    print(f"Received event: {event['event_type']}")
                    await self.process_notification_event(event)
                except Exception as e:
                    print(f"Error processing message: {e}")
        except Exception as e:
            print(f"Consumer error: {e}")
        finally:
            await self.consumer.stop()
            if self.smtp_connection:
                self.smtp_connection.quit()

notification_service = NotificationService()

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(notification_service.start_consuming())

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)