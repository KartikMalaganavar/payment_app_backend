from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from passlib.context import CryptContext
import razorpay
import asyncio
from aiokafka import AIOKafkaProducer
import json
import os
from models import Base, Transaction, UpiTransferRequest, UpiTransferResponse, TransactionResponse, TransactionStatus

# Configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/upi_app")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
RAZORPAY_KEY = os.getenv("RAZORPAY_KEY", "rzp_test_RYBI4WUnM7rsfD")
RAZORPAY_SECRET = os.getenv("RAZORPAY_SECRET", "RtTnGS2viOgYAf7AYtsdrFxV")

app = FastAPI(title="UPI Service", version="1.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database setup
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create tables
Base.metadata.create_all(bind=engine)

# Razorpay client
razorpay_client = razorpay.Client(auth=(RAZORPAY_KEY, RAZORPAY_SECRET))

# Kafka producer
kafka_producer = None

async def get_kafka_producer():
    global kafka_producer
    if kafka_producer is None:
        kafka_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
        await kafka_producer.start()
    return kafka_producer

# Database dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Password context for UPI PIN
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Mock user UPI PINs (in production, store securely)
user_upi_pins = {
    "user@upi": pwd_context.hash("1234"),
    "receiver@upi": pwd_context.hash("5678")
}

@app.post("/transfer", response_model=UpiTransferResponse)
async def upi_transfer(
    transfer_data: UpiTransferRequest,
    sender_upi: str,  # This would come from authenticated user context
    db: Session = Depends(get_db)
):
    # Verify UPI PIN
    if sender_upi not in user_upi_pins or not pwd_context.verify(transfer_data.pin, user_upi_pins[sender_upi]):
        raise HTTPException(status_code=400, detail="Invalid UPI PIN")
    
    # Create transaction record
    transaction = Transaction(
        sender_upi=sender_upi,
        receiver_upi=transfer_data.receiver_upi,
        amount=transfer_data.amount,
        status=TransactionStatus.PROCESSING
    )
    db.add(transaction)
    db.commit()
    db.refresh(transaction)
    
    try:
        # Simulate Razorpay UPI payment
        # In real implementation, use Razorpay UPI API
        payment_data = {
            "amount": int(transfer_data.amount * 100),  # Convert to paise
            "currency": "INR",
            "payment_capture": 1,
            "method": "upi",
            "notes": {
                "sender_upi": sender_upi,
                "receiver_upi": transfer_data.receiver_upi
            }
        }
        
        # Create Razorpay payment (mock for demo)
        # payment = razorpay_client.payment.create(payment_data)
        razorpay_payment_id = f"rzp_{transaction.id}_{int(asyncio.get_event_loop().time())}"
        
        # Simulate payment processing
        await asyncio.sleep(2)  # Simulate API call
        
        # For demo, assume payment is successful
        transaction.status = TransactionStatus.SUCCESS
        transaction.razorpay_payment_id = razorpay_payment_id
        db.commit()
        
        # Send notification event to Kafka
        producer = await get_kafka_producer()
        
        # Transaction event for logging
        transaction_event = {
            "event_type": "transaction_completed",
            "transaction_id": transaction.id,
            "sender_upi": sender_upi,
            "receiver_upi": transfer_data.receiver_upi,
            "amount": transfer_data.amount,
            "status": "success",
            "razorpay_payment_id": razorpay_payment_id,
            "timestamp": transaction.created_at.isoformat()
        }
        await producer.send("transaction_events", json.dumps(transaction_event).encode())
        
        # Notification events
        sender_notification = {
            "event_type": "transaction_debit",
            "user_upi": sender_upi,
            "transaction_id": transaction.id,
            "amount": transfer_data.amount,
            "counterparty": transfer_data.receiver_upi,
            "type": "debit",
            "timestamp": transaction.created_at.isoformat()
        }
        await producer.send("notification_events", json.dumps(sender_notification).encode())
        
        receiver_notification = {
            "event_type": "transaction_credit",
            "user_upi": transfer_data.receiver_upi,
            "transaction_id": transaction.id,
            "amount": transfer_data.amount,
            "counterparty": sender_upi,
            "type": "credit",
            "timestamp": transaction.created_at.isoformat()
        }
        await producer.send("notification_events", json.dumps(receiver_notification).encode())
        
        return UpiTransferResponse(
            transaction_id=transaction.id,
            status=TransactionStatus.SUCCESS,
            razorpay_payment_id=razorpay_payment_id,
            message="Transaction completed successfully"
        )
        
    except Exception as e:
        transaction.status = TransactionStatus.FAILED
        transaction.failure_reason = str(e)
        db.commit()
        
        # Send failure event
        producer = await get_kafka_producer()
        failure_event = {
            "event_type": "transaction_failed",
            "transaction_id": transaction.id,
            "sender_upi": sender_upi,
            "receiver_upi": transfer_data.receiver_upi,
            "amount": transfer_data.amount,
            "failure_reason": str(e),
            "timestamp": transaction.created_at.isoformat()
        }
        await producer.send("transaction_events", json.dumps(failure_event).encode())
        
        raise HTTPException(status_code=400, detail=f"Transaction failed: {str(e)}")

@app.get("/transactions/{upi_id}")
async def get_transactions(upi_id: str, db: Session = Depends(get_db)):
    transactions = db.query(Transaction).filter(
        (Transaction.sender_upi == upi_id) | (Transaction.receiver_upi == upi_id)
    ).order_by(Transaction.created_at.desc()).all()
    return transactions

@app.get("/transaction/{transaction_id}", response_model=TransactionResponse)
async def get_transaction(transaction_id: int, db: Session = Depends(get_db)):
    transaction = db.query(Transaction).filter(Transaction.id == transaction_id).first()
    if not transaction:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return transaction

@app.on_event("shutdown")
async def shutdown_event():
    if kafka_producer:
        await kafka_producer.stop()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)