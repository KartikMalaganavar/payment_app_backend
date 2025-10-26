from sqlalchemy import Column, Integer, String, DateTime, Float, Enum as SQLEnum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from pydantic import BaseModel
from datetime import datetime
from enum import Enum

Base = declarative_base()

class TransactionStatus(str, Enum):
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"
    PROCESSING = "processing"

class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True)
    sender_upi = Column(String, nullable=False)
    receiver_upi = Column(String, nullable=False)
    amount = Column(Float, nullable=False)
    status = Column(SQLEnum(TransactionStatus), default=TransactionStatus.PENDING)
    razorpay_payment_id = Column(String, nullable=True)
    failure_reason = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

class UpiTransferRequest(BaseModel):
    receiver_upi: str
    amount: float
    pin: str

class UpiTransferResponse(BaseModel):
    transaction_id: int
    status: TransactionStatus
    razorpay_payment_id: str = None
    message: str = None

    class Config:
        from_attributes = True

class TransactionResponse(BaseModel):
    id: int
    sender_upi: str
    receiver_upi: str
    amount: float
    status: TransactionStatus
    razorpay_payment_id: str = None
    failure_reason: str = None
    created_at: datetime

    class Config:
        from_attributes = True