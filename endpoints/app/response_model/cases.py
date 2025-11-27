from datetime import datetime
from pydantic import BaseModel
from typing import Optional

class DisputeStatus(BaseModel):
    dispute_id: int
    status: str

class ExistingDisputeInfo(BaseModel):
    dispute_id: int
    transaction_id: int
    buyer_token: str
    vendor_token: str
    description: str
    amount: float
    opened_ts: datetime
    closed_ts: Optional[datetime] = None