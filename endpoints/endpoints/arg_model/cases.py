from datetime import datetime
from pydantic import BaseModel
from typing import Optional

class DisputeLookup(BaseModel):
    dispute_id: int

class DisputeSearch(BaseModel):
    dispute_id: Optional[int]=None
    transaction_id: Optional[int]=None
    buyer_token: Optional[str]=None
    vendor_token: Optional[str]=None
    amount: Optional[float]=None
    description: Optional[str]=None
    opened_ts: Optional[datetime]=None
    closed_ts: Optional[datetime]=None

class DisputeInfo(BaseModel):
    transaction_id: int
    buyer_token: str
    vendor_token: str
    amount: float
    description: str