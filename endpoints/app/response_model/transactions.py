from datetime import datetime
from decimal import Decimal
from pydantic import BaseModel
from typing import Optional

class TransactionInfo(BaseModel):
    """
    * Transaction information for display purposes.
    """
    transaction_id: int
    buyer_token: int
    buyer_first_name: str
    buyer_last_name: str
    vendor_token: int
    vendor_first_name: str
    vendor_last_name: str
    vendor_corp_name: str
    escrow_account_id: int
    escrow_account_number: str
    escrow_routing_number: str
    transaction_amount: Decimal
    description: str
    opened_ts: datetime

class TransactionParty(BaseModel):
    """
    * Specific party information.
    """
    transaction_id:int
    buyer_user_token:str
    buyer_first_name:str 
    buyer_last_name:str
    vendor_user_token: str
    vendor_first_name:str
    vendor_last_name:str
    vendor_corp_name:Optional[str] = None
    vendor_n_strikes:Optional[int] = 0

class TransactionDocument(BaseModel):
    raw_text: str

class TransactionLookup(BaseModel):
    id: int

class TransactionStatus(BaseModel):
    transaction_id: int
    status: str