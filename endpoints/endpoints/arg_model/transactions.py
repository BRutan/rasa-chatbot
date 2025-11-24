from datetime import datetime
from decimal import Decimal
from pydantic import BaseModel
from typing import Optional

class SpecificTransactionLookup(BaseModel):
    """
    * Lookup a specific transaction id.
    """
    id: int

class TransactionLookup(BaseModel):
    """
    * Lookup for specific transaction.
    """
    id: Optional[int] = None
    vendor_token: Optional[str] = None
    buyer_token: Optional[str] = None
    transaction_amount: Optional[float] = None

class TransactionSearch(BaseModel):
    """
    * Transaction information for display purposes.
    """
    transaction_id: Optional[int] = None
    buyer_token: Optional[str] = None
    vendor_token: Optional[str] = None
    buyer_first_name: Optional[str] = None
    buyer_last_name: Optional[str] = None
    vendor_first_name: Optional[str] = None
    vendor_last_name: Optional[str] = None
    vendor_corp_name: Optional[str] = None
    escrow_account_id: Optional[int] = None
    escrow_routing_number: Optional[str] = None
    escrow_account_number: Optional[str] = None
    transaction_amount: Optional[Decimal] = None
    description: Optional[str] = None
    opened_ts: Optional[datetime] = None

class TransactionCreation(BaseModel):
    buyer_token: str
    vendor_token: str
    transaction_amount: float
    description: str

class TransactionContract(BaseModel):
    """
    * Specific contract corresponding to a transaction.
    """
    pass
