from pydantic import BaseModel
from typing import Optional

class UserLookup(BaseModel):
    token: str

class VendorLookup(BaseModel):
    user_token: str

class UserSearch(BaseModel):
    user_token: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    phone_number: Optional[str] = None

class VendorSearch(BaseModel):
    user_token: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    phone_number: Optional[str] = None
    corp_name: Optional[str] = None
    n_strikes: Optional[int] = None
    address: Optional[str] = None
    city: Optional[str] = None
    zip_code: Optional[str] = None
    state: Optional[str] = None
    account_number: Optional[str] = None
    routing_number: Optional[str] = None

class UserInfo(BaseModel):
    user_token: str
    first_name: str
    last_name: str 
    email: str
    phone_number: str 
    drivers_license: Optional[str] = None
    passport: Optional[str] = None
    non_drivers_license: Optional[str] = None
    address: str
    city: str
    state: str
    zip_code: str
    account_number: str
    routing_number: str

