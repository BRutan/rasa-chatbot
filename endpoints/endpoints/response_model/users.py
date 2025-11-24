from pydantic import BaseModel
from typing import Optional

class UserBasicInfo(BaseModel):
    token: str
    first_name: str
    last_name:str 
    email:str
    phone_number:str
    address:str
    city:str
    zip_code:str

class UserIDInfo(BaseModel):
    token: str
    drivers_license: Optional[str]
    passport:Optional[str]
    non_drivers_license: Optional[str]

class UserStatus(BaseModel):
    status: str

class VendorStatus(BaseModel):
    status: str

class VendorInfo(BaseModel):
    user_token: str
    first_name: str
    last_name: str
    email: str
    phone_number: str
    address: str
    city: str
    state: str
    zip_code: str
    corp_name: str
    account_number: str
    routing_number: str
    n_strikes: Optional[int] = None

class VendorBasicInfo(BaseModel):
    first_name: str
    last_name: str
    email: str
    corp_name: str
