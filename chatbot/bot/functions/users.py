from chatbot.functions.shared import async_log_execution, log_execution, try_raise, logger, normalize_text
from rasa_sdk import Tracker
import httpx
import json
import os
import re
from typing import Any, Dict


BASE_URL = os.environ["ENDPOINTS_HOST"] + ":" + os.environ["ENDPOINTS_PORT"] + "/users"
HEADER = {"Authorization": "Token 1"}

async def load_user_info(user_token:str, data:dict):
    """
    * Load user information into backend.    
    """
    async with httpx.AsyncClient() as client:
        data = {**data, "user_token": user_token}
        response = await client.post(BASE_URL + "/register", data=json.dumps(data), headers=HEADER)
        try_raise(response)
        if response.json():
            return response.json()["status"]
        return None
    
async def load_vendor_info(user_token:str, data:dict):
    """
    * Load vendor information into backend.    
    """
    async with httpx.AsyncClient() as client:
        data = {**data, "user_token": user_token}
        response = await client.post(BASE_URL + "/vendors/register", data=json.dumps(data), headers=HEADER)
        try_raise(response)
        logger.info("response.json(): %s", response.json())
        if response.json():
            return response.json()["status"]
        return None

async def search_users(first_name:str, 
                       last_name:str, 
                       email:str=None, 
                       phone_number:str=None) -> Dict[str, Any]:
    """
    * Search for user by possible lookup values.
    """
    async with httpx.AsyncClient() as client:
        data = {"first_name": first_name, 
                "last_name": last_name,
                "email": email,
                "phone_number": phone_number}
        data = {c: v for c, v in data.items() if v is not None}
        response = await client.post(BASE_URL + "/info", data=json.dumps(data), headers=HEADER)
        try_raise(response)
        if response.json():
            return response.json()["status"]
        return None

async def vendor_exists(user_token:str) -> bool:
    """
    * Retrieve current user token from tracker table:
    # Determine if user exists in backend with that token.
    """
    vendor_info = await get_vendor_meta(user_token)
    return vendor_info is not None

async def user_exists(user_token:str) -> bool:
    """
    * Retrieve current user token from tracker table:
    # Determine if user exists in backend with that token.
    """
    user_info = await get_user_info_from_token(user_token)
    return user_info is not None

def get_user_token_from_tracker(tracker:Tracker, is_vendor:bool=False) -> str:
    """
    * Retrieve the user token from tracker. 
    It is expected to be sent from the frontend.
    """
    metadata = tracker.get_slot("session_metadata")
    metadata = {} if not metadata else metadata.get("metadata", {})
    user_id = metadata.get("user_id", "1" if not is_vendor else "2")
    return user_id

async def get_user_info_from_token(user_token:str) -> Dict[str, Any]:
    """
    * Get user information associated with particular
    session id/user token.
    """
    async with httpx.AsyncClient() as client:
        data = {"user_token": user_token}
        response = await client.post(BASE_URL + "/info", data=json.dumps(data), headers=HEADER)
        try_raise(response)
        if response.json():
            return response.json()
        return None

async def lookup_vendor_token(first_name:str=None,
                              last_name:str=None,
                              email:str=None,
                              corp_name:str=None,
                              vendor_name:str=None):
    """
    * Lookup vendor token based on based search criteria.
    """
    logger.info("vendor_name: %s", vendor_name)
    if vendor_name is not None and vendor_is_corp_name(vendor_name):
        corp_name = vendor_name
    elif vendor_name is not None:
        first_name, last_name = vendor_name.split(" ")
    async with httpx.AsyncClient() as client:
        data = {"first_name": first_name, 
                "last_name": last_name, 
                "email": email, 
                "corp_name": corp_name}
        data = {c: normalize_text(v) for c, v in data.items() if v is not None}
        response = await client.post(BASE_URL + "/vendors/info", data=json.dumps(data), headers=HEADER)
        try_raise(response)
        if response.json():
            result = response.json()[0]
            return result["user_token"]
        return None

async def lookup_user_token(first_name:str, last_name:str) -> str:
    """
    * Retrieve the token associated with user,
    if they are registered, based on
    first name and last name.
    """
    async with httpx.AsyncClient() as client:
        data = {"first_name": first_name, "last_name": last_name}
        response = await client.post(BASE_URL + "/info", data=json.dumps(data), headers=HEADER)
        try_raise(response)
        if response.json():
            result = response.json()[0]
            return result["token"]
        return None

async def get_vendor_token(first_name:str, last_name:str) -> str:
    """
    * Determine the vendor token
    using the above lookup.
    """
    async with httpx.AsyncClient() as client:
        data = {"first_name": first_name, "last_name": last_name}
        response = await client.post(BASE_URL + "/vendor/info", data=json.dumps(data), headers=HEADER)
        try_raise(response)
        if response.json():
            return response.json()["user_token"]
        return None

async def get_vendor_meta(user_token:str) -> Dict[str, Any]:
    """
    * Get vendor statistics.
    """
    async with httpx.AsyncClient() as client:
        data = {"user_token": user_token}
        response = await client.post(BASE_URL + "/vendors/info", data=json.dumps(data), headers=HEADER)
        try_raise(response)
        logger.info("response.json(): %s", response.json())
        if response.json() and len(response.json()) > 0:
            return response.json()
        return None

async def user_is_vendor(user_token:str) -> bool:
    """
    * Determine if the current user is a vendor.
    """
    meta = await get_vendor_meta(user_token)
    return meta is not None

@log_execution
def vendor_is_corp_name(vendor:str) -> bool:
    """
    * Get the slot mapping for vendor based on pattern.
    """
    mtch = re.search(r"^\s*(?P<corp_name>[\w\s]+(inc|llc|corp))\s*$", vendor, flags=re.IGNORECASE)
    return mtch is not None