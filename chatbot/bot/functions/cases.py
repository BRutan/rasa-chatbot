from chatbot.functions.shared import logger, try_raise, async_log_execution, log_execution, normalize_text
from datetime import datetime
import httpx
import json
import os
from typing import Dict

BASE_URL = os.environ["ENDPOINTS_HOST"] + ":" + os.environ["ENDPOINTS_PORT"] + "/cases"
HEADER = {"Authorization": "Token 1"}

@async_log_execution
async def lookup_dispute(transaction_id:int=None, 
                         buyer_token:str=None, 
                         vendor_token:str=None,
                         description:str=None,
                         amount:float=None,
                         open_ts:datetime=None,
                         closed_ts:datetime=None):
    async with httpx.AsyncClient() as client:
        data = {"transaction_id": transaction_id,
                "buyer_token": buyer_token,
                "vendor_token": vendor_token,
                "description": description,
                "amount": amount,
                "open_ts": open_ts,
                "closed_ts": closed_ts}
        data = {c: normalize_text(v) for c, v in data.items() if v is not None}
        result = await client.post(BASE_URL + "/lookup", data=json.dumps(data), headers=HEADER)
        try_raise(result)
        if result.json():
            return result.json()
        return None
    
@async_log_execution
async def dispute_exists(transaction_id:int=None, 
                         buyer_token:str=None, 
                         vendor_token:str=None,
                         description:str=None,
                         amount:float=None,
                         open_ts:datetime=None,
                         closed_ts:datetime=None) -> bool:
    """
    * Indicate that at least one dispute with lookup
    criteria exists.
    """
    matches = await lookup_dispute(transaction_id, 
                                   buyer_token, 
                                   vendor_token, 
                                   description, 
                                   amount, 
                                   open_ts, 
                                   closed_ts)
    return matches is not None

@async_log_execution
async def create_dispute_from_slots(buyer_token, tracker) -> int:
    """
    * Create a dispute from the current tracker.
    """
    transaction_id = tracker.get_slot("dispute_transaction_id")
    vendor_token = tracker.get_slot("dispute_vendor_token")
    description = tracker.get_slot("dispute_description")
    amount = tracker.get_slot("dispute_amount")
    data = {"transaction_id": transaction_id,
            "buyer_token": buyer_token,
            "vendor_token": vendor_token,
            "description": description,
            "amount": amount}
    logger.info("data: ")
    logger.info(json.dumps(data, indent=2))
    dispute_id = await create_dispute(data)
    logger.info("Created dispute with id %s.", dispute_id)
    logger.info("Moving to evidence aggregation form.")
    return dispute_id

@async_log_execution
async def create_dispute(data:dict) -> int:
    """
    * Generate a new dispute. Retrieve the dispute_id.
    """
    async with httpx.AsyncClient() as client:
        data = {c: normalize_text(v) for c, v in data.items() if v is not None}
        logger.info("data: ")
        logger.info(json.dumps(data, indent=2))
        result = await client.post(BASE_URL + "/create", data=json.dumps(data), headers=HEADER)
        try_raise(result)
        if result.json():
            return result.json()["dispute_id"]
        return None