from chatbot.functions.shared import logger, try_raise, async_log_execution, log_execution, normalize_text
from datetime import datetime
import functions.users as u_funcs
import httpx
import json
import os
import re
from typing import Any, Dict, List

BASE_URL = os.environ["ENDPOINTS_HOST"] + ":" + os.environ["ENDPOINTS_PORT"] + "/transactions"
HEADER = {"Authorization": "Token 1"}

@async_log_execution
async def buyer_has_transaction_with_vendor(buyer_token:int, vendor_name:str) -> bool:
    """
    * Indicate that there are outstanding transactions with vendor.
    """
    vendor_token = await u_funcs.lookup_vendor_token(vendor_name=vendor_name)
    if vendor_token is None:
        raise RuntimeError(f"Could not find vendor_token for {vendor_name}.")
    trans_info = await lookup_transactions(buyer_token, vendor_token)
    return trans_info is not None

@async_log_execution
async def get_transaction_id(data:dict) -> int:
    """
    * Retrieve the transaction id associated
    with transaction details.     
    """
    async with httpx.AsyncClient() as client:
        logger.info("data: ")
        logger.info(json.dumps(data, indent=2))
        response = await client.post(BASE_URL + "/lookup", data=json.dumps(data), headers=HEADER)
        try_raise(response)
        if response.json():
            return response.json()
        return None

async def user_has_transactions(user_token:str, vendor_token:str=None) -> bool:
    """
    * Check that the user has transactions.
    """
    transactions = await lookup_transactions(user_token, vendor_token)
    return transactions is not None

async def load_transaction(data:dict) -> str:
    """
    * Load transaction data. Retrieve the transaction id.
    """
    async with httpx.AsyncClient() as client:
        logger.info("data: ")
        logger.info(json.dumps(data, indent=2))
        response = await client.post(BASE_URL + "/create", data=json.dumps(data), headers=HEADER)
        try_raise(response)
        if response.json():
            return response.json()["transaction_id"]
        return None
    
async def lookup_transactions(user_token:str, 
                              vendor_token:str=None,
                              amount:float=None,
                              description:str=None,
                              opened_ts:datetime=None) -> List[Dict[str, Any]]:
    """
    * Look for transactions that the user or user pairs have.
    """
    async with httpx.AsyncClient() as client:
        data = { "buyer_token": user_token, 
                 "vendor_token": vendor_token,
                 "transaction_amount": amount,
                 "description": description,
                 "opened_ts": opened_ts}
        data = {c: normalize_text(v) for c, v in data.items() if v is not None}
        logger.info("data: ")
        logger.info(json.dumps(data, indent=2))
        response = await client.post(BASE_URL + "/lookup", data=json.dumps(data), headers=HEADER)
        try_raise(response)
        if response.json():
            return response.json()
        return None