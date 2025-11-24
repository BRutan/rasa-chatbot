from asyncpg.pool import PoolConnectionProxy
from endpoints.functions.backend import RASA_CONVERSATIONS_URL, RASA_TRACKER_URL
from endpoints.shared import async_log_execution
from httpx import AsyncClient
import json
import os
from time import sleep
from typing import Any, Dict, List

@async_log_execution
async def chatbot_is_running(header:Dict[str, Any], n_retries:int=10, wait_secs:int=1) -> bool:
    """
    * Detect if chatbot is running.
    """
    async with AsyncClient() as client:
        for _ in range(n_retries):
            result = await client.get(RASA_CONVERSATIONS_URL + "/status")
            if result.status_code == 200:
                break
            sleep(wait_secs)
        return result.status_code == 200
    
@async_log_execution
#@cached(ttl=10, max_size=256)
async def get_required_slots(form:str, client:AsyncClient) -> List[str]:
    """
    * Get the form's required slots.
    """
    result = await client.get(RASA_TRACKER_URL + "/domain", 
                              headers={"Accept": "application/json"})
    result.raise_for_status()
    if form not in result.json()["forms"]:
        return []
    return result.json()["forms"][form]["required_slots"]

@async_log_execution
async def get_conversation_meta(user_token:str, client:AsyncClient) -> str:
    """
    * Retrieve current conversation intent.
    """
    conv_url = os.path.join(RASA_CONVERSATIONS_URL, f"{user_token}/tracker")
    result = await client.get(conv_url)
    result.raise_for_status()
    result_json = result.json()
    intent = result_json["latest_message"]["intent"].get("name")
    active_loop = result_json["active_loop"].get("name")
    required_slots = await get_required_slots(active_loop, client) if active_loop else []
    current_slot = get_current_slot(result_json, required_slots)
    addl_slots = get_slot_values(result_json, ["transaction_id", "dispute_id"])
    return { **addl_slots,
             "intent": intent, 
             "active_loop": active_loop,
             "current_slot": current_slot }

@async_log_execution
async def get_conversation_id(user_token:str, client:AsyncClient) -> int:
    """
    * Retrieve the current conversation id.
    """
    pass

@async_log_execution
async def get_sarcasm(conn:PoolConnectionProxy) -> str:
    """
    * Retrieve random sarcasm from backend.
    """
    query = "select ANY_VALUE(text) from chatbot.sarcasm"
    return await conn.fetchval(query)

def get_slot_values(tracker_json, slots:List[str]) -> Dict[str, Any]:
    """
    * Get slot values if the slots have been set.
    """
    slot_values = tracker_json.get("slots", {})
    slot_values = {s: slot_values.get(s) for s in slots}
    return {s: str(v) if v is not None else v for s,v in slot_values.items()}

def get_current_slot(tracker_json:Dict[str, Any], required_slots:List[str]=None):
    """
    Returns the current unfilled slot in the active form.
    
    Parameters:
        tracker_json (dict): JSON returned from /conversations/<sender_id>/tracker
                                    
    Returns:
        str or None: name of the current slot expected by the bot, or None if no form active or all slots filled
    """
    active_loop = tracker_json.get("active_loop", {})
    form_name = active_loop.get("name")
    slots = tracker_json.get("slots", {})
    events = tracker_json.get("events", [])
    # If we have form_required_slots, find first unfilled slot
    if required_slots:# and form_name in required_slots:
        for slot_name in required_slots:#[form_name]:
            if slots.get(slot_name) in (None, ""):
                return slot_name

    # Fallback: infer from last utter_ask_* action
    for event in reversed(events):
        if event.get("event") == "action" and event.get("name", "").startswith("utter_ask_"):
            # Assume action name matches slot: utter_ask_<slot_name>
            slot_candidate = event["name"][len("utter_ask_"):]
            if slots.get(slot_candidate) in (None, ""):
                return slot_candidate
    return None