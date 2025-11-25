from chatbot.functions.shared import try_raise
import httpx
import json
import os

BASE_URL = os.environ["ENDPOINTS_HOST"] + ":" + os.environ["ENDPOINTS_PORT"] + "/backend"
HEADER = {"Authorization": "Token 1"}

async def reset_backend():
    """
    * Reset the backend.
    """
    async with httpx.AsyncClient() as client:
        response = await client.post(BASE_URL + "/reset", headers=HEADER)
        try_raise(response)
