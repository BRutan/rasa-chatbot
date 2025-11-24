from endpoints.dependencies import get_auth_token_header
from fastapi import APIRouter, Depends, Request, WebSocket, WebSocketDisconnect
from endpoints.arg_model.chatbot import ChatbotPrompt
from endpoints.functions.chatbot import get_conversation_meta, get_sarcasm
from endpoints.response_model.chatbot import ChatbotResponse
from endpoints.shared import logger
import httpx
import json
import os
from pydantic import ValidationError
from typing import Optional

# Used to retrieve tracker, i.e. current intent:
RASA_URL = "{host}:{port}/webhooks/rest/webhook".format(host=os.environ["RASA_HOST"], port=os.environ["RASA_PORT"])

router = APIRouter(prefix="/chatbot",
                   #dependencies=[Depends(get_auth_token_header)],
                   responses={404: {"description": "not found"}})

@router.websocket("/interface")
async def chatbot_interface(websocket:WebSocket):
    """
    * Retrieve the prompt at the hard coded step.
    """
    await websocket.accept()
    try:
        while True:
            # Receive data and convert to pydantic model:
            data = await websocket.receive_json()
            logger.info("input: ")
            logger.info(json.dumps(data, indent=2))
            try:
                prompt = ChatbotPrompt.model_validate(data)
            except ValidationError as e:
                await websocket.send_json({
                    "error": "Invalid input",
                    "details": e.errors()
                })
                continue
            # If content was attached then load to prefix in
            # standard raw format:
            if prompt.content and not prompt.intent:
                await websocket.send_json({"message": ["This file's purpose is unknown."]})
                continue
            # Send the prompt and retrieve current message and intent:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(RASA_URL, json={"sender": prompt.token, "message": prompt.message})
                response.raise_for_status()
                conv_meta = await get_conversation_meta(prompt.token, client)
                
                response_text = ["<NO RESPONSE>"] if not response.json() else [r["text"] for r in response.json()]
                
                output = {"message": response_text}
                output.update(conv_meta)
                logger.info("output:")
                logger.info(json.dumps(output, indent=2))
                await websocket.send_json(output)
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected")

@router.post("/sarcasm", response_model=Optional[ChatbotResponse])
async def chatbot_sarcasm(request:Request):
    """
    * Retrieve the prompt at the hard coded step.
    """
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        message = await get_sarcasm(conn)
        return { "message": message }
    
@router.post("/verbatim", response_model=Optional[ChatbotResponse])
async def chatbot_verbatim(request:Request):
    pass