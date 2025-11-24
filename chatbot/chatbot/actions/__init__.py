import asyncio
import httpx
import os
from chatbot.functions.backend import reset_backend
from chatbot.functions.shared import logger

# Run if RESET_BACKEND is set to 1:
if os.getenv("RESET_BACKEND", "0") == "1":
    logger.info("Resetting backend.")
    asyncio.run(reset_backend())