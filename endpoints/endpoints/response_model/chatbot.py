from datetime import datetime
from decimal import Decimal
from pydantic import BaseModel
from typing import List, Optional

# chatbot:
class ChatbotResponse(BaseModel):
    message: List[str]
    intent: Optional[str] = None
    dispute_id: Optional[str] = None