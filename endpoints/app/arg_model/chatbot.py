from pydantic import BaseModel
from typing import List, Optional, Union

class ChatbotPrompt(BaseModel):
    token: str
    message: str
    content: Optional[List[str]]=None
    intent: Optional[str]=None
