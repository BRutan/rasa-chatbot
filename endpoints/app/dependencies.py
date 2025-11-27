from __future__ import annotations
from fastapi import Header, HTTPException, Request
from objects.functions.logger import get_logger
from typing_extensions import Annotated

logger = get_logger()

async def get_auth_token_header(authorization: Annotated[str, Header()]):
    if not authorization.startswith("Token"):
        detail = "Invalid auth header format. Expecting {'Authorization': 'Token <token>'}"
        detail += f" found {authorization}."
        raise HTTPException(status_code=401, detail=detail)
    admin_token = authorization.replace("Token ", "")
    #if user_token not in _API_TOKEN_USERS:
    #    raise HTTPException(status_code=400, detail="Auth token invalid.")