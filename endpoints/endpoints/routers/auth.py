from endpoints.dependencies import get_auth_token_header
from fastapi import APIRouter, Depends, Request
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel

class NewUserToken(BaseModel):
    username: str
    password: str

router = APIRouter(prefix="/auth",
                   dependencies=[Depends(get_auth_token_header)],
                   responses={404: {"description": "not found"}})

@router.post("/token")
async def create_retrieve_user_token(request:Request, form_data: NewUserToken):
    pool = request.app.state.db_pool
    # Create and return the token if username not registered. 
    # If already registered then return the token assuming the passed password is valid.
    async with pool.acquire() as conn:
        auth_token = ""
        #auth_token = await make_retrieve_token(conn, form_data)
    return {"auth_token": auth_token}
