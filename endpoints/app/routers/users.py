from dependencies import get_auth_token_header
from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from arg_model.users import UserInfo, UserLookup, UserSearch, VendorSearch
from dependencies import logger
import functions.users as user_funcs
from response_model.users import UserStatus, UserBasicInfo, VendorInfo, VendorStatus
from shared import async_log_execution
import os
from typing import List, Optional

router = APIRouter(prefix="/users",
                   dependencies=[Depends(get_auth_token_header)],
                   responses={404: {"description": "not found"}})

RASA_URL = "http://{host}:{port}/webhooks/rest/webhook".format(host=os.environ["RASA_HOST"], port=os.environ["RASA_PORT"])

@router.post("/register", response_model=UserStatus)
async def users_register(request:Request, info:UserInfo):
    """
    * Register new user.
    Return the status, whether registered or not.
    """
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        exists = await user_funcs.user_exists(info, conn)
        if exists:
            logger.info("User already exists.")
            return {"status": "registered"}
        else:
            logger.info("Creating new user.")
            await user_funcs.register_user(info, conn)
            return {"status": "created"}
        
@router.post("/identification/upload")
async def users_identification_upload(
    request: Request,
    file: UploadFile = File(...),
    id: str = Form(...)
):
    """
    * Upload documentation for an existing transaction.
    """
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        await user_funcs.load_user_identification(file, id, conn)
        
@router.post("/vendors/register", response_model=VendorStatus)
async def users_vendors_register(request:Request, info:VendorInfo):
    """
    * Register new vendor.
    Return the status, whether registered or not.
    """
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        exists = await user_funcs.vendor_exists(info.user_token, conn)
        if exists:
            logger.info("Vendor already exists.")
            return {"status": "registered"}
        else:
            logger.info("Creating new vendor.")
            await user_funcs.register_vendor(info, conn)
            return {"status": "created"}

@router.post("/token", response_model=Optional[UserLookup])
@async_log_execution
async def users_token(request:Request, data:UserSearch):
    """
    * Retrieve user token associated 
    with other user.
    """
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        return await user_funcs.get_user_info_from_token(data, conn)
    
@router.post("/info", response_model=Optional[List[UserBasicInfo]])
async def users_info(request:Request, data:UserSearch):
    """
    * Retrieve user information associated 
    with lookup information, 
    including first_name, last_name, phone_number
    """
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        return await user_funcs.lookup_user_info(data, conn)
 
@router.post("/vendors/info", response_model=Optional[List[VendorInfo]])
async def users_vendors_meta(request:Request, info:VendorSearch):
    """
    * Retrieve vendor information.
    """
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        return await user_funcs.lookup_vendor_info(info, conn)