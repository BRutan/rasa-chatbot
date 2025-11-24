from asyncpg.pool import PoolConnectionProxy
from fastapi import File
from fastapi.exceptions import HTTPException
from endpoints.arg_model.users import UserSearch, UserLookup, UserInfo, VendorSearch, VendorLookup
from endpoints.functions.backend import BACKEND_CONN, USER_ID_DIR
import endpoints.functions.evidence as ev_funcs
from endpoints.response_model.users import UserBasicInfo, VendorInfo
from endpoints.shared import logger, async_log_execution, normalize_data
import json
import os
from typing import List

VENDOR_COLUMNS = BACKEND_CONN.get_column_schema("users.vendors", names_only=True)
USER_INFO_COLUMNS = BACKEND_CONN.get_column_schema("users.user_info", names_only=True)
BANK_ACCOUNT_COLUMNS = BACKEND_CONN.get_column_schema("accounts.bank_accounts", names_only=True)
VENDOR_INSERT_COLUMNS = [c for c in VENDOR_COLUMNS if c not in ["id", "timestamp"]]
USER_INFO_INSERT_COLUMNS = [c for c in USER_INFO_COLUMNS if c not in ["id", "timestamp"]]
BANK_ACCOUNT_INSERT_COLUMNS = [c for c in BANK_ACCOUNT_COLUMNS if c not in ["id", "timestamp"]]

@async_log_execution
async def get_user_token(info:UserSearch, conn:PoolConnectionProxy) -> str:
    """
    * Retrieve existing user token if exists.
    """
    data = {c: v for c, v in info.model_dump().items() if c in USER_INFO_COLUMNS and v is not None}
    data = normalize_data(data)
    search_stmt = " and ".join([f"{c} = ${idx+1}" for idx, c in enumerate(data)])
    values = list(data.values())
    query = f"""
    select
        c.token
    from users.user_info as u
    join users.credentials as c
    on u.user_token = c.token
    where 
        {search_stmt}
    """
    logger.info("query: ")
    logger.info(query)
    return await conn.fetchval(query, *values)

@async_log_execution
async def get_user_info_from_token(info:UserLookup, 
                                   conn:PoolConnectionProxy) -> UserBasicInfo:
    """
    * Retrieve user information based on lookup.
    """
    query = """
    select
        c.token as token,
        u.first_name,
        u.last_name,
        u.email,
        u.phone_number,
        u.address,
        u.city,
        u.zip_code,
        u.state
    from users.user_info as u
    join users.credentials as c
    on u.user_token = c.token
    where c.token = $1
    """
    logger.info("query: ")
    logger.info(query)
    return await conn.fetchval(query, info.token)

@async_log_execution
async def get_vendor_info_from_token(user_token:str, conn:PoolConnectionProxy) -> VendorInfo:
    """
    * Retrieve vendor information based on token lookup.
    """
    query = """
    select
        v.user_token,
        u.first_name,
        u.last_name,
        u.email,
        u.phone_number,
        u.address,
        u.city,
        u.state,
        u.zip_code,
        v.corp_name,
        bu.account_number,
        bu.routing_number,
        v.n_strikes
    from users.vendors as v
    inner join users.user_info as u
    on u.user_token = v.user_token
    inner join accounts.bank_accounts as bu
    on bu.user_token = v.user_token
    where v.user_token = $1
    """
    logger.info("query: ")
    logger.info(query)
    result = await conn.fetchval(query, user_token)
    return VendorInfo.model_validate(dict(result)) if result else None

async def vendor_exists(user_token:str, conn:PoolConnectionProxy) -> bool:
    """
    * Determine if vendor exists based on token.
    """
    exists = await get_vendor_info_from_token(user_token, conn)
    return exists is not None

@async_log_execution
async def lookup_vendor_info(info:VendorSearch, conn:PoolConnectionProxy) -> List[VendorInfo]:
    """
    * Retrieve vendor information based on lookup.
    """
    logger.info("info.model_dump(): ")
    logger.info(json.dumps(info.model_dump(), indent=2))
    logger.info("VENDOR_COLUMNS: %s", ",".join(VENDOR_COLUMNS))
    alias_mp = {}
    alias_mp.update({c: "v" for c in VENDOR_INSERT_COLUMNS})
    alias_mp.update({c: "u" for c in USER_INFO_INSERT_COLUMNS if c not in VENDOR_INSERT_COLUMNS})
    alias_mp.update({c: "bu" for c in BANK_ACCOUNT_INSERT_COLUMNS if c not in VENDOR_INSERT_COLUMNS})
    logger.info("alias_mp: ")
    logger.info(json.dumps(alias_mp, indent=2))
    data = {c: v for c,v in info.model_dump().items() if v is not None and c in alias_mp}
    if not data:
        raise HTTPException(status_code=500, detail="At least one lookup must be provided.")
    data = normalize_data(data)
    lookup_stmt = [f"{alias_mp.get(c, '') + '.' if alias_mp.get(c) else ''}{c} = ${idx+1}" 
                   for idx, c in enumerate(data)]
    lookup_stmt_str = " and ".join(lookup_stmt)
    query = f"""
    select
        c.token as user_token,
        u.first_name,
        u.last_name,
        u.email,
        v.corp_name,
        v.n_strikes,
        u.phone_number,
        u.address,
        u.city,
        u.zip_code,
        u.state,
        bu.account_number,
        bu.routing_number,
        v.n_strikes
    from users.vendors as v
    join users.user_info as u
    on v.user_token = u.user_token
    join users.credentials as c
    on u.user_token = c.token
    inner join accounts.bank_accounts as bu
    on bu.user_token = v.user_token
    where 
        {lookup_stmt_str}
    """
    logger.info("query: ")
    logger.info(query)
    records = await conn.fetch(query, *list(data.values()))
    logger.info("records: %s", records)
    if records:
        return [VendorInfo.model_validate(dict(r)) for r in records]
    return None

@async_log_execution
async def lookup_user_info(info:UserSearch, 
                           conn:PoolConnectionProxy) -> List[UserBasicInfo]:
    """
    * Retrieve user information based on lookup.
    """
    lookup_vals = {c: v for c,v in info.model_dump().items() if v is not None}
    if not lookup_vals:
        raise HTTPException(500, "At least one lookup must be provided.")
    lookup_vals = normalize_data(lookup_vals)
    lookup_stmt = [f"{c} = ${idx+1}" for idx, c in enumerate(lookup_vals)]
    lookup_stmt_str = " and ".join(lookup_stmt)
    lookup_vals = [v.lower() if isinstance(v, str) else v for v in lookup_vals.values()]
    query = f"""
    select
        c.token as token,
        u.first_name,
        u.last_name,
        u.email,
        u.phone_number,
        u.address,
        u.city,
        u.zip_code,
        u.state
    from users.user_info as u
    join users.credentials as c
    on u.user_token = c.token
    where 
        {lookup_stmt_str}
    """
    logger.info("query: ")
    logger.info(query)
    records = await conn.fetch(query, *lookup_vals)
    if records:
        logger.info("records: ")
        logger.info(records)
        return [UserBasicInfo.model_validate(dict(r)) for r in records]
    return None

@async_log_execution
async def user_token_exists(info:UserLookup, conn:PoolConnectionProxy) -> bool:
    """
    * Determine if user has been registered or not.
    """
    result = await conn.fetchval("select 1 from users.credentials where token = $", info.token)
    return True if result is not None else False

@async_log_execution
async def user_exists(info:UserSearch, conn:PoolConnectionProxy) -> bool:
    """
    * Determine if user has been registered or not.
    """
    return True if await get_user_token(info, conn) else False

@async_log_execution
async def register_user(info:UserInfo, conn:PoolConnectionProxy):
    """
    * Register both the token for the user and the user information
    associated with the token.
    """
    try:
        async with conn.transaction():
            query = """
            insert into users.credentials (token)
            values ($1)
            """
            logger.info("query: ")
            logger.info(query)
            await conn.execute(query, info.user_token)
            data = info.model_dump()
            # Normalize strings:
            data = normalize_data(data)
            user_info_data = {c: v for c, v in data.items() if c in USER_INFO_INSERT_COLUMNS}
            logger.info("user_info_data: ")
            logger.info(json.dumps(user_info_data, indent=2))
            header_str = ",".join(user_info_data)
            value_str = ",".join([f"${idx+1}" for idx in range(len(user_info_data))])
            user_info_values = list(user_info_data.values())
            query = f"""
            insert into users.user_info ({header_str})
            values ({value_str})
            """
            logger.info("query: ")
            logger.info(query)
            await conn.execute(query, *user_info_values)
            # Register bank account information:
            bank_account_data = {c: v for c, v in data.items() if c in BANK_ACCOUNT_INSERT_COLUMNS}
            header_str = ",".join(bank_account_data)
            value_str = ",".join([f"${idx+1}" for idx in range(len(bank_account_data))])
            bank_account_data_values = list(bank_account_data.values())
            query = f"""
            insert into accounts.bank_accounts ({header_str})
            values ({value_str})
            """
            await conn.execute(query, *bank_account_data_values)
    except Exception as ex:
        logger.error("Rolling back due to exception: %s", str(ex))
        raise ex
    
@async_log_execution
async def register_vendor(info:VendorInfo, conn:PoolConnectionProxy):
    """
    * Register both the token for the user and the user information
    associated with the token.
    """
    try:
        async with conn.transaction():
            query = """
            insert into users.credentials (token)
            values ($1)
            on conflict do nothing;
            """
            logger.info("query: ")
            logger.info(query)
            await conn.execute(query, info.user_token)
            data = info.model_dump()
            logger.info("data: ")
            logger.info(json.dumps(data, indent=2))
            header_str = ",".join(VENDOR_INSERT_COLUMNS)
            value_str = ",".join([f"${idx+1}" for idx in range(len(VENDOR_INSERT_COLUMNS))])
            data = normalize_data(data)
            values = [data[c] for c in VENDOR_INSERT_COLUMNS if c in data]
            logger.info("values: %s", values)
            query = f"""
            insert into users.vendors ({header_str})
            values ({value_str})
            on conflict do nothing;
            """
            logger.info("query: ")
            logger.info(query)
            await conn.execute(query, *values)
            # Insert into the user_info table if not already present:
            is_registered = await get_user_info_from_token(UserLookup.model_validate({"token": info.user_token}), conn)
            if not is_registered:
                logger.info("Registering vendor as user since was not present.")
                user_data = {c: v for c, v in data.items() if c in USER_INFO_INSERT_COLUMNS}
                if not user_data:
                    raise RuntimeError("No user_info columns present in passed VendorInfo.")
                user_data = normalize_data(user_data)
                # Normalize strings:
                values = list(user_data.values())
                header_str = ",".join([c for c in user_data])
                value_str = ",".join([f"${idx+1}" for idx in range(len(user_data))])
                query = f"""
                insert into users.user_info ({header_str})
                values ({value_str})
                """
                logger.info("query: ")
                logger.info(query)
                await conn.execute(query, *values)
            # Insert into bank.accounts table if not already present:
            bank_account_data = {c: v for c, v in data.items() if c in BANK_ACCOUNT_INSERT_COLUMNS}
            header_str = ",".join(bank_account_data)
            value_str = ",".join([f"${idx+1}" for idx in range(len(bank_account_data))])
            values = list(bank_account_data.values())
            query = f"""
            insert into accounts.bank_accounts ({header_str})
            values ({value_str})
            """
            await conn.execute(query, *values)
    except Exception as ex:
        logger.error("Rolling back due to exception: %s", str(ex))
        raise ex
    
async def load_user_identification(file:File, user_token:int, conn:PoolConnectionProxy):
    """
    * Load transaction documentation.
    """
    folder = os.path.join(USER_ID_DIR, str(user_token))
    logger.info("Writing to folder: %s", folder)
    os.makedirs(folder, exist_ok=True)
    if ev_funcs.is_image(file):
        return await ev_funcs.load_image_file(file, folder)
    elif ev_funcs.is_text(file):
        return await ev_funcs.load_text_file(file, folder)
    elif ev_funcs.is_video(file):
        return await ev_funcs.load_video_file(file, folder)
    elif ev_funcs.is_document(file):
        return await ev_funcs.load_document_file(file, folder)
    else:
        raise ValueError(f"Extension not supported for {file.filename}")