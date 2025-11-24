from asyncpg.pool import PoolConnectionProxy
from decimal import Decimal
from endpoints.arg_model.transactions import TransactionCreation, SpecificTransactionLookup, TransactionSearch
import endpoints.functions.evidence as ev_funcs
from endpoints.response_model.transactions import TransactionInfo
from endpoints.functions.backend import BACKEND_CONN, TRANS_DOC_DIR
from endpoints.shared import async_log_execution, logger, normalize_data
import exrex
from fastapi import File
from fastapi.exceptions import HTTPException
import json
import os
from typing import Dict, List

TRANSACTION_COLUMNS = BACKEND_CONN.get_column_schema("transactions.transactions")
TRANSACTION_INSERT_COLUMNS = [c for c in TRANSACTION_COLUMNS if c not in ["id", "timestamp"]]

@async_log_execution
async def search_transactions(data:TransactionSearch, 
                              conn:PoolConnectionProxy) -> List[TransactionInfo]:
    """
    * Look for existing transaction.
    """
    lookups = {c: v for c, v in data.model_dump().items() if v is not None}
    if not lookups:
        raise HTTPException(500, detail="Must provide at least one lookup criterion.")
    lookups = normalize_data(lookups)
    lookup_str = " and ".join([f"{c} = ${idx+1}" for idx, c in enumerate(lookups)])
    lookup_vals = list(lookups.values())
    query = f"""
    with transactions as 
    (
        select
            t.id as transaction_id,
            t.buyer_token as buyer_token,
            bu.first_name as buyer_first_name,
            bu.last_name as buyer_last_name,
            t.vendor_token as vendor_token,
            vu.first_name as vendor_first_name,
            vu.last_name as vendor_last_name,
            v.corp_name as vendor_corp_name,
            e.id as escrow_account_id,
            e.account_number as escrow_account_number,
            e.routing_number as escrow_routing_number,
            cast(regexp_replace(t.transaction_amount::text, '[\$,]', '', 'g') as numeric) as transaction_amount,
            t.description,
            t.opened_ts
        from transactions.transactions t
        inner join users.user_info bu
        on bu.user_token = t.buyer_token
        inner join users.vendors v
        on v.user_token = t.vendor_token
        inner join users.user_info vu
        on vu.user_token = t.vendor_token
        and vu.user_token = v.user_token
        inner join accounts.escrow e
        on t.escrow_account_id = e.id
    )
    select * 
    from transactions
    where 
        {lookup_str}
    """
    logger.info("query: ")
    logger.info(query)
    results = await conn.fetch(query, *lookup_vals)
    if results:
        return [TransactionInfo.model_validate(dict(r)) for r in results]
    return results

@async_log_execution
async def get_transaction_id(data:TransactionCreation, conn:PoolConnectionProxy) -> int:
    """
    * Retrieve the transaction id corresponding
    to the passed data.
    """
    query = """
    select id from transactions.transactions 
    where 
        buyer_token = $1 
        and vendor_token = $2
        and transaction_amount = $3
    """
    return await conn.fetchval(query, data.buyer_token, data.vendor_token, str(data.transaction_amount))

@async_log_execution
async def transaction_exists(data:TransactionCreation, conn:PoolConnectionProxy) -> bool:
    """
    * Indicate that a transaction exists with
    passed data.
    """
    info = await get_transaction_id(data, conn)
    return info is not None

@async_log_execution
async def make_transaction(data:TransactionCreation, escrow_account_id:int, conn:PoolConnectionProxy) -> int:
    """
    * Load new transaction into backend.
    """
    data = {c: v for c,v in data.model_dump().items() if c in TRANSACTION_INSERT_COLUMNS}
    data = normalize_data(data)
    data["escrow_account_id"] = int(escrow_account_id)
    logger.info("data: ")
    logger.info(json.dumps(data, indent=2))
    headers = [c for c in data]
    header_str = ",".join(headers)
    value_str = ",".join([f"${idx+1}" for idx in range(len(headers))])
    query = f"""
    insert into transactions.transactions ({header_str})
    values ({value_str})
    returning id;
    """
    transaction_id = await conn.fetchval(query, *list(data.values()))
    return transaction_id

@async_log_execution
async def make_escrow_account(info:TransactionCreation, conn:PoolConnectionProxy) -> int:
    """
    * Create new escrow account specifically for this transaction.
    """
    source_account_id = await get_user_bank_account_id(info.buyer_token, conn)
    dest_account_id = await get_user_bank_account_id(info.vendor_token, conn)
    errs = []
    if source_account_id is None:
        errs.append(f"source_account_id is null. buyer_token {info.buyer_token} does not have an account.")
    if dest_account_id is None:
        errs.append(f"dest_account_id is null. vendor_token {info.vendor_token} does not have an account.")
    if errs:
        raise RuntimeError("\n".join(errs))
    query = """
    insert into accounts.escrow(account_number, routing_number, source_account_id, dest_account_id)
    values ($1, $2, $3, $4)
    returning id;
    """
    data = {}
    #TMP: randomly generate the account and routing numbers:
    data["account_number"] = exrex.getone(r"\d{9}")
    data["routing_number"] = exrex.getone(r"\d{9}")
    data["source_account_id"] = source_account_id
    data["dest_account_id"] = dest_account_id
    escrow_acct_id = await conn.fetchval(query, *list(data.values()))
    logger.info("row (%s): %s", type(escrow_acct_id).__name__, escrow_acct_id)
    return escrow_acct_id

@async_log_execution
async def get_user_bank_account_id(user_token:str, conn:PoolConnectionProxy) -> int:
    """
    * Retrieve the user bank account id.
    """
    query = """
    select
        id
    from accounts.bank_accounts
    where user_token = $1
    """
    return await conn.fetchval(query, user_token)

async def load_transaction_documentation(file:File, transaction_id:int, conn:PoolConnectionProxy):
    """
    * Load transaction documentation.
    """
    folder = os.path.join(TRANS_DOC_DIR, str(transaction_id))
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