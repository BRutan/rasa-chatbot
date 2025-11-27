from arg_model.transactions import TransactionLookup, TransactionCreation, SpecificTransactionLookup, TransactionSearch
from dependencies import get_auth_token_header, logger
import functions.transactions as trans_funcs
from response_model.transactions import TransactionInfo, TransactionStatus
from fastapi import APIRouter, Depends, File, Form, Request, UploadFile
from pydantic import BaseModel
from typing import List, Optional

router = APIRouter(prefix="/transactions",
                   dependencies=[Depends(get_auth_token_header)],
                   responses={404: {"description": "not found"}})

@router.post("/create", response_model=TransactionStatus)
async def transactions_create(request:Request, trans:TransactionCreation):
    """
    * Create a new transaction.
    """
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        transaction_id = await trans_funcs.get_transaction_id(trans, conn)
        if transaction_id is not None:
            logger.info("Transaction already exists as transaction id %s.")
            return { "status": "exists", "id": transaction_id }
        # Generate a new escrow account for the transaction:
        escrow_acct_id = await trans_funcs.make_escrow_account(trans, conn)
        transaction_id = await trans_funcs.make_transaction(trans, escrow_acct_id, conn)
        return { "status": "created", "transaction_id": transaction_id }

@router.post("/end", response_model=TransactionStatus)
async def transactions_end(request:Request, data:TransactionLookup):
    """
    * Mark a transaction as closed. Pay out the withheld amounts
    or the judged amounts.
    """
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        pass
    pass

@router.post("/lookup", response_model=Optional[List[TransactionInfo]])
async def transactions_lookup(request:Request, lookup:TransactionSearch):
    """
    * Check if a transaction exists or not.
    """
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        matches = await trans_funcs.search_transactions(lookup, conn)
        if matches:
            return matches
        else:
            return None
        
@router.post("/status", response_model=TransactionStatus)
async def transactions_status(request:Request, lookup:SpecificTransactionLookup):
    """
    * Retrieve the transaction status corresponding to specific id.
    """
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        pass

@router.post("/documentation/upload")
async def transactions_documentation_upload(
    request: Request,
    file: UploadFile = File(...),
    id: str = Form(...)
):
    """
    * Upload documentation for an existing transaction.
    """
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        size = await trans_funcs.load_transaction_documentation(file, id, conn)
    return {
        "filename": file.filename,
        "content_type": file.content_type,
        "size_bytes": size
    }
    