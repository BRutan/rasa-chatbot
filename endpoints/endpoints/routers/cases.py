from asyncpg.pool import PoolConnectionProxy
from endpoints.arg_model.cases import DisputeInfo, DisputeLookup, DisputeSearch
from endpoints.dependencies import get_auth_token_header
from endpoints.functions.cases import case_exists, create_case, lookup_case
from endpoints.functions.evidence import load_evidence
from endpoints.response_model.cases import DisputeStatus, ExistingDisputeInfo
from endpoints.shared import logger
from fastapi import APIRouter, Depends, File, Form, Request, UploadFile
from typing import List, Optional
from typing import Any, List, Optional

router = APIRouter(prefix="/cases",
                   dependencies=[Depends(get_auth_token_header)],
                   responses={404: {"description": "not found"}})

@router.post("/create", response_model=DisputeStatus)
async def cases_create(request:Request, info:DisputeInfo):
    """
    * Create a case. Return the status.
    """
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        exists = await case_exists(info, conn)
        if not exists:
            logger.info("Case does not exist.")
            dispute_id = await create_case(info, conn)
            return {"status": "created", "dispute_id": dispute_id}
        else:
            logger.info("Case already exists.")
            case_info = await lookup_case(info, conn)
            case_info = case_info[0]
            return {"status": "exists", "dispute_id": case_info.dispute_id}

@router.post("/lookup", response_model=Optional[List[ExistingDisputeInfo]])
async def cases_lookup(request:Request, info:DisputeSearch):
    """
    * List all cases that match passed lookup values.
    """
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        return await lookup_case(info, conn)
    
@router.post("/evidence/upload")
async def disputes_evidence_upload(request:Request, file:UploadFile = File(...), dispute_id: str = Form(...)):
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        size = await load_evidence(file, dispute_id, conn)
    return {
        "filename": file.filename,
        "content_type": file.content_type,
        "size_bytes": size
    }

