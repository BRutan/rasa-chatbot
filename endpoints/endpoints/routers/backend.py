from endpoints.dependencies import get_auth_token_header
from endpoints.shared import async_log_execution
from fastapi import APIRouter, Depends, HTTPException, Request


router = APIRouter(prefix="/backend",
                   dependencies=[Depends(get_auth_token_header)],
                   responses={404: {"description": "not found"}})

@router.post("/reset")
async def backend_reset(request:Request):
    """
    * Reset the entire backend.
    """
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        await conn.execute("call demo.reset_demo_tables()")
        return { "status": "reset" }

