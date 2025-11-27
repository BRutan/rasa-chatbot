from asyncpg.pool import PoolConnectionProxy
from arg_model.cases import DisputeInfo, DisputeSearch
from functions.backend import BACKEND_CONN
from response_model.cases import ExistingDisputeInfo
from shared import async_log_execution, logger, normalize_data
from fastapi.exceptions import HTTPException
import json
from typing import List, Optional

CASE_COLUMNS = BACKEND_CONN.get_column_schema("cases.disputes", names_only=True)
CASE_INSERT_COLUMNS = [c for c in CASE_COLUMNS if c not in ["id", "timestamp"]]

@async_log_execution
async def case_exists(info:DisputeSearch, conn:PoolConnectionProxy) -> bool:
    """
    * Indicate that a case exists.
    """
    cases = await lookup_case(info, conn)
    return cases is not None

@async_log_execution
async def lookup_case(info:DisputeSearch, conn:PoolConnectionProxy) -> Optional[List[ExistingDisputeInfo]]:
    """
    * Lookup existing cases based on search criteria.
    """
    lookup_elems = {c: v for c, v in info.model_dump().items() if v is not None}
    if not lookup_elems:
        raise HTTPException(500, detail="At least one lookup must be provided.")
    lookup_elems = normalize_data(lookup_elems)
    logger.info("lookup_elems: ")
    logger.info(json.dumps(lookup_elems, indent=2))
    lookup_str = []
    for idx, c in enumerate(lookup_elems):
        if c == "amount":
            lookup_str.append(f"cast(regexp_replace({c}::text, '[\$,]', '', 'g') as numeric) = ${idx+1}")
        else:
            lookup_str.append(f"{c} = ${idx+1}")
    lookup_str = " and ".join(lookup_str)
    values = list(lookup_elems.values())
    query = f"""
    select
        id as dispute_id,
        transaction_id,
        buyer_token,
        vendor_token,
        description,
        cast(regexp_replace(amount::text, '[\$,]', '', 'g') as numeric) as amount,
        opened_ts,
        closed_ts
    from cases.disputes
    where 
        {lookup_str}
    """
    logger.info("query: ")
    logger.info(query)
    results = await conn.fetch(query, *values)
    if results:
        return [ExistingDisputeInfo.model_validate(dict(r)) for r in results]
    return None

@async_log_execution
async def create_case(info:DisputeInfo, conn:PoolConnectionProxy) -> int:
    """
    * Generate a new case. Return the generated case id.
    """
    data = {c: v for c, v in info.model_dump().items() if c in CASE_INSERT_COLUMNS}
    data = normalize_data(data)
    logger.info("data: ")
    logger.info(json.dumps(data, indent=2))
    headers = [c for c in data]
    header_str = ",".join(headers)
    values_str = ",".join([f"${idx+1}" for idx in range(len(headers))])
    values = list(data.values())
    query = f"""
    insert into cases.disputes ({header_str})
    values ({values_str})
    returning id;
    """
    logger.info("query: ")
    logger.info(query)
    case_id = await conn.fetchval(query, *values)
    return case_id
    