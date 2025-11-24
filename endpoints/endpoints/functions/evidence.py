from asyncpg.pool import PoolConnectionProxy
from endpoints.functions.backend import EVIDENCE_DIR
from endpoints.shared import async_log_execution, logger
from objects.functions.files import is_video, is_image
from fastapi import File
import os
import re

@async_log_execution
async def load_evidence(file:File, case_id:str, conn:PoolConnectionProxy):
    """
    * Load evidence into the backend.
    Optionally push to S3 and store the pointer to it.
    """
    folder = os.path.join(EVIDENCE_DIR, case_id)
    os.makedirs(folder, exist_ok=True)
    if is_video(file):
        return await load_video_file(file, folder)
    elif is_image(file):
        return await load_image_file(file, folder)
    else:
        return await load_text_file(file, folder)

@async_log_execution
async def load_video_file(file:File, folder:str):
    """
    * Load video evidence.
    """
    _, file_name = os.path.split(file.filename)
    file_path = make_file_path(folder, file_name)
    contents = await file.read()
    logger.info("Writing to %s", file_path)
    with open(file_path, "wb") as f:
        f.write(contents)
    return os.path.getsize(file_path)

@async_log_execution
async def load_image_file(file:File, folder:str):
    """
    * Load image evidence.
    """
    _, file_name = os.path.split(file.filename)
    file_path = make_file_path(folder, file_name)
    contents = await file.read()
    logger.info("Writing to %s", file_path)
    with open(file_path, "wb") as f:
        f.write(contents)
    return os.path.getsize(file_path)

@async_log_execution
async def load_text_file(file:File, folder:str):
    """
    * Load text evidence.
    """
    _, file_name = os.path.split(file.filename)
    file_path = make_file_path(folder, file_name)
    contents = await file.read()
    logger.info("Writing to %s", file_path)
    with open(file_path, "wb") as f:
        f.write(contents)
    return os.path.getsize(file_path)

@async_log_execution
async def load_document_file(file:File, folder:str):
    _, file_name = os.path.split(file.filename)
    file_path = make_file_path(folder, file_name)
    contents = await file.read()
    logger.info("Writing to %s", file_path)
    with open(file_path, "wb") as f:
        f.write(contents)
    return os.path.getsize(file_path)

def make_file_path(folder, file_name):
    """
    * Make the file path. Append a _{i} to end if already exists.
    """
    i = 0
    file_path = os.path.join(folder, file_name)
    orig_file_name, ext = os.path.splitext(file_path)
    while os.path.exists(file_path):
        file_path = os.path.join(folder, orig_file_name + f"_{i}.{ext}")
        i += 1
    return file_path
