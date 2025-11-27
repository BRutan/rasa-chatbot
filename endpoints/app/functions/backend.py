import httpx
from objects.functions.database import connect_to_backend
from shared import logger, log_execution
import os
import shutil

BACKEND_CONN = connect_to_backend()
RASA_TRACKER_URL = "{host}:{port}".format(host=os.environ["RASA_HOST"], port=os.environ["RASA_PORT"])
RASA_CONVERSATIONS_URL = "{url}/conversations".format(url=RASA_TRACKER_URL)
DATA_PATH = os.path.join(os.environ["HOME"], "data")
EVIDENCE_DIR = os.environ["EVIDENCE_DIR"]
TRANS_DOC_DIR = os.environ["TRANS_DOC_DIR"]
USER_ID_DIR = os.environ["USER_ID_DIR"]

def reset_document_storage():
    """
    * Remove all documents in the evidence and transaction dir.
    """
    for dir in [EVIDENCE_DIR, TRANS_DOC_DIR]:
        logger.info("Replacing %s", dir)
        shutil.rmtree(dir, ignore_errors=True)
        os.makedirs(dir, exist_ok=True)

def reset_tracker(conv_id:str, header:dict=None):
    """
    * Reset the tracker.
    """
    result = httpx.delete(f"{RASA_TRACKER_URL}/{conv_id}", headers=header)
    if result.status_code == 404:
        return None
    result.raise_for_status()
    return result.json()
    #result = httpx.delete(tracker_url + "/conversations/tracker", headers=header)
    #result.raise_for_status()
    #return result.json() 