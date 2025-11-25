from functools import wraps
from httpx import Response
import inspect
import logging
import json
from json.decoder import JSONDecodeError
import os
from rasa.exceptions import RasaException
from rasa_sdk.events import ActionExecuted, ActiveLoop, EventType, FollowupAction, SlotSet, SessionStarted
import re
import string
from typing import Any, Dict, List
import yaml

VIDEO_EXTS = [
    ".mp4", ".m4v", ".mov", ".avi", ".wmv", ".flv", ".f4v", ".webm", ".mkv",
    ".ts", ".m2ts", ".3gp", ".3g2", ".mpg", ".mpeg", ".mpe", ".mts", ".trp",
    ".mxf", ".gxf", ".braw", ".r3d", ".cine", ".dpx", ".yuv", ".vob", ".dat",
    ".ogv", ".rm", ".rmvb", ".asf", ".divx", ".fli", ".flc", ".swf", ".vro",
    ".hevc", ".av1", ".h265", ".h264", ".vp9", ".vp8", ".mod", ".tod", ".rec",
    ".tp", ".tivo", ".ismv", ".ism", ".isma", ".mpsub", ".nut", ".drp", ".ivf"
]

IMAGE_EXTS = [
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".bmp",
    ".tiff", ".tif", ".heic", ".heif", ".ico", ".cur",
    ".cr2", ".cr3", ".nef", ".arw", ".srf", ".sr2", ".rw2", ".orf",
    ".raf", ".pef", ".dng", ".raw", ".x3f", ".srw", ".erf", ".kdc",
    ".mef", ".mos", ".mrw", ".bay", ".cap", ".iiq", ".rwl",
    ".3fr", ".fff",
    ".psd", ".psb", ".xcf", ".pat", ".exr", ".hdr", ".tga", ".sgi",
    ".jp2", ".j2k", ".jpf", ".jpx", ".jpm", ".mj2", ".pgm", ".ppm",
    ".pbm", ".pnm", ".dds", ".dib", ".icns", ".pct", ".pic",
    ".pnz", ".qoi", ".ras", ".bin"
]

DOC_EXTS = [".pdf", ".docx", ".xlsx"]
RAW_TEXT_EXTS = [".txt", ".csv"]

IMAGE_EXTS = [re.escape(ext) for ext in IMAGE_EXTS]
VIDEO_EXTS = [re.escape(ext) for ext in VIDEO_EXTS]
DOC_EXTS = [re.escape(ext) for ext in DOC_EXTS]
RAW_TEXT_EXTS = [re.escape(ext) for ext in RAW_TEXT_EXTS]

ALL_EXTS = IMAGE_EXTS + VIDEO_EXTS + DOC_EXTS + RAW_TEXT_EXTS
ALL_PATT = re.compile("(" + "|".join(ALL_EXTS) + ")$")

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = True

NO_PUNCT = re.compile("[" + re.escape(string.punctuation) + "]")
STRIP = re.compile(r"^\s}|\s+$")
EOS_PUNCT = re.compile(r"[.!?]+$")

def log_execution(func):
    """
    * Log start end end of logs.
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        logger.info(f"Starting {func.__name__}()")
        if inspect.ismethod(func):
            result = func(self, *args, **kwargs)
        else:
            args = tuple([self] + list(args))
            result = func(*args, **kwargs)
        logger.info(f"Finished {func.__name__}()")
        return result
    return wrapper

def async_log_execution(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        logger.info(f"Starting {func.__name__}()")
        if inspect.ismethod(func):
            result = await func(self, *args, **kwargs)
        else:
            args = tuple([self] + list(args))
            result = await func(*args, **kwargs)
        logger.info(f"Finished {func.__name__}()")
        return result
    return wrapper

@log_execution
def present_money(amt:float) -> str:
    """
    * Prepare money amount for presentation.
    """
    return "$" + f"{amt:,.2f}"

@log_execution
def present_name(name:str) -> str:
    """
    * Retrieve the normalized vendor full name 
    based on possible patterns.
    """
    tokens = name.split(" ")
    tokens = [tk.title() for tk in tokens]
    tokens = [tk.upper() if re.match("(llc|inc)", tk, flags=re.IGNORECASE) else tk 
              for tk in tokens]
    return " ".join(tokens)

@log_execution
def clear_active_loop_slots(tracker, domain):
    """
    * Clear all slots in the active loop.
    """
    active_loop = tracker.active_loop
    logger.info("active_loop: %s", active_loop)
    form_name = active_loop.get("name") if active_loop else None
    logger.info("form_name: %s", form_name)
    if not form_name:
        return []
    required_slots = domain.get("forms", {}).get(form_name, {}).get("required_slots", [])
    logger.info("required_slots: %s", required_slots)
    if not required_slots:
        return []
    # Clear all current form slots dynamically:
    events = []
    for slot_name, _ in tracker.current_slot_values().items():
        if slot_name in required_slots:
            events.append(SlotSet(slot_name, None))
    return events

def normalize_text(val:str, keep_punct:bool=False, keep_toks:List[str]=None) -> str:
    """
    * Normalize all text.
    """
    if not isinstance(val, str):
        return val
    val = STRIP.sub("", val).lower()
    val = EOS_PUNCT.sub("", val)
    if keep_punct:
        return val
    elif keep_toks and any(tk in string.punctuation for tk in keep_toks):
        remaining = "".join([tk for tk in string.punctuation if tk not in keep_toks])
        return re.sub("[" + re.escape(remaining) + "]", "", val)
    else:
        return NO_PUNCT.sub("", val)
    
def normalize_numeric_text(val:str) -> str:
    """
    * Normalize numeric text.
    """
    normalized = re.sub(r"^\s*\$", "", val)
    normalized = re.sub(",", "", normalized)
    return normalize_text(normalized)

def try_convert(val:str, type:type) -> Any:
    try:
        return type(val)
    except:
        return None
    
def is_file_name(val:str) -> bool:
    """
    * Indicate that value is a valid file name.
    """
    return ALL_PATT.search(val) is not None

def get_form_slots() -> Dict[str, Any]:
    """
    * Retrieve forms and required slots.
    """
    root = os.path.split(__file__)[0]
    domain_yml = os.path.join(root, "../domain.yml")
    if not os.path.exists(domain_yml):
        raise RuntimeError(f"domain.yml not present at {domain_yml}.")
    with open(domain_yml, "r") as f:
        domain_dict = yaml.safe_load(f)
    forms = domain_dict["forms"]
    out = {}
    for form, req_slots in forms.items():
        out[form] = req_slots["required_slots"]
    return out

def get_validation_patts() -> Dict[str, Any]:
    """
    * Load all validation patterns for slots.    
    """
    root = os.path.split(__file__)[0]
    validation_yaml = os.path.join(root, "../validation.yml")
    if not os.path.exists(validation_yaml):
        raise RuntimeError(f"validation.yml not present at {validation_yaml}.")
    # Expecting { form -> slot -> patt }
    with open(validation_yaml, "r") as f:
        return yaml.safe_load(f)

def try_raise(response:Response):
    try:
        response.raise_for_status()
        logger.info("response: ")
        logger.info(json.dumps(response.json(), indent=2))
    except Exception as ex:
        logger.error(f"error: {response.text}")
        response_json = handle_decode_error(response)
        if response_json:
            logger.error(json.dumps(response_json, indent=2))
        raise ex
    
def handle_decode_error(response) -> Dict[str, Any]:
    try:
        return response.json()
    except JSONDecodeError:
        return {}