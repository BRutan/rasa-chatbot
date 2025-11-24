import re
from typing import ForwardRef, Union

File = ForwardRef("File")

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
RAW_TEXT_EXTS = [re.escape(ext) for ext in RAW_TEXT_EXTS]
DOC_EXTS = [re.escape(ext) for ext in DOC_EXTS]

IMAGE_PATT = re.compile("(" + "|".join(IMAGE_EXTS) + ")$")
VIDEO_PATT = re.compile("(" + "|".join(VIDEO_EXTS) + ")$")
RAW_TEXT_PATT = re.compile("(" + "|".join(RAW_TEXT_EXTS) + ")$")
DOC_PATT = re.compile("(" + "|".join(DOC_EXTS) + ")$")

def is_text(file:Union[File, str]):
    filename = file.filename if not isinstance(file, str) else file
    return RAW_TEXT_PATT.search(filename) is not None

def is_video(file:File):
    filename = file.filename if not isinstance(file, str) else file
    return VIDEO_PATT.search(filename) is not None

def is_image(file:File):
    filename = file.filename if not isinstance(file, str) else file
    return IMAGE_PATT.search(filename) is not None

def is_document(file:File):
    filename = file.filename if not isinstance(file, str) else file
    return DOC_PATT.search(filename) is not None