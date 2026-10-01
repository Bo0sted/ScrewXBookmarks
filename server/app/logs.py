"""Compact, colored console logs: `HH:MM:SS  component  message`. Set NO_COLOR=1 to disable colors."""

import logging
import os
import sys
import time

_COLOR = not os.environ.get("NO_COLOR")
_RESET = "\033[0m" if _COLOR else ""
_DIM = "\033[2m" if _COLOR else ""
_LEVEL = {
    logging.WARNING: "\033[33m" if _COLOR else "",
    logging.ERROR: "\033[31m" if _COLOR else "",
    logging.CRITICAL: "\033[31m" if _COLOR else "",
}
_NAME = {
    "sync": "\033[36m",
    "downloads": "\033[35m",
    "x-api": "\033[34m",
    "archive": "\033[96m",
    "thumbs": "\033[94m",
    "web": "\033[32m",
    "reset": "\033[31m",
}


def fmt_duration(seconds: float) -> str:
    s = max(0, int(round(seconds)))
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m{s % 60:02d}s"
    return f"{s // 3600}h{s % 3600 // 60:02d}m"


def fmt_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} GB"


class _Formatter(logging.Formatter):
    def format(self, r: logging.LogRecord) -> str:
        ts = time.strftime("%H:%M:%S", time.localtime(r.created))
        name = {"uvicorn": "web", "uvicorn.error": "web"}.get(r.name, r.name)
        name_color = (_NAME.get(name, "") if _COLOR else "")
        msg = r.getMessage()
        level_color = _LEVEL.get(r.levelno, "")
        if level_color:
            msg = f"{level_color}{msg}{_RESET}"
        if r.exc_info:
            msg += "\n" + self.formatException(r.exc_info)
        return f"{_DIM}{ts}{_RESET}  {name_color}{name:<9}{_RESET}  {msg}"


def setup() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_Formatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)
    for name in ("uvicorn", "uvicorn.error"):
        lg = logging.getLogger(name)
        lg.handlers[:] = [handler]
        lg.propagate = False
    # Per-request noise: every HTTP call and every page view.
    for name in ("httpx", "httpcore", "uvicorn.access"):
        logging.getLogger(name).setLevel(logging.WARNING)

    # twscrape logs through loguru; route its warnings/errors into the same format.
    from loguru import logger as loguru_logger

    def bridge(message) -> None:
        rec = message.record
        logging.getLogger("x-api").log(rec["level"].no, rec["message"])

    loguru_logger.remove()
    loguru_logger.add(bridge, level="WARNING", format="{message}")
