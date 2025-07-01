import logging
import logging.handlers
import os
from datetime import datetime
from typing import Optional, List

from app import config

__all__ = ["setup_logger"]

# ---------------------------------------------------------------------------
# Root logger configuration (one-time) – idempotent
# ---------------------------------------------------------------------------

def _configure_root_logger() -> None:
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    log_cfg = config.get("logging", {})
    lvl_cfg = log_cfg.get("level", {})

    # ---------- Console handler (ensure one human-readable) ----------
    console_level = getattr(logging, lvl_cfg.get("console", "INFO").upper(), logging.INFO)

    if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        ch = logging.StreamHandler()
        ch.setLevel(console_level)
        ch.setFormatter(logging.Formatter("%(name)s - %(levelname)s - %(message)s"))
        root.addHandler(ch)

    # ---------- Rotating file handler (central) ----------
    rotation_cfg = log_cfg.get("rotation", {})
    max_bytes = rotation_cfg.get("max_bytes", 10 * 1024 * 1024)
    backup_count = rotation_cfg.get("backup_count", 5)
    encoding = rotation_cfg.get("encoding", "utf-8")

    file_level = getattr(logging, lvl_cfg.get("file", "DEBUG").upper(), logging.DEBUG)

    logs_dir = config.get("base_dirs", {}).get("logs", "logs")
    os.makedirs(logs_dir, exist_ok=True)
    file_path = os.path.join(logs_dir, "app.log")

    if not any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == file_path for h in root.handlers):
        fh = logging.handlers.RotatingFileHandler(
            file_path, mode="a", maxBytes=max_bytes, backupCount=backup_count, encoding=encoding
        )
        fh.setLevel(file_level)
        fh.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        root.addHandler(fh)


# ---------------------------------------------------------------------------
# Public helper
# ---------------------------------------------------------------------------


def setup_logger(name: str, *, sql_files: Optional[List[str]] = None, api_name: Optional[str] = None):
    _configure_root_logger()

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Execution-specific handler (optional) ---------------------------------
    if sql_files:
        logs_dir = config.get("base_dirs", {}).get("logs", "logs")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        service_name = api_name or name

        exec_parent = os.path.join(logs_dir, "db_execution")
        os.makedirs(exec_parent, exist_ok=True)

        exec_dir = os.path.join(exec_parent, f"{service_name}_execution_{timestamp}")
        os.makedirs(exec_dir, exist_ok=True)

        exec_file = os.path.join(exec_dir, "execution.log")

        if not any(isinstance(h, logging.FileHandler) and h.baseFilename == exec_file for h in logger.handlers):
            exec_fh = logging.FileHandler(exec_file, mode="w", encoding="utf-8")
            exec_fh.setLevel(logging.DEBUG)
            exec_fh.setFormatter(
                logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            )
            logger.addHandler(exec_fh)
            logger.info("Execution logging to: %s", exec_dir)

    return logger 

# ---------------------------------------------------------------------------

_FRAMEWORK_PREFIXES = (
    "uvicorn",   # ASGI server
    "watchfiles" # Dev auto-reloader
)


def _file_filter(record: logging.LogRecord) -> bool:
    """Skip framework loggers in app.log; let everything else through."""

    return not record.name.startswith(_FRAMEWORK_PREFIXES) 