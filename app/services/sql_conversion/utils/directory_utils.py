from datetime import datetime
from pathlib import Path
from app.utils.path_utils import workspace_path

__all__ = ["get_timestamp", "create_run_directory"]


def get_timestamp() -> str:
    """Return current timestamp as YYYYMMDD_HHMMSS string."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def create_run_directory(
    subfolder: str | None = "runs",
    prefix: str | None = None,
    run_timestamp: str | None = None,
) -> Path:
    """Create and return a new *timestamped* run directory inside the workspace.

    Final path layout::

        workspace/<subfolder>/<prefix_>TIMESTAMP

    • *subfolder*   – Logical parent folder under the workspace.  If ``None`` or
      blank, we default to ``"runs"``.  Empty strings are *not* allowed.
    • *prefix*      – Optional descriptor inserted before the timestamp.
    • *run_timestamp* – Allow callers to inject a previously generated
      timestamp.  If omitted, the helper will call :pyfunc:`get_timestamp()`.
    """

    # Sanitise / default arguments ------------------------------------------------
    subfolder = subfolder or "runs"
    if not str(subfolder).strip():
        raise ValueError("'subfolder' must be a non-empty string in create_run_directory().")

    ts: str = run_timestamp or get_timestamp()
    if not str(ts).strip():
        raise ValueError("'run_timestamp' resolved to an empty string in create_run_directory().")

    dir_name = f"{prefix + '_' if prefix else ''}{ts}"

    run_dir = workspace_path(subfolder, dir_name)
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir 