from pathlib import Path
from app.config import config
from typing import Optional
from datetime import datetime

__all__ = [
    "workspace_path",
    "build_samples_path",
    "create_samples_run_dirs",
    "resolve_converted_run",
]


def workspace_path(*parts) -> Path:

    if not parts:
        return Path(config["base_dirs"]["workspace"])

    invalid_parts = [p for p in parts if p is None or str(p).strip() == ""]
    if invalid_parts:
        raise ValueError(
            "workspace_path parts cannot be empty or None. "
            f"Received invalid segment(s): {invalid_parts}"
        )

    return Path(config["base_dirs"]["workspace"]).joinpath(*parts)


# ---------------------------------------------------------------------------
# Test-data path helper (factored out of API logic)
# ---------------------------------------------------------------------------


def build_samples_path(
    db_type: str,
    *,
    run_timestamp: str,
    custom_subdir: Optional[str] = None,
) -> Path:

    if not db_type or not str(db_type).strip():
        raise ValueError("'db_type' must be a non-empty string in build_samples_path().")

    if not run_timestamp or not str(run_timestamp).strip():
        raise ValueError("'run_timestamp' must be a non-empty string in build_samples_path().")

    ws_root = Path(config["base_dirs"]["workspace"])
    td_subdir = config["workspace_sub_dirs"]["samples"]

    scripts_parent = config["workspace_sub_dirs"].get("scripts_parent", "sql_files")
    source_subdir_name = config["workspace_sub_dirs"].get("scripts_source", "source")
    converted_name = config["workspace_sub_dirs"].get("scripts_converted", "converted")

    # Determine the final subdir name (case-insensitive match)
    if custom_subdir is None:
        target_name = source_subdir_name
    else:
        _norm = custom_subdir.lower()
        if _norm == source_subdir_name.lower():
            target_name = source_subdir_name
        elif _norm == converted_name.lower():
            target_name = converted_name
        else:
            raise ValueError(
                f"Invalid custom_subdir '{custom_subdir}'. Must be '{source_subdir_name}' or '{converted_name}'."
            )

    return ws_root / td_subdir / db_type.lower() / run_timestamp / scripts_parent / target_name


# Temporary backward-compat alias (remove later)
build_testdata_path = build_samples_path


# ---------------------------------------------------------------------------
# Helper to create a brand-new LLM-generated samples run directory
# ---------------------------------------------------------------------------


def create_samples_run_dirs(db_type: str) -> tuple[Path, Path]:

    if not db_type or not str(db_type).strip():
        raise ValueError("'db_type' must be provided for create_samples_run_dirs().")

    samples_subdir_name = config["workspace_sub_dirs"]["samples"]

    # workspace/samples/<db_type>/<timestamp>/
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_base_dir = workspace_path(samples_subdir_name, db_type.lower(), timestamp)

    # workspace/samples/<db_type>/<timestamp>/sql_files/source/
    source_dir = build_samples_path(db_type, run_timestamp=timestamp)

    source_dir.mkdir(parents=True, exist_ok=True)

    return run_base_dir, source_dir


# ---------------------------------------------------------------------------
# Helper to select a specific or latest converted sub-run directory
# ---------------------------------------------------------------------------


def resolve_converted_run(base_converted_dir: Path | str, sub_timestamp: str | None = None) -> Path:

    base_converted_dir = Path(base_converted_dir)

    if sub_timestamp:
        candidate = base_converted_dir / sub_timestamp
        if candidate.is_dir():
            return candidate
        raise FileNotFoundError(
            f"Converted run '{sub_timestamp}' not found under {base_converted_dir}"
        )

    if not base_converted_dir.exists():
        raise FileNotFoundError(f"Converted folder does not exist: {base_converted_dir}")

    subdirs = [d for d in base_converted_dir.iterdir() if d.is_dir()]
    if not subdirs:
        raise FileNotFoundError(f"No converted runs present under {base_converted_dir}")

    return max(subdirs, key=lambda p: p.name) 