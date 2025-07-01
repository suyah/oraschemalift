# New util – centralised path resolver for *source SQL* folders
from __future__ import annotations

"""Utilities for resolving logical workspace descriptors to concrete paths.

This centralises the logic so that **all** endpoints (REST or CLI) can share one
implementation and we avoid copy-pasting path fiddling code.
"""

from pathlib import Path
from typing import Dict, Any

from app.config import config as _cfg
from .path_utils import build_samples_path, resolve_converted_run

__all__ = ["resolve_source_sql_path"]


def resolve_source_sql_path(
    *,
    input_path_type: str,
    input_path_config: Dict[str, Any],
    db_type_for_path: str,
) -> Path:
    ws_root = Path(_cfg["base_dirs"]["workspace"]).resolve()
    wsd = _cfg["workspace_sub_dirs"]
    scripts_parent = wsd.get("scripts_parent", "sql_files")

    custom = (input_path_config.get("custom") or "source").lower()
    cv_ts = input_path_config.get("converted_timestamp")

    # ----------------------- samples -----------------------
    if input_path_type == "samples":
        run_ts = input_path_config["run_timestamp"]
        base = build_samples_path(
            db_type_for_path,
            run_timestamp=run_ts,
            custom_subdir=("source" if custom == "source" else "converted"),
        )
        return (
            base if custom == "source" else resolve_converted_run(base, cv_ts)
        ).resolve()

    # ----------------------- extracts ----------------------
    if input_path_type == "extracts":
        folder = input_path_config.get("user_folder") or input_path_config["run_timestamp"]
        root = ws_root / wsd["extracts"] / db_type_for_path / folder
        if custom == "source":
            return (root / scripts_parent / "source").resolve()
        conv_base = root / scripts_parent / "converted"
        return resolve_converted_run(conv_base, cv_ts).resolve()

    # ----------------------- uploads -----------------------
    if input_path_type == "uploads":
        project = input_path_config["project_name"]
        root = ws_root / wsd["uploads"] / scripts_parent / db_type_for_path / project
        if custom == "source":
            return (root / "source").resolve()
        return resolve_converted_run(root / "converted", cv_ts).resolve()

    raise ValueError(f"Unsupported input_path_type: {input_path_type}")