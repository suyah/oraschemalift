from typing import Dict, Any

from app.api.routes import _resolve_connection
from app.services.data_unload import run_unload
from app.services.data_load import run_load

DEFAULT_FILE_URI_ROOT = "https://"


def migrate_table(payload: Dict[str, Any]) -> Dict[str, Any]:
    """End-to-end single-table migration: unload from Snowflake, load into Oracle.

    Expected top-level keys in payload:
        source: dict with keys required by /data/unload/table
        target: dict with keys required by /data/load/table
        file_uri_root: prefix to prepend when building file_uri_prefix
    """
    source = payload.get("source", {})
    target = payload.get("target", {})
    if not source or not target:
        raise ValueError("'source' and 'target' sections are required in payload")

    file_root = payload.get("file_uri_root", DEFAULT_FILE_URI_ROOT)

    # Resolve connections
    src_conn_cfg, src_db_type = _resolve_connection(source)
    tgt_conn_cfg, tgt_db_type = _resolve_connection(target)

    # Currently we only support snowflake -> oracle
    if src_db_type != "snowflake" or tgt_db_type != "oracle":
        raise ValueError("migrate_table currently supports snowflake -> oracle only")

    # 1. Unload
    unload_res = run_unload(src_db_type, "table", src_conn_cfg, source)
    if unload_res.get("status") != "success":
        return {"status": "error", "phase": "unload", **unload_res}

    prefix = unload_res["unload_prefix"]
    file_uri_prefix = f"{file_root}{prefix}*.parquet"

    # 2. Load
    target_table = target.get("target_table") or prefix.upper()
    load_params = {
        "table": target_table,
        "credential_name": target["credential_name"],
        "file_uri_prefix": file_uri_prefix,
        "format": source.get("file_format", "parquet"),
    }
    load_res = run_load(tgt_db_type, "table", tgt_conn_cfg, load_params)
    if load_res.get("status") != "success":
        return {"status": "error", "phase": "load", **load_res}

    return {
        "status": "success",
        "file_uri_prefix": file_uri_prefix,
        "unload": unload_res,
        "load": load_res,
    } 