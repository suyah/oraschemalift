# Lightweight registry for unload handlers; currently Snowflake only.
from typing import Dict, Any

_HANDLERS: Dict[str, Dict[str, callable]] = {}

# --- Snowflake --------------------------------------------------------------
try:
    from .snowflake_unload import unload_table, unload_schema  # noqa: WPS433 – runtime import

    _HANDLERS["snowflake"] = {
        "table": unload_table,
        "schema": unload_schema,
    }
except ModuleNotFoundError:
    # Optional dependency not installed; handler will error at runtime
    pass


def run_unload(db_type: str, mode: str, connection: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Dispatch to a dialect-specific unload implementation."""
    db_type = (db_type or "").lower()
    mode = (mode or "").lower()

    if db_type not in _HANDLERS or mode not in _HANDLERS[db_type]:
        raise ValueError(f"Unsupported unload operation: {db_type}.{mode}")

    return _HANDLERS[db_type][mode](connection, params)