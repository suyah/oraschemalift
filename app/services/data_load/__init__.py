from typing import Dict, Any

# Dynamically populated registry: { db_type: { mode: handler } }
_HANDLERS: Dict[str, Dict[str, callable]] = {}

try:
    from .oracle_load import load_table, load_schema

    _HANDLERS["oracle"] = {
        "table": load_table,
        "schema": load_schema,
    }
except ModuleNotFoundError:
    pass


def run_load(db_type: str, mode: str, connection: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    db_type = (db_type or "").lower()
    mode = (mode or "").lower()
    if db_type not in _HANDLERS or mode not in _HANDLERS[db_type]:
        raise ValueError(f"Unsupported load operation: {db_type}.{mode}")
    return _HANDLERS[db_type][mode](connection, params) 