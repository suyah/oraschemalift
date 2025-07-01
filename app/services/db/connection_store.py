from app.config import config
from pathlib import Path
from typing import Dict, Any
import json
from app.utils.path_utils import workspace_path

_WORKSPACE = workspace_path()
_USERDATA_SUBDIR = Path(config['workspace_sub_dirs'].get('userdata', 'userdata'))
_CONNECTIONS_DIR_NAME = 'connections'


def _conn_file(db_type: str, name: str) -> Path:
    return _WORKSPACE / _USERDATA_SUBDIR / db_type.lower() / _CONNECTIONS_DIR_NAME / f"{name}.json"


def list_connections() -> Dict[str, Any]:
    base_dir = _WORKSPACE / _USERDATA_SUBDIR
    result: Dict[str, Any] = {}
    if not base_dir.exists():
        return result
    for db_dir in base_dir.iterdir():
        conn_dir = db_dir / _CONNECTIONS_DIR_NAME
        if conn_dir.is_dir():
            for file in conn_dir.glob("*.json"):
                result[file.stem] = json.loads(file.read_text())
    return result


def save_connection(name: str, db_type: str, payload: Dict[str, Any]) -> None:
    """Save a connection profile.

    Raises:
        ValueError: if a profile with the same name already exists for the
        given RDBMS to prevent silent overwrites.
    """
    file_path = _conn_file(db_type, name)
    if file_path.exists():
        raise ValueError(f"Connection '{name}' for '{db_type}' already exists. Use a different name or delete the existing profile first.")

    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(json.dumps(payload, indent=2))


def delete_connection(name: str, db_type: str) -> bool:
    file_path = _conn_file(db_type, name)
    if file_path.exists():
        file_path.unlink()
        return True
    return False 