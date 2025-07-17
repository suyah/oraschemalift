from typing import Dict, Any
import logging
import os
import re
from app import config as _app_config

# Third-party client libraries
try:
    import snowflake.connector  # type: ignore
except ImportError:  # pragma: no cover – optional dependency
    snowflake = None  # noqa: N816
else:
    snowflake = snowflake.connector

try:
    import oracledb  # type: ignore
except ImportError:  # pragma: no cover – optional dependency
    oracledb = None

try:
    import psycopg2  # type: ignore
except ImportError:
    psycopg2 = None

try:
    from google.cloud import bigquery  # type: ignore
except ImportError:  # pragma: no cover
    bigquery = None

try:
    import pyodbc  # type: ignore
except ImportError:  # pragma: no cover – optional dependency
    pyodbc = None

logger = logging.getLogger(__name__)

__all__ = ["get_connection"]


def _require(fields, cfg: Dict[str, Any], db_name: str):
    missing = [f for f in fields if not cfg.get(f)]
    if missing:
        raise ValueError(f"Missing {db_name} connection fields: {', '.join(missing)}")


def get_connection(db_type: str, cfg: Dict[str, Any]):
    """Return a live DB-API-ish connection for the requested RDBMS.

    The *cfg* dict is the same structure that the existing /db/test-connection
    endpoint uses (user, password, account, etc.).
    """
    db_type = (db_type or "").lower()

    if db_type == "snowflake":
        if snowflake is None:
            raise RuntimeError("snowflake-connector-python is not installed")
        _require(["user", "password", "account"], cfg, "Snowflake")
        return snowflake.connect(
            user=cfg["user"],
            password=cfg["password"],
            account=cfg["account"],
            warehouse=cfg.get("warehouse"),
            role=cfg.get("role"),
            database=cfg.get("database"),
            schema=cfg.get("schema"),
        )

    if db_type == "oracle":
        if oracledb is None:
            raise RuntimeError("oracledb package is not installed")
        _require(["user", "password", "dsn"], cfg, "Oracle")
        # Build connection kwargs dynamically to include optional wallet parameters only if provided
        conn_kwargs = {
            "user": cfg["user"],
            "password": cfg["password"],
            "dsn": cfg["dsn"],
        }

        # --------------------------------------------------
        # Wallet handling
        # --------------------------------------------------
        # We accept either:
        #   • wallet_dir  (relative folder name under workspace/uploads/wallets/oracle)
        # We resolve wallet_dir to an absolute path so that every caller gets a
        # fully-qualified directory that the Oracle driver can read.

        wallet_dir_param = cfg.get("wallet_dir")

        actual_wallet_path: str | None = None

        if wallet_dir_param:
            # Resolve relative wallet_dir against workspace base
            if os.path.isabs(wallet_dir_param):
                actual_wallet_path = wallet_dir_param
            else:
                workspace_base_dir = _app_config.get("base_dirs", {}).get("workspace")
                oracle_wallets_subdir = _app_config.get("workspace_sub_dirs", {}).get("oracle_wallets_base", "uploads/wallets/oracle")

                if workspace_base_dir:
                    actual_wallet_path = os.path.abspath(os.path.join(workspace_base_dir, oracle_wallets_subdir, wallet_dir_param))
                else:
                    # Fallback: treat as-is (may still succeed if relative to CWD)
                    actual_wallet_path = wallet_dir_param

        # If we resolved a wallet path, populate the driver kwargs
        if actual_wallet_path and os.path.isdir(actual_wallet_path):
            # Pass the wallet directory to python-oracledb (thin mode)
            conn_kwargs["config_dir"] = actual_wallet_path
            conn_kwargs["wallet_location"] = cfg.get("wallet_location", actual_wallet_path)
            if cfg.get("wallet_password"):
                conn_kwargs["wallet_password"] = cfg["wallet_password"]
        else:
            # No wallet provided or resolution failed -> proceed without wallet
            pass

        return oracledb.connect(**conn_kwargs)

    if db_type in ("postgres", "postgresql", "greenplum"):
        if psycopg2 is None:
            raise RuntimeError("psycopg2 is not installed")
        _require(["user", "password", "database", "host"], cfg, "Postgres")
        return psycopg2.connect(
            host=cfg["host"],
            port=cfg.get("port", 5432),
            database=cfg["database"],
            user=cfg["user"],
            password=cfg["password"],
        )

    if db_type in ("sqlserver", "mssql", "sql_server"):
        if pyodbc is None:
            raise RuntimeError("pyodbc package is not installed")
        # 'database' is optional; if omitted we connect to the default DB and the
        # caller can switch later with a USE statement.
        _require(["user", "password", "host"], cfg, "SQL Server")
        port = cfg.get("port", 1433)
        driver = cfg.get("driver", "ODBC Driver 18 for SQL Server")
        server = f"{cfg['host']},{port}"
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"UID={cfg['user']};"
            f"PWD={cfg['password']};"
            f"TrustServerCertificate=yes;"
        )
        if cfg.get("database"):
            conn_str += f"DATABASE={cfg['database']};"
        return pyodbc.connect(conn_str, autocommit=True)

    raise ValueError(f"Unsupported db_type '{db_type}' in connection_factory") 