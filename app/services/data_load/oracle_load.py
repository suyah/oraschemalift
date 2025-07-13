from typing import Dict, Any, List

from app.services.db.connection_factory import get_connection as _get_conn
from app.utils.logger import setup_logger

logger = setup_logger('oracle_load')

__all__ = ["load_table", "load_schema"]

_PLSQL_TEMPLATE = (
    "BEGIN\n"
    "   DBMS_CLOUD.COPY_DATA(\n"
    "      table_name      => '{table}',\n"
    "      credential_name => '{credential}',\n"
    "      file_uri_list   => '{uri}',\n"
    "      format          => '{{\"type\":\"{fmt}\"}}'\n"
    "   );\n"
    "END;"
)


def _connect(cfg: Dict[str, Any]):
    return _get_conn("oracle", cfg)


def load_table(conn_cfg: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    raw_table = params.get("table")
    credential = params.get("credential_name")
    uri = params.get("file_uri_prefix")
    if not all([raw_table, credential, uri]):
        raise ValueError("'table', 'credential_name' and 'file_uri_prefix' are required")

    # Determine schema and table components
    schema_param = params.get("schema")
    schema_part = None
    table_part = raw_table

    if "." in raw_table:
        schema_part, table_part = raw_table.split(".", 1)
    elif schema_param:
        schema_part = schema_param

    # Build PL/SQL with optional schema_name argument
    fmt = params.get("format", "parquet")

    if schema_part:
        plsql = (
            "BEGIN\n"
            "   DBMS_CLOUD.COPY_DATA(\n"
            "      table_name      => '{tbl}',\n"
            "      credential_name => '{cred}',\n"
            "      file_uri_list   => '{uri}',\n"
            "      schema_name     => '{sch}',\n"
            "      format          => '{{\"type\":\"{fmt}\"}}'\n"
            "   );\n"
            "END;"
        ).format(tbl=table_part, cred=credential, uri=uri, sch=schema_part, fmt=fmt)
        fq_table_name = f"{schema_part}.{table_part}"
    else:
        plsql = _PLSQL_TEMPLATE.format(table=table_part, credential=credential, uri=uri, fmt=fmt)
        fq_table_name = table_part

    logger.info("DBMS_CLOUD.COPY_DATA PL/SQL prepared: %s", plsql.replace("\n", " "))

    conn = _connect(conn_cfg)
    try:
        cur = conn.cursor()
        try:
            cur.execute(plsql)
            conn.commit()
            logger.info("COPY_DATA completed for table %s", fq_table_name)
            return {
                "status": "success",
                "table": fq_table_name,
                "executed_plsql": plsql,
            }
        except Exception as exec_err:
            # Rollback is unnecessary for COPY_DATA but harmless
            try:
                conn.rollback()
            except Exception:
                pass
            logger.error("COPY_DATA failed for table %s: %s", fq_table_name, exec_err)
            return {
                "status": "error",
                "table": fq_table_name,
                "executed_plsql": plsql,
                "error": str(exec_err),
            }
        finally:
            cur.close()
    finally:
        conn.close()


def _list_tables(conn, schema: str) -> List[str]:
    q = "SELECT table_name FROM all_tables WHERE owner = :1"
    cur = conn.cursor()
    try:
        cur.execute(q, [schema.upper()])
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()


def load_schema(conn_cfg: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    schema = params.get("schema")
    credential = params.get("credential_name")
    uri_root = params.get("file_uri_root")
    if not all([schema, credential, uri_root]):
        raise ValueError("'schema', 'credential_name', 'file_uri_root' are required")

    fmt = params.get("format", "parquet")

    conn = _connect(conn_cfg)
    try:
        tables = _list_tables(conn, schema)
    finally:
        conn.close()

    results = []
    for tbl in tables:
        uri = f"{uri_root}parquet_{schema.lower()}_{tbl}*.parquet"
        res = load_table(conn_cfg, {
            "table": f"{schema}.{tbl}",
            "credential_name": credential,
            "file_uri_prefix": uri,
            "format": fmt,
        })
        results.append({"table": tbl, **res})

    return {
        "status": "success",
        "schema": schema,
        "table_count": len(results),
        "details": results,
    } 