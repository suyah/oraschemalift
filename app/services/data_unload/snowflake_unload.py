from typing import Dict, Any, List

from app.services.db.connection_factory import get_connection as _get_conn
from app.utils.logger import setup_logger

logger = setup_logger('snowflake_unload')

__all__ = ["unload_table", "unload_schema"]

_COPY_INTO_TEMPLATE = (
    "COPY INTO @{stage}/{prefix} "
    "FROM (SELECT * FROM {table}) "
    "FILE_FORMAT=(TYPE='{file_format}') "
    "OVERWRITE=TRUE "
    "HEADER={header};"
)


def _derive_prefix(table: str, explicit_prefix: str | None = None) -> str:
    if explicit_prefix:
        return explicit_prefix
    parts = table.split(".")
    if len(parts) == 3:
        _, schema, tbl = parts
    elif len(parts) == 2:
        schema, tbl = parts
    else:
        raise ValueError("table name must be schema.table or db.schema.table")
    return f"parquet_{schema}_{tbl}"


def _use_schema(cur, database: str | None, schema: str | None):
    """Ensure current schema is set when both database and schema are provided.

    We no longer fall back to the non-existent `public` schema because many
    Snowflake accounts do not have it.  If *schema* is None we leave the
    session’s current schema unchanged.
    """
    if not schema:
        return
    if database:
        cur.execute(f"USE SCHEMA {database}.{schema}")
    else:
        cur.execute(f"USE SCHEMA {schema}")


def unload_table(conn_cfg: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    table = params.get("table")
    stage = params.get("stage")
    if not table or not stage:
        raise ValueError("'table' and 'stage' are required unload parameters")

    file_format = params.get("file_format", "parquet")
    header = str(params.get("header", True)).lower()
    prefix = _derive_prefix(table, params.get("prefix"))

    sql = _COPY_INTO_TEMPLATE.format(stage=stage, prefix=prefix, table=table, file_format=file_format, header=header)

    logger.info("COPY INTO prepared: %s", sql)

    conn = _get_conn("snowflake", conn_cfg)
    try:
        cur = conn.cursor()
        # ensure current schema
        parts = table.split(".")
        db_part = None
        schema_part = None
        if len(parts) == 3:
            db_part, schema_part, _ = parts
        elif len(parts) == 2:
            schema_part, _ = parts
            db_part = conn_cfg.get("database")
        _use_schema(cur, db_part, schema_part)

        try:
            cur.execute(sql)
            rows = cur.fetchall()
            job_info = [
                {
                    "rows_unloaded": r[0],
                    "bytes_uncompressed": r[1],
                    "bytes_compressed": r[2],
                }
                for r in rows
            ]
            logger.info("COPY INTO succeeded for table %s", table)
            status = "success"
            error_msg = None
        except Exception as exec_err:
            logger.error("COPY INTO failed for table %s: %s", table, exec_err)
            status = "error"
            job_info = []
            error_msg = str(exec_err)
        finally:
            cur.close()
    finally:
        conn.close()

    result = {
        "status": status,
        "copy_command": sql,
        "unload_prefix": prefix,
        "snowflake_job_info": job_info,
    }
    if error_msg:
        result["error"] = error_msg
    return result


def _list_tables(conn, database: str | None, schema: str) -> List[str]:
    # Snowflake Python connector uses the %%s placeholder style.
    q = "SELECT table_name FROM information_schema.tables WHERE table_schema = %s"
    binds = [schema.upper()]
    if database:
        q += " AND table_catalog = %s"
        binds.append(database.upper())
    cur = conn.cursor()
    try:
        cur.execute(q, binds)
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()


def unload_schema(conn_cfg: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    schema = params.get("schema")
    stage = params.get("stage")
    if not schema or not stage:
        raise ValueError("'schema' and 'stage' are required unload parameters")

    file_format = params.get("file_format", "parquet")
    header = params.get("header", True)

    import time

    start_ts = time.time()

    conn = _get_conn("snowflake", conn_cfg)
    try:
        database = conn_cfg.get("database")

        # Ensure current database & schema so information_schema query works
        try:
            cur = conn.cursor()
            _use_schema(cur, database, schema)
        finally:
            cur.close()

        tables = _list_tables(conn, database, schema)
        results = []
        total_rows = total_uncomp = total_comp = 0
        for tbl in tables:
            full_name = f"{schema}.{tbl}" if not database else f"{database}.{schema}.{tbl}"
            res = unload_table(
                conn_cfg,
                {
                    "table": full_name,
                    "stage": stage,
                    "file_format": file_format,
                    "header": header,
                },
            )
            results.append({"table": full_name, **res})
            if res.get("status") == "success":
                for jf in res.get("snowflake_job_info", []):
                    total_rows += jf["rows_unloaded"]
                    total_uncomp += jf["bytes_uncompressed"]
                    total_comp += jf["bytes_compressed"]
    finally:
        conn.close()

    overall_status = "success" if all(r.get("status") == "success" for r in results) else "error"

    duration_s = round(time.time() - start_ts, 2)
    transfer_rate = None
    if duration_s > 0 and total_comp > 0:
        transfer_rate = round((total_comp / 1_048_576) / duration_s, 2)  # MB/s

    summary = {
        "status": overall_status,
        "schema": schema,
        "table_count": len(results),
        "total_rows": total_rows,
        "total_uncompressed_mb": round(total_uncomp / 1_048_576, 2),
        "total_compressed_mb": round(total_comp / 1_048_576, 2),
        "duration_s": duration_s,
        "transfer_rate_mb_s": transfer_rate,
        "details": results,
    }
    return summary 