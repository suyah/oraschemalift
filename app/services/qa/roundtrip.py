from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, List, Tuple
import json

from app.services.db.db_service import DBService
from app.services.llm_service import LLMService
from app.services.sql_conversion import ConversionOrchestrator
from app.utils.logger import setup_logger
from app.services.llm_service.utils.schema_file_generator import parse_and_write_schema_files
from app.utils.workspace_resolver import resolve_source_sql_path
from app import config

logger = setup_logger("qa.roundtrip")

db_service = DBService()
llm_service = LLMService()

# ---------------------------------------------------------------------------
# Public helper
# ---------------------------------------------------------------------------

def run_roundtrip(
    *,
    source_conn: Dict[str, Any],
    target_conn: Dict[str, Any],
    mode: str,
    mode_payload: Dict[str, Any],
    generate_cleanup: bool = True,
) -> Dict[str, Any]:
    """Run a round-trip test and return a consolidated result dict.

    Parameters
    ----------
    source_conn / target_conn
        Connection payloads understood by :pyclass:`DBService`.
    mode
        "llm" – generate SQL via LLM (requires keys in *mode_payload*).
        "directory" – use already-present ``*.sql`` files under
        ``mode_payload['path']``.
    mode_payload
        Additional keys depending on the *mode*.
    generate_cleanup
        When *True* the conversion phase writes a ``00_cleanup.sql`` file so
        the round-trip can be re-run idempotently.  Defaults to *True*.
    """

    # ---------------------------------------------------------------------
    # 1. Build or locate source SQL directory
    # ---------------------------------------------------------------------
    if mode == "llm":
        src_type: str = mode_payload["source_type"]
        table_cnt: int = mode_payload.get("table_count", 10)

        logger.info("Generating schema via LLM …")
        llm_resp, source_dir = llm_service.generate_schema(src_type, table_cnt)

        # Parse LLM response and materialise *.sql files in source_dir
        parse_and_write_schema_files(
            schema_definition=llm_resp,
            output_dir=source_dir,
            source_type=src_type,
            logger=logger,
        )
    elif mode == "directory":
        # Caller specifies *logical* location, mirroring /db/execute
        input_path_type = mode_payload.get("input_path_type")
        input_path_config = mode_payload.get("input_path_config")

        if not all([input_path_type, input_path_config]):
            raise ValueError("directory mode requires input_path_type and input_path_config in mode_payload")

        source_dir = resolve_source_sql_path(
            input_path_type=input_path_type,
            input_path_config=input_path_config,
            db_type_for_path=source_conn.get("db_type"),
        )

        src_type = source_conn.get("db_type")
    else:
        raise ValueError("mode must be 'llm' or 'directory'.")

    # ---------------------------------------------------------------------
    # 2. Execute on source DB
    # ---------------------------------------------------------------------
    logger.info("Executing source SQL …")
    src_exec = db_service.execute_scripts(source_conn, str(source_dir))
    if src_exec.get("status") != "success":
        return {
            "status": "error",
            "phase": "source_execute",
            "details": src_exec,
        }

    # ---------------------------------------------------------------------
    # 3. Convert to target dialect
    # ---------------------------------------------------------------------
    tgt_type = target_conn.get("db_type")
    conv = ConversionOrchestrator(
        source_dialect=src_type,
        target_dialect=tgt_type,
        generate_cleanup=generate_cleanup,
    )
    conv_result = conv.convert(source_dir=str(source_dir))
    if conv_result.get("status") != "success":
        return {
            "status": "error",
            "phase": "conversion",
            "details": conv_result,
        }
    converted_dir = conv_result["output_dir"]

    # ---------------------------------------------------------------------
    # 4. Execute on target DB
    # ---------------------------------------------------------------------
    logger.info("Executing converted SQL on target …")
    tgt_exec = db_service.execute_scripts(target_conn, converted_dir)

    # ---------------------------------------------------------------------
    # 5. Persist full artefact + prepare human-friendly summary
    # ---------------------------------------------------------------------

    full_result: Dict[str, Any] = {
        "status": "success" if tgt_exec.get("status") == "success" else "partial",
        "source_execute": src_exec,
        "conversion": conv_result,
        "target_execute": tgt_exec,
    }

    # Persist the noisy output next to the converted SQL so users can inspect it.
    output_json_path = Path(converted_dir) / "output.json"
    output_json_path.write_text(json.dumps(full_result, indent=2))

    # ---------------- summary table ----------------
    def _collect_rows(phase_key: str, label: str) -> List[Tuple[str, str, str]]:
        phase = full_result.get(phase_key, {})
        entries = phase.get("results", phase.get("file_results", []))
        rows: List[Tuple[str, str, str]] = []
        for entry in entries:
            status = entry.get("status", "-")
            fn = entry.get("file") or entry.get("file_name")
            short = Path(fn).name if fn else "-"
            rows.append((label, short, status))
        return rows

    rows: List[Tuple[str, str, str]] = []
    if mode == "llm":
        # representation for schema generation step
        rows.append(("llm-gen", "schema", "success"))

    rows += _collect_rows("source_execute", "source")
    rows += _collect_rows("conversion", "conversion")
    rows += _collect_rows("target_execute", "target")

    md_lines = [
        "| Phase       | File               | Status  |",
        "|-------------|--------------------|---------|",
    ]
    for phase, file, status in rows:
        md_lines.append(f"| {phase:<11} | {file:<18} | {status:<7} |")
    summary_md = "\n".join(md_lines)

    # ---------------- log locations -------------
    workspace_root = Path(config.get('base_dirs', {}).get('workspace', '.')).resolve()

    def _rel(p: str | Path | None) -> str:
        if not p:
            return "-"
        try:
            return str(Path(p).resolve().relative_to(workspace_root))
        except Exception:
            return str(p)

    log_locations: List[str] = [
        f"source_execution: {_rel(src_exec.get('execution_id'))}",
        f"conversion_output_dir: {_rel(conv_result.get('output_dir'))}",
        f"target_execution: {_rel(tgt_exec.get('execution_id'))}",
        f"roundtrip_output_json: {_rel(output_json_path)}",
    ]

    # Persist compact summary for quick troubleshooting
    summary_txt_path = Path(converted_dir) / "summary.txt"
    summary_txt_path.write_text(summary_md + "\n\n" + "\n".join(log_locations))

    summary_rows = [
        {"phase": phase, "file": file, "status": status}
        for phase, file, status in rows
    ]

    compact_resp: Dict[str, Any] = {
        "status": full_result["status"],
        "summary": summary_rows,
        "logs": log_locations,
    }

    return compact_resp 