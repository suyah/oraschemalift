from fastapi import APIRouter, HTTPException, Body, Form, File, UploadFile
from fastapi.responses import JSONResponse
from typing import Dict, Any, List
from app.utils.timing import timed
import os
import zipfile
import shutil
from pathlib import Path

from app import config  # Global config

# Apply the custom sqlglot patch at application startup
from app.services.sql_conversion.utils.sqlglot_patch import apply_sqlglot_snowflake_patch
apply_sqlglot_snowflake_patch()

from ..services.llm_service import LLMService
from ..services.db.db_service import DBService
from ..services.sql_conversion import ConversionOrchestrator
from app.services.llm_service.utils.schema_file_generator import parse_and_write_schema_files
from app.utils.file_utils import make_relative_path
from app.utils.logger import setup_logger
from app.utils.path_utils import build_samples_path, resolve_converted_run
import oci
from oci.generative_ai_inference import GenerativeAiInferenceClient
from app.services.db.connection_store import (
    list_connections as _list_conn,
    save_connection as _save_conn,
    delete_connection as _del_conn,
)
from app.services.qa import run_roundtrip
from app.services.data_unload import run_unload

api_router = APIRouter(prefix='/api/v1')

# Initialize stateful services only (services that manage connections/state)
llm_service = LLMService()
db_service = DBService()

# Setup logger for API
logger = setup_logger('api_routes')

@api_router.post('/llm/generate_schema')
def generate_testdata(data: Dict[str, Any] = Body(...)):
    """Generate SQL test data files using LLM."""
    db_type = data.get('db_type')
    table_count = data.get('table_count', 10)
    oracle_version = data.get('oracle_version', '19c')
    
    if not db_type:
        raise HTTPException(status_code=400, detail='db_type is required')

    try:
        llm_response, actual_scripts_source_dir = llm_service.generate_schema(
            source_type=db_type, 
            table_count=table_count,
            oracle_version=oracle_version
        )
        
        # Parse LLM response and write organized SQL files
        generated_sql_files = parse_and_write_schema_files(
            schema_definition=llm_response, 
            output_dir=actual_scripts_source_dir, 
            source_type=db_type,
            logger=logger
        )
        
        project_root = Path(__file__).parent.parent.parent
        relative_file_paths = [make_relative_path(fp, project_root) for fp in generated_sql_files]

        relative_generated_scripts_dir = make_relative_path(actual_scripts_source_dir, project_root)
        # run_base_directory is .../<timestamp>/
        run_base_directory = make_relative_path(os.path.dirname(os.path.dirname(actual_scripts_source_dir)), project_root)

        return {
            'message': 'Test data generation initiated.',
            'source_sql_files': relative_file_paths,
            'generated_scripts_directory': relative_generated_scripts_dir,
            'run_base_directory': run_base_directory
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.post('/db/test-connection')
def test_connection(data: Dict[str, Any] = Body(...)):
    try:
        result = db_service.test_connection(data)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({
            'status': 'error',
            'message': str(e)
        }, status_code=400)

@api_router.post('/db/execute')
def execute_sql(data: Dict[str, Any] = Body(...)):
    try:
        if not data:
            return JSONResponse({
                'status': 'error',
                'message': 'No data provided'
            }, status_code=400)
            
        connection = data.get('connection')
        if not connection:
            return JSONResponse({
                'status': 'error',
                'message': 'Missing connection parameters'
            }, status_code=400)
            
        input_path_type = data.get('input_path_type')
        input_path_config = data.get('input_path_config')
        
        db_type_for_path_construction = None
        if input_path_type == 'samples' and input_path_config and input_path_config.get('db_type_for_path'):
            db_type_for_path_construction = input_path_config.get('db_type_for_path')
            logger.info(f"Using 'db_type_for_path': '{db_type_for_path_construction}' from input_path_config for samples path.")
        elif connection.get('db_type'):
            db_type_for_path_construction = connection.get('db_type')
            logger.info(f"Using 'db_type': '{db_type_for_path_construction}' from connection for path construction.")
        else:
            return JSONResponse({'error': 'Cannot determine db_type for path construction from connection or input_path_config.'}, status_code=400)

        if not all([db_type_for_path_construction, input_path_type, input_path_config]):
            missing = []
            if not db_type_for_path_construction: missing.append("db_type (for path construction, from connection.db_type or input_path_config.db_type_for_path for samples)")
            if not input_path_type: missing.append("input_path_type")
            if not input_path_config: missing.append("input_path_config")
            return JSONResponse({'error': f'Missing required fields: {", ".join(missing)}'}, status_code=400)

        workspace_base_dir = config['base_dirs']['workspace']

        # --------------------------------------------------
        # Unified path resolution across samples / extracts / uploads
        # --------------------------------------------------

        custom_val = (input_path_config.get('custom') or 'source').lower()
        converted_ts = input_path_config.get('converted_timestamp')

        try:
            if input_path_type == 'samples':
                run_timestamp = input_path_config.get('run_timestamp')
                if not run_timestamp:
                    return JSONResponse({'error': 'For samples, run_timestamp is required in input_path_config'}, status_code=400)

                base_path = build_samples_path(
                    db_type_for_path_construction,
                    run_timestamp=run_timestamp,
                    custom_subdir='source' if custom_val == 'source' else 'converted',
                )

                if custom_val == 'converted':
                    base_path = resolve_converted_run(Path(base_path), converted_ts)

            elif input_path_type == 'extracts':
                folder_name = (
                    input_path_config.get('user_folder')
                    or input_path_config.get('run_timestamp')
                )
                if not folder_name:
                    return JSONResponse({'error': 'For extracts, provide "user_folder" or "run_timestamp" in input_path_config'}, status_code=400)

                extracts_root = Path(workspace_base_dir) / config['workspace_sub_dirs']['extracts'] / db_type_for_path_construction / folder_name
                scripts_parent = config['workspace_sub_dirs'].get('scripts_parent', 'sql_files')

                if custom_val == 'source':
                    base_path = extracts_root / scripts_parent / 'source'
                else:
                    conv_base = extracts_root / scripts_parent / 'converted'
                    base_path = resolve_converted_run(conv_base, converted_ts)

            elif input_path_type == 'uploads':
                folder_name = input_path_config.get('project_name')
                if not folder_name:
                    return JSONResponse({'error': 'For uploads, provide "project_name" in input_path_config'}, status_code=400)

                uploads_root = Path(workspace_base_dir) / config['workspace_sub_dirs']['uploads'] / config['workspace_sub_dirs'].get('scripts_parent', 'sql_files') / db_type_for_path_construction / folder_name

                if custom_val == 'source':
                    base_path = uploads_root / 'source'
                else:
                    conv_base = uploads_root / 'converted'
                    base_path = resolve_converted_run(conv_base, converted_ts)

            else:
                return JSONResponse({'error': f'Invalid input_path_type: {input_path_type}'}, status_code=400)

            actual_input_path = str(base_path)

        except (ValueError, FileNotFoundError) as err:
            return JSONResponse({'error': str(err)}, status_code=400)

        if not actual_input_path or not os.path.exists(actual_input_path):
             return JSONResponse({'error': f'Constructed input path does not exist or is invalid: {actual_input_path}'}, status_code=404)
            
        # Execute scripts using the determined path
        result = db_service.execute_scripts(
            connection_params=connection,
            input_path=actual_input_path
        )
        if isinstance(result, tuple):  # Handle unexpected tuple
            return JSONResponse({
                'status': 'error',
                'message': 'Internal server error'
            }, status_code=500)
            
        return JSONResponse(result)
        
    except Exception as e:
        return JSONResponse({
            'status': 'error',
            'message': str(e)
        }, status_code=400)

@api_router.post('/sql/convert')
def convert_sql_endpoint(payload: Dict[str, Any] = Body(...)):
    try:
        if not payload:
            return JSONResponse({'error': 'No JSON data provided'}, status_code=400)
            
        db_type = payload.get('db_type')
        target_type = payload.get('target_type', 'oracle')
        target_version = payload.get('target_version', '19c')
        
        # Default ON for cleanup script generation
        generate_cleanup = payload.get('generate_cleanup', True)
        
        input_path_type = payload.get('input_path_type')
        input_path_config = payload.get('input_path_config')
        
        if not all([db_type, input_path_type, input_path_config]):
            return JSONResponse({'error': 'Missing required fields: db_type, input_path_type, input_path_config'}, status_code=400)

        actual_input_path = None
        workspace_base_dir = config['base_dirs']['workspace']
        original_run_timestamp_for_service = None

        if input_path_type == 'samples':
            run_timestamp = input_path_config.get('run_timestamp')
            original_run_timestamp_for_service = run_timestamp
            if not run_timestamp:
                return JSONResponse({'error': 'For samples, run_timestamp is required in input_path_config'}, status_code=400)
            
            testdata_subdir_name = config['workspace_sub_dirs']['samples']
            scripts_parent_folder_name = config['workspace_sub_dirs'].get('scripts_parent', 'sql_files')
            source_subfolder_name = config['workspace_sub_dirs'].get('scripts_source', 'source')
            
            try:
                actual_input_path = str(
                    build_samples_path(
                        db_type,
                        run_timestamp=run_timestamp,
                        custom_subdir=None,
                    )
                )
            except ValueError as ve:
                return JSONResponse({'error': str(ve)}, status_code=400)
        elif input_path_type == 'extracts':
            folder_name = (
                input_path_config.get('user_folder')
                or input_path_config.get('run_timestamp')
            )
            if not folder_name:
                return JSONResponse({'error': 'For extracts, provide "user_folder" or "run_timestamp" in input_path_config'}, status_code=400)

            userdata_subdir_name = config['workspace_sub_dirs']['extracts']
            scripts_parent = config['workspace_sub_dirs'].get('scripts_parent', 'sql_files')
            source_subdir = config['workspace_sub_dirs'].get('scripts_source', 'source')
            actual_input_path = os.path.join(workspace_base_dir, userdata_subdir_name, db_type, folder_name, scripts_parent, source_subdir)
        elif input_path_type == 'uploads':
            folder_name = input_path_config.get('project_name')
            if not folder_name:
                return JSONResponse({'error': 'For uploads, provide "project_name" in input_path_config'}, status_code=400)

            uploads_subdir_name = config['workspace_sub_dirs']['uploads']
            scripts_parent = config['workspace_sub_dirs'].get('scripts_parent', 'sql_files')
            source_subdir = config['workspace_sub_dirs'].get('scripts_source', 'source')
            actual_input_path = os.path.join(workspace_base_dir, uploads_subdir_name, scripts_parent, db_type, folder_name, source_subdir)
        else:
            return JSONResponse({'error': f'Invalid input_path_type: {input_path_type}'}, status_code=400)

        if not actual_input_path or not os.path.exists(actual_input_path):
             return JSONResponse({'error': f'Constructed input path does not exist or is invalid: {actual_input_path}'}, status_code=404)

        # Create a new orchestrator instance for each request
        orchestrator = ConversionOrchestrator(
            source_dialect=db_type, 
            target_dialect=target_type,
            generate_cleanup=generate_cleanup
        )
        result_dict = orchestrator.convert(source_dir=actual_input_path)
        return JSONResponse(result_dict)
        
    except ValueError as ve:
        return JSONResponse({'error': str(ve)}, status_code=400)
    except Exception as e:
        logger.error(f"An unhandled exception occurred in /sql/convert: {e}", exc_info=True)
        return JSONResponse({'error': 'An internal server error occurred.', 'details': str(e)}, status_code=500)

@api_router.post('/sql/llm_convert')
def llm_convert_sql_endpoint(payload: Dict[str, Any] = Body(...)):
    try:
        if not payload:
            return JSONResponse({'error': 'No JSON data provided'}, status_code=400)
        
        db_type = payload.get('db_type')
        target_type = payload.get('target_type', 'oracle')
        target_version = payload.get('target_version', '19c')
        model_id = payload.get('model_id', config['llm']['model_id'])
        input_path_type = payload.get('input_path_type')
        input_path_config = payload.get('input_path_config')
        prompt_filename = payload.get('prompt_filename')

        if not all([db_type, input_path_type, input_path_config, prompt_filename]):
            missing_fields = []
            if not db_type: missing_fields.append('db_type')
            if not input_path_type: missing_fields.append('input_path_type')
            if not input_path_config: missing_fields.append('input_path_config')
            if not prompt_filename: missing_fields.append('prompt_filename')
            return JSONResponse({'error': f'Missing required fields: {", ".join(missing_fields)}'}, status_code=400)

        actual_input_path = None
        workspace_base_dir = config['base_dirs']['workspace']
        original_run_timestamp_for_service = None

        # Path resolution logic (adapted from /sql/convert)
        if input_path_type == 'samples':
            run_timestamp = input_path_config.get('run_timestamp')
            original_run_timestamp_for_service = run_timestamp
            if not run_timestamp:
                return JSONResponse({'error': 'For samples, run_timestamp is required in input_path_config'}, status_code=400)
            
            testdata_subdir_name = config['workspace_sub_dirs']['samples']
            scripts_parent_folder_name = config['workspace_sub_dirs'].get('scripts_parent', 'sql_files')
            source_subfolder_name = config['workspace_sub_dirs'].get('scripts_source', 'source')
            actual_input_path = os.path.join(workspace_base_dir, testdata_subdir_name, 
                                            db_type, run_timestamp, 
                                            scripts_parent_folder_name, source_subfolder_name)
        elif input_path_type == 'extracts':
            folder_name = (
                input_path_config.get('user_folder')
                or input_path_config.get('run_timestamp')
            )
            if not folder_name:
                return JSONResponse({'error': 'For extracts, provide "user_folder" or "run_timestamp" in input_path_config'}, status_code=400)

            userdata_subdir_name = config['workspace_sub_dirs']['extracts']
            scripts_parent = config['workspace_sub_dirs'].get('scripts_parent', 'sql_files')
            source_subdir = config['workspace_sub_dirs'].get('scripts_source', 'source')
            actual_input_path = os.path.join(workspace_base_dir, userdata_subdir_name, db_type, folder_name, scripts_parent, source_subdir)
        elif input_path_type == 'uploads':
            folder_name = input_path_config.get('project_name')
            if not folder_name:
                return JSONResponse({'error': 'For uploads, provide "project_name" in input_path_config'}, status_code=400)
            
            uploads_subdir_name = config['workspace_sub_dirs']['uploads']
            scripts_parent = config['workspace_sub_dirs'].get('scripts_parent', 'sql_files')
            source_subdir = config['workspace_sub_dirs'].get('scripts_source', 'source')
            actual_input_path = os.path.join(workspace_base_dir, uploads_subdir_name, scripts_parent, db_type, folder_name, source_subdir)
        else:
            return JSONResponse({'error': f'Invalid input_path_type: {input_path_type}'}, status_code=400)

        if not actual_input_path or not os.path.exists(actual_input_path):
             return JSONResponse({'error': f'Constructed input path does not exist or is invalid: {actual_input_path}'}, status_code=404)

        # Call the LLM service method
        result = llm_service.convert_sql_with_prompt(
            input_path=actual_input_path,
            source_type=db_type,
            target_type=target_type,
            target_version=target_version,
            prompt_filename=prompt_filename,
            original_run_timestamp=original_run_timestamp_for_service
        )
        
        return JSONResponse(result)

    except ValueError as ve:
        return JSONResponse({'error': str(ve)}, status_code=400)
    except Exception as e:
        logger.error(f"Unexpected error in /sql/llm_convert endpoint: {str(e)}", exc_info=True)
        return JSONResponse({'error': f'An unexpected error occurred: {str(e)}'}, status_code=500)


@api_router.get('/')
def root():
    """Root endpoint of the API.

    Returns a simple JSON message indicating the API is running.
    {
        "message": "API is running"
    }
    """
    return JSONResponse({"message": "API is running"})

# ----------------------- DB Connection Store -----------------------

@api_router.get('/db/connections')
def list_db_connections():
    """List all saved DB connection configs."""
    return JSONResponse(_list_conn())

@api_router.post('/db/connections')
def save_db_connection(data: Dict[str, Any] = Body(...)):
    name = data.get('name')
    payload = data.get('payload')
    db_type = data.get('db_type')
    if not all([name, payload, db_type]):
        return JSONResponse({'error': 'name, db_type and payload required'}, status_code=400)
    try:
        _save_conn(name, db_type, payload)
        return JSONResponse({'status': 'success'})
    except ValueError as ve:
        return JSONResponse({'error': str(ve)}, status_code=400)

@api_router.delete('/db/connections/{db_type}/{name}')
def delete_db_connection(db_type, name):
    if _del_conn(name, db_type):
        return JSONResponse({'status': 'deleted'})
    return JSONResponse({'error': 'not found'}, status_code=404)

@api_router.post('/qa/roundtrip')
def qa_roundtrip(payload: Dict[str, Any] = Body(...)):
    """Light-weight round-trip (source→convert→target) validation.

    Expected JSON schema::

        {
          "source_conn": { ... },
          "target_conn": { ... },
          "mode": "llm" | "directory",
          "mode_payload": { ... }
        }
    """
    try:
        source_conn = payload.get("source_conn")
        target_conn = payload.get("target_conn")
        mode = payload.get("mode")
        mode_payload = payload.get("mode_payload", {})

        # Basic validation for legacy payload structure
        if not all([source_conn, target_conn, mode]):
            return JSONResponse(
                {"error": "source_conn, target_conn and mode are required"},
                status_code=400,
            )

        try:
            result = run_roundtrip(
                source_conn=source_conn,
                target_conn=target_conn,
                mode=mode,
                mode_payload=mode_payload,
                generate_cleanup=payload.get("generate_cleanup", True),
            )
        except Exception as exc:
            logger.error("roundtrip failed", exc_info=True)
            return JSONResponse({"error": str(exc)}, status_code=500)

        status_code = 200 if result.get("status") == "success" else 400
        return JSONResponse(result, status_code=status_code)
    except KeyError as ke:
        return JSONResponse({"error": f"Missing field: {ke}"}, status_code=400)
    except ValueError as ve:
        return JSONResponse({"error": str(ve)}, status_code=400)

# ------------------- Extraction -------------------

@api_router.post('/db/extract')
def extract_db(payload: Dict[str, Any] = Body(...)):
    """
    Extracts DDL and other metadata from a source database.
    Payload should contain 'db_type' and 'connection' details.
    """
    db_type = payload.get('db_type')
    connection_details = payload.get('connection')
    generate_cleanup = payload.get('generate_cleanup', True)

    if not db_type or not connection_details:
        return JSONResponse({'error': "Missing 'db_type' or 'connection' in payload"}, status_code=400)
    
    # Dynamically select the extractor based on db_type
    if db_type == "snowflake":
        from app.services.extractors import SnowflakeExtractor
        extractor = SnowflakeExtractor()
    else:
        return JSONResponse({"error": f"Unsupported db_type '{db_type}'"}, status_code=400)

    result = extractor.extract(connection_details)
    result["db_type"] = db_type
    return JSONResponse(result)

@api_router.post('/uploads/sql_files')
async def upload_sql_files(
    db_type: str = Form(...),
    project_name: str = Form(...),
    files: List[UploadFile] = File(...)
):
    """Upload one or more SQL (or ZIP of SQL) files into workspace/uploads/sql_files."""

    uploads_root = config["workspace_sub_dirs"].get("uploads", "uploads")
    workspace_root = config["base_dirs"]["workspace"]
    scripts_parent = config["workspace_sub_dirs"].get("scripts_parent", "sql_files")
    source_subdir = config["workspace_sub_dirs"].get("scripts_source", "source")

    dest_dir = os.path.join(workspace_root, uploads_root, scripts_parent, db_type, project_name, source_subdir)
    os.makedirs(dest_dir, exist_ok=True)

    saved_files = []
    for uf in files:
        if uf.filename.lower().endswith(".zip"):
            # Unzip
            with zipfile.ZipFile(await uf.read()) as zf:
                zf.extractall(dest_dir)
            saved_files.append(f"unzipped:{uf.filename}")
        else:
            file_path = os.path.join(dest_dir, uf.filename)
            with open(file_path, "wb") as out:
                shutil.copyfileobj(uf.file, out)
            saved_files.append(uf.filename)

    return JSONResponse({
        "status": "success",
        "upload_path": os.path.relpath(dest_dir, workspace_root),
        "saved": saved_files
    })

# ------------------------------------------------------------------
# Wallet upload (Oracle Autonomous etc.)
# ------------------------------------------------------------------

@api_router.post('/uploads/wallet')
async def upload_wallet(
    db_type: str = Form(...),
    connection_name: str = Form(...),
    wallet_zip: UploadFile = File(...)
):
    """Upload a ZIP archive containing DB connection wallet files."""
    uploads_root = config["workspace_sub_dirs"].get("uploads", "uploads")
    workspace_root = config["base_dirs"]["workspace"]

    # Wallets are stored under workspace/uploads/wallets/<db_type>/<connection_name>
    dest_dir = os.path.join(workspace_root, uploads_root, "wallets", db_type, connection_name)
    os.makedirs(dest_dir, exist_ok=True)

    zip_path = os.path.join(dest_dir, wallet_zip.filename)
    with open(zip_path, "wb") as out:
        shutil.copyfileobj(wallet_zip.file, out)

    # Unzip into same folder
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(dest_dir)
    except zipfile.BadZipFile:
        return JSONResponse({"error": "Uploaded file is not a valid ZIP"}, status_code=400)

    return JSONResponse({
        "status": "success",
        "connection_folder": os.path.relpath(dest_dir, workspace_root),
        "unzipped_files": zf.namelist()
    })

# Helper: resolve connection either by inline dict or by saved name
def _resolve_connection(payload: Dict[str, Any]):
    """Return a connection dict from payload which may contain either
    `connection` directly or (`connection_name` & `db_type`)."""
    if payload.get("connection"):
        if not payload.get("db_type"):
            raise ValueError("'db_type' is required when passing connection inline.")
        return payload["connection"], payload["db_type"].lower()

    name = payload.get("connection_name")
    db_type = (payload.get("db_type") or "").lower()
    if not name:
        raise ValueError("Provide either 'connection' object or 'connection_name'.")
    saved = _list_conn()
    if name not in saved:
        raise ValueError(f"Connection '{name}' not found.")
    conn_payload = saved[name]
    # if caller omitted db_type we can try to infer
    return conn_payload, db_type or conn_payload.get("db_type")

# ------------------- Data unload -------------------

@api_router.post('/data/unload/table')
def unload_table_endpoint(payload: Dict[str, Any] = Body(...)):
    """Unload a single table to cloud storage (Snowflake COPY INTO…)."""
    try:
        conn_cfg, db_type = _resolve_connection(payload)
        from app.services.data_unload import run_unload  # local import to avoid cycles

        result = timed(run_unload, db_type=db_type, mode="table", connection=conn_cfg, params=payload)
        status_code = 200 if result.get("status") == "success" else 400
        return JSONResponse(result, status_code=status_code)
    except ValueError as ve:
        return JSONResponse({"error": str(ve)}, status_code=400)
    except Exception as exc:
        logger.error("table unload failed", exc_info=True)
        return JSONResponse({"error": str(exc)}, status_code=500)

@api_router.post('/data/unload/schema')
def unload_schema_endpoint(payload: Dict[str, Any] = Body(...)):
    """Unload every table in a schema."""
    try:
        conn_cfg, db_type = _resolve_connection(payload)
        from app.services.data_unload import run_unload

        result = timed(run_unload, db_type=db_type, mode="schema", connection=conn_cfg, params=payload)
        status_code = 200 if result.get("status") == "success" else 400
        return JSONResponse(result, status_code=status_code)
    except ValueError as ve:
        return JSONResponse({"error": str(ve)}, status_code=400)
    except Exception as exc:
        logger.error("schema unload failed", exc_info=True)
        return JSONResponse({"error": str(exc)}, status_code=500)

# ------------------- Data load -------------------

@api_router.post('/data/load/table')
def load_table_endpoint(payload: Dict[str, Any] = Body(...)):
    """Load a single table into Oracle with DBMS_CLOUD.COPY_DATA."""
    try:
        conn_cfg, db_type = _resolve_connection(payload)
        from app.services.data_load import run_load

        result = timed(run_load, db_type=db_type, mode="table", connection=conn_cfg, params=payload)
        status_code = 200 if result.get("status") == "success" else 400
        return JSONResponse(result, status_code=status_code)
    except ValueError as ve:
        return JSONResponse({"error": str(ve)}, status_code=400)
    except Exception as exc:
        logger.error("table load failed", exc_info=True)
        return JSONResponse({"error": str(exc)}, status_code=500)

@api_router.post('/data/load/schema')
def load_schema_endpoint(payload: Dict[str, Any] = Body(...)):
    """Load all tables listed in manifest or schema."""
    try:
        conn_cfg, db_type = _resolve_connection(payload)
        from app.services.data_load import run_load

        result = timed(run_load, db_type=db_type, mode="schema", connection=conn_cfg, params=payload)
        status_code = 200 if result.get("status") == "success" else 400
        return JSONResponse(result, status_code=status_code)
    except ValueError as ve:
        return JSONResponse({"error": str(ve)}, status_code=400)
    except Exception as exc:
        logger.error("schema load failed", exc_info=True)
        return JSONResponse({"error": str(exc)}, status_code=500)
