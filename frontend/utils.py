import streamlit as st
import requests
import os
import zipfile
import io
from pathlib import Path

# --- Configuration ---
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:5001/api/v1")
# This should be the absolute path to the 'workspace' directory on the server
# where the backend Flask app saves generated files.
# Example: "/Users/suyahuang/Work/learning/sqlconverter2025 cursor/workspace"
# If running Streamlit and Flask in different environments (e.g., Docker containers without shared volumes),
# this approach for file access will not work, and API endpoints to serve files would be needed.
WORKSPACE_BASE_PATH = os.getenv("WORKSPACE_BASE_PATH", str(Path(__file__).resolve().parent.parent / "workspace"))
SUPPORTED_RDBMS = ["snowflake", "oracle", "postgresql", "greenplum", "bigquery"]
LOGS_BASE_PATH = os.getenv("LOGS_BASE_PATH", str(Path(__file__).resolve().parent.parent / "logs"))


# --- API Call Functions ---

def api_post_request(endpoint, payload):
    """Helper function to make POST requests to the API."""
    try:
        response = requests.post(f"{API_BASE_URL}/{endpoint}", json=payload)
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        st.error(f"HTTP error occurred: {http_err} - Response: {response.text}")
        try:
            return response.json() # Try to return JSON error details if possible
        except ValueError:
            return {"error": response.text, "status_code": response.status_code}
    except requests.exceptions.RequestException as req_err:
        st.error(f"Request error occurred: {req_err}")
        return {"error": str(req_err)}
    except ValueError as json_err: # Handle cases where response is not JSON
        st.error(f"JSON decode error: {json_err} - Response: {response.text}")
        return {"error": "Failed to decode JSON response", "raw_response": response.text}

def api_get_request(endpoint, params=None):
    """Helper function to make GET requests to the API."""
    try:
        response = requests.get(f"{API_BASE_URL}/{endpoint}", params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        st.error(f"HTTP error occurred: {http_err} - Response: {response.text}")
        try:
            return response.json()
        except ValueError:
            return {"error": response.text, "status_code": response.status_code}
    except requests.exceptions.RequestException as req_err:
        st.error(f"Request error occurred: {req_err}")
        return {"error": str(req_err)}
    except ValueError as json_err:
        st.error(f"JSON decode error: {json_err} - Response: {response.text}")
        return {"error": "Failed to decode JSON response", "raw_response": response.text}

def api_delete_request(endpoint):
    """Helper function to make DELETE requests to the API."""
    try:
        response = requests.delete(f"{API_BASE_URL}/{endpoint}")
        response.raise_for_status()
        # DELETE requests might return 204 No Content, or a JSON body
        if response.status_code == 204:
            return {"status": "success", "message": "Delete successful, no content returned."}
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        st.error(f"HTTP error occurred: {http_err} - Response: {response.text}")
        try:
            return response.json()
        except ValueError:
            return {"error": response.text, "status_code": response.status_code}
    except requests.exceptions.RequestException as req_err:
        st.error(f"Request error occurred: {req_err}")
        return {"error": str(req_err)}
    except ValueError as json_err:
        st.error(f"JSON decode error: {json_err} - Response: {response.text}")
        return {"error": "Failed to decode JSON response", "raw_response": response.text}

def generate_test_data_api(source_type, table_count, oracle_version=None):
    """Calls the /llm/generate_schema endpoint."""
    payload = {"source_type": source_type, "table_count": int(table_count)}
    if oracle_version and source_type.lower() == "oracle":
        payload["oracle_version"] = oracle_version
    return api_post_request("llm/generate_schema", payload)

def test_db_connection_api(connection_payload):
    """Calls the /db/test-connection endpoint."""
    return api_post_request("db/test-connection", connection_payload)

def execute_sql_script_api(connection_payload, input_path_type, input_path_config):
    """Calls the /db/execute endpoint."""
    payload = {
        "connection": connection_payload,
        "input_path_type": input_path_type,
        "input_path_config": input_path_config
    }
    return api_post_request("db/execute", payload)

def sql_convert_api(source_type, target_type, target_version, input_path_type, input_path_config):
    """Calls the /sql/convert endpoint."""
    payload = {
        "source_type": source_type,
        "target_type": target_type,
        "target_version": target_version,
        "input_path_type": input_path_type,
        "input_path_config": input_path_config
    }
    return api_post_request("sql/convert", payload)

def llm_sql_convert_api(source_type, target_type, target_version, input_path_type, input_path_config, prompt_filename):
    """Calls the /sql/llm_convert endpoint."""
    payload = {
        "source_type": source_type,
        "target_type": target_type,
        "input_path_type": input_path_type,
        "input_path_config": input_path_config,
        "prompt_filename": prompt_filename
    }
    if target_version: # Only include if provided
        payload["target_version"] = target_version
    return api_post_request("sql/llm_convert", payload)

# --- NEW API UTILITY FUNCTIONS ---

def list_conversion_configs_api():
    """Calls GET /sql/conversion_configs endpoint."""
    return api_get_request("sql/conversion_configs")

def get_conversion_config_api(config_name):
    """Calls GET /sql/conversion_configs/{config_name} endpoint."""
    if not config_name:
        return {"error": "Config name is required."}
    return api_get_request(f"sql/conversion_configs/{config_name}")

def list_testdata_configs_api():
    """Calls GET /testdata/configs endpoint. (Assumed endpoint)"""
    # This endpoint is assumed; it might need to be created in the backend.
    # For now, returning a placeholder or an error if it's not ready.
    # return api_get_request("testdata/configs") 
    return {"message": "list_testdata_configs_api - Endpoint not yet implemented in backend or utils."}

def get_db_connection_configs_api():
    """Calls GET /db/connections endpoint. (Assumed endpoint for listing)"""
    # This assumes an endpoint exists for listing all stored DB connections.
    # If db_connections.json is managed client-side only for persistence, 
    # then this function might not call an API but rather interact with the local load/save.
    # For now, let's assume it's meant to be an API call if a centralized store existed.
    # Given current db_connections.json logic, this might be better handled directly in streamlit_app.py
    # For now, returning a placeholder.
    # return api_get_request("db/connections")
    return {"message": "get_db_connection_configs_api - Management is local via db_connections.json for now."}

def save_db_connection_config_api(name, connection_payload):
    """Calls POST /db/connections/{name} or similar endpoint. (Assumed endpoint for saving one)"""
    # This assumes an endpoint for saving/updating a single named connection.
    # Current logic uses local file save.
    # return api_post_request(f"db/connections/{name}", connection_payload) # Or just "db/connections" with name in payload
    return {"message": "save_db_connection_config_api - Management is local via db_connections.json for now."}

def delete_db_connection_config_api(config_name):
    """Calls DELETE /db/connections/{config_name} endpoint. (Assumed endpoint)"""
    # This assumes an endpoint for deleting a named connection.
    # Current logic uses local file save.
    # return api_delete_request(f"db/connections/{config_name}")
    return {"message": "delete_db_connection_config_api - Management is local via db_connections.json for now."}

def list_prompts_api(prompt_type):
    """Calls GET /prompts/{prompt_type} endpoint. (Assumed endpoint)"""
    # Example: prompt_type could be 'generation' or 'conversion'
    # This endpoint needs to exist on the backend.
    # return api_get_request(f"prompts/{prompt_type}")
    return {"message": f"list_prompts_api for {prompt_type} - Endpoint not yet implemented in backend or utils."}

def get_prompt_content_api(prompt_type, filename):
    """Calls GET /prompts/{prompt_type}/{filename} endpoint. (Assumed endpoint)"""
    # This endpoint needs to exist on the backend.
    # return api_get_request(f"prompts/{prompt_type}/{filename}")
    return {"message": f"get_prompt_content_api for {prompt_type}/{filename} - Endpoint not yet implemented."}

# --- File Utility Functions ---

def get_absolute_path(relative_path_str):
    """Converts a relative path from the API (assumed to be relative to WORKSPACE_BASE_PATH)
       to an absolute path."""
    # The relative paths from API often start with "workspace/"
    # We need to strip that if WORKSPACE_BASE_PATH already includes "workspace"
    if relative_path_str.startswith("workspace/"):
        path_suffix = relative_path_str[len("workspace/"):]
    else:
        path_suffix = relative_path_str
    
    # Ensure WORKSPACE_BASE_PATH is a Path object
    base = Path(WORKSPACE_BASE_PATH)
    abs_path = base / path_suffix
    return abs_path.resolve()


def create_zip_from_files(file_paths, generated_scripts_directory_rel):
    """
    Creates a ZIP archive in memory from a list of absolute file paths.
    Uses generated_scripts_directory_rel to create relative paths within the ZIP.
    
    Args:
        file_paths (list): List of absolute paths to files to be zipped.
        generated_scripts_directory_rel (str): The relative directory path (e.g., "workspace/testdata/snowflake/timestamp/sql_files/source")
                                               This is used to determine arcnames in the zip.
    Returns:
        io.BytesIO: A BytesIO object containing the ZIP data.
    """
    zip_buffer = io.BytesIO()
    
    # Normalize generated_scripts_directory_rel to ensure it's a Path object
    # and get its parent to correctly form arcnames.
    # The goal is that if files are in "..../source/01_file.sql", the zip should have "source/01_file.sql" or "01_file.sql"
    # depending on what feels more natural. Let's aim for including the immediate parent folder ("source" or "converted").

    # First, get the absolute path of the generated_scripts_directory
    abs_generated_scripts_dir = get_absolute_path(generated_scripts_directory_rel)

    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        for file_path_abs_str in file_paths:
            file_path_abs = Path(file_path_abs_str)
            # Arcname should be relative to the parent of abs_generated_scripts_dir if we want "source/file.sql"
            # Or relative to abs_generated_scripts_dir if we want "file.sql"
            # Let's try to make it relative to the parent of the script directory, to include the "source"/"converted" folder name.
            
            # Example:
            # file_path_abs = /path/to/workspace/testdata/snowflake/TS/sql_files/source/01.sql
            # abs_generated_scripts_dir = /path/to/workspace/testdata/snowflake/TS/sql_files/source
            # We want arcname to be like "source/01.sql" if zip is for "sql_files"
            # Or if zip is for the run_base_dir, then "sql_files/source/01.sql"

            # For simplicity, let's make arcname relative to the parent of the script directory
            # e.g. if generated_scripts_directory is '.../sql_files/source', files will be 'source/file.sql' in zip
            arcname_parent = abs_generated_scripts_dir.parent
            arcname = file_path_abs.relative_to(arcname_parent)
            zip_file.write(file_path_abs, arcname=arcname)
            
    zip_buffer.seek(0)
    return zip_buffer

def create_zip_from_directory(directory_path_abs_str):
    """
    Creates a ZIP archive in memory from all files in a given directory.
    Args:
        directory_path_abs_str (str): Absolute path to the directory to be zipped.
    Returns:
        io.BytesIO: A BytesIO object containing the ZIP data, or None if dir not found.
    """
    dir_path = Path(directory_path_abs_str)
    if not dir_path.is_dir():
        st.error(f"Log directory not found: {dir_path}")
        return None

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
        for file_path in dir_path.rglob('*'): # rglob gets all files in subdirectories too
            if file_path.is_file():
                arcname = file_path.relative_to(dir_path) # Path in zip relative to dir_path
                zipf.write(file_path, arcname)
    
    zip_buffer.seek(0)
    return zip_buffer

def get_file_content(relative_file_path):
    """Reads content of a file given its relative path from the API."""
    abs_path = get_absolute_path(relative_file_path)
    try:
        with open(abs_path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return f"Error: File not found at {abs_path}"
    except Exception as e:
        return f"Error reading file {abs_path}: {str(e)}"

def get_log_file_content(log_path_or_id):
    """
    Reads content of a log file.
    log_path_or_id could be an absolute path (if execution_id is one) 
    or a relative one that needs resolving against LOGS_BASE_PATH.
    For now, assumes execution_id might be a directory containing logs.
    We'll try to find common log files like 'db_service.log' or 'raw_output.json' if it's a directory.
    """
    if os.path.isabs(log_path_or_id):
        base_log_path = Path(log_path_or_id)
    else:
        base_log_path = Path(LOGS_BASE_PATH) / log_path_or_id

    log_contents = {}
    
    if base_log_path.is_dir():
        # Try to find specific log files within the directory
        # This is relevant for 'execution_id' from /db/execute
        raw_output_file = base_log_path / "raw_output.json"
        db_service_log_file = base_log_path / "db_service.log" # Assuming this naming convention

        if raw_output_file.exists():
            try:
                with open(raw_output_file, "r", encoding="utf-8") as f:
                    log_contents["execution_summary (raw_output.json)"] = f.read()
            except Exception as e:
                log_contents["execution_summary (raw_output.json)"] = f"Error reading log: {str(e)}"
        
        if db_service_log_file.exists():
            try:
                with open(db_service_log_file, "r", encoding="utf-8") as f:
                    log_contents["execution_detail (db_service.log)"] = f.read()
            except Exception as e:
                log_contents["execution_detail (db_service.log)"] = f"Error reading log: {str(e)}"
        
        if not log_contents: # If specific files not found, list dir or give message
            log_contents["info"] = f"Log directory found at {base_log_path}. Searched for raw_output.json, db_service.log."
            
    elif base_log_path.is_file(): # If a direct file path is given
         try:
            with open(base_log_path, "r", encoding="utf-8") as f:
                log_contents[base_log_path.name] = f.read()
         except Exception as e:
            log_contents[base_log_path.name] = f"Error reading log: {str(e)}"
    else:
        # For general app logs like app.log (not tied to a specific run_id from API response)
        # This part needs more thought on how to make it useful and specific.
        # For now, if it's not a dir or specific file, try reading 'app.log' as a general fallback.
        app_log_file = Path(LOGS_BASE_PATH) / "app.log"
        if app_log_file.exists():
            try:
                with open(app_log_file, "r", encoding="utf-8") as f:
                    log_contents["general_app_log (app.log)"] = f.read()
            except Exception as e:
                log_contents["general_app_log (app.log)"] = f"Error reading app.log: {str(e)}"
        else:
            log_contents["error"] = f"Log path {base_log_path} not found or not a recognized log type."
            
    return log_contents

# --- Payload Templates for DB Connections ---
DB_CONNECTION_PAYLOAD_TEMPLATES = {
    "oracle": {
        "db_type": "oracle",
        "user": "your_oracle_user",
        "password": "your_oracle_password",
        "dsn": "hostname:port/service_name",
        "wallet_dir": "(optional) my_wallet_dirname",
        "wallet_location": "(optional) /path/to/wallet or same as wallet_dir",
        "wallet_password": "(optional) wallet_password"
    },
    "snowflake": {
        "db_type": "snowflake",
        "user": "your_snowflake_user",
        "password": "your_snowflake_password",
        "account": "your_snowflake_account_identifier",
        "warehouse": "(optional) your_warehouse",
        "database": "(optional) your_database",
        "schema": "(optional) your_schema",
        "role": "(optional) your_role"
    },
    "postgresql": {
        "db_type": "postgresql",
        "user": "your_pg_user",
        "password": "your_pg_password",
        "host": "your_pg_host",
        "port": 5432,
        "database": "your_pg_database_name"
    },
    "greenplum": {
        "db_type": "greenplum",
        "user": "your_gp_user",
        "password": "your_gp_password",
        "host": "your_gp_host",
        "port": 5432,
        "database": "your_gp_database_name"
    },
    "bigquery": {
        "db_type": "bigquery",
        "project_id": "(optional) your_gcp_project_id"
    }
}

def list_db_connections_api():
    return api_get_request("db/connections")

def save_db_connection_api(name, db_type, payload):
    return api_post_request("db/connections", {"name": name, "db_type": db_type, "payload": payload})

def delete_db_connection_api(db_type, name):
    return api_delete_request(f"db/connections/{db_type}/{name}") 