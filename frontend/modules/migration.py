import json
import streamlit as st

from utils import (
    sql_convert_api,
    execute_sql_script_api,
    test_db_connection_api,
    SUPPORTED_RDBMS,
    get_log_file_content,
    get_absolute_path,
    DB_CONNECTION_PAYLOAD_TEMPLATES,
    list_db_connections_api,
    delete_db_connection_api,
    save_db_connection_api,
)

__all__ = [
    "display_source_ddl_input_page",
    "display_sql_conversion_page",
    "display_db_connections_page",
    "display_execute_sql_page",
]

# ------------------------------------------------------------------
# Shared helpers
# ------------------------------------------------------------------

# Helpers

def clear_execute_prefill_state():
    st.session_state.execute_path_suggestion = None
    st.session_state.execute_original_run_timestamp = None
    st.session_state.execute_source_db_type_of_converted = None


def on_script_source_type_change():
    if st.session_state.get("exec_input_type_v2") != "Previously Converted Scripts":
        clear_execute_prefill_state()

# ------------------------------------------------------------------
# Individual page renderers (abridged versions to keep code light)
# ------------------------------------------------------------------

def display_source_ddl_input_page():
    st.header("Source DDL Input")
    st.info("Upload DDL scripts or (future) connect to source DB to extract them.")

    method = st.radio(
        "Input Method",
        ("Upload DDL Files", "Connect to Database (Coming Soon)"),
        key="ddl_input_method",
    )
    if method == "Upload DDL Files":
        files = st.file_uploader("Upload .sql / .ddl", accept_multiple_files=True, type=["sql", "ddl", "txt"])
        if files:
            st.success(f"{len(files)} file(s) uploaded.")


def display_sql_conversion_page():
    st.header("Execute SQL Conversion")

    source_type = st.selectbox("Source DB", SUPPORTED_RDBMS, key="conv_src")
    target_type = st.selectbox("Target DB", SUPPORTED_RDBMS, key="conv_tgt")
    target_version = st.text_input("Target DB Version (optional)", key="conv_ver")

    input_abs_path = st.text_input("Absolute path to source SQL directory", key="conv_input_path")
    if st.button("Run Conversion", key="run_conv_btn"):
        if not input_abs_path:
            st.warning("Please provide the input path")
            return
        with st.spinner("Converting…"):
            resp = sql_convert_api(
                source_type,
                target_type,
                target_version,
                "custom",
                {"absolute_path": input_abs_path},
            )
        st.json(resp)
        # auto-prefill execute page suggestion
        if resp.get("converted_output_dir_rel"):
            st.session_state.execute_path_suggestion = resp["converted_output_dir_rel"]
            st.session_state.current_page = "Validate SQL Scripts"


def display_db_connections_page():
    st.header("Configure Database Connections")

    # --- existing connections
    if "db_connections" not in st.session_state:
        st.session_state.db_connections = {
            k: v for k, v in list_db_connections_api().items()
        }

    conn_names = list(st.session_state.db_connections.keys())
    if conn_names:
        st.subheader("Saved Connections")
        sel = st.selectbox("Select", conn_names)
        st.json(st.session_state.db_connections[sel])
        if st.button("Delete", key="del_conn_btn"):
            sel_db_type = st.session_state.db_connections[sel].get("db_type", "generic")
            delete_db_connection_api(sel_db_type, sel)
            del st.session_state.db_connections[sel]
            st.rerun()
    else:
        st.info("No connections saved yet.")

    # --- add new connection
    st.subheader("Add / Test Connection")
    db_type = st.selectbox("DB Type", list(DB_CONNECTION_PAYLOAD_TEMPLATES.keys()))
    payload_template = DB_CONNECTION_PAYLOAD_TEMPLATES[db_type]
    payload_str = st.text_area(
        "Connection JSON", json.dumps(payload_template, indent=2), height=200
    )
    if st.button("Test", key="test_conn_btn"):
        try:
            payload = json.loads(payload_str)
        except json.JSONDecodeError:
            st.error("Invalid JSON")
            return
        with st.spinner("Testing…"):
            res = test_db_connection_api(payload)
        st.json(res)
        if res.get("status") == "success":
            conn_name = st.text_input("Save as", key="save_conn_name")
            if st.button("Save", key="save_conn_btn") and conn_name:
                save_db_connection_api(conn_name, db_type, payload)
                st.session_state.db_connections[conn_name] = payload
                st.success("Saved")
                st.rerun()


def display_execute_sql_page():
    st.header("Validate SQL Scripts")

    if "db_connections" not in st.session_state or not st.session_state.db_connections:
        st.warning("No DB connections configured. Please add one first.")
        return

    conn_name = st.selectbox("Choose connection", list(st.session_state.db_connections.keys()), key="exec_conn_sel")
    conn_payload = st.session_state.db_connections[conn_name]

    scripts_path = st.text_input(
        "Absolute path to directory containing SQL scripts", st.session_state.get("execute_path_suggestion", ""), key="exec_scripts_path"
    )
    if st.button("Execute", key="exec_run_btn"):
        if not scripts_path:
            st.warning("Please provide the path to scripts")
            return
        payload_cfg = {"absolute_path": scripts_path}
        with st.spinner("Executing…"):
            resp = execute_sql_script_api(conn_payload, "custom", payload_cfg)
        st.json(resp)
        if resp.get("execution_id"):
            logs = get_log_file_content(resp["execution_id"])
            for name, content in logs.items():
                with st.expander(f"Log: {name}"):
                    st.text_area(name, content, height=300) 