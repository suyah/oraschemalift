import streamlit as st
import json # For displaying JSON payloads nicely
import os
from pathlib import Path
import yaml # Added for YAML parsing
from utils import (
    generate_test_data_api, 
    test_db_connection_api, 
    execute_sql_script_api,
    sql_convert_api,
    llm_sql_convert_api,
    create_zip_from_files,
    create_zip_from_directory,
    get_file_content,
    get_log_file_content,
    SUPPORTED_RDBMS,
    DB_CONNECTION_PAYLOAD_TEMPLATES,
    get_absolute_path,
    WORKSPACE_BASE_PATH,
    list_conversion_configs_api,
    get_conversion_config_api,
    list_testdata_configs_api,
    get_db_connection_configs_api,
    save_db_connection_config_api,
    delete_db_connection_config_api,
    list_prompts_api,
    get_prompt_content_api,
    API_BASE_URL
)
from dataclasses import dataclass
from typing import Callable, List, Dict
from modules.general import (
    display_home_page,
    display_generate_test_data_page,
    display_view_prompts_page,
    display_llm_sql_conversion_page,
    display_login_page,
)
from modules.migration import (
    display_source_ddl_input_page,
    display_sql_conversion_page,
    display_db_connections_page,
    display_execute_sql_page,
)

# Local JSON cache for saved DB connection profiles (created on demand by the UI)
DB_CONNECTIONS_FILE = Path(__file__).parent / "db_connections.json"

# --- Page definitions (by user role) ---
# New Page Lists based on User Roles / Functionality
migration_workbench_pages = [
    "Source DDL Input", 
    "Execute SQL Conversion",
    "Configure Database Connections",
    "Validate SQL Scripts"
]
engineering_workbench_pages = [
    "Generate Test Data",
    "LLM SQL Conversion",
    "View LLM Prompts"
]

# --- Helper functions for persisting connection profiles ---
def load_persistent_db_connections():
    if DB_CONNECTIONS_FILE.exists():
        try:
            with open(DB_CONNECTIONS_FILE, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            st.error(f"Error loading saved DB connections: {e}. Starting fresh.")
            return {}
    return {}

def save_persistent_db_connections(connections_dict):
    try:
        with open(DB_CONNECTIONS_FILE, "w") as f:
            json.dump(connections_dict, f, indent=2)
    except IOError as e:
        st.error(f"Error saving DB connections: {e}")

# Helper to clear cached suggestions on the Validate-SQL page
def clear_execute_prefill_state():
    st.session_state.execute_path_suggestion = None
    st.session_state.execute_original_run_timestamp = None
    st.session_state.execute_source_db_type_of_converted = None

def on_script_source_type_change():
    """Signal handler for the script-source selectbox: clears cached hints
    when the user switches away from the "Previously Converted Scripts" option."""
    # This function will be called when the exec_input_type_v2 selectbox changes.
    # We check the CURRENT value of the selectbox (via its key in session_state)
    # and if it's NOT "Previously Converted Scripts", we clear the pre-fill suggestions.
    if st.session_state.get("exec_input_type_v2") != "Previously Converted Scripts":
        clear_execute_prefill_state()

# --- Page Configuration & Session State Initialization ---
st.set_page_config(layout="wide", page_title="OraSchemaLift - SQL Transformation Suite")

# Hide the (irrelevant) Streamlit "Deploy" button in local runs
hide_streamlit_style = """
            <style>
            /* Hide deploy button from toolbar if present */
            div[data-testid="stToolbar"] button[title*="Deploy"] {
                display: none !important;
            }
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True) 

if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'generated_data' not in st.session_state: # To store results from llm/generate_schema
    st.session_state.generated_data = None
if 'conversion_data' not in st.session_state: # To store results from sql/convert
    st.session_state.conversion_data = None
if 'llm_conversion_data' not in st.session_state: # To store results from sql/llm_convert
    st.session_state.llm_conversion_data = None
if 'db_connections' not in st.session_state: # To store saved DB connections
    st.session_state.db_connections = load_persistent_db_connections()
if 'current_page' not in st.session_state:
    st.session_state.current_page = "Login"

# For DB Connections page specifically
if 'last_tested_payload' not in st.session_state:
    st.session_state.last_tested_payload = None
if 'last_test_successful' not in st.session_state:
    st.session_state.last_test_successful = False
if 'test_results_for_saved_conn' not in st.session_state:
    st.session_state.test_results_for_saved_conn = {}

if 'execute_path_suggestion' not in st.session_state: # for execute page prefill
    st.session_state.execute_path_suggestion = None
if 'execute_original_run_timestamp' not in st.session_state:
    st.session_state.execute_original_run_timestamp = None
if 'execute_source_db_type_of_converted' not in st.session_state:
    st.session_state.execute_source_db_type_of_converted = None

if 'user_role' not in st.session_state: # For role simulation
    st.session_state.user_role = "Customer" # Default role

# Home Page definition imported from modules.general

# --- Page Navigation Logic ---
def set_page(page_name):
    st.session_state.current_page = page_name
    st.rerun()

@dataclass
class Page:
    title: str
    render: Callable[[], None]
    roles: List[str]
    category: str  # e.g. "Migration", "Engineering", "General"

# Registry of available pages – add/remove entries as needed
PAGES: List[Page] = [
    Page("Home", display_home_page, ["Customer", "Engineer"], "General"),
    Page("Source DDL Input", display_source_ddl_input_page, ["Customer", "Engineer"], "Migration"),
    Page("Execute SQL Conversion", display_sql_conversion_page, ["Customer", "Engineer"], "Migration"),
    Page("Configure Database Connections", display_db_connections_page, ["Customer", "Engineer"], "Migration"),
    Page("Validate SQL Scripts", display_execute_sql_page, ["Customer", "Engineer"], "Migration"),
    Page("Generate Test Data", display_generate_test_data_page, ["Engineer"], "Engineering"),
    Page("LLM SQL Conversion", display_llm_sql_conversion_page, ["Engineer"], "Engineering"),
    Page("View LLM Prompts", display_view_prompts_page, ["Engineer"], "Engineering"),
]

# Utility: map title -> Page for quick lookup
_PAGE_MAP: Dict[str, Page] = {p.title: p for p in PAGES}

# Sidebar / navigation helpers
def _render_sidebar(current_role: str):
    """Render sidebar navigation dynamically from the PAGES registry."""
    # Role switcher
    prev_role = st.session_state.user_role
    selected_role = st.sidebar.selectbox(
        "Choose Your Role:",
        ("Customer", "Engineer"),
        index=0 if prev_role == "Customer" else 1,
        key="role_selector_sidebar",
    )
    if selected_role != prev_role:
        st.session_state.user_role = selected_role
        st.rerun()

    # Group pages by category for the current role
    pages_by_cat: Dict[str, List[Page]] = {}
    for page in PAGES:
        if current_role in page.roles and page.category != "General":
            pages_by_cat.setdefault(page.category, []).append(page)

    # Home shortcut
    if st.sidebar.button("🏠 Home", key="nav_btn_home_main", type="primary" if st.session_state.current_page == "Home" else "secondary", use_container_width=True):
        st.session_state.current_page = "Home"
        st.rerun()

    # Render category sections
    CATEGORY_ICONS = {
        "Migration": "🛠️ Migration Tools",
        "Engineering": "🔬 Engineering Tools",
    }

    for category, pages in pages_by_cat.items():
        header = CATEGORY_ICONS.get(category, category)
        st.sidebar.markdown(f"**{header}**")
        for page in pages:
            btn_key = f"nav_btn_{page.title.replace(' ', '_').lower()}"
            btn_type = "primary" if st.session_state.current_page == page.title else "secondary"
            if st.sidebar.button(page.title, key=btn_key, type=btn_type, use_container_width=True):
                st.session_state.current_page = page.title
                st.rerun()

    # Logout
    if st.sidebar.button("Logout", key="logout_button_main_sidebar", use_container_width=True):
        st.session_state.logged_in = False
        st.session_state.current_page = 'Login'
        st.session_state.user_role = "Customer" 
        st.rerun()

# ------------------------------------------------------------------
# Main application dispatch
# ------------------------------------------------------------------

if not st.session_state.logged_in:
    display_login_page()
else:
    # Ensure a default page is set
    if 'current_page' not in st.session_state or st.session_state.current_page not in _PAGE_MAP:
        st.session_state.current_page = "Home"

    _render_sidebar(st.session_state.user_role)

    # Finally render the chosen page
    _PAGE_MAP[st.session_state.current_page].render() 