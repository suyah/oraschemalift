import streamlit as st
from utils import SUPPORTED_RDBMS, list_db_connections_api

__all__ = ["display_home_page", "display_generate_test_data_page", "display_view_prompts_page", "display_llm_sql_conversion_page", "display_login_page"]


def display_home_page():
    st.title("Welcome to OraSchemaLift!")
    st.markdown(
        """
        OraSchemaLift is a comprehensive suite for SQL schema transformation and validation. 
        It assists in migrating database schemas between different RDBMS by leveraging both 
        rule-based conversions and advanced AI-assisted techniques for complex scenarios.

        **Key Features:**

        *   **Rule-Based SQL Conversion:** Convert SQL scripts using predefined and customizable mapping rules.
        *   **Database Connectivity:** Configure and test connections to various databases.
        *   **SQL Script Execution:** Validate converted scripts by executing them directly against target databases.

        Use the sidebar to navigate through the different modules of the application.
        """
    )
    if st.session_state.user_role == "Customer":
        st.info("You are currently in the Customer view. Explore the Migration Workbench tools.")
    elif st.session_state.user_role == "Engineer":
        st.info("You are currently in the Engineer view. You have access to Migration Workbench and Engineering Workbench tools.")
        st.markdown(
            """
            **Engineering Tools Include:**
            *   Test Data Generation
            *   LLM-Assisted SQL Conversion (Experimental)
            *   LLM Prompt Viewer
            """
        )

# ------------------------------------------------------------------
# Engineering helper pages that are generic (not migration-specific)
# ------------------------------------------------------------------
from utils import (
    generate_test_data_api,
    list_prompts_api,
    get_prompt_content_api,
    llm_sql_convert_api,
    get_absolute_path,
)

def display_generate_test_data_page():
    st.header("Generate Test Data")

    source_type = st.selectbox("Select Source Database Type", SUPPORTED_RDBMS, key="gen_source_type")
    table_count = st.number_input(
        "Number of Tables to Generate", min_value=1, max_value=50, value=10, key="gen_table_count"
    )
    oracle_version_ui = None
    if source_type.lower() == "oracle":
        oracle_version_ui = st.text_input(
            "Oracle Version (e.g., 19c, 23ai)", value="19c", key="gen_oracle_version"
        )

    st.info(
        "💡 AI data generation can take some time. The UI will show a spinner while processing. Please wait."
    )

    if st.button("Generate Test Scripts", key="generate_scripts_button_main"):
        with st.spinner("Generating test data via API…"):
            api_response = generate_test_data_api(source_type, table_count, oracle_version_ui)
            st.session_state.generated_data = api_response

    if st.session_state.generated_data:
        response_data = st.session_state.generated_data
        st.subheader("Generation API Response:")
        if response_data.get("error"):
            st.error(f"Error: {response_data.get('error')}")
            if response_data.get("raw_response"):
                st.text_area("Raw Error Response", response_data["raw_response"], height=100)
        else:
            st.success(response_data.get("message", "Successfully initiated generation."))
            st.json(response_data)


def display_view_prompts_page():
    st.header("View LLM Prompts")
    st.info("Prompts management is a placeholder and will fetch data from the backend once implemented.")

    prompt_type = st.selectbox("Prompt Type", ("generation", "conversion"), key="prompt_type_sel")

    if st.button("List Prompts", key="list_prompts_btn"):
        with st.spinner("Fetching prompt list…"):
            response = list_prompts_api(prompt_type)
            st.json(response)

    filename = st.text_input("Prompt Filename")
    if st.button("Get Prompt Content", key="get_prompt_btn") and filename:
        with st.spinner("Fetching prompt content…"):
            response = get_prompt_content_api(prompt_type, filename)
            st.json(response)


def display_llm_sql_conversion_page():
    st.header("LLM SQL Conversion (Experimental)")
    st.info("This feature converts SQL using LLM prompts. Use cautiously – results may need review.")

    source_type = st.selectbox("Source DB", SUPPORTED_RDBMS, key="llm_conv_src")
    target_type = st.selectbox("Target DB", SUPPORTED_RDBMS, key="llm_conv_tgt")
    target_version = st.text_input("Target DB Version (optional)", key="llm_conv_ver")

    input_path = st.text_input("Absolute path to source SQL dir", key="llm_conv_in")
    prompt_filename = st.text_input("Prompt filename", key="llm_conv_prompt")

    if st.button("Run LLM Conversion", key="llm_conv_btn"):
        if not input_path or not prompt_filename:
            st.warning("Input path and prompt filename are required.")
        else:
            input_cfg = {"absolute_path": input_path}
            with st.spinner("Calling backend…"):
                resp = llm_sql_convert_api(
                    source_type,
                    target_type,
                    target_version,
                    "custom",
                    input_cfg,
                    prompt_filename,
                )
            st.json(resp)

def display_login_page():
    st.header("Login")
    username = st.text_input("Username", key="login_user")
    password = st.text_input("Password", type="password", key="login_pass")
    if st.button("Login", key="login_button"):
        if username == "admin" and password == "password":
            st.session_state.logged_in = True
            st.session_state.user_role = "Engineer"
            st.session_state.current_page = "Home"
            st.session_state.db_connections = list_db_connections_api()
            st.rerun()
        elif username == "user" and password == "user":
            st.session_state.logged_in = True
            st.session_state.user_role = "Customer"
            st.session_state.current_page = "Home"
            st.session_state.db_connections = list_db_connections_api()
            st.rerun()
        else:
            st.error("Invalid username or password") 