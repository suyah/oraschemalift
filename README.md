# OraSchemaLift

OraSchemaLift helps teams migrate non-Oracle schemas to Oracle databases with minimal manual effort.  
Phase 1 automates Snowflake-to-Oracle table DDL conversion and provides a test-driven feedback loop. Future phases will add data movement and support for additional source dialects such as Cloud database service like BigQuery and others.
Phase 2 introduces a performant bulkload **data-movement pipeline**: Snowflake `COPY INTO` helpers that unload tables (or whole schemas) to object-store stages, ready for downstream loading into Oracle.

---

## Project Description

OraSchemaLift streamlines both metadata (schema objects) and, in upcoming releases, data migration from other database platforms to Oracle. The current workflow:

1. Parse Snowflake DDL into an AST using `sqlglot`.
2. Apply **configuration-driven rewrite rules** (datatype mapping, clause cleanup) to that AST, then regenerate Oracle-compatible SQL with `sqlglot`.
3. Optionally use an LLM to build test schemas and auto-generate edge-case SQL for testing.
4. Run a "round-trip" QA cycle: execute the generated SQL on the source database for LLM generated test SQL, convert it, then execute the result on the target database to validate the translated syntax.

---

## Key Features

- **Configuration-Driven Framework**: The conversion engine is a plug-in framework driven entirely by JSON configuration. This allows for user-defined mappings (e.g., data types) and validation rules (e.g., clause removal), enabling new dialects to be supported with minimum effort on changing core Python code.
- **Round-Trip QA Validation** – A built-in workflow that uses an LLM to generate test SQL, validates it against the source DB, converts it, and runs it against the target DB to confirm the new syntax is valid.
- **Modern RESTful API & Streamlit UI** – FastAPI provides programmatic access while a companion Streamlit front-end offers an interactive user friendly console for conversions, testing and log viewing.
- **Centralised Logging** – Opinionated filter keeps `logs/app.log` free of framework noise while still streaming full output to the console.

---

## Roadmap

| Stage        | Status      | Planned Features                                                                                    |
|--------------|-------------|-----------------------------------------------------------------------------------------------------|
| Phase 1      | Completed   | Snowflake -> Oracle Table DDL conversion; LLM-driven test SQL and QA loop                           |
| Phase 2      | In progress | Snowflake -> Oracle Data-movement pipeline (Snowflake → Cloud Object Store → Oracle DB)             |
| Phase 3      | Planned     | Additional source dialects (BigQuery etc.)                                                          |

---

## Tech Stack

- **Python** – Core language  
- **FastAPI** – REST layer  
- **sqlglot** – SQL parser and transpiler (MIT-licensed)  
- **Configurable LLM driver** – Generates edge-case SQL for QA testing
- **Streamlit** – Optional graphical interface for hands-on exploration and demo purposes

---

## Quick Start

```bash
git clone https://github.com/your-org/oraschemalift.git
cd oraschemalift
pip install -r requirements.txt

# Launch API (default port 8000)
uvicorn app:app --reload --host 0.0.0.0 --port 5001
```

## Getting Started

### Prerequisites

-   Python 3.9+
-   Access to an LLM provider (optional)
-   Access to source and target databases for testing (optional)

### Installation

1.  **Clone the repository:**
    ```bash
    git clone <your-repository-url>
    cd OraSchemaLift
    ```

2.  **Create and activate the Python virtual environment:**
    The project is configured to work with a specific virtual environment.
    ```bash
    python3 -m venv .OraSchemaLift
    source .OraSchemaLift/bin/activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure Environment Variables:**
    **1 LLM / OCI Credentials (environment variables)**
    ```bash
    export OCI_COMPARTMENT_ID="ocid1.compartment.oc1..aaaa..."
    # Optional overrides (otherwise taken from settings.yaml defaults):
    export OCI_MODEL_ID="cohere.command-a-03-2025"
    export OCI_SERVICE_ENDPOINT="https://inference.generativeai.us-chicago-1.oci.oraclecloud.com"
    ```

    **2 Database Credentials (request payload)**
    Runtime connection details (`user`, `password`, `account`, etc.) are supplied **in the JSON body** of each `/db/*` or `/qa/roundtrip`