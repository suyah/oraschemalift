"""Snowflake DDL / privilege extractor.

Writes SQL and CSV artefacts under workspace/userdata/snowflake/<ts>/.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Dict, Any, List

from app.services.db.connection_factory import get_connection as _get_conn

from app.utils.logger import setup_logger
from app.utils.file_utils import write_file_content, write_csv
from app.utils.path_utils import workspace_path
from app.services.sql_conversion.utils.directory_utils import get_timestamp
from app import config

__all__ = ["SnowflakeExtractor", "ExtractionResult"]


class ExtractionResult(Dict[str, Any]):
    """Thin alias to tag result dictionary."""


class SnowflakeExtractor:
    """Extract entire database DDL + grants from Snowflake."""

    def __init__(self):
        self.logger = setup_logger("SnowflakeExtractor")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract(self, conn_params: Dict[str, Any]) -> ExtractionResult:
        """Run extraction and return summary dict."""

        db_name = conn_params.get("database")
        if not db_name:
            raise ValueError("connection params must include 'database'.")

        run_dir = self._prepare_run_dir("snowflake")
        src_sql_dir = run_dir / "sql_files" / "source"
        grants_dir = run_dir / "grants"
        src_sql_dir.mkdir(parents=True, exist_ok=True)
        grants_dir.mkdir(parents=True, exist_ok=True)

        # ------------------------------------------------------------------
        # Connect via shared factory
        # ------------------------------------------------------------------
        conn_params["db_type"] = "snowflake"
        conn = _get_conn("snowflake", conn_params)
        cur = conn.cursor()

        try:
            # DATABASE DDL ---------------------------------------------------
            self.logger.info("Fetching database DDL …")
            cur.execute(f"SELECT GET_DDL('DATABASE', '{db_name}', TRUE);")
            ddl_rows = cur.fetchall()
            ddl_sql = "\n\n".join(row[0] for row in ddl_rows)
            write_file_content(src_sql_dir / "database.sql", ddl_sql)

            # GRANTS ---------------------------------------------------------
            self._dump_show(cur, "SHOW GRANTS ON ACCOUNT", grants_dir / "account_grants.csv")
            self._dump_show(cur, f"SHOW GRANTS ON DATABASE {db_name}", grants_dir / "db_grants.csv")

            # ------------------------------------------------------------------
            # Roles and their grants (aggregate into ONE file)
            # ------------------------------------------------------------------
            roles_headers, roles_rows = self._dump_show(cur, "SHOW ROLES", grants_dir / "roles.csv", return_rows=True)
            try:
                role_name_idx = [h.lower() for h in roles_headers].index("name")
            except ValueError:
                role_name_idx = 1  # Fallback to previous assumption

            all_role_grants: List[Any] = []
            role_grants_headers: List[str] | None = None

            for role in roles_rows:
                role_name = role[role_name_idx]
                self.logger.info('Running SHOW GRANTS TO ROLE "%s"', role_name)
                cur.execute(f'SHOW GRANTS TO ROLE "{role_name}"')
                role_rows = cur.fetchall()
                r_headers = [d[0] for d in cur.description]
                if role_grants_headers is None:
                    role_grants_headers = r_headers + ["role_name"]
                # append role name column to each row
                for r in role_rows:
                    all_role_grants.append(r + (role_name,))

            if role_grants_headers is not None:
                write_csv(grants_dir / "role_grants.csv", role_grants_headers, all_role_grants)

            # ------------------------------------------------------------------
            # Users (optional) and their grants (aggregate into ONE file)
            # ------------------------------------------------------------------
            users_headers, users_rows = self._dump_show(cur, "SHOW USERS", grants_dir / "users.csv", return_rows=True)
            try:
                user_name_idx = [h.lower() for h in users_headers].index("name")
            except ValueError:
                user_name_idx = 1  # Fallback

            all_user_grants: List[Any] = []
            user_grants_headers: List[str] | None = None

            for user in users_rows:
                user_name = user[user_name_idx]
                self.logger.info('Running SHOW GRANTS TO USER "%s"', user_name)
                cur.execute(f'SHOW GRANTS TO USER "{user_name}"')
                u_rows = cur.fetchall()
                u_headers = [d[0] for d in cur.description]
                if user_grants_headers is None:
                    user_grants_headers = u_headers + ["user_name"]
                for r in u_rows:
                    all_user_grants.append(r + (user_name,))

            if user_grants_headers is not None:
                write_csv(grants_dir / "user_grants.csv", user_grants_headers, all_user_grants)

        finally:
            cur.close()
            conn.close()

        return ExtractionResult(
            status="success",
            run_folder=str(run_dir.relative_to(workspace_path()))
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _prepare_run_dir(self, dialect: str) -> Path:
        ts = get_timestamp()
        return workspace_path("extracts", dialect, ts)

    def _dump_show(self, cursor, sql: str, dest: Path, *, return_rows: bool = False):
        self.logger.info("Running %s", sql)
        cursor.execute(sql)
        rows = cursor.fetchall()
        headers = [d[0] for d in cursor.description]
        write_csv(dest, headers, rows)
        if return_rows:
            # Return both headers and rows so that the caller can locate columns
            return headers, rows
        return None 