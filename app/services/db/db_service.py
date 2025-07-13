from typing import Dict, List, Optional
import snowflake.connector
import oracledb
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from google.cloud import bigquery
from app import config
from app.utils.logger import setup_logger
import os
import json
import re # Added for parsing database name
from app.utils.file_utils import find_sql_files, read_file_content, write_file_content
from app.services.db.connection_factory import get_connection as _get_conn

class DBService:
    def __init__(self):
        self.config = config
        self.connection = None
        self.db_type = None
        self.logger = setup_logger('db_service')
    
    def test_connection(self, connection_params: Dict) -> Dict:
        """Test database connection and get system information"""
        try:
            self.db_type = connection_params.get('db_type', '').lower()
            conn = self._get_connection(connection_params)
            
            results = {}
            with conn.cursor() as cursor:
                # Load and execute test queries from file
                test_sql_file = os.path.join(
                    config['base_dirs']['app'], 'config/rdbms',
                    f'{self.db_type}/{self.db_type}.sql'
                )
                content = read_file_content(test_sql_file)
                if content:
                    test_queries = [q.strip() for q in content.split(';') if q.strip()]
                else:
                    test_queries = []
                    
                for raw_q in test_queries:
                    if not raw_q.strip() or raw_q.strip().startswith('--'):
                        continue  # skip empty/comment lines
                    query = raw_q.rstrip(';')  # strip trailing semicolon for parsing
                    if not query:
                        continue
                    if query.upper().startswith('SELECT'):
                        cursor.execute(query)
                        # Use column label from cursor.description which is reliable for all DBs
                        col_name = cursor.description[0][0].lower()
                        val = cursor.fetchone()[0]
                        results[col_name] = val
                    elif query.upper().startswith('SHOW') and self.db_type == 'snowflake':
                        cursor.execute(query)
                        hdrs = [d[0].lower() for d in cursor.description]
                        try:
                            name_idx = hdrs.index('name')
                        except ValueError:
                            name_idx = 0
                        key = f"available_{query.split()[1].lower()}"
                        results[key] = [row[name_idx] for row in cursor.fetchall()]
                    else:
                        # Fallback execute but ignore output
                        cursor.execute(query)

                # After executing all queries ensure JSON serialisable
                for k, v in list(results.items()):
                    def _to_serializable(x):
                        if hasattr(x, 'isoformat'):
                            return x.isoformat()
                        return x

                    if isinstance(v, (list, tuple)):
                        results[k] = [_to_serializable(item) for item in v]
                    else:
                        results[k] = _to_serializable(v)

            conn.close()

            return {
                "status": "success",
                "message": f"Successfully connected to {self.db_type}",
                "connection_info": results
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Connection failed: {str(e)}"
            }

    def execute_scripts(
        self,
        connection_params: Dict,
        input_path: str,
    ) -> Dict:
        """Execute SQL scripts from a directory

        Args:
            connection_params (Dict): Database connection parameters
            input_path (str): Path to directory containing SQL files

        Returns:
            Dict: Execution results
        """
        results = []
        failed_statements = []
        
        try:
            # Validate inputs first
            if not connection_params:
                return {
                    "status": "error",
                    "message": "Missing connection parameters"
                }
            
            if not input_path:
                return {
                    "status": "error",
                    "message": "Input path is required"
                }
                
            if not os.path.exists(input_path):
                return {
                    "status": "error",
                    "message": f"Input path does not exist: {input_path}"
                }
            
            # Find SQL files in the directory
            sql_files = find_sql_files(input_path)
            
            if not sql_files:
                return {
                    "status": "error",
                    "message": "No SQL files found in input path"
                }
            
            # Get database connection
            try:
                self.db_type = connection_params.get('db_type', '').lower()
                conn = self._get_connection(connection_params)
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Database connection failed: {str(e)}"
                }

            # Setup logger (no extra execution-specific handler needed here)
            self.logger = setup_logger('db_service')

            execution_dir = input_path  # Artefacts (execution_summary.json) will be stored alongside SQL files
            
            # Store original connection_params for potential reconnection
            original_connection_params = connection_params.copy()

            for i, sql_file in enumerate(sql_files):
                current_sql_content = "" # For logging/reporting in case of read error
                file_had_critical_reconnect_error = False
                attempt_reconnect_after_file0 = False # Flag to control reconnection logic

                try:
                    current_sql_content = read_file_content(sql_file)
                    
                    current_sql_content = '\n'.join([
                        line for line in current_sql_content.split('\n')
                        if not (line.strip().startswith('--') or line.strip().startswith('//'))
                    ]).strip()
                    
                    if not current_sql_content:
                        results.append({
                            "file": sql_file,
                            "statement": "EMPTY FILE (after comment removal)",
                            "status": "skipped"
                        })
                        continue
                    
                    # Check connection status differently for Snowflake
                    connection_is_closed = self._is_connection_closed(conn)
                    
                    if connection_is_closed:
                        self.logger.error(f"Connection found closed before executing {sql_file}.")
                        raise Exception("Database connection was found closed before executing a script.")

                    sql_execution_status = "success"
                    sql_execution_error_message = None

                    if self.db_type == 'snowflake':
                        try:
                            conn.execute_string(current_sql_content)
                            self.logger.info(f"Successfully executed SQL from {os.path.basename(sql_file)} for Snowflake.")
                        except Exception as e_snow:
                            sql_execution_status = "error"
                            sql_execution_error_message = str(e_snow)
                            self.logger.error(f"Error executing Snowflake SQL from {sql_file}: {e_snow}")
                    elif self.db_type == 'oracle': # Specifically handle Oracle for multi-statement
                        statements = self._split_sql_statements(current_sql_content)
                        if not statements:
                            self.logger.info(f"No executable statements found in {sql_file} after splitting.")
                            # No need to set status to error, it's just an empty/commented file effectively
                        else:
                            with conn.cursor() as cursor:
                                statement_errors: List[str] = []
                                for stmt_idx, statement_text in enumerate(statements):
                                    if not statement_text.strip():
                                        continue
                                    try:
                                        self.logger.debug(
                                            f"Executing Oracle statement #{stmt_idx+1} from {sql_file}: {statement_text[:100]}…"
                                        )
                                        cursor.execute(statement_text)
                                    except Exception as stmt_err:
                                        err_msg = str(stmt_err)
                                        statement_errors.append(f"[#{stmt_idx+1}] {err_msg}")
                                        self.logger.error(
                                            f"Error executing Oracle statement #{stmt_idx+1} in {sql_file}: {err_msg}"
                                        )
                                        # Continue with next statement instead of aborting the whole file

                                if statement_errors:
                                    sql_execution_status = "partial"
                                    sql_execution_error_message = "; ".join(statement_errors[:3]) + (
                                        " …" if len(statement_errors) > 3 else ""
                                    )
                                else:
                                    self.logger.info(
                                        f"Successfully executed all statements from {os.path.basename(sql_file)} for Oracle."
                                    )
                    else: 
                        with conn.cursor() as cursor:
                            original_isolation_level = None
                            try:
                                is_create_db_statement = self.db_type in ('postgres', 'postgresql', 'greenplum') and \
                                                         current_sql_content.strip().upper().startswith('CREATE DATABASE')

                                if is_create_db_statement:
                                    original_isolation_level = conn.isolation_level
                                    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                                
                                cursor.execute(current_sql_content)
                                
                                # If CREATE DATABASE was successful (no exception)
                                if i == 0 and is_create_db_statement:
                                    attempt_reconnect_after_file0 = True
                                
                            except Exception as e_pg_exec:
                                sql_execution_status = "error"
                                sql_execution_error_message = str(e_pg_exec)
                                self.logger.error(f"Error executing SQL from {sql_file} for {self.db_type}: {e_pg_exec}")
                                
                                if i == 0 and is_create_db_statement and "already exists" in str(e_pg_exec).lower():
                                    attempt_reconnect_after_file0 = True # OK to reconnect if DB already exists
                                    self.logger.info(f"Database in {sql_file} already exists, will still attempt context switch.")
                                elif is_create_db_statement: # CREATE DB failed for other reasons
                                    attempt_reconnect_after_file0 = False # Explicitly false
                                    self.logger.error(f"CREATE DATABASE in {sql_file} failed (not 'already exists'). No context switch will be attempted. Error: {e_pg_exec}")

                                # Rollback for errors not related to "CREATE DB already exists"
                                if not (is_create_db_statement and "already exists" in str(e_pg_exec).lower()):
                                    # Oracle rollback is now handled differently if an error occurs during statement iteration
                                    if self.db_type == 'oracle':
                                        # This path (inside non-oracle, non-snowflake) should not be hit for Oracle anymore.
                                        # However, if it were, conn.rollback() is appropriate here.
                                        # For safety, keeping a log if this state is ever reached.
                                        self.logger.warning("Oracle reached generic rollback logic; this should be handled by the Oracle-specific block.")
                                        try:
                                            conn.rollback()
                                            self.logger.info(f"Oracle (generic path): Rolled back transaction for {sql_file} due to error: {e_pg_exec}")
                                        except oracledb.Error as rb_ex_oracle:
                                            self.logger.error(f"Oracle (generic path): Failed to rollback for {sql_file}: {rb_ex_oracle}")
                                    elif hasattr(conn, 'closed') and conn and not conn.closed: # For psycopg2
                                        try:
                                            conn.rollback()
                                            self.logger.info(f"Rolled back transaction for {sql_file} due to error: {e_pg_exec}")
                                        except Exception as rb_ex:
                                            self.logger.error(f"Failed to rollback for {sql_file}: {rb_ex}")
                                    elif not hasattr(conn, 'closed') and conn: # Fallback for others if rollback is supported
                                        try:
                                            conn.rollback()
                                            self.logger.info(f"Generic: Rolled back transaction for {sql_file} due to error: {e_pg_exec}")
                                        except Exception as rb_ex_generic:
                                            self.logger.warning(f"Generic: Rollback attempt failed or not supported for {sql_file}: {rb_ex_generic}")
                            finally:
                                if original_isolation_level is not None and self.db_type not in ['oracle', 'snowflake']:
                                    # Reset isolation level if it was changed (primarily for psycopg2)
                                    conn.set_isolation_level(original_isolation_level)
                        
                        if sql_execution_status == "success":
                            try:
                                # Commit should only happen if no error occurred during statement execution for Oracle
                                # or if the entire file was successful for other DBs.
                                conn.commit()
                                self.logger.info(f"Successfully committed SQL from {os.path.basename(sql_file)} for {self.db_type}.")
                            except Exception as commit_ex:
                                self.logger.error(f"Error committing transaction for {sql_file}: {commit_ex}")
                                sql_execution_status = "error"
                                sql_execution_error_message = sql_execution_error_message or str(commit_ex) # Keep original error if commit fails

                    # --- PostgreSQL/Greenplum Reconnection Logic ---
                    if i == 0 and attempt_reconnect_after_file0: # Only if flagged
                        self.logger.info(f"Attempting PostgreSQL/Greenplum context switch after file {sql_file}.")
                        try:
                            match = re.search(r'CREATE\s+DATABASE\s+([A-Za-z0-9_]+)', current_sql_content, re.IGNORECASE)
                            if match:
                                new_db_name = match.group(1).lower()
                                self.logger.info(f"Target database from script: '{new_db_name}'. Proceeding with reconnection.")

                                if self.db_type == 'snowflake':
                                    if not conn.is_closed(): # Snowflake check
                                        conn.close()
                                elif not conn.closed: # Other DBs
                                    conn.close()
                                self.logger.info("Old connection closed.")

                                new_connection_params = original_connection_params.copy()
                                new_connection_params['database'] = new_db_name
                                
                                conn = self._get_connection(new_connection_params)
                                self.logger.info(f"Successfully reconnected to new database: '{new_db_name}'.")
                            else:
                                self.logger.warning(f"Could not parse database name from CREATE DATABASE statement in {sql_file}. No reconnection performed.")
                        except Exception as reconnect_e:
                            self.logger.error(f"CRITICAL RECONNECT ERROR for file {sql_file}: {reconnect_e}. Subsequent scripts will likely fail.", exc_info=True)

                            if sql_execution_status != "error": # If SQL succeeded but reconnect failed
                                sql_execution_status = "error"
                                sql_execution_error_message = f"SQL OK, but Critical Reconnect Failed: {reconnect_e}"
                            else: # SQL already failed, append reconnect failure info
                                sql_execution_error_message = f"{sql_execution_error_message}; Critical Reconnect Failed: {reconnect_e}"
                            file_had_critical_reconnect_error = True
                    
                    results.append({
                        "file": sql_file,
                        "statement": current_sql_content[:100] + "...",
                        "status": sql_execution_status,
                        "error": sql_execution_error_message if sql_execution_error_message else None
                    })
                    if sql_execution_status == "error":
                        failed_statements.append({
                            "file": sql_file,
                            "statement": current_sql_content,
                            "error": sql_execution_error_message
                        })
                    
                    if file_had_critical_reconnect_error:
                        self.logger.error("Halting further script execution due to critical reconnection failure.")
                        break 

                except Exception as e: # This is the file-level exception handler
                    self.logger.error(f"Error processing file {sql_file}: {str(e)}", exc_info=True)
                    # Ensure sql_execution_status and message reflect the caught error if not already set
                    if sql_execution_status == "success": # Error happened outside specific execution blocks
                        sql_execution_status = "error"
                        sql_execution_error_message = str(e)
                    
                    # For Oracle, if an error occurred, try to rollback
                    if self.db_type == 'oracle' and conn:
                        try:
                            conn.rollback()
                            self.logger.info(f"Oracle: Rolled back transaction for file {sql_file} due to error: {e}")
                        except oracledb.Error as rb_ex:
                            self.logger.error(f"Oracle: Failed to rollback transaction for file {sql_file}: {rb_ex}")
                        except Exception as gen_rb_ex: # Catch any other rollback errors
                            self.logger.error(f"Oracle: Generic error during rollback for file {sql_file}: {gen_rb_ex}")

                    results.append({
                        "file": sql_file,
                        "statement": current_sql_content[:100]+"..." if current_sql_content else "SQL not read/available",
                        "status": sql_execution_status,
                        "error": sql_execution_error_message if sql_execution_error_message else str(e)
                    })
                    failed_statements.append({
                        "file": sql_file,
                        "statement": current_sql_content if current_sql_content else "N/A",
                        "error": sql_execution_error_message if sql_execution_error_message else str(e)
                    })
                    if "Database connection was found closed" in str(e):
                        self.logger.error("Aborting due to closed connection.")
                        break
            
            # Close connection differently for different DB types
            if conn:
                if self.db_type == 'snowflake':
                    if not conn.is_closed():
                        conn.close()
                elif self.db_type == 'oracle':
                    try:
                        # Oracle connections are closed by calling close(). 
                        # There isn't a direct 'is_closed' attribute to check before calling.
                        # If conn object exists, we assume we should try to close it if it wasn't an error that already invalidated it.
                        conn.close()
                        self.logger.info(f"Oracle connection closed for db_type: {self.db_type}.")
                    except oracledb.Error as e:
                        # Log if closing fails, but don't let it stop the flow if already in an error state.
                        self.logger.warning(f"Error trying to close Oracle connection (it might have been already closed or in an error state): {e}")
                elif hasattr(conn, 'closed') and not conn.closed: # For psycopg2 and others
                    conn.close()
                elif not hasattr(conn, 'closed'): # For other DBs that don't have 'closed' but have 'close()'
                    try:
                        conn.close()
                        self.logger.info(f"Connection closed for db_type: {self.db_type} (generic close).")
                    except Exception as e:
                        self.logger.warning(f"Error trying to close connection for {self.db_type} (generic close): {e}")
            
            # Log summary
            success_count = len([r for r in results if r['status'] == 'success'])
            error_count = len([r for r in results if r['status'] == 'error'])
            summary = f"Execution completed. Success: {success_count}, Errors: {error_count}"
            self.logger.info(summary) # Log summary instead of printing
            
            if failed_statements:
                for stmt in failed_statements:
                    self.logger.error(f"Failed SQL in {stmt['file']}:\n{stmt['statement']}\nError: {stmt['error']}\n")
            
            # Write raw output
            raw_output_path = os.path.join(execution_dir, 'execution_summary.json')
            final_status = "success" if error_count == 0 else "partial"
            output = {
                "status": final_status,
                "results": results,
                "summary": {
                    "total": len(results),
                    "success": success_count,
                    "errors": error_count
                },
                "execution_id": execution_dir
            }
            
            content = json.dumps(output, indent=2)
            write_file_content(raw_output_path, content)
            
            return output
            
        except Exception as e:
            return {
                "status": "error",
                "message": str(e),
                "results": results
            }

    def _get_connection(self, connection_params: Dict):
        """Get database connection based on type"""
        try:
            from app.services.db.connection_factory import get_connection as _get_conn

            db_type = (connection_params.get('db_type') or '').lower()
            self.logger.info("Attempting to connect to database type: %s", db_type)
            
            # Delegate connection creation entirely to connection_factory which
            # now handles wallet_dir resolution for Oracle. This removes
            # duplicate logic and ensures consistent behaviour across all
            # code paths (db_service, data_load, etc.).

            self.logger.info(
                "Connecting to %s with user: %s, dsn: %s",
                db_type.capitalize(),
                connection_params.get("user"),
                connection_params.get("dsn"),
            )

            return _get_conn(db_type, connection_params)
            
        except Exception as e:
            self.logger.error(f"Failed to connect to {connection_params.get('type', 'unknown')}: {str(e)}", exc_info=True)
            raise Exception(f"Failed to connect to {connection_params.get('type', 'unknown')}: {str(e)}")

    def _split_sql_statements(self, sql: str) -> List[str]:
        """Split SQL into individual statements"""
        # Basic splitting on semicolon
        statements = [s.strip() for s in sql.split(';') if s.strip()] # ensure stripped statements are non-empty before adding
        
        return [s for s in statements if s and not s.isspace()]

    def _read_sql_file(self, sql_file: str) -> str:
        """Read SQL file content"""
        content = read_file_content(sql_file)
        return content or "" 

    def _is_connection_closed(self, conn):
        """Return True if the DB-API connection object is no longer usable."""
        if self.db_type == 'snowflake':
            return bool(getattr(conn, 'is_closed', lambda: False)())
        if self.db_type == 'oracle':
            try:
                conn.ping()
                return False
            except oracledb.Error:
                return True
        if hasattr(conn, 'closed'):
            return bool(conn.closed)
        return False 