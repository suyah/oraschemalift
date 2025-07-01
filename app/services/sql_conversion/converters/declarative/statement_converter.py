import logging
import re
from sqlglot import exp, transpile
from app.services.sql_conversion.utils.parser_utils import safe_parse_one

from app.utils.logger import setup_logger
from app.services.sql_conversion.converters.base_converter import BaseConverter
from app.services.sql_conversion.utils.dialect_utils import get_sqlglot_dialect
from app.services.sql_conversion.converters.declarative.ddl_handler import DdlHandler

class StatementConverter(BaseConverter):
    """
    Acts as a router, inspecting the SQL statement and delegating to the
    appropriate specialized handler.
    """
    def __init__(self, source_dialect: str, target_dialect: str, target_version: str = None, manual_review_logger=None):
        super().__init__(source_dialect, target_dialect)
        self.logger = setup_logger('StatementConverter')
        self.target_version = target_version
        self.manual_review_logger = manual_review_logger
        self.ddl_handler = DdlHandler(source_dialect, target_dialect, manual_review_logger=self.manual_review_logger)

    def convert_statement(self, ast: exp.Expression) -> tuple[list[str], list[dict]]:
        """
        Inspects a single SQL AST and routes it to the appropriate
        conversion logic based on its type.
        
        Args:
            ast: A sqlglot Expression object.
        """
        all_logs = []

        # Route to the correct handler based on AST type
        if self._is_create_table(ast):
            # If the parser produced a generic Command node (usually because of
            # unsupported trailing clauses like WITH ROW ACCESS POLICY), try a
            # second parse after temporarily stripping those clauses.  This
            # gives us a proper exp.Create that DdlHandler can walk.
            if isinstance(ast, exp.Command):
                raw_sql = ast.sql()
                cleaned_sql = self._strip_unparseable_clauses(raw_sql)

                reparsed_ast, err = safe_parse_one(
                    cleaned_sql,
                    get_sqlglot_dialect(self.source_dialect)
                )
                if not err and isinstance(reparsed_ast, exp.Create):
                    self.logger.debug("Re-parsed CREATE TABLE into exp.Create after removing unsupported clauses.")
                    ast = reparsed_ast
                else:
                    self.logger.debug("Reparse attempt failed or did not yield exp.Create (err=%s). Proceeding with original AST.", err)

            self.logger.info("Routing CREATE TABLE statement to DdlHandler.")
            converted_statements, ddl_logs = self.ddl_handler.handle(ast)
            all_logs.extend(ddl_logs)
            return converted_statements, all_logs
        
        # --- Add other routers here as new handlers are built ---

        else:
            # For any other statement, use the AST's sql() method as a fallback.
            self.logger.info(f"Statement is not a CREATE TABLE. Routing to basic transpiler.")
            # This is the correct way to convert an existing AST to a new dialect.
            converted_sql = ast.sql(dialect=self.target_dialect, pretty=True)
            all_logs.append({'action': 'transpile_fallback', 'details': f'Used basic transpiler for statement type: {type(ast).__name__}'})
            return [converted_sql], all_logs

    def _is_create_table(self, stmt: exp.Expression) -> bool:
        """Checks if the statement is a CREATE TABLE statement."""
        # --- 1. Normal path: CREATE … TABLE … ---------------------------------
        if isinstance(stmt, exp.Create):
            # `kind` is reliable when sqlglot recognises the statement, but it
            # becomes None when non-standard clauses (e.g. "CLUSTER BY" before
            # the column list) confuse the parser.  Fall back to inspecting the
            # inner node: a true table DDL will have .this instanceof exp.Table.
            if stmt.kind == 'TABLE':
                return True
            try:
                from sqlglot.expressions import Table  # delayed import to avoid circulars
                if isinstance(stmt.this, Table):
                    return True
            except Exception:
                # Defensive: if sqlglot refactors internals we simply ignore.
                pass

        # --- 2. Fallback path: unparsed COMMAND --------------------------------
        if isinstance(stmt, exp.Command):
            # sqlglot sometimes stores the raw SQL inside .this (str) but can
            # also keep a nested expression.  Safest is to get the full SQL
            # string representation.
            try:
                text_sql = stmt.sql().upper().lstrip()
            except Exception:
                text_sql = str(stmt.this).upper().lstrip() if stmt.this else ""

            if re.match(r"CREATE\s+(OR\s+REPLACE\s+)?TABLE", text_sql):
                return True

        return False

    def _strip_unparseable_clauses(self, sql_text: str) -> str:
        """
        Strips unparseable clauses from the SQL text.
        
        Args:
            sql_text: The SQL text to strip.
        """
        patterns = [
            # Snowflake: WITH ROW ACCESS POLICY <policy_name> ON (col1, col2)
            # This full clause is unsupported by sqlglot and causes the
            # statement to be tokenised as exp.Command.  Strip exactly one
            # occurrence to aid reparsing; DdlHandler will remove the clause
            # later anyway.
            re.compile(r"\s+WITH\s+ROW\s+ACCESS\s+POLICY\s+[A-Za-z0-9_\"\.]+\s+ON\s*\([^)]*\)", re.IGNORECASE),
            # NOTE: We avoid stripping "CLUSTER BY" here as sqlglot can parse it correctly.
            # This list should only contain clauses that break the parser entirely.
        ]

        cleaned_sql = sql_text
        for pat in patterns:
            cleaned_sql = pat.sub("", cleaned_sql, count=1)
        return cleaned_sql