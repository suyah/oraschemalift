import sqlglot
from sqlglot import exp
import logging

logger = logging.getLogger(__name__)

def safe_parse_one(sql: str, dialect: str) -> tuple[exp.Expression | None, str | None]:
    """
    Safely parses a single SQL statement into an AST.

    Args:
        sql: The SQL statement string to parse.
        dialect: The sqlglot dialect to use for parsing.

    Returns:
        A tuple containing (ast, error_message).
        If successful, ast is the parsed expression and error_message is None.
        If fails, ast is None and error_message is a formatted error string.
    """
    try:
        ast = sqlglot.parse_one(sql, read=dialect)
        return ast, None
    except Exception as e:
        logger.error(f"Failed to parse statement: {e}", exc_info=True)
        error_message = f"-- CONVERSION ERROR: Failed to parse statement due to: {e}\n{sql}"
        return None, error_message