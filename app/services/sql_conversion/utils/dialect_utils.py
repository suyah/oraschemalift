"""
SQLGlot dialect utilities for SQL conversion.
Handles mapping between database types and their corresponding SQLGlot dialects.
"""
import sqlglot


def get_sqlglot_dialect(source_type: str):
    """
    Get the appropriate SQLGlot dialect for parsing.
    
    Args:
        source_type: Database type (e.g., 'snowflake', 'oracle', 'postgres')
        
    Returns:
        SQLGlot dialect string or None for default behavior
    """
    dialect_map = {
        'snowflake': 'snowflake',
        'oracle': 'oracle',
        'mysql': 'mysql', 
        'postgresql': 'postgres',
        'bigquery': 'bigquery',
        'greenplum': None
    }
    
    if source_type.lower() == 'greenplum':
        try:
            from app.dialects.greenplum import Greenplum
            return Greenplum
        except ImportError:
            return 'postgres'
    
    return dialect_map.get(source_type.lower()) 