"""
SQL content parsing, function mapping, and file-writing utilities.

This module provides functions to:
- Parse SQL content into a list of statements using SQLGlot, handling common parsing errors.
- Apply configuration-driven function and syntax mappings to SQL strings.
- Write lists of converted SQL statements to an output file with basic formatting.
"""
import re
import os
import json
from typing import Dict, List, Any, Tuple
from pathlib import Path
from app.utils.file_utils import ensure_directory_exists, read_file_content, write_file_content
from .regex_utils import re_flags

import sqlglot
import sqlglot.errors
from .dialect_utils import get_sqlglot_dialect


def parse_file_into_statements(content: str, logger, source_type: str, file_path_for_logging: str) -> Tuple[List[Any], List[Dict]]:
    """
    Preprocesses SQL content and parses it into statements using SQLGlot.
    This version returns the raw ASTs and lets the caller handle detailed analysis.
    """
    errors = []
    
    current_content = content 
    try:
        # Preprocessing is minimal now, mainly for cleaning line endings and BOM
        if current_content.startswith('\ufeff'):
            current_content = current_content[1:]
        current_content = current_content.replace('\r\n', '\n').replace('\r', '\n')

        dialect_hint = get_sqlglot_dialect(source_type)
        
        # We use 'IGNORE' to ensure that even partially-unsupported statements are returned
        # as `exp.Command` objects, rather than raising a ParseError.
        parsed_statements = sqlglot.parse(current_content, read=dialect_hint, error_level="IGNORE")
        
        if not parsed_statements:
            logger.debug(f"No statements parsed from {os.path.basename(file_path_for_logging)}")
            return [], []
        
        logger.debug(f"Successfully parsed {len(parsed_statements)} statement AST(s) from {os.path.basename(file_path_for_logging)}")
        return parsed_statements, errors
        
    except sqlglot.errors.ParseError as pe:
        error_msg = f"Critical SQLGlot parsing error in {os.path.basename(file_path_for_logging)}: {str(pe)[:200]}..."
        logger.warning(error_msg)
        errors.append({"original_sql_snippet": current_content[:200] + "...", "error_type": "ParseError", "error_message": str(pe)})
        return [], errors
    except Exception as e:
        error_msg = f"Unexpected error during SQL parsing in {os.path.basename(file_path_for_logging)}: {str(e)[:100]}..."
        logger.error(error_msg, exc_info=True)
        errors.append({"original_sql_snippet": current_content[:200] + "...", "error_type": "UnexpectedErrorDuringParse", "error_message": str(e)})
        return [], errors


def apply_function_mappings(sql: str, source_type: str, target_type: str, logger) -> str:
    """
    Apply function mappings and syntax transformations using configuration.
    
    Args:
        sql: SQL statement to transform
        source_type: Source database type
        target_type: Target database type
        logger: Logger instance
        
    Returns:
        SQL with function mappings applied
    """
    config_data = None
    config_filename = f"{source_type.lower()}_{target_type.lower()}.json"

    try:
        config_file_path = (
            Path(__file__).parent.parent.parent.parent / 
            'config' / 'conversion' / 'functions' / config_filename
        )

        if config_file_path.exists() and config_file_path.is_file():
            file_content = read_file_content(str(config_file_path))
            if file_content:
                try:
                    config_data = json.loads(file_content)
                except json.JSONDecodeError as jde:
                    logger.error("Failed to decode JSON from %s: %s", config_file_path, jde, exc_info=True)
            else:
                logger.warning("Could not read or config file is empty: %s", config_file_path)

            if config_data:
                for section_name in ['syntax_fixes', 'preprocessing', 'cleanup_fixes']:
                    for fix in config_data.get(section_name, []):
                        regex_pattern = fix.get('regex', '')
                        replacement_str = fix.get('replacement', '')
                        flags_str = fix.get('flags', '')
                        fix_name = fix.get('name', 'Unnamed Rule')

                        if regex_pattern:
                            flags = re_flags(flags_str)
                            old_sql = sql
                            sql = re.sub(regex_pattern, replacement_str, sql, flags=flags)
                            if sql != old_sql:
                                logger.debug("Applied function mapping '%s' from section '%s' (%s -> %s, file: %s)",
                                             fix_name, section_name,
                                             source_type, target_type, config_filename)
                logger.info("Applied function transformations from %s", config_filename)
        else:
            logger.info("No function transformation config file found for %s -> %s at %s",
                        source_type, target_type, config_file_path)
                        
    except FileNotFoundError:
        logger.warning("Function transformation config file not found (FileNotFoundError): %s", config_file_path, exc_info=True)
    except (IOError, OSError) as ioe:
        logger.error("File system error (IOError/OSError) loading function transformation config %s: %s",
                      config_filename, ioe, exc_info=True)
    except Exception as e:
        logger.warning("Unexpected error during function mapping for %s (using %s -> %s config): %s",
                       config_filename, source_type, target_type, e, exc_info=True)
            
    hardcoded_replacements = [
        (r'\bCURRENT_DATE\s*\(\s*\)', 'SYSDATE'),
        (r'\bCURRENT_TIMESTAMP\s*\(\s*\)', 'SYSTIMESTAMP'),
        (r'\bGETDATE\s*\(\s*\)', 'SYSDATE'),
    ]
    
    for pattern, replacement_val in hardcoded_replacements:
        sql = re.sub(pattern, replacement_val, sql, flags=re.IGNORECASE)
    
    return sql


def write_converted_sql_file(output_file: str, converted_statements: List[str], 
                           logger, original_file: str) -> Dict:
    """
    Write converted SQL statements to output file.
    
    Args:
        output_file: Path to output file
        converted_statements: List of converted SQL statements
        logger: Logger instance
        original_file: Original source file path
        
    Returns:
        Dictionary with status and any error messages
    """
    try:
        ensure_directory_exists(os.path.dirname(output_file))
        
        output_content = []
        for stmt in converted_statements:
            stmt_cleaned = stmt.strip()
            if stmt_cleaned and not stmt_cleaned.endswith(';'):
                stmt_cleaned += ';'
            
            if not stmt_cleaned.endswith('\n'):
                stmt_cleaned += '\n'
            output_content.append(stmt_cleaned)
        
        write_file_content(output_file, ''.join(output_content))
        
        logger.info(f"Successfully wrote {len(converted_statements)} statement(s) to: {output_file} (from {original_file})")
        return {"status": "success"}
        
    except Exception as e:
        logger.error(f"Error writing output file {output_file}: {e}")
        return {"status": "error", "error_message": str(e)} 