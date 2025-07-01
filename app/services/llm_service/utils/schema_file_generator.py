"""
LLM Schema File Generator - Processes LLM schema responses into organized SQL files.

This module contains LLM-specific business logic for:
- Parsing LLM JSON/text responses
- Organizing schema definitions into structured SQL files
- Handling markdown formatting from LLM responses
"""
import os
import json
from typing import List, Optional


def parse_and_write_schema_files(schema_definition, output_dir: str, source_type: str, logger=None) -> List[str]:
    """
    Parse LLM schema definition and write organized SQL files.
    
    This function handles complex parsing of LLM responses (JSON or raw text) and creates
    multiple organized SQL files with proper naming conventions and content structure.
    
    Args:
        schema_definition: Dictionary with SQL statements organized by type OR raw string from LLM
        output_dir: Directory to write SQL files
        source_type: Source database type
        logger: Optional logger instance
        
    Returns:
        List of generated file paths
    """
    try:
        sql_files = []
        os.makedirs(output_dir, exist_ok=True)
        
        # Handle string response from LLM (parse or save as single file)
        if isinstance(schema_definition, str):
            if logger:
                logger.info("LLM returned string response, attempting to parse")
            
            # Remove markdown code block formatting if present
            content = schema_definition.strip()
            if content.startswith('```json'):
                # Extract JSON from markdown code block
                lines = content.split('\n')
                # Find the start and end of the JSON block
                json_start = 1
                json_end = len(lines)
                for i in range(len(lines) - 1, -1, -1):
                    if lines[i].strip() == '```':
                        json_end = i
                        break
                content = '\n'.join(lines[json_start:json_end])
                if logger:
                    logger.info("Extracted JSON from markdown code block")
            elif content.startswith('```') and content.endswith('```'):
                # Handle other code block formats
                content = content[3:-3].strip()
                if logger:
                    logger.info("Extracted content from code block")
            
            # Try to parse JSON
            if content.strip().startswith('{'):
                try:
                    schema_definition = json.loads(content)
                    if logger:
                        logger.info("Successfully parsed JSON response from LLM")
                except json.JSONDecodeError as e:
                    if logger:
                        logger.warning(f"Failed to parse JSON: {e}, treating as raw SQL")
                    # Save as single SQL file
                    file_path = os.path.join(output_dir, f"{source_type}_generated.sql")
                    with open(file_path, 'w') as f:
                        f.write(f"-- Generated SQL for {source_type}\n\n")
                        f.write(schema_definition)
                    sql_files.append(file_path)
                    return sql_files
            else:
                # Save as single SQL file
                file_path = os.path.join(output_dir, f"{source_type}_generated.sql")
                with open(file_path, 'w') as f:
                    f.write(f"-- Generated SQL for {source_type}\n\n")
                    f.write(schema_definition)
                sql_files.append(file_path)
                return sql_files
        
        # Handle dictionary response (original logic)
        if not isinstance(schema_definition, dict):
            raise ValueError(f"Expected dict or str, got {type(schema_definition)}")
        
        # File order for consistent output with prefixes
        object_type_order = [
            'database', 'schemas', 'tables', 'views', 'functions', 
            'procedures', 'sequences', 'indexes', 'triggers', 'tasks', 
            'security', 'dml', 'queries'
        ]
        
        # Create prefix mapping for consistent file naming
        prefix_mapping = {
            'database': '01',
            'schemas': '02', 
            'tables': '02',
            'views': '03',
            'functions': '03',
            'procedures': '04',
            'sequences': '05',
            'indexes': '05',
            'triggers': '06',
            'tasks': '05',
            'security': '06',
            'dml': '07',
            'queries': '08'
        }

        # Process in defined order, then any remaining keys
        sorted_keys = [key for key in object_type_order if key in schema_definition]
        remaining_keys = [key for key in schema_definition if key not in object_type_order]
        
        for object_type in sorted_keys + remaining_keys:
            statements = schema_definition.get(object_type)
            if statements and isinstance(statements, list) and all(isinstance(stmt, str) for stmt in statements):
                # Use prefix for consistent naming like the original
                prefix = prefix_mapping.get(object_type, '99')
                filename = f"{prefix}_{object_type}.sql"
                file_path = os.path.join(output_dir, filename)
                
                with open(file_path, 'w') as f:
                    f.write(f"-- SQL Statements for: {object_type.replace('_', ' ').title()}\n\n")
                    f.write('\n\n'.join(statements))
                
                if logger:
                    logger.info(f"Created file: {file_path}")
                sql_files.append(file_path)
            elif statements:
                if logger:
                    logger.warning(f"Skipping {object_type} as its content is not a list of SQL strings.")
        
        return sql_files
    
    except Exception as e:
        if logger:
            logger.error(f"Error generating SQL files from schema_definition: {str(e)}", exc_info=True)
        raise e 