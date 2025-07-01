"""
Common file utilities used across the application.
Consolidates file discovery, output setup, and logging directory management.
"""
import os
import json
from typing import List, Dict, Tuple, Any, Optional
from datetime import datetime
from pathlib import Path


def find_sql_files(input_path: str, exclude_dirs: List[str] = None) -> List[str]:
    """
    Find all SQL files in a given path.
    
    Args:
        input_path: Path to a directory or a SQL file
        exclude_dirs: List of directory names to exclude (e.g., ['converted', 'logs'])
        
    Returns:
        List of paths to SQL files
    """
    if exclude_dirs is None:
        exclude_dirs = ['converted', 'logs', 'conversion_logs', 'llm_logs', '__pycache__']
    
    sql_files = []
    normalized_input_path = os.path.normpath(input_path)

    if os.path.isdir(normalized_input_path):
        for root, dirs, files in os.walk(normalized_input_path):
            # Remove excluded directories from the walk
            dirs[:] = [d for d in dirs if d not in exclude_dirs]
            
            for file in files:
                if file.endswith('.sql'):
                    sql_files.append(os.path.join(root, file))
    elif os.path.isfile(normalized_input_path) and normalized_input_path.endswith('.sql'):
        sql_files = [normalized_input_path]
    
    return sorted(sql_files)


def setup_output_directory(input_path: str, timestamp_suffix: bool = True) -> Tuple[str, str, str]:
    """
    Set up consistent output directory structure for conversion.
    
    Args:
        input_path: Input path containing SQL files
        timestamp_suffix: Whether to add timestamp to output directory
        
    Returns:
        Tuple of (source_dir, output_dir, log_dir)
    """
    input_path = os.path.abspath(input_path)
    
    if os.path.isfile(input_path):
        source_dir = os.path.dirname(input_path)
    else:
        source_dir = input_path
    
    # Create output directory with timestamp
    if timestamp_suffix:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = os.path.join(os.path.dirname(source_dir), f"converted_{timestamp}")
    else:
        output_dir = os.path.join(os.path.dirname(source_dir), "converted")
    
    # Consolidate logs into a single directory structure
    log_dir = os.path.join(os.path.dirname(source_dir), "logs")
    
    # Create directories
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)
    
    return source_dir, output_dir, log_dir


def create_processing_stats() -> Dict[str, int]:
    """Create standard processing statistics dictionary for tracking file/statement operations."""
    return {
        'total_files': 0,
        'total_statements': 0,
        'converted': 0,
        'no_conversion_needed': 0,
        'skipped': 0,
        'errors': 0
    }


def make_relative_path(file_path: str, base_path: str) -> str:
    """
    Make a file path relative to a base path, with error handling.
    
    Args:
        file_path: Absolute file path
        base_path: Base path to make relative to
        
    Returns:
        Relative path or original path if conversion fails
    """
    if not file_path or not base_path:
        return file_path
    
    try:
        return os.path.relpath(file_path, base_path)
    except (ValueError, OSError):
        # Return original path if relative path conversion fails
        return file_path


def read_file_content(file_path: str) -> Optional[str]:
    """
    Read content from a file with proper error handling.
    
    Args:
        file_path: Path to the file to read
        
    Returns:
        File content as string, or None if file can't be read
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if not content.strip():
            return None
        
        return content
        
    except Exception:
        return None


def write_file_content(file_path: str | Path, content: str):
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)


def ensure_directory_exists(directory_path: str) -> None:
    """
    Ensure that a directory exists, creating it if necessary.
    
    Args:
        directory_path: Path to the directory to create
    """
    os.makedirs(directory_path, exist_ok=True)


# ---------------------------------------------------------------------------
# CSV helper (simple, no extra deps)
# ---------------------------------------------------------------------------


def write_csv(file_path: str | Path, headers: list[str], rows: list[tuple | list]):
    """Write *rows* to CSV with *headers*.

    Parameters
    ----------
    file_path: str | Path
        Destination path.
    headers: list[str]
        Column names.
    rows: list[tuple | list]
        Iterable of row tuples/lists.
    """
    import csv

    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)


 