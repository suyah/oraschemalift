"""
Result formatting utilities for SQL conversion.
Handles creation of standardized result dictionaries and summary data.
"""
from typing import Dict, List, Optional


def create_result_dictionary(status: str, message: str, stats: dict, results: list, output_dir: str = None, source_file: str = None, **kwargs) -> dict:
    """
    Create standardized result dictionary for conversion operations.
    
    Args:
        status: Overall conversion status ('success', 'error', 'partial_success')
        message: Human-readable status message
        stats: Conversion statistics dictionary
        results: List of individual file conversion results
        output_dir: Output directory path (optional)
        source_file: Source file path (optional)
        **kwargs: Additional keyword arguments
        
    Returns:
        Standardized result dictionary with aggregated stats and conversion summary
    """
    successful_files = len([r for r in results if r.get('status') == 'success'])
    failed_files = len([r for r in results if r.get('status') == 'error'])
    
    conversion_summary = {}
    for result in results:
        for conversion in result.get('conversions', []):
            conv_type = conversion.get('type', 'unknown')
            if conv_type not in conversion_summary:
                conversion_summary[conv_type] = {
                    'count': 0,
                    'description': conversion.get('description', '')
                }
            conversion_summary[conv_type]['count'] += conversion.get('count', 1)
    
    result = {
        "status": status,
        "message": message,
        "stats": {
            **stats,
            "files_successful": successful_files,
            "files_failed": failed_files
        },
        "conversion_summary": conversion_summary,
        "results": results
    }
    
    if output_dir:
        result["output_directory"] = output_dir
    if source_file:
        result["source_file"] = source_file
    
    return result 