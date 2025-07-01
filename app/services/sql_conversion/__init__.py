"""
SQL Conversion Package - Comprehensive SQL database conversion system.

This package provides a complete solution for converting SQL between different database dialects.

Main Components:
    - ConversionOrchestrator: Main entry point for SQL conversion operations
    - DDL Converter: Handles table creation and schema conversion
    - Procedural Converters: Extract and convert functions/procedures  
    - Basic Converter: General SQL statement conversion
    - Utils: Supporting utilities for preprocessing, dialects, etc.

Usage:
    from app.services.sql_conversion import ConversionOrchestrator
    
    orchestrator = ConversionOrchestrator()
    result = orchestrator.orchestrate_sql_conversion(
        input_path="source_files/",
        source_type="snowflake", 
        target_type="oracle"
    )
"""

from .orchestrator import ConversionOrchestrator

__all__ = ['ConversionOrchestrator']
