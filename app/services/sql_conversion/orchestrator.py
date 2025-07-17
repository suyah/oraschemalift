"""ConversionOrchestrator – high-level driver for declarative SQL conversion.

Responsibilities
----------------
1. Locate input *.sql files.
2. Prepare output directories (`converted/<timestamp>`).
3. Create and configure `StatementConverter` (plus procedural converters if
   enabled).
4. For each file:
     • parse → filter skip-patterns (config-driven)
     • delegate to converter(s)
     • write converted SQL and collect stats.
5. Produce `conversion_summary.json`.

All detailed rewrite logic lives in the converter layer; orchestrator only
handles I/O, logging and aggregation.

WHAT THIS CLASS DOES:
====================
- Sets up conversion environment (directories, file discovery).
- Creates converter instances (DDL, procedural components).
- Processes SQL files through appropriate converters, delegating detailed parsing and rule application.
- Aggregates results and generates a final conversion summary.

FUNCTIONS:
==========
Public Functions (called by external code):
  - orchestrate_sql_conversion(): Main entry point, coordinates entire conversion.
  - setup_converters(): Creates all converter instances.

Private Functions (internal helpers, start with _):
  Key private methods manage stages such as:
  - Setting up specific converter types (_create_procedural_converters).
  - Loading source-specific configurations (_get_source_config, e.g., skip patterns from dialect_behaviors.json via config_loader).
  - Routing and processing individual files based on type (_process_sql_file, _process_procedural_file, _process_regular_sql_file).
  - Converting lists of statements using the appropriate DDL or basic converters (_convert_statement_list).
  - Formatting file-level results and logging (_create_file_error_result, _create_file_skip_result, _log_file_result).
  - Preparing and writing the final summary (_create_conversion_summary, _write_conversion_summary_to_file using file_utils).

CONVERTERS USED/CREATED:
======================
- TableDdlConverter: Handles CREATE TABLE statements and DDL conversions.
- ProceduralCodeExtractor: Extracts functions/procedures from SQL content.
- DeclarativeProceduralConverter: Converts procedural blocks using templates.
- Basic Conversion Functions (from basic_converter.py): Used for general SQL statement transformation (non-DDL, non-procedural).

NOTE: Functions starting with _ are private (internal use only).
      Functions without _ are public (intended for external calling).
"""

# Standard library imports
import os
import json
import logging
import sqlglot
from sqlglot import exp, parse
from pathlib import Path
from typing import Dict, Optional, List, Any, Tuple
import sqlglot.errors
from datetime import datetime
import time
import re

# Local application imports
from app.utils.file_utils import ensure_directory_exists, find_sql_files, read_file_content, write_file_content, create_processing_stats
from app.utils.logger import setup_logger
from .utils.directory_utils import get_timestamp, create_run_directory
from .utils.result_formatter import create_result_dictionary
from .utils.sql_preprocessing import parse_file_into_statements, write_converted_sql_file
from .utils.dialect_utils import get_sqlglot_dialect
from .converters.declarative.statement_converter import StatementConverter
from .utils.config_loader import load_json_from_conversion_config


def create_result_dictionary(status: str, message: str, stats: dict, errors: list, output_dir: str) -> dict:
    """Helper to create a consistent result dictionary."""
    return { "status": status, "message": message, "stats": stats, "errors": errors, "output_dir": output_dir }

class ConversionOrchestrator:

    def __init__(self, source_dialect: str, target_dialect: str, *, output_dir: Optional[str] = None, generate_cleanup: bool = False):
        self.logger = setup_logger("ConversionOrchestrator")
        self.logger.info(f"Starting SQL conversion: {source_dialect} -> {target_dialect}")

        self.source_dialect = source_dialect
        self.target_dialect = target_dialect
        self.output_dir = output_dir
        self.generate_cleanup = generate_cleanup
        self.behavior_config = None
        self.statement_converter = None

    def _create_converters(self):
        """Creates converter instances based on the specified dialects."""
        pair_name = f"{self.source_dialect}_{self.target_dialect}"
        self.statement_converter = StatementConverter(self.source_dialect, self.target_dialect)
        self.logger.info(f"Created converters: statement for {pair_name}")

    def _discover_files(self, source_dir: str) -> list[str]:
        """Discovers all .sql files in the source directory."""
        if not os.path.isdir(source_dir): return []
        return [os.path.join(source_dir, f) for f in os.listdir(source_dir) if f.endswith(".sql")]

    def convert(self, source_dir: str, output_dir_override: Optional[str] = None) -> dict:
        self._create_converters()
        self.behavior_config = self._get_behavior_config()
        
        source_files = sorted(self._discover_files(source_dir))
        if not source_files:
            return {
                "status": "error", 
                "message": "No SQL files found in the source directory.",
                "output_dir": None,
                "file_results": [],
                "errors": []
            }

        output_dir = self._setup_output_dir(source_dir, output_dir_override)
        self.logger.info(f"Processing {len(source_files)} SQL files from: {source_dir}")
        self.logger.info(f"Output directory: {output_dir}")
        
        # Ensure manual review logger is ready for this run
        try:
            # Import lazily to avoid circularities in case of partial environment
            from .utils.manual_review_logger import ManualReviewLogger

            self.manual_review_logger = ManualReviewLogger(
                output_dir=str(output_dir), logger=self.logger
            )
        except Exception:
            # Fallback: attribute stub to avoid AttributeError if utils unavailable
            class _Noop:
                def write_manual_review_log(self):
                    pass

            self.manual_review_logger = _Noop()

        overall_stats = {
            "files_processed": len(source_files),
            "files_converted": 0,
            "statements_converted": 0,
        }
        file_results: list[dict] = []
        aggregated_logs: list[dict] = []
        created_objects: list[tuple[str, str]] = []

        for i, file_path in enumerate(source_files):
            self.logger.info(f"[{i + 1}/{len(source_files)}] Processing: {os.path.basename(file_path)}")
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            
            result = self._process_file(file_path, content, output_dir, overall_stats)
            
            file_results.append({
                "file_name": os.path.basename(file_path),
                "status": result.get("status", "error"),
                "message": result.get("message", "")
            })
            if result.get("errors"):
                aggregated_logs.extend(result["errors"])

            if result.get("converted_statements"):
                self.logger.info(f"Successfully wrote {len(result['converted_statements'])} statement(s) to: {result['output_file']} (from {file_path})")
                # Collect object names for cleanup
                obj_regex = re.compile(r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(TABLE|VIEW|SEQUENCE|PROCEDURE|FUNCTION|PACKAGE|MATERIALIZED\s+VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w\"\.]+)", re.IGNORECASE | re.MULTILINE)
                for stmt in result['converted_statements']:
                    # Search across the whole statement in case a leading comment exists
                    m = obj_regex.search(stmt)
                    if m:
                        created_objects.append((m.group(1).upper(), m.group(2)))

        # Optionally generate cleanup script
        if self.generate_cleanup and created_objects:
            cleanup_path = os.path.join(output_dir, "00_cleanup.sql")
            drop_lines = []
            for obj_type, obj_name in sorted(set(created_objects), key=lambda x: (x[0], x[1])):
                obj_name = obj_name.strip('"')  # remove quotes if any
                if obj_type == "TABLE":
                    drop_lines.append(f"DROP TABLE {obj_name} CASCADE CONSTRAINTS;")
                elif obj_type == "VIEW":
                    drop_lines.append(f"DROP VIEW {obj_name};")
                elif obj_type == "SEQUENCE":
                    drop_lines.append(f"DROP SEQUENCE {obj_name};")
                elif obj_type == "MATERIALIZED VIEW":
                    drop_lines.append(f"DROP MATERIALIZED VIEW {obj_name};")
                elif obj_type in ("PROCEDURE", "FUNCTION"):
                    drop_lines.append(f"DROP {obj_type} {obj_name};")
                elif obj_type == "PACKAGE":
                    drop_lines.append(f"DROP PACKAGE {obj_name};")

            if drop_lines:
                with open(cleanup_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(drop_lines))
                self.logger.info("Cleanup script written: %s", cleanup_path)

        # --------------------------------------------------
        # Write detailed summary (including logs) to JSON file
        # --------------------------------------------------

        summary_payload = {
            "overall_statistics": overall_stats,
            "files": file_results,
            "conversion_logs": aggregated_logs,
            "output_directory": output_dir,
        }

        self._write_conversion_summary_to_file(summary_payload, output_dir)

        # Write manual-review log if any entries were captured
        self.manual_review_logger.write_manual_review_log()

        # --------------------------------------------------
        # Return *slim* response: no detailed logs
        # --------------------------------------------------

        return {
            "status": "success",
            "message": f"Conversion finished for {len(source_files)} files.",
            "output_dir": output_dir,
            "file_results": file_results,
            "summary_file": os.path.join(output_dir, "conversion_summary.json"),
        }

    def _process_file(self, file_path: str, content: str, output_dir: str, overall_stats: dict) -> Dict:
        filename = os.path.basename(file_path)
        self.logger.info(f"Processing as regular SQL file: {filename}")

        # --------------------------------------------------
        # Optional: strip procedural BEGIN … END blocks before parsing
        # --------------------------------------------------
        try:
            if self.behavior_config.get("strip_procedural_blocks", False):
                content = self._strip_procedural_blocks(content)
        except Exception as e:
            self.logger.warning("Failed to strip procedural blocks in %s: %s", filename, e, exc_info=True)
        
        skip_patterns = self.behavior_config.get("statement_skipping", {}).get("patterns", [])
        statements, errors = self._parse_and_filter_statements(content, file_path, skip_patterns)

        
        if errors:
            return create_result_dictionary("error", f"Error parsing {filename}", overall_stats, errors, output_dir)
        
        conversion_errors = []
        final_sql_statements = []
        
        for ast in statements:
            converted_sql, logs = self.statement_converter.convert_statement(ast)
            final_sql_statements.extend(converted_sql)
            if logs:
                for log_entry in logs:
                    log_entry["file"] = filename
                conversion_errors.extend(logs)

        if not final_sql_statements:
            self.logger.warning(f"No statements were converted for file {filename}. Skipping output file creation.")
            skipped_result = create_result_dictionary("skipped", f"No convertible statements found in {filename}", overall_stats, conversion_errors, output_dir)
            skipped_result["converted_statements"] = []
            skipped_result["output_file"] = None
            return skipped_result

        output_path = self._write_converted_file(output_dir, file_path, final_sql_statements)
        self.logger.info(f"Successfully wrote {len(final_sql_statements)} statement(s) to: {output_path} (from {file_path})")

        overall_stats["files_converted"] += 1
        overall_stats["statements_converted"] += len(final_sql_statements)

        success_result = create_result_dictionary("success", f"Successfully converted {filename}", overall_stats, conversion_errors, output_dir)
        success_result["converted_statements"] = final_sql_statements
        success_result["output_file"] = output_path
        return success_result


    def _get_behavior_config(self) -> dict:
        """Loads the dialect-specific behavior configuration."""
        self.logger.debug(f"Loading behavior config for {self.source_dialect}->{self.target_dialect}")
        config = load_json_from_conversion_config(
            logger=self.logger,
            source_type=self.source_dialect,
            target_type=self.target_dialect,
            rules_subdirectory="ddl_conversion_rules",
            config_filename="dialect_behaviors.json"
        )
        if not config:
            self.logger.warning(f"Could not load behavior configuration. Proceeding with defaults.")
            return {}
        return config

    def _setup_output_dir(self, source_dir: str, output_dir_override: Optional[str] = None) -> str:
        """Creates the output directory for converted files."""
        if output_dir_override:
            final_output_dir = output_dir_override
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_dir = os.path.join(os.path.dirname(source_dir), "converted")
            final_output_dir = os.path.join(base_dir, timestamp)

            # Create both base and timestamped directories
            try:
                os.makedirs(final_output_dir, exist_ok=True)
            except OSError as e:
                self.logger.error(
                    "Failed to create output directory '%s': %s", final_output_dir, e, exc_info=True
                )
                raise

        return final_output_dir

    def _write_converted_file(self, output_dir: str, original_file_path: str, statements: list[str]) -> str:
        """Writes the converted SQL statements to a new file and ensures every
        statement – including the final one – is terminated with a semicolon."""

        filename = os.path.basename(original_file_path)
        output_path = os.path.join(output_dir, filename)

        joined_sql = ";\n\n".join(statements)
        if not joined_sql.rstrip().endswith(';'):
            joined_sql = f"{joined_sql.rstrip()} ;"  # append missing terminator

        # Normalise line-endings and guarantee single trailing newline
        joined_sql = joined_sql.replace('\r\n', '\n').replace('\r', '\n')
        if not joined_sql.endswith('\n'):
            joined_sql += '\n'

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(joined_sql)

        return output_path

    def orchestrate_sql_conversion(self, input_path: str, source_type: str, 
                   target_type: str = 'oracle', target_version: str = '19c',
                   output_dir_base: Optional[str] = None,
                   original_run_timestamp: Optional[str] = None, 
                   initial_source_type: Optional[str] = None) -> Dict:
        """
        Main orchestration method - coordinates the entire SQL conversion process.
        
        Args:
            input_path: Directory or file containing SQL files to convert
            source_type: Source database type (e.g., 'snowflake', 'postgres')
            target_type: Target database type (default: 'oracle')
            target_version: Target database version (for API compatibility, may be unused)
            output_dir_base: Base directory for output (optional)
            original_run_timestamp: Existing timestamp for run continuation (optional)
            initial_source_type: Original source type for multi-step conversions (optional, typically same as source_type)
            
        Returns:
            Dictionary containing conversion results and statistics
            
        Note: Some parameters are kept for API backward compatibility but may not be actively used.
        """
        effective_source_type = initial_source_type or source_type
        
        self.logger.info(f"Starting SQL conversion: {effective_source_type} -> {target_type}")
        
        run_timestamp = original_run_timestamp or get_timestamp()
        output_dir = create_run_directory(output_dir_base, effective_source_type, run_timestamp)
        self.manual_review_logger = ManualReviewLogger(output_dir=str(output_dir), logger=self.logger)
        
        sql_files = find_sql_files(input_path)
        if not sql_files:
            self.logger.warning(f"No SQL files found in: {input_path}")
            empty_stats = create_processing_stats()
            return create_result_dictionary(
                "error", f"No SQL files found in {input_path}", 
                empty_stats, [], output_dir
            )

        converters = self.setup_converters(source_type, target_type, target_version)
        
        self.logger.info(f"Processing {len(sql_files)} SQL files from: {input_path}")
        self.logger.info(f"Output directory: {output_dir}")
        
        stats = create_processing_stats()
        # Ensure stats dictionary has all keys the orchestrator will use.
        # This is a localized fix to avoid changing a shared utility function.
        stats.setdefault('files_successful', 0)
        stats.setdefault('files_failed', 0)
        stats.setdefault('files_skipped', 0)
        stats.setdefault('statements_converted', 0)
        stats.setdefault('statements_with_errors', 0)
        stats.setdefault('skipped', 0)
        stats['total'] = len(sql_files)
        
        all_results = []
        
        for i, file_path in enumerate(sql_files, 1):
            self.logger.info(f"[{i}/{len(sql_files)}] Processing: {os.path.basename(file_path)}")
            
            source_conf = self._get_source_config(source_type, target_type)
            
            file_result = self._process_sql_file(
                file_path, input_path, output_dir, source_type, target_type, 
                source_conf, converters, stats
            )
            all_results.append(file_result)
            
            self._log_file_result(file_result)

        return self._create_conversion_summary(all_results, stats, output_dir)

    def setup_converters(self, source_type: str, target_type: str, target_version: str) -> Dict[str, Any]:
        """
        Create and configure all converter instances for the given database pair.
        Now uses a single statement converter that handles all non-procedural statement types.
        
        Args:
            source_type: Source database type
            target_type: Target database type  
            target_version: Target database version (may not be used by all converters)
            
        Returns:
            Dictionary of converter instances keyed by type
        """
        converters = {}
        db_pair = f"{source_type}_{target_type}"

        # Main statement converter handles all non-procedural SQL statements
        converters['statement'] = StatementConverter(
            source_dialect=source_type, 
            target_dialect=target_type, 
            target_version=target_version,
            manual_review_logger=self.manual_review_logger
        )

        # Procedural converters (FUNCTION/PROCEDURE handling) have been removed. Focusing solely on declarative DDL.

        converter_types = list(converters.keys())
        if converter_types:
            self.logger.info(f"Created converters: {', '.join(converter_types)} for {db_pair}")
        else:
            self.logger.warning(f"No converters available for {db_pair}")
        
        return converters

    # ========================================
    # FILE PROCESSING (Integrated from sql_conversion_utils.py)
    # ========================================

    def _process_sql_file(self, file_path: str, source_dir: str, output_dir: str, source_type: str, 
                         target_type: str, source_conf: dict, converters: Dict[str, Any], 
                         stats: dict) -> Dict:
        """
        Process a single SQL file through the appropriate converter.
        
        Args:
            file_path: Path to SQL file to process
            source_dir: Source directory path
            output_dir: Output directory path
            source_type: Source database type
            target_type: Target database type
            source_conf: Source-specific configuration
            converters: Dictionary of available converters
            stats: Statistics dictionary to update
            
        Returns:
            Dictionary containing file processing results
        """
        try:
            content = read_file_content(file_path)

            if not content:
                return self._create_file_error_result(file_path, "Could not read file", stats)
            
            # Procedural extractor logic removed — only declarative statements are processed.
            
            return self._process_regular_sql_file(file_path, content, source_dir, output_dir, 
                                               source_type, target_type, source_conf, converters, stats)
            
        except Exception as e:
            self.logger.error(f"Error processing {file_path}: {e}")
            return self._create_file_error_result(file_path, f"Processing error: {e}", stats)

    def _process_regular_sql_file(self, file_path: str, content: str, source_dir: str, output_dir: str,
                                source_type: str, target_type: str, source_conf: dict, 
                                converters: Dict[str, Any], stats: dict) -> Dict:
        """
        Process a regular (non-procedural) SQL file.
        """
        filename = os.path.basename(file_path)
        self.logger.info(f"Processing as regular SQL file: {filename}")
        
        skip_patterns = source_conf.get("skip_execution_patterns", [])
        statements, errors = self._parse_and_filter_statements(content, file_path, skip_patterns)
        
        if errors:
            stats['files_failed'] += 1
            return self._create_file_error_result(file_path, "Failed to parse file", stats, errors=errors)

        if not statements:
            stats['files_skipped'] += 1
            return self._create_file_skip_result(file_path, "No convertible statements found", stats)
        
        converted_statements, conversion_errors, conversion_skipped = self._convert_statement_list(
            statements, file_path, source_type, target_type, skip_patterns, converters, stats
        )
        stats['statements_converted'] += len(converted_statements)
        stats['statements_with_errors'] += len(conversion_errors)
        stats['skipped'] += conversion_skipped

        if not converted_statements and not conversion_errors:
            stats['files_skipped'] += 1
            return self._create_file_skip_result(file_path, "All statements skipped or failed", stats)

        output_file_path = os.path.join(output_dir, os.path.relpath(file_path, source_dir))
        write_result = write_converted_sql_file(output_file_path, converted_statements, self.logger, file_path)
        
        if write_result['status'] == 'success':
            stats['files_successful'] += 1
        else:
            stats['files_failed'] += 1
        
        return create_result_dictionary("success", f"Successfully converted {filename}", stats, 
                                        conversion_errors, output_dir)

    def _convert_statement_list(self, statements: List[Any], file_path: str, source_type: str, target_type: str,
                              skip_patterns: list, converters: Dict[str, Any], stats: dict) -> Tuple[List[str], List[Dict], int]:
        """
        Converts a list of sqlglot ASTs using the appropriate converter.
        This version assumes pre-filtering of skippable statements has occurred.
        """
        converted_statements = []
        errors = []
        
        converter = converters.get('statement')
        if not converter:
            self.logger.error("StatementConverter not found, cannot process regular SQL file.")
            for stmt in statements:
                errors.append({'original_sql': stmt.sql(), 'error': 'StatementConverter not found'})
            return [], errors, 0

        for stmt in statements:
            try:
                converted_parts, conversion_logs = converter.convert_statement(stmt)
                
                if converted_parts:
                    converted_statements.extend(converted_parts)
                
                for log in conversion_logs:
                    if log.get('action') == 'error':
                        errors.append({'original_sql': stmt.sql(), 'error': log.get('details', 'Unknown error from handler')})
                
            except Exception as e:
                self.logger.error(f"Error converting statement in {os.path.basename(file_path)}: {e}", exc_info=True)
                errors.append({'original_sql': stmt.sql(), 'error': str(e)})

        # Skipped count is now handled before this method, so we return 0
        return converted_statements, errors, 0

    # ========================================
    # HELPER METHODS
    # ========================================

    def _write_discarded_file(self, file_path: str, statements: List[str]):
        """Writes a list of discarded SQL statements to a file."""
        try:
            ensure_directory_exists(os.path.dirname(file_path))
            content = ';\n\n'.join(stmt.strip() for stmt in statements) + ';\n'
            write_file_content(file_path, content)
            self.logger.info(f"Wrote {len(statements)} discarded statement(s) to: {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to write discarded statements file to {file_path}: {e}", exc_info=True)

    def _create_file_error_result(self, file_path: str, message: str, stats: dict, 
                                errors: List = None) -> Dict:
        """Create standardized error result for file processing"""
        stats['files_failed'] += 1
        return create_result_dictionary("error", message, stats, errors or [], 
                                        source_file=file_path)

    def _create_file_skip_result(self, file_path: str, message: str, stats: dict) -> Dict:
        """Create standardized skip result for file processing"""
        stats['files_skipped'] += 1
        return create_result_dictionary("skipped", message, stats, [], source_file=file_path)

    def _get_source_config(self, source_type: str, target_type: str) -> Dict:
        """
        Get source-specific configuration settings, primarily skip patterns,
        from the appropriate dialect_behaviors.json file.
        """
        self.logger.debug(
            f"Loading 'dialect_behaviors.json' for {source_type}->{target_type} to extract skip patterns."
        )
        
        config = load_json_from_conversion_config(
            self.logger, 
            source_type, 
            target_type, 
            'ddl_conversion_rules', 
            'dialect_behaviors.json'
        )

        skip_patterns_list = []
        if config:
            skip_config = config.get('statement_skipping', {})
            if skip_config.get('enabled', False):
                skip_patterns_list = skip_config.get('patterns', [])
                self.logger.debug(f"Found {len(skip_patterns_list)} enabled skip patterns.")
            else:
                self.logger.debug("Statement skipping is disabled in config.")
        else:
            self.logger.warning(
                "Could not load 'dialect_behaviors.json'. Proceeding with no skip patterns."
            )

        return {"skip_execution_patterns": skip_patterns_list}

    def _log_file_result(self, file_result: Dict):
        """Logs the outcome of a single file's conversion."""
        status = file_result.get('status', 'unknown')
        filename = os.path.basename(file_result.get('source_file', 'Unknown file'))
        
        if status == 'success':
            self.logger.info(f"Successfully processed: {filename}")
        elif status == 'error':
            self.logger.error(f"Failed to process: {filename} - {file_result.get('message', 'Unknown error')}")
        elif status == 'skipped':
            skip_msg = file_result.get('message', 'Skipped')
            self.logger.info(f"Skipped: {filename} - {skip_msg}")
        elif status == 'success_with_warnings':
            self.logger.warning(f"Processed with warnings: {filename} - {file_result.get('message', 'Warnings occurred')}")
        else:
            self.logger.warning(f"Processed {filename} with unknown status: {status}")

        stats = file_result.get('statistics', {})
        if stats:
             self.logger.debug(
                 f"Stats for {filename}: "
                 f"Statements: {stats.get('statements_converted', 0)} converted, "
                 f"{stats.get('skipped', 0)} skipped, "
                 f"{stats.get('statements_with_errors', 0)} errors"
            )


    def _create_conversion_summary(self, all_results: List[Dict], stats: Dict, output_dir: str) -> Dict:
        """
        Create the final summary dictionary for the entire conversion run.
        """
        self.logger.info("="*50)
        self.logger.info("SQL CONVERSION SUMMARY")
        self.logger.info("="*50)
        self.logger.info(f"Total files processed: {stats['total']}")
        self.logger.info(f"  - Successful: {stats['files_successful']}")
        self.logger.info(f"  - Failed: {stats['files_failed']}")
        self.logger.info(f"  - Skipped: {stats['files_skipped']}")
        
        # The 'stats' dictionary is the final aggregate, so we use it directly.
        total_statements_converted = stats.get('statements_converted', 0)
        total_statements_with_errors = stats.get('statements_with_errors', 0)
        
        self.logger.info(f"Total statements converted: {total_statements_converted}")
        self.logger.info(f"Total statements with errors: {total_statements_with_errors}")

        summary = {
            "overall_statistics": stats,
            "files": all_results,
            "output_directory": output_dir
        }
        
        self._write_conversion_summary_to_file(summary, output_dir)
        
        # Finalize and write manual review log if entries were added
        self.manual_review_logger.write_manual_review_log()
        
        return summary

    def _write_conversion_summary_to_file(self, summary_data_dict: Dict, output_dir: str):
        """
        Writes the conversion summary to a JSON file in the output directory.
        """
        summary_file_path = os.path.join(output_dir, 'conversion_summary.json')
        try:
            # A default function for json.dump to handle non-serializable objects
            def json_default(o):
                if isinstance(o, (Path)):
                    return str(o)
                return f"<<non-serializable: {type(o).__name__}>>"

            with open(summary_file_path, 'w', encoding='utf-8') as f:
                json.dump(summary_data_dict, f, indent=4, default=json_default)
            self.logger.info(f"Conversion summary written to: {summary_file_path}")
        except Exception as e:
            self.logger.error(f"Failed to write conversion summary: {e}", exc_info=True)

    # ------------------------------------------------------------------
    # Unified helper – supports two historical call signatures:
    #   (content, file_path, source_dialect, source_conf, stats)
    #   (content, file_path, behavior_config)
    # We detect by arg count and map accordingly.
    # ------------------------------------------------------------------
    def _parse_and_filter_statements(self, content: str, file_path: str, skip_patterns: list[str]) -> tuple[list[exp.Expression], list[dict]]:
        """Parse SQL text and drop statements matching any skip pattern."""

        try:
            all_statements = parse(content, read=self.source_dialect)
        except Exception as e:
            self.logger.error("Error parsing %s: %s", os.path.basename(file_path), e)
            return [], [{"file": os.path.basename(file_path), "error": str(e)}]

        filtered: list[exp.Expression] = []
        for stmt in all_statements:
            # Temporarily drop comments when matching regexes
            original_comments = stmt.comments
            stmt.comments = []
            stmt_sql = stmt.sql(dialect=self.source_dialect).strip()
            stmt.comments = original_comments

            if any(re.search(p, stmt_sql, re.IGNORECASE) for p in skip_patterns):
                self.logger.info("Skipping statement in %s: %.100s", os.path.basename(file_path), stmt_sql)
                continue
            filtered.append(stmt)

        return filtered, []

    # ------------------------------------------------------------------
    # Procedural block stripper – generic (CREATE FUNCTION/PROCEDURE … END;)
    # ------------------------------------------------------------------
    @staticmethod
    def _strip_procedural_blocks(sql_text: str) -> str:
        begin_rx = re.compile(r"^\s*BEGIN\b", re.I)
        end_rx   = re.compile(r"^\s*END\s*;?\s*$", re.I)

        out_lines: list[str] = []
        depth = 0  # track nested BEGIN … END pairs

        for line in sql_text.splitlines():
            # Enter a procedural block
            if begin_rx.match(line):
                depth += 1
                continue  # strip the BEGIN line itself

            # Exit the current block (only when depth > 0)
            if depth and end_rx.match(line):
                depth -= 1
                continue  # strip the END line

            # Keep the line only when we are **not** inside any block
            if depth == 0:
                out_lines.append(line)

        return "\n".join(out_lines)