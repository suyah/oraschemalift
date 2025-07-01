"""
Handles the conversion of Data Definition Language (DDL) statements,
specifically focusing on AST-based transformations for CREATE TABLE.
"""
from typing import List, Dict, Any, Optional, Tuple
from sqlglot import exp, parse_one
from app.services.sql_conversion.converters.base_converter import BaseConverter
from app.services.sql_conversion.utils.config_loader import load_json_from_conversion_config
from app.utils.logger import setup_logger
import re

class DdlHandler(BaseConverter):
    """
    Handles Data Definition Language (DDL) statements by applying a series of
    configured, feature-based transformations to the AST.
    """
    def __init__(self, source_dialect: str, target_dialect: str, manual_review_logger: Optional[Any] = None):
        super().__init__(source_dialect, target_dialect)
        self.logger = setup_logger('DdlHandler')
        self.manual_review_logger = manual_review_logger
        self.behavior_config = self._load_behavior_config()
        self.data_type_mapping = self._load_data_type_mapping()
        self.paramless_targets = self._load_paramless_targets()
        self.dynamic_rules = self._load_dynamic_rules()
        self.output_aliases = self._load_output_aliases()
        self.logger.info("DdlHandler initialized with feature-centric configuration.")
        self.logger.debug(f"Behavior config loaded: {self.behavior_config}")

    def _load_behavior_config(self) -> Dict[str, Any]:
        """Loads behavior configuration from a JSON file."""
        try:
            return load_json_from_conversion_config(
                self.logger, self.source_dialect, self.target_dialect, 'ddl_conversion_rules', 'dialect_behaviors.json'
            )
        except FileNotFoundError:
            self.logger.warning("dialect_behaviors.json not found. DDL handler will use default behaviors.")
            return {}

    def _load_data_type_mapping(self) -> Dict[str, Any]:
        """Return a *flat* dict of source_type ➜ target_type mappings.

        The original implementation simply returned the raw contents of
        ``data_types.json``.  Since that file was refactored to the following
        structure:

        {
            "default": { "INT": "NUMBER(38,0)", … },
            "version_overrides": {
                "19c": { "default": { … } },
                "21c": { "default": { … } }
            }
        }

        the rest of the handler – which expects a *flat* mapping – silently
        stopped matching most data-types.  We now load the JSON, merge the
        top-level ``default`` section with any version-specific ``default``
        overrides (if available) and return that flattened dict.
        """

        try:
            raw_cfg: Dict[str, Any] = load_json_from_conversion_config(
                logger=self.logger,
                source_type=self.source_dialect,
                target_type=self.target_dialect,
                rules_subdirectory='ddl_conversion_rules',
                config_filename='data_types.json'
            )

            if not raw_cfg:
                # Missing or empty file – nothing we can map.
                return {}

            flat_map: Dict[str, str] = {}

            # Always start with the global defaults.
            if isinstance(raw_cfg.get('default'), dict):
                flat_map.update(raw_cfg['default'])

            # Merge in version-specific defaults if we know the target version
            # (e.g. Oracle "19c", "21c").  We access the attribute via
            # ``getattr`` so this code works even when the caller never sets
            # ``self.target_version``.
            target_ver = getattr(self, 'target_version', None)
            if target_ver and isinstance(raw_cfg.get('version_overrides'), dict):
                ver_dict = raw_cfg['version_overrides'].get(str(target_ver), {})
                if isinstance(ver_dict.get('default'), dict):
                    flat_map.update(ver_dict['default'])

            # -- Build underscore-less aliases ---------------------------------

            alias_map = {
                k.replace('_', ''): v
                for k, v in flat_map.items()
                if '_' in k and k.replace('_', '') not in flat_map
            }

            flat_map.update(alias_map)

            return flat_map

        except FileNotFoundError:
            self.logger.warning("data_types.json not found. DDL handler will not perform data type conversions.")
            return {}
        except Exception as e:
            # Any unexpected issue: log it and fall back to no conversion to
            # keep the pipeline running.
            self.logger.error(f"Failed to load/flatten data type mapping: {e}", exc_info=True)
            return {}

    def handle(self, ast: exp.Create) -> Tuple[List[str], List[Dict[str, Any]]]:
        """Main entry point for converting a single CREATE TABLE AST."""
        logs = []
        if not isinstance(ast, exp.Create) or ast.kind != 'TABLE':
            self.logger.warning(f"DdlHandler received a non-CREATE TABLE statement: {ast.sql()}. Skipping.")
            return [ast.sql(dialect=self.target_dialect)], []

        try:
            table_name = ast.this.this.name
            self.logger.debug(f"Handling DDL for table: {table_name}")

            # --- Apply sequence of AST transformations ---
            self._apply_data_type_conversions(ast)
            self._handle_virtual_columns(ast)
            self._remove_unsupported_clauses(ast)
            self._remove_with_properties(ast)
            
            table_comment, column_comments = self._extract_and_format_comments(ast)

            if ast.args.get('replace'):
                ast.set('replace', False)
                logs.append({'action': 'remove_replace', 'details': f"Changed 'CREATE OR REPLACE' to 'CREATE' for '{table_name}'."})

            # --- Transpile and generate final SQL ---
            converted_create_sql = ast.sql(dialect=self.target_dialect, pretty=True)
            converted_create_sql = self._post_cleanup_sql(converted_create_sql)
            final_sql_statements = [converted_create_sql]
            
            comment_sqls = self._generate_comment_sql(table_name, table_comment, column_comments)
            final_sql_statements.extend(comment_sqls)

            self.logger.info(f"Successfully converted data types for table '{table_name}'.")

        except Exception as e:
            error_log_message = f"Error handling statement: {e}. SQL: {ast.sql()}"
            self.logger.error(error_log_message, exc_info=True)
            logs.append({'action': 'error', 'details': error_log_message})
            final_sql_statements = [f"-- ERROR: {error_log_message}"]
        
        return final_sql_statements, logs

    def _apply_data_type_conversions(self, ast: exp.Expression):
        """Converts data types for all columns using the loaded mapping."""
        # The mapping is pre-flattened and already contains underscore-less
        # aliases, but we upper-case the keys for a quick lookup.
        type_map: Dict[str, str] = {k.upper(): v for k, v in self.data_type_mapping.items()}

        table_name = ast.this.this.name
        
        for kind_node in ast.find_all(exp.DataType):
            type_name = kind_node.this.name.upper()
            if type_name in type_map:
                target_type_str = type_map[type_name]

                # ---- Apply dynamic sizing rules ---------------------------------

                dyn_rule = self.dynamic_rules.get(type_name)
                if dyn_rule and kind_node.expressions:
                    try:
                        size_token = kind_node.expressions[0]
                        # Drill down if the param wraps a nested Literal.
                        if hasattr(size_token, 'this') and isinstance(size_token.this, exp.Literal):
                            size_literal = size_token.this.this
                        else:
                            size_literal = getattr(size_token, 'this', None)
                        size_val = int(size_literal) if size_literal is not None else None
                        if size_val is not None:
                            if size_val > dyn_rule.get('max_size', 4000):
                                target_type_str = dyn_rule.get('overflow_type', target_type_str)
                                # size no longer needed when overflowing
                                kind_node.set('expressions', None)
                            else:
                                tmpl = dyn_rule.get('template')
                                if tmpl:
                                    target_type_str = tmpl.format(size=size_val)
                    except Exception as e:
                        self.logger.warning(f"Dynamic rule evaluation failed for {type_name}: {e}")

                original_sql = kind_node.sql()
                try:
                    # ------------------------------------------------------------------
                    # 1. Choose the safest parse strategy
                    # ------------------------------------------------------------------
                    if target_type_str.upper() in self.paramless_targets:
                        # For BLOB, CLOB, etc. we parse the *literal* to keep the
                        # exact token and guarantee no size/precision slips in.
                        try:
                            new_kind_node = parse_one(target_type_str, read=self.target_dialect)
                        except Exception:
                            # Fallback to CAST path if literal is not accepted by parser
                            new_expression = parse_one(f"CAST(NULL AS {target_type_str})", read=self.target_dialect)
                            new_kind_node = new_expression.to if isinstance(new_expression, exp.Cast) else new_expression
                    else:
                        # All sized / complex targets still use the CAST helper so
                        # that precision & scale get rendered exactly once.
                        new_expression = parse_one(f"CAST(NULL AS {target_type_str})", read=self.target_dialect)
                        new_kind_node = new_expression.to if isinstance(new_expression, exp.Cast) else new_expression

                    # ------------------------------------------------------------------
                    # 2. Attach source precision only when allowed
                    # ------------------------------------------------------------------
                    if kind_node.expressions and new_kind_node.this.name.upper() not in self.paramless_targets:
                        new_kind_node.set('expressions', kind_node.expressions)

                    # ------------------------------------------------------------------
                    # 3. Replace node in AST
                    # ------------------------------------------------------------------
                    kind_node.replace(new_kind_node)
                    if kind_node.parent:
                        kind_node.parent.set('kind', new_kind_node)

                    self.logger.debug(f"Data Type: Replaced '{original_sql}' with '{new_kind_node.sql()}'.")

                except Exception as e:
                    self.logger.error(
                        f"Critical error during data type AST node replacement for '{type_name}': {e}",
                        exc_info=True,
                    )

        self.logger.info(f"Successfully converted data types for table '{table_name}'.")

    def _handle_virtual_columns(self, ast: exp.Create):
        """
        Converts Snowflake-style generated columns (e.g., `col AS (expr)`) to
        Oracle-style virtual columns (`col GENERATED ALWAYS AS (expr) VIRTUAL`).
        This method identifies columns with a `ComputedColumnConstraint`.
        """
        config = self.behavior_config.get("virtual_column_conversion", {})
        if not config.get("enabled"):
            self.logger.debug("Virtual column conversion is disabled in config. Skipping.")
            return

        self.logger.debug("Virtual column conversion is enabled. Checking for generated columns.")

        for column_def in ast.find_all(exp.ColumnDef):
            computed_constraint_node = column_def.find(exp.ComputedColumnConstraint)
            
            # Check if it's a computed column and not an IDENTITY column
            if computed_constraint_node and not column_def.find(exp.GeneratedAsIdentityColumnConstraint):
                column_name = column_def.this.sql()
                self.logger.debug(f"Found virtual column definition for: {column_name}")

                # The expression is the 'this' attribute of the ComputedColumnConstraint
                expression = computed_constraint_node.this
                
                # The node to remove is the parent ColumnConstraint wrapper
                constraint_to_remove = computed_constraint_node.parent

                try:
                    # Create the new Oracle-style constraint
                    oracle_constraint = exp.ColumnConstraint(
                        kind=exp.GeneratedAsIdentityColumnConstraint(
                            this=expression.copy(),  # Use a copy of the expression
                            always=True,
                            virtual=True
                        )
                    )
                    
                    # Modify the list of constraints
                    if constraint_to_remove in column_def.constraints:
                        column_def.constraints.remove(constraint_to_remove)
                    
                    column_def.constraints.append(oracle_constraint)
                    
                    self.logger.info(f"Successfully converted virtual column '{column_name}' to Oracle syntax.")
                
                except Exception as e:
                    self.logger.error(f"Failed to convert virtual column '{column_name}': {e}", exc_info=True)

    def _remove_unsupported_clauses(self, ast: exp.Expression):
        """Removes configured top-level clauses like CLUSTER BY from the AST."""
        config = self.behavior_config.get("clause_removal", {})
        if not config.get("enabled", False):
            return

        # 1) Remove explicit Cluster nodes within the AST.
        for cluster_node in list(ast.find_all(exp.Cluster)):
            parent = cluster_node.parent
            if parent and hasattr(parent, 'expressions') and cluster_node in parent.expressions:
                parent.expressions.remove(cluster_node)
                self.logger.debug("Removed CLUSTER BY clause via AST walk.")

        # 2) Run any additional clause-specific callbacks (extendable via map).
        #    Currently only explicit Cluster handled above; other clauses are
        #    cleaned up later in the post-processing regex step.

    def _remove_with_properties(self, ast: exp.Expression):
        """Removes configured properties from the WITH clause of a CREATE TABLE statement."""
        config = self.behavior_config.get("with_property_removal", {})
        if not config.get("enabled", False):
            return

        properties_to_remove = {prop.upper() for prop in config.get("properties", [])}
        properties_node = ast.args.get('properties')

        if not properties_node or not isinstance(properties_node, exp.Properties):
            return

        kept_properties = []
        for prop in properties_node.expressions:
            should_remove = False
            tag_indicator = 'TAG' in prop.sql().upper()
            if hasattr(prop, 'arg_key') and prop.arg_key in ['row_access_policy', 'tags']:
                should_remove = True
            # Handle TAG expressions not captured above
            elif tag_indicator:
                should_remove = True
            # Handle standard named properties from config
            elif isinstance(prop, exp.Property) and hasattr(prop, 'this') and hasattr(prop.this, 'name'):
                if prop.this.name.upper() in properties_to_remove:
                    should_remove = True
            
            if should_remove:
                self.logger.debug(f"Removed WITH property: {prop.sql()}")
            else:
                kept_properties.append(prop)
        
        if kept_properties:
            properties_node.set('expressions', kept_properties)
        else:
            ast.set('properties', None)

    def _extract_and_format_comments(self, ast: exp.Create) -> tuple[Optional[str], list[tuple[str, str]]]:
        """
        Extracts table and column comments from the AST, removes them, and formats
        them for SQL comments.
        """
        table_comment = None
        column_comments: list[tuple[str, str]] = []

        # Extract table comment
        table_comment_node = ast.find(exp.SchemaCommentProperty)
        if table_comment_node:
            table_comment = table_comment_node.this.this
            if table_comment_node.parent and hasattr(table_comment_node.parent, 'expressions'):
                try:
                    table_comment_node.parent.expressions.remove(table_comment_node)
                except ValueError:
                    self.logger.warning("Could not remove table comment node from AST.")

        # Extract column comments
        for column_def in ast.find_all(exp.ColumnDef):
            new_constraints = []
            for constraint in column_def.constraints:
                if isinstance(constraint.kind, exp.CommentColumnConstraint):
                    col_name = column_def.this.name  # original column identifier
                    comment_text = constraint.kind.this.this
                    column_comments.append((col_name, comment_text))
                else:
                    new_constraints.append(constraint)
            column_def.set('constraints', new_constraints)
            
        if table_comment or column_comments:
            self.logger.debug(f"Extracted comments. Table: {bool(table_comment)}, Columns: {len(column_comments)}")
            
        return table_comment, column_comments

    def _generate_comment_sql(self, table_name: str, table_comment: Optional[str], column_comments: list[tuple[str, str]]) -> List[str]:
        """Generates COMMENT ON SQL statements based on extracted comments and config templates."""
        config = self.behavior_config.get("comment_conversion", {})
        if not config.get("enabled", False):
            return []

        comment_sqls = []
        table_template = config.get("target_table_template")
        column_template = config.get("target_column_template")

        if table_comment and table_template:
            escaped_comment = table_comment.replace("'", "''")
            comment_sqls.append(table_template.format(table_name=table_name, comment_text=escaped_comment))

        if column_comments and column_template:
            for col_name, comment_text in column_comments:
                escaped_comment = comment_text.replace("'", "''")
                comment_sqls.append(
                    column_template.format(table_name=table_name, column_name=col_name, comment_text=escaped_comment)
                )
        return comment_sqls

    # ---------------------------------------------------------------------
    # Helper loaders / cleaners
    # ---------------------------------------------------------------------

    def _load_paramless_targets(self) -> set[str]:
        """Return the set of *target* data-types that must not keep size / precision.

        The list now lives inside ``data_types.json`` under the key
        "paramless_targets" so that integrators only have to manage **one**
        conversion file.  We keep the legacy .json file fallback for backward
        compatibility.
        """

        # Prefer the embedded list if present
        embedded = getattr(self, 'embedded_paramless', None)
        if embedded and isinstance(embedded, list):
            return {t.upper() for t in embedded}

        # If not yet loaded try the new section in data_types.json
        try:
            raw_cfg: Dict[str, Any] = load_json_from_conversion_config(
                logger=self.logger,
                source_type=self.source_dialect,
                target_type=self.target_dialect,
                rules_subdirectory='ddl_conversion_rules',
                config_filename='data_types.json'
            )
            lst = raw_cfg.get('paramless_targets', []) if isinstance(raw_cfg, dict) else []
            if lst:
                return {t.upper() for t in lst}
        except FileNotFoundError:
            pass

        # Fallback – also supports legacy standalone file for now.
        try:
            targets = load_json_from_conversion_config(
                logger=self.logger,
                source_type=self.source_dialect,
                target_type=self.target_dialect,
                rules_subdirectory='ddl_conversion_rules',
                config_filename='paramless_targets.json'
            )
            return {t.upper() for t in targets}
        except FileNotFoundError:
            return {'CLOB', 'BLOB', 'SDO_GEOMETRY', 'BOOLEAN', 'TEXT', 'NCLOB'}

    def _load_dynamic_rules(self) -> Dict[str, Any]:
        try:
            raw_cfg: Dict[str, Any] = load_json_from_conversion_config(
                logger=self.logger,
                source_type=self.source_dialect,
                target_type=self.target_dialect,
                rules_subdirectory='ddl_conversion_rules',
                config_filename='data_types.json'
            )
            return raw_cfg.get('dynamic_rules', {}) if isinstance(raw_cfg, dict) else {}
        except FileNotFoundError:
            return {}

    def _post_cleanup_sql(self, sql: str) -> str:
        """Strip lines that still contain disallowed clauses (config-driven)."""
        removal_clauses = [c.upper() for c in self.behavior_config.get('clause_removal', {}).get('clauses', [])]
        cleaned_lines = [
            line for line in sql.splitlines()
            if not any(clause in line.upper() for clause in removal_clauses)
        ]
        sql_out = "\n".join(cleaned_lines)

        # Replace any final aliases (e.g. TIMESTAMPLTZ -> TIMESTAMP WITH LOCAL TIME ZONE)
        if hasattr(self, 'output_aliases') and self.output_aliases:
            for alias, long_form in self.output_aliases.items():
                sql_out = sql_out.replace(alias, long_form)

        # ------------------------------------------------------------------
        # Fix TIMESTAMP WITH [LOCAL] TIME ZONE precision ordering
        # ------------------------------------------------------------------
        # sqlglot (and therefore our AST path) renders precision after the
        # zone phrase: "TIMESTAMP WITH LOCAL TIME ZONE(9)".  Oracle expects
        # the precision *before* the zone phrase: "TIMESTAMP(9) WITH LOCAL
        # TIME ZONE" (same for "WITH TIME ZONE").  Apply a simple, safe
        # regex rewrite so the entire codebase benefits without special AST
        # branches.

        def _reorder_ts_precision(match: re.Match) -> str:
            local_kw = match.group(1) or ""
            precision = match.group(2)
            local_kw_clean = ("LOCAL " if local_kw else "")
            return f"TIMESTAMP({precision}) WITH {local_kw_clean}TIME ZONE"

        sql_out = re.sub(
            r"TIMESTAMP WITH (LOCAL )?TIME ZONE\((\d+)\)",
            _reorder_ts_precision,
            sql_out,
            flags=re.IGNORECASE,
        )

        return sql_out

    def _load_output_aliases(self) -> Dict[str, str]:
        """Return a mapping of final type tokens to their verbose spelling.

        Stored under ``output_aliases`` in data_types.json to avoid another
        scattered config file.
        """
        try:
            raw_cfg: Dict[str, Any] = load_json_from_conversion_config(
                logger=self.logger,
                source_type=self.source_dialect,
                target_type=self.target_dialect,
                rules_subdirectory='ddl_conversion_rules',
                config_filename='data_types.json'
            )
            aliases = raw_cfg.get('output_aliases', {})
            return {k.upper(): v for k, v in aliases.items()} if isinstance(aliases, dict) else {}
        except FileNotFoundError:
            return {}