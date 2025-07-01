"""
Sqlglot Monkey-Patch for Custom Snowflake Syntax

This module extends sqlglot's built-in Snowflake dialect to support
custom, non-standard DDL clauses that are specific to a particular
environment. By centralizing the patch here, we can apply it once
at application startup.

WHAT THIS FILE DOES:
====================
- Defines custom Expression types for proprietary syntax 
  (e.g., `_RowAccessPolicyProperty`, `_TagProperty`).
- Creates a patched Snowflake Parser that can recognize this syntax.
- Creates a patched Snowflake Generator that can correctly transpile it.
- Provides a single function, `apply_sqlglot_snowflake_patch`, to 
  apply these changes to the sqlglot library at runtime.
- Uses a guard (`_patched`) to ensure the patch is applied only once.

"""
import logging
from sqlglot import exp
from sqlglot.dialects.snowflake import Snowflake
from sqlglot.generator import Generator as SQLGlotGenerator
from sqlglot.parser import Parser as SQLGlotParser
from sqlglot.tokens import TokenType as SQLGlotTokenType

# 1. Define Custom Expression Types
# ---------------------------------
# These classes represent the custom syntax elements in the AST.

class _RowAccessPolicyProperty(exp.Property):
    """Represents `WITH ROW ACCESS POLICY <name> ON (<cols>...)`"""
    arg_key = 'row_access_policy'

class _TagProperty(exp.Property):
    """Represents `WITH TAG (<tag_name> = '<tag_value>', ...)`"""
    arg_key = 'tags'


# 2. Create a Patched Parser
# --------------------------
# This parser inherits from the standard Snowflake parser and adds
# logic to handle the custom properties.

class _PatchedSnowflakeParser(Snowflake.Parser):
    
    def _parse_property(self) -> exp.Expression:
        """
        Overrides the base property parser to look for the custom clauses.
        """
        # Use _match_text_seq for robust, case-insensitive keyword matching.
        if self._match_text_seq('ROW', 'ACCESS', 'POLICY'):
            name = self._parse_id_var()
            self._match(SQLGlotTokenType.ON)
            columns = self._parse_wrapped_csv(self._parse_id_var)
            return self.expression(_RowAccessPolicyProperty, this=name, expressions=columns)

        if self._match_text_seq('TAG'):
            self._advance() # Consume the 'TAG' keyword
            # _parse_eq handles `key = value` expressions.
            tags = self._parse_wrapped_csv(self._parse_eq)
            return self.expression(_TagProperty, this=tags)
        
        return super()._parse_property()


# 3. Create a Patched Generator
# -----------------------------
# This generator knows how to convert the custom Expression types back into SQL.

class _PatchedSnowflakeGenerator(Snowflake.Generator):
    
    TRANSFORMS = {
        **Snowflake.Generator.TRANSFORMS,
        # The `this` argument corresponds to the policy name.
        # The `expressions` argument holds the list of columns.
        _RowAccessPolicyProperty: lambda self, e: f"ROW ACCESS POLICY {self.sql(e, 'this')} ON ({self.expressions(e, 'expressions')})",
        
        # The `this` argument holds the list of `key = value` tag expressions.
        # self.sql(e, 'this') correctly formats the parenthesized list.
        _TagProperty: lambda self, e: f"TAG {self.sql(e, 'this')}",
    }


# 4. Create the Public Patching Function
# --------------------------------------
# This is the single entry point to activate the patch.

def apply_sqlglot_snowflake_patch():
    """
    Applies the custom parser and generator to the sqlglot Snowflake dialect.
    
    This function is designed to be called once at application startup.
    It includes a guard to prevent it from running multiple times.
    """
    # Guard against multiple applications of the patch.
    if getattr(Snowflake, "_custom_patched", False):
        return

    Snowflake.Parser = _PatchedSnowflakeParser
    Snowflake.Generator = _PatchedSnowflakeGenerator
    
    Snowflake._custom_patched = True
    logging.info("Applied custom monkey-patch to sqlglot for Snowflake DDL.")