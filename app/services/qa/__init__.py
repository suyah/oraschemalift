"""QA utilities subpackage.

Exports helper(s) for running quick source→convert→target validation flows.
"""

from .roundtrip import run_roundtrip  # noqa: F401

__all__ = ["run_roundtrip"] 