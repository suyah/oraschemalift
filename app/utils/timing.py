from time import time
from typing import Any, Callable, Dict

def timed(func: Callable, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Run *func* and return its result dict augmented with duration_s.

    This helper keeps timing logic in one place, avoiding scattered
    time.time() calls throughout the codebase.
    """
    start = time()
    result = func(*args, **kwargs)
    if isinstance(result, dict):
        result["duration_s"] = round(time() - start, 2)
    return result 