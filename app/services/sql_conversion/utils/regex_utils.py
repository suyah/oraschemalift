import re

def re_flags(flags_str: str) -> int:
    """
    Convert a flags string (e.g., 'IGNORECASE|DOTALL') into combined re flags.

    Args:
        flags_str: String containing flag names separated by '|'.

    Returns:
        Combined re flags integer.
    """
    flags = 0
    if not flags_str:
        return flags
    for part in flags_str.split('|'):
        p = part.strip().upper()
        if p == 'IGNORECASE':
            flags |= re.IGNORECASE
        elif p == 'DOTALL':
            flags |= re.DOTALL
        elif p == 'MULTILINE':
            flags |= re.MULTILINE
    return flags 