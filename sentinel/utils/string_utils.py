# string_utils.py
"""
Utility functions for string manipulations.
"""


def to_snake(string: str) -> str:
    """Convert camelCase or PascalCase string to snake_case."""

    return ''.join(
        ['_' + i.lower() if i.isupper() else i for i in string]
    ).lstrip('_')
