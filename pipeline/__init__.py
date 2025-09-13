# pipeline/__init__.py
"""
Package initializer for pipeline.

Important:
- Do NOT import collectors or services here.
- Keep only lightweight metadata and safe exports.
"""

__version__ = "20.1"

# Nothing else is imported at package import time.
# Collectors should be imported explicitly from `pipeline.collectors`.
__all__ = ["__version__"]
