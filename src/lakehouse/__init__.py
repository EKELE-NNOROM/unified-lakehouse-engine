"""Unified Lakehouse Engine — distributed analytics platform.

Public API:

- :class:`~lakehouse.engine.LakehouseEngine` — main orchestrator
- Connectors and pipelines — import from subpackages as needed

CLI (optional): install the package and run ``lakehouse --help``.
See :mod:`lakehouse.cli` for what the command-line layer does.
"""

from lakehouse.engine import LakehouseEngine

__all__ = ["LakehouseEngine"]
__version__ = "0.1.0"
