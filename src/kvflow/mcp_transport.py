"""Public MCP transport surface.

The transport implementation lives in :mod:`kvflow.core.mcp_transport`. This
module re-exports it so callers (and the boundary test suite) have one stable
name, and so the transport can be versioned independently of the tool service.
"""

from __future__ import annotations

from .core.mcp_transport import *  # noqa: F401,F403
from .core.mcp_transport import __all__ as _CORE_ALL

__all__ = list(_CORE_ALL)
