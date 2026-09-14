"""KVFlow core: the generic, project-independent orchestration layer.

Everything in this package was extracted from the frozen Agent OS v1.0 release
and keeps its behaviour. It contains no product-specific domain code and imports
nothing from a host application: a project enters only through the registry, a
declarative profile and the capability ticket issued for one run.
"""

CORE_VERSION = "1.0.0"
