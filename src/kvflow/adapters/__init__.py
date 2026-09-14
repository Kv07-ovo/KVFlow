"""Optional, explicitly enabled project adapters.

An adapter may know about one product's domain (KVStock is the first one). It is
never imported by :mod:`kvflow.core`: the core must start and complete a task in
an environment where that product, its database, its environment variables and
its data do not exist.
"""

__all__ = ["kvstock"]
