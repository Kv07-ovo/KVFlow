"""Optional, explicitly enabled project adapters.

An adapter may know about one product's domain (KVStock is the first one). It is
never imported by :mod:`kvflow.core`: the core must start and complete a task in
an environment where that product, its database, its environment variables and
its data do not exist.

Two halves keep that honest:

* :mod:`kvflow.adapters.registry` is the durable opt-in. An adapter is off until
  a user enables it in the runtime home they are actually using, and every read
  path goes through ``require_enabled`` first, so "the adapter is off" is a
  refusal that names the command which would turn it on.
* each adapter module is a read-only reader. It cannot write to the adapted
  project, open a network connection or execute one of that project's commands.
"""

__all__ = ["kvstock", "registry", "kvstock_adapter"]
