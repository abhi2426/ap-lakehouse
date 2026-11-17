"""ap_platform — the metadata-driven AP lakehouse platform.

Modules here are imported by the notebooks in ``/notebooks``. Nothing in this
package reads a hardcoded table, tenant or path: everything that varies between
tenants comes from ``ap_control`` at runtime, seeded from the YAML in ``/config``.
"""

__version__ = "0.2.0"          # 0.1 = control plane, 0.2 = + synthetic sources
