"""
lint — assert that platform code is tenant-agnostic.

Parses each module with ``ast`` and reports string literals in EXECUTABLE code
that name a tenant (TEN001...) or an ERP (SAP, Oracle, Coupa). Docstrings and
comments are ignored: explaining why the design exists is allowed, branching
on a tenant is not. A hit means onboarding a new tenant would need a code
change, which is exactly what the metadata-driven design rules out.
"""

from __future__ import annotations

import ast
import os
import re

PATTERN = re.compile(r"\bTEN\d{3}\b|\b(SAP|ORACLE|COUPA|ORAFUSION)\b", re.IGNORECASE)


def _docstring_nodes(tree: ast.AST) -> set[int]:
    ids = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = getattr(node, "body", [])
            if body and isinstance(body[0], ast.Expr) and isinstance(getattr(body[0], "value", None), ast.Constant) \
                    and isinstance(body[0].value.value, str):
                ids.add(id(body[0].value))
    return ids


def scan_for_tenant_literals(path: str) -> list[str]:
    findings = []
    files = [path] if path.endswith(".py") else [
        os.path.join(d, f) for d, _, fs in os.walk(path) for f in fs if f.endswith(".py")]
    for fp in sorted(files):
        with open(fp, encoding="utf-8") as fh:
            tree = ast.parse(fh.read(), fp)
        docs = _docstring_nodes(tree)
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docs:
                m = PATTERN.search(node.value)
                if m:
                    findings.append(f"{os.path.basename(fp)}:{node.lineno}: '{m.group(0)}' in {node.value[:60]!r}")
    return findings
