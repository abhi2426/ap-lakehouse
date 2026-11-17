"""
contracts — what a source file is expected to look like, and how it changed.

A schema contract is the list of columns a source delivers, with a coarse type
class per column, captured from a known-good file. Every later file is
profiled the same way and compared to it. The comparison is the drift
detector, and its classification decides what ingestion does next:

==================  ===============  =============================================
drift_type          classification   action
==================  ===============  =============================================
COLUMN_ADDED        ADDITIVE         AUTO_ACCEPTED: log once, rescue the values
COLUMN_REMOVED      BREAKING         HALTED: this source only
COLUMN_RENAMED      BREAKING         HALTED: one column vanished, one appeared
TYPE_CHANGED        BREAKING         HALTED: a NUMBER column arrives as text
==================  ===============  =============================================

Why profile in Python rather than Spark
---------------------------------------
Drift is a property of each FILE, and a header-based Spark reader given several
files silently applies the first file's header to all of them. Profiling each
file's header and a sample of its rows with the standard library is cheap at
landing-zone sizes, sees exactly what the file says, and lets ingestion read
files with identical shapes together and files with different shapes apart.

Type classes are deliberately coarse: NUMBER, STRING, BOOLEAN. The contract is
there to catch "the amount is now text", not to re-implement type inference.
A column is NUMBER when at least ``NUMBER_SHARE`` of its non-empty sample
values parse as numbers, so the occasional injected decimal comma does not
masquerade as drift, while a source that switched to "$1,009,737.72" does.
"""

from __future__ import annotations

import csv
import io
import json
import re
from dataclasses import dataclass, field
from typing import Any

NUMBER_SHARE = 0.90       # baseline: a column is NUMBER if >= 90% of values parse
DRIFT_SHARE = 0.50        # drift: a NUMBER column has changed type if < 50% parse...
DRIFT_MIN_BAD = 3         # ...and at least this many values do not (small delta files)
SAMPLE_ROWS = 400
_NUM_RX = re.compile(r"^\s*[-+]?(\d+(\.\d*)?|\.\d+)([eE][-+]?\d+)?\s*$")


class PoisonFile(Exception):
    """The file cannot be read as the declared format at all."""


@dataclass
class Column:
    name: str
    type_class: str          # NUMBER | STRING | BOOLEAN
    ordinal: int
    numeric_share: float = 1.0   # observed share of non-empty values that parse as numbers
    non_numeric: int = 0         # observed count of non-empty values that do not
    optional: bool = False       # may be absent without counting as drift (rename transitions)

    def as_dict(self) -> dict:
        d = {"name": self.name, "type_class": self.type_class, "ordinal": self.ordinal}
        if self.optional:
            d["optional"] = True
        return d


@dataclass
class Profile:
    """What one file actually contains."""
    path: str
    columns: list[Column]
    rows_sampled: int
    malformed_rows: int = 0

    @property
    def names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def signature(self) -> str:
        return "|".join(self.names)


@dataclass
class Drift:
    drift_type: str
    classification: str
    column_name: str
    expected_type: str | None = None
    observed_type: str | None = None

    @property
    def action(self) -> str:
        return "AUTO_ACCEPTED" if self.classification == "ADDITIVE" else "HALTED"


@dataclass
class Contract:
    tenant_id: str
    entity: str
    version: int
    columns: list[Column] = field(default_factory=list)

    @property
    def names(self) -> list[str]:
        return [c.name for c in self.columns]

    def type_of(self, name: str) -> str | None:
        for c in self.columns:
            if c.name == name:
                return c.type_class
        return None

    def to_json(self) -> str:
        return json.dumps([c.as_dict() for c in self.columns])

    @staticmethod
    def from_json(tenant_id: str, entity: str, version: int, payload: str) -> "Contract":
        cols = [Column(c["name"], c["type_class"], int(c["ordinal"]), optional=bool(c.get("optional")))
                for c in json.loads(payload)]
        return Contract(tenant_id, entity, version, cols)


# ---------------------------------------------------------------------------
# Profiling
# ---------------------------------------------------------------------------

def _classify(values: list[Any]) -> tuple[str, float, int]:
    """(type_class, numeric_share, non_numeric_count) for a column sample."""
    vals = [v for v in values if v is not None and str(v).strip() != ""]
    if not vals:
        return "STRING", 0.0, 0
    if all(isinstance(v, bool) for v in vals):
        return "BOOLEAN", 0.0, 0
    numeric = sum(1 for v in vals if (isinstance(v, (int, float)) and not isinstance(v, bool))
                  or (isinstance(v, str) and _NUM_RX.match(v)))
    share = numeric / len(vals)
    return ("NUMBER" if share >= NUMBER_SHARE else "STRING"), share, len(vals) - numeric


def _col(name: str, values: list[Any], ordinal: int) -> Column:
    t, share, bad = _classify(values)
    return Column(name, t, ordinal, share, bad)


def profile_file(path: str, source_format: str, *, delimiter: str | None = None,
                 has_header: bool | None = None, quote_char: str | None = None,
                 encoding: str = "UTF-8", positional_names: list[str] | None = None) -> Profile:
    """Profile one landed file. Raises PoisonFile if it cannot be read at all."""
    fmt = (source_format or "").upper()
    try:
        with open(path, "rb") as fh:
            raw = fh.read()
    except OSError as exc:
        raise PoisonFile(f"cannot open: {exc}") from exc
    if not raw.strip():
        raise PoisonFile("empty file")
    try:
        text = raw.decode(encoding or "UTF-8")
    except (UnicodeDecodeError, LookupError) as exc:
        raise PoisonFile(f"not decodable as {encoding}: {exc}") from exc

    if fmt == "JSON":
        return _profile_json(path, text)
    if fmt in ("CSV", "DELIMITED"):
        return _profile_delimited(path, text, delimiter or ",", bool(has_header), quote_char,
                                  positional_names or [])
    raise PoisonFile(f"unsupported source_format {source_format}")


def _profile_json(path: str, text: str) -> Profile:
    order: list[str] = []
    samples: dict[str, list[Any]] = {}
    n = bad = 0
    for line in text.splitlines():
        if not line.strip():
            continue
        n += 1
        try:
            obj = json.loads(line)
            if not isinstance(obj, dict):
                raise ValueError("not an object")
        except ValueError:
            bad += 1
            continue
        if n <= SAMPLE_ROWS:
            for k, v in obj.items():
                if k not in samples:
                    order.append(k)
                    samples[k] = []
                samples[k].append(v)
    if n == 0 or bad / n > 0.5:
        raise PoisonFile(f"{bad} of {n} lines are not JSON objects")
    cols = [_col(k, samples[k], i) for i, k in enumerate(order)]
    return Profile(path, cols, min(n, SAMPLE_ROWS), bad)


def _profile_delimited(path: str, text: str, delimiter: str, has_header: bool,
                       quote_char: str | None, positional_names: list[str]) -> Profile:
    if quote_char:
        reader = csv.reader(io.StringIO(text), delimiter=delimiter, quotechar=quote_char)
    else:
        reader = csv.reader(io.StringIO(text), delimiter=delimiter, quoting=csv.QUOTE_NONE)
    rows = []
    try:
        for i, r in enumerate(reader):
            if r and any(x.strip() for x in r):
                rows.append(r)
            if len(rows) > SAMPLE_ROWS:
                break
    except csv.Error as exc:
        raise PoisonFile(f"csv parse error: {exc}") from exc
    if not rows:
        raise PoisonFile("no rows")
    if has_header:
        header, data = [h.strip() for h in rows[0]], rows[1:]
    else:
        width = max(len(r) for r in rows)
        header = [positional_names[i] if i < len(positional_names) and positional_names[i] else f"_c{i}"
                  for i in range(width)]
        data = rows
    width = len(header)
    malformed = sum(1 for r in data if len(r) != width)
    if data and malformed / len(data) > 0.5:
        raise PoisonFile(f"{malformed} of {len(data)} sampled rows have the wrong field count (expected {width})")
    cols = []
    for i, name in enumerate(header):
        cols.append(_col(name, [r[i] if i < len(r) else None for r in data], i))
    return Profile(path, cols, len(data), malformed)


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------

def compare(contract: Contract, prof: Profile) -> list[Drift]:
    """Classify how a file differs from its contract. Empty list = conforms."""
    exp, obs = contract.names, prof.names
    optional = {c.name for c in contract.columns if c.optional}
    added = [c for c in obs if c not in exp]
    removed = [c for c in exp if c not in obs and c not in optional]
    out: list[Drift] = []
    if removed and added and len(removed) == len(added):
        for old, new in zip(removed, added):
            out.append(Drift("COLUMN_RENAMED", "BREAKING", f"{old} -> {new}"))
    else:
        out += [Drift("COLUMN_REMOVED", "BREAKING", c) for c in removed]
        out += [Drift("COLUMN_ADDED", "ADDITIVE", c) for c in added]
    obs = {c.name: c for c in prof.columns}
    for c in contract.columns:
        o = obs.get(c.name)
        # Evidence, not a vote: a few injected "1.234,56" values in a 6-row
        # delta file are data defects for DQ to catch, not a schema change.
        if (c.type_class == "NUMBER" and o is not None and o.non_numeric >= DRIFT_MIN_BAD
                and o.numeric_share < DRIFT_SHARE):
            out.append(Drift("TYPE_CHANGED", "BREAKING", c.name, "NUMBER", "STRING"))
    return out


def accept(contract: Contract, prof: Profile) -> Contract:
    """The contract a steward approves when they accept a drifted file's shape.

    Renamed and added columns adopt the observed names; a NUMBER column that
    arrived as text becomes STRING so the drift stops firing. Note what this
    does NOT do: it does not make the values usable. That is a mapping change
    (a transform_expr that strips the currency symbol), which is the point of
    exercise E2 — accepting the shape and fixing the meaning are separate acts.
    """
    observed = set(prof.names)
    cols = []
    for i, c in enumerate(prof.columns):
        prev = contract.type_of(c.name)
        drifted = prev == "NUMBER" and c.non_numeric >= DRIFT_MIN_BAD and c.numeric_share < DRIFT_SHARE
        t = c.type_class if prev is None else ("STRING" if drifted else prev)
        # A column new in this shape is optional: other feeds into the same
        # entity (a corrections export, say) may not have moved to it yet.
        cols.append(Column(c.name, t, i, optional=prev is None or _is_opt(contract, c.name)))
    # A column the new shape dropped stays in the contract as optional, so a
    # feed still sending the old name keeps loading during the transition.
    for c in contract.columns:
        if c.name not in observed:
            cols.append(Column(c.name, c.type_class, len(cols), optional=True))
    return Contract(contract.tenant_id, contract.entity, contract.version + 1, cols)


def _is_opt(contract: Contract, name: str) -> bool:
    return any(c.optional for c in contract.columns if c.name == name)
