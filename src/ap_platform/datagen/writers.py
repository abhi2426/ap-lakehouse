"""
writers — render generated AP data into each tenant's native file format and
land it in the bronze inbox volume under a dated folder structure.

What this module owns
---------------------
1. **Format.** TEN001 gets headerless pipe-delimited ISO-8859-1 extracts with a
   positional layout; TEN002 gets newline-delimited JSON with CDC columns;
   TEN003 gets quoted UTF-8 CSV with hyphenated headers.
2. **Representational defects.** Date formats, unparseable dates, thousands
   separators, decimal commas, currency symbols, blank currency codes, and
   whitespace or case damage on join keys. These are facts about the *file*, so
   they belong here and not in ``generators``.
3. **Delivery.** One initial load plus ~20 daily deltas, each in its own dated
   folder, honouring each tenant's declared ``load_type``:
   FULL_SNAPSHOT (TEN001), CDC (TEN002), INCREMENTAL (TEN003).
4. **Schema drift.** The three ``DriftEvent`` entries from ``defects`` are
   applied from their effective day onwards, in the shape each format makes
   possible: an appended column, a retyped value, a renamed header.

Why delivery is modelled as an event log
----------------------------------------
Each record becomes one or more events: I on the day it first appears, U when a
later delta restates it, D when it is removed. A full snapshot for day *d* is
then a fold of every event up to *d*; a CDC delta for day *d* is the slice of
events at *d*. Modelling it this way is what lets the three load types come out
of one plan, and it is why the initial load and the deltas reconcile: the same
log produces both.

The load types are not interchangeable, and the differences are deliberate:

* TEN001 (FULL_SNAPSHOT) expresses a deletion by *absence* from the next
  snapshot. Ingestion must therefore diff, not merge blindly.
* TEN002 (CDC) states the operation explicitly and carries a sequence, so
  out-of-order delivery is recoverable.
* TEN003 (INCREMENTAL, watermark on ``last-modified``) **cannot express a
  deletion at all.** A cancelled invoice simply stops being emitted, and an
  incremental reader will never notice. That is a real control gap, it is left
  in on purpose, and Step 6 records it in the control register rather than
  papering over it with a reconciliation hack.

Idempotency
-----------
Everything is derived from the seed in ``WriteConfig``, so a re-run writes
byte-identical files. ``purge=True`` clears each tenant's inbox subtree first,
so a re-run at a different ``scale`` cannot leave a previous run's files behind
to be silently picked up by the next ingestion batch.

Free Edition note
-----------------
TEN001 is a full-snapshot source, so in production it would land a complete
extract of every entity every day: 21 snapshots of the invoice ledger. That is
the correct behaviour and the Step 3 framework handles it identically, but
materialising it here would write hundreds of thousands of rows of landing files
for no extra teaching value. ``snapshot_days`` therefore materialises three
snapshots by default (first day, the drift day, last day) and the rest of the
days have no TEN001 files. Set ``snapshot_days=None`` to write all of them.
"""

from __future__ import annotations

import csv
import io
import json
import os
import random
import shutil
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from itertools import count
from typing import Any, Callable, Iterable, Iterator

from . import defects as dfx
from . import generators as gen

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class WriteConfig:
    """Where files land, over what days, and how much of the data is held back."""

    root: str = "/Volumes/ap_bronze/landing/inbox"
    initial_load_date: date = date(2026, 7, 1)
    n_delta_days: int = 20

    # A slice of each parent entity is withheld from the initial load and released
    # across the delta days, so the deltas contain genuine inserts and not only
    # restatements. Without this, nothing in Step 3 ever exercises an insert path.
    holdback_invoice_pct: float = 0.04
    holdback_po_pct: float = 0.01
    holdback_payment_pct: float = 0.03

    update_pct: float = 0.008          # share of day-0 rows restated per delta day
    delete_pct: float = 0.0008         # CDC deletes; only TEN002 can express these
    late_correction_from_day: int = 6  # TEN003 post-close corrections start here

    snapshot_days: tuple[int, ...] | None = ()   # () = auto, None = every day
    seed: str = "ap-poc-v1"
    purge: bool = True

    def extract_date(self, day: int) -> date:
        return self.initial_load_date + timedelta(days=day)


# ---------------------------------------------------------------------------
# Value formatting. Every defect style in these tables maps to a DefectSpec.
# ---------------------------------------------------------------------------

_MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
           "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def fmt_date(value: date | datetime | None, style: str) -> str:
    if value is None:
        # SAP's own null-date convention is eight zeroes, not an empty string.
        return "00000000" if style.startswith("SAP") else ""
    if isinstance(value, datetime):
        value = value.date() if style not in ("ISO_TS", "ISO_TZ", "SAP_TS") else value
    if style == "SAP":
        return value.strftime("%Y%m%d")
    if style == "SAP_TS":
        return value.strftime("%Y%m%d%H%M%S")
    if style == "SAP_NULL":
        return "00000000"
    if style == "ISO":
        return value.strftime("%Y-%m-%d")
    if style == "ISO_TS":
        return value.strftime("%Y-%m-%dT%H:%M:%S")
    if style == "ISO_TZ":
        # Same instant, offset notation. try_to_date on substr(1,10) survives it;
        # try_to_timestamp with a fixed pattern does not. That asymmetry is the point.
        return value.strftime("%Y-%m-%dT%H:%M:%S+10:00")
    if style == "DMY":
        return value.strftime("%d/%m/%Y")
    if style == "MDY":
        return value.strftime("%m/%d/%Y")
    if style == "DMON":
        return f"{value.day}-{_MONTHS[value.month - 1]}-{value.year}"
    if style == "IMPOSSIBLE":
        # Well-formed and completely invalid: the 31st of February.
        return f"31/02/{value.year}"
    if style == "GARBAGE":
        return "N/A"
    raise ValueError(f"unknown date style {style!r}")


_CCY_SYMBOL = {"SYMBOL_EUR": "\u20ac", "SYMBOL_USD": "$", "SYMBOL_AUD": "A$"}


def fmt_amount(value: float | None, style: str, numeric: bool = False) -> Any:
    if value is None:
        return None if numeric and style == "PLAIN" else ""
    if style == "PLAIN":
        return round(float(value), 2) if numeric else f"{value:.2f}"
    neg = value < 0
    body = f"{abs(value):,.2f}"                     # 1,234.56
    if style == "THOUSANDS":
        out = body
    elif style == "EU_COMMA":                       # 1234,56
        out = f"{abs(value):.2f}".replace(".", ",")
    elif style == "EU_FULL":                        # 1.234,56
        out = body.replace(",", "\x00").replace(".", ",").replace("\x00", ".")
    elif style == "SPACED":
        out = f" {abs(value):.2f} "
    elif style in _CCY_SYMBOL:
        out = f"{_CCY_SYMBOL[style]}{body}"
    else:
        raise ValueError(f"unknown amount style {style!r}")
    return f"-{out}" if neg else out


def fmt_key(value: str | None, style: str) -> str:
    if value is None:
        return ""
    if style == "PLAIN":
        return value
    if style == "TRAIL":
        return value + "  "
    if style == "LEAD":
        return " " + value
    if style == "BOTH":
        return f" {value} "
    if style == "LOWER":
        return value.lower()
    if style == "MIXED":
        return "".join(c.lower() if i % 2 else c.upper() for i, c in enumerate(value))
    raise ValueError(f"unknown key style {style!r}")


def fmt_ccy(value: str | None, style: str) -> str:
    if style == "PLAIN":
        return value or ""
    if style == "BLANK":
        return ""
    if style in _CCY_SYMBOL:
        return _CCY_SYMBOL[style]
    raise ValueError(f"unknown currency style {style!r}")


# Which defect code drives which kind of column, and which style pool it draws from.
_REPR_PLAN: tuple[tuple[str, str, str], ...] = (
    ("DATE_FORMAT_CHAOS", "date", "date_alt"),
    ("DATE_UNPARSEABLE", "date", "date_bad"),
    ("CURRENCY_FORMAT_CHAOS", "amount", "amt_alt"),
    ("CURRENCY_CODE_INVALID", "ccy", "ccy_bad"),
    ("KEY_WHITESPACE", "key", "key_ws"),
    ("KEY_CASE_INCONSISTENT", "key", "key_case"),
)

# Each tenant damages its data in the ways its own stack makes possible. A German
# SAP extract produces decimal commas and eight-zero null dates; a Coupa CSV
# produces three date formats and dollar signs in amount columns.
_STYLE_POOL: dict[str, dict[str, tuple[str, ...]]] = {
    "TEN001": {
        "date_alt": ("DMY", "ISO"),
        "date_bad": ("SAP_NULL", "GARBAGE"),
        "amt_alt": ("EU_COMMA", "EU_FULL", "SPACED"),
        "ccy_bad": ("BLANK", "SYMBOL_EUR"),
        "key_ws": ("TRAIL", "LEAD"),
        "key_case": ("LOWER",),
    },
    "TEN002": {
        "date_alt": ("ISO_TZ", "MDY"),
        "date_bad": ("GARBAGE",),
        "amt_alt": ("THOUSANDS", "SYMBOL_USD"),
        "ccy_bad": ("BLANK", "SYMBOL_USD"),
        "key_ws": ("TRAIL",),
        "key_case": ("LOWER",),
    },
    "TEN003": {
        "date_alt": ("DMY", "DMON"),
        "date_bad": ("IMPOSSIBLE", "GARBAGE"),
        "amt_alt": ("THOUSANDS", "SYMBOL_AUD", "EU_FULL"),
        "ccy_bad": ("BLANK", "SYMBOL_AUD"),
        "key_ws": ("TRAIL", "LEAD", "BOTH"),
        "key_case": ("LOWER", "MIXED"),
    },
}


class RowCtx:
    """Per-record rendering context: default styles, plus any injected defect."""

    __slots__ = ("extract_date", "day", "dec", "numeric", "seq", "op")

    def __init__(self, extract_date: date, day: int, dec: dict[str, str],
                 numeric: bool, seq: int = 0, op: str = "I") -> None:
        self.extract_date = extract_date
        self.day = day
        self.dec = dec
        # `numeric` marks a JSON source: amounts stay numbers and an absent value
        # is a real null. Delimited and CSV sources have no null, only emptiness,
        # which is why their reader_options declare nullValue explicitly.
        self.numeric = numeric
        self.seq = seq
        self.op = op

    def d(self, col: str, value: Any, default: str) -> Any:
        out = fmt_date(value, self.dec.get(col, default))
        return (None if out == "" else out) if self.numeric else out

    def a(self, col: str, value: Any) -> Any:
        style = self.dec.get(col, "PLAIN")
        return fmt_amount(value, style, numeric=self.numeric and style == "PLAIN")

    def k(self, col: str, value: Any) -> str:
        return fmt_key(value, self.dec.get(col, "PLAIN"))

    def c(self, col: str, value: Any) -> str:
        return fmt_ccy(value, self.dec.get(col, "PLAIN"))


# ---------------------------------------------------------------------------
# Layouts. One per tenant per entity: the source column names, in order, and a
# function that renders one neutral record into them.
# ---------------------------------------------------------------------------


@dataclass
class Layout:
    entity: str                       # canonical entity = folder name in the inbox
    stem: str                         # file name stem, must match ingestion file_glob
    attr: str                         # attribute on TenantData
    columns: tuple[str, ...]
    build: Callable[[dict, RowCtx], dict]
    numeric_amounts: bool = False
    defect_targets: dict[str, tuple[str, ...]] = field(default_factory=dict)


def _pk(*parts: Any) -> str:
    return "|".join("" if p is None else str(p) for p in parts)


def _surrogate(value: str | None) -> str | None:
    """The numeric tail of a business key, used where a source carries an
    integer surrogate id rather than the business number (Oracle does this for
    every parent key, which is why PO_HEADER_ID and SEGMENT1 both have to be
    kept and mapped separately)."""
    if not value:
        return None
    digits = "".join(ch for ch in value if ch.isdigit())
    return digits or None


# --- TEN001: SAP-style positional -----------------------------------------

def _t1_layouts() -> list[Layout]:
    def lfa1(r: dict, c: RowCtx) -> dict:
        return {
            "LIFNR": r["vendor_id"], "NAME1": r["vendor_name"],
            "NAME2": r["vendor_name_2"] or "", "LAND1": r["country_code"],
            "ORT01": r["city"], "PSTLZ": r["postal_code"],
            "STCD1": r["tax_id"] or "", "STCEG": r["vat_registration"] or "",
            "ZTERM": r["payment_terms_src"],
            "SPERR": "X" if r["is_blocked"] else "",
            "LOEVM": "X" if r["is_marked_delete"] else "",
            "ERDAT": c.d("ERDAT", r["created_date"], "SAP"),
            "ERNAM": r["created_by"],
        }

    def lfbk(r: dict, c: RowCtx) -> dict:
        return {
            "LIFNR": r["vendor_id"], "BANKS": r["bank_country"],
            "BANKL": r["bank_key"], "BANKN": r["bank_account"],
            "BKONT": r["bank_control_key"] or "", "KOINH": r["account_holder"],
            "BVTYP": r["bank_type"] or "",
            "CHANGE_DATE": c.d("CHANGE_DATE", r["change_date"], "SAP"),
            "CHANGE_USER": r["changed_by"],
            "VERIFIED_FLAG": "X" if r["is_verified"] else "",
        }

    def ekko(r: dict, c: RowCtx) -> dict:
        return {
            "EBELN": r["po_number"], "LIFNR": r["vendor_id"],
            "BUKRS": r["company_code"],
            "BEDAT": c.d("BEDAT", r["po_date"], "SAP"),
            "WAERS": r["currency"], "EKGRP": r["buyer"],
            "BSART": r["po_type"], "ZTERM": r["terms_src"], "ERNAM": r["created_by"],
        }

    def ekpo(r: dict, c: RowCtx) -> dict:
        return {
            "EBELN": r["po_number"], "EBELP": f"{r['po_line_number']:05d}",
            "TXZ01": r["material_desc"],
            "MENGE": c.a("MENGE", r["quantity"]), "MEINS": r["uom"],
            "NETPR": c.a("NETPR", r["unit_price"]), "NETWR": c.a("NETWR", r["net_value"]),
            "EINDT": c.d("EINDT", r["delivery_date"], "SAP"),
            "SAKTO": r["gl_account"], "KOSTL": r["cost_center"],
            "MWSKZ": r["tax_code"] or "",
            "PSTYP": "9" if r["is_service"] else "0",
            "WAERS": r["currency"],
        }

    def ekbe(r: dict, c: RowCtx) -> dict:
        rev = r["is_reversal"]
        return {
            "EBELN": r["po_number"], "EBELP": f"{r['po_line_number']:05d}",
            "ZEKKN": "00",
            # VGABE 1 = goods movement. Invoice receipts would be 2; this generator
            # emits only goods movements here, and silver must not assume that.
            "VGABE": "1", "GJAHR": str(r["receipt_date"].year),
            "BELNR": r["receipt_doc_id"], "BUZEI": f"{r['receipt_line']:04d}",
            "BWART": r["movement_type"],
            "BUDAT": c.d("BUDAT", r["receipt_date"], "SAP"),
            "MENGE": c.a("MENGE", abs(r["quantity"])), "MEINS": r["uom"],
            "DMBTR": c.a("DMBTR", abs(r["amount"])), "WAERS": r["currency"],
            "ERNAM": r["posted_by"],
            "SHKZG": "H" if rev else "S",
        }

    def rbkp(r: dict, c: RowCtx) -> dict:
        # Trap 2: SAP emits the amount unsigned and carries the sign in SHKZG.
        credit = r["gross_amount"] < 0 or r["doc_type"] == "CREDIT_MEMO"
        return {
            "BUKRS": r["company_code"], "BELNR": r["invoice_doc_id"],
            "GJAHR": str(r["fiscal_year"]),
            "XBLNR": c.k("XBLNR", r["supplier_invoice_number"]),
            "LIFNR": c.k("LIFNR", r["vendor_id"]),
            "BLDAT": c.d("BLDAT", r["invoice_date"], "SAP"),
            "BUDAT": c.d("BUDAT", r["posting_date"], "SAP"),
            "CPUDT": c.d("CPUDT", r["entry_date"], "SAP"),
            "RMWWR": c.a("RMWWR", abs(r["gross_amount"])),
            "WMWST": c.a("WMWST", abs(r["tax_amount"])),
            "SHKZG": "H" if credit else "S",
            "WAERS": c.c("WAERS", r["currency"]),
            "ZTERM": r["terms_src"], "ZLSPR": r["payment_block"] or "",
            "BLART": "RE" if r["doc_type"] == "INVOICE" else "RG",
            "EBELN": r["po_number"] or "",
            "SOURCE_CHANNEL": r["capture_channel"],
            "CAPTURE_TS": c.d("CAPTURE_TS", r["capture_ts"], "SAP_TS"),
        }

    def rseg(r: dict, c: RowCtx) -> dict:
        credit = r["net_amount"] < 0
        return {
            "BUKRS": r["company_code"], "BELNR": r["invoice_doc_id"],
            "GJAHR": str(r["fiscal_year"]), "BUZEI": f"{r['line_number']:04d}",
            "EBELN": r["po_number"] or "",
            "EBELP": f"{r['po_line_number']:05d}" if r["po_line_number"] else "",
            "MENGE": c.a("MENGE", r["quantity"]), "BSTME": r["uom"],
            "WRBTR": c.a("WRBTR", abs(r["net_amount"])),
            "SHKZG": "H" if credit else "S",
            "MWSKZ": r["tax_code"] or "", "SAKTO": r["gl_account"],
            "KOSTL": r["cost_center"],
        }

    def reguh(r: dict, c: RowCtx) -> dict:
        return {
            "ZBUKR": r["company_code"],
            "LAUFD": c.d("LAUFD", r["payment_date"], "SAP"),
            "LAUFI": r["run_id"], "VBLNR": r["payment_doc_id"],
            "GJAHR": str(r["fiscal_year"]), "LIFNR": r["vendor_id"],
            "ZALDT": c.d("ZALDT", r["payment_date"], "SAP"),
            "RWBTR": c.a("RWBTR", r["amount"]), "WAERS": r["currency"],
            "RZAWE": r["payment_method"], "XVORL": "",
            "VOID_FLAG": "X" if r["is_void"] else "",
        }

    def regup(r: dict, c: RowCtx) -> dict:
        return {
            "ZBUKR": r["company_code"],
            "LAUFD": c.d("LAUFD", r["payment_date"], "SAP"),
            "LAUFI": "PR" + r["payment_doc_id"][-6:],
            "VBLNR": r["payment_doc_id"], "BUKRS": r["company_code"],
            "BELNR": r["invoice_doc_id"], "GJAHR": str(r["invoice_fiscal_year"]),
            "BUZEI": "001",
            "WRBTR": c.a("WRBTR", r["applied_amount"]),
            "SKNTO": c.a("SKNTO", r["discount_taken"]),
            "WAERS": r["currency"],
        }

    def apprv(r: dict, c: RowCtx) -> dict:
        return {
            "BUKRS": r["company_code"], "BELNR": r["invoice_doc_id"],
            "GJAHR": str(r["fiscal_year"]), "EVENT_SEQ": str(r["event_seq"]),
            "EVENT_TYPE": r["event_type"], "ACTOR": r["actor"],
            "EVENT_TS": c.d("EVENT_TS", r["event_ts"], "SAP_TS"),
            "COMMENTS": r["comment"],
        }

    return [
        Layout("vendor_master", "LFA1", "vendors",
               ("LIFNR", "NAME1", "NAME2", "LAND1", "ORT01", "PSTLZ", "STCD1",
                "STCEG", "ZTERM", "SPERR", "LOEVM", "ERDAT", "ERNAM"), lfa1),
        Layout("vendor_bank", "LFBK", "vendor_banks",
               ("LIFNR", "BANKS", "BANKL", "BANKN", "BKONT", "KOINH", "BVTYP",
                "CHANGE_DATE", "CHANGE_USER", "VERIFIED_FLAG"), lfbk),
        Layout("po_header", "EKKO", "po_headers",
               ("EBELN", "LIFNR", "BUKRS", "BEDAT", "WAERS", "EKGRP", "BSART",
                "ZTERM", "ERNAM"), ekko),
        Layout("po_line", "EKPO", "po_lines",
               ("EBELN", "EBELP", "TXZ01", "MENGE", "MEINS", "NETPR", "NETWR",
                "EINDT", "SAKTO", "KOSTL", "MWSKZ", "PSTYP", "WAERS"), ekpo),
        Layout("po_history", "EKBE", "receipts",
               ("EBELN", "EBELP", "ZEKKN", "VGABE", "GJAHR", "BELNR", "BUZEI",
                "BWART", "BUDAT", "MENGE", "MEINS", "DMBTR", "WAERS", "ERNAM",
                "SHKZG"), ekbe),
        Layout("invoice_header", "RBKP", "invoice_headers",
               ("BUKRS", "BELNR", "GJAHR", "XBLNR", "LIFNR", "BLDAT", "BUDAT",
                "CPUDT", "RMWWR", "WMWST", "SHKZG", "WAERS", "ZTERM", "ZLSPR",
                "BLART", "EBELN", "SOURCE_CHANNEL", "CAPTURE_TS"), rbkp,
               defect_targets={"date": ("BLDAT", "BUDAT"),
                               "amount": ("RMWWR", "WMWST"),
                               "ccy": ("WAERS",),
                               "key": ("XBLNR", "LIFNR")}),
        Layout("invoice_line", "RSEG", "invoice_lines",
               ("BUKRS", "BELNR", "GJAHR", "BUZEI", "EBELN", "EBELP", "MENGE",
                "BSTME", "WRBTR", "SHKZG", "MWSKZ", "SAKTO", "KOSTL"), rseg),
        Layout("payment_header", "REGUH", "payments",
               ("ZBUKR", "LAUFD", "LAUFI", "VBLNR", "GJAHR", "LIFNR", "ZALDT",
                "RWBTR", "WAERS", "RZAWE", "XVORL", "VOID_FLAG"), reguh),
        Layout("payment_application", "REGUP", "payment_applications",
               ("ZBUKR", "LAUFD", "LAUFI", "VBLNR", "BUKRS", "BELNR", "GJAHR",
                "BUZEI", "WRBTR", "SKNTO", "WAERS"), regup),
        Layout("approval_event", "APPRV", "approval_events",
               ("BUKRS", "BELNR", "GJAHR", "EVENT_SEQ", "EVENT_TYPE", "ACTOR",
                "EVENT_TS", "COMMENTS"), apprv),
    ]


# --- TEN002: Oracle-style JSON with CDC -----------------------------------

def _t2_layouts() -> list[Layout]:
    def _cdc(c: RowCtx) -> dict:
        return {"CDC_SEQ": c.seq, "CDC_OP": c.op,
                "LAST_UPDATE_DATE": fmt_date(c.extract_date, "ISO_TS")}

    def suppliers(r: dict, c: RowCtx) -> dict:
        return {
            "VENDOR_ID": r["vendor_id"], "SEGMENT1": _surrogate(r["vendor_id"]),
            "VENDOR_NAME": r["vendor_name"], "COUNTRY": r["country_code"],
            "CITY": r["city"], "POSTAL_CODE": r["postal_code"],
            "NUM_1099": r["tax_id"], "VAT_REGISTRATION_NUM": r["vat_registration"],
            "TERMS_NAME": r["payment_terms_src"],
            "HOLD_FLAG": "Y" if r["is_blocked"] else "N",
            "ENABLED_FLAG": "N" if r["is_marked_delete"] else "Y",
            "CREATION_DATE": c.d("CREATION_DATE", r["created_date"], "ISO_TS"),
            "CREATED_BY": r["created_by"],
            "INVOICE_CURRENCY_CODE": r["currency"], **_cdc(c),
        }

    def banks(r: dict, c: RowCtx) -> dict:
        return {
            "EXT_BANK_ACCOUNT_ID": _pk(r["vendor_id"], r["bank_account"]).replace("|", "-"),
            "VENDOR_ID": r["vendor_id"], "BANK_COUNTRY": r["bank_country"],
            "BANK_BRANCH_NUM": r["bank_key"], "BANK_ACCOUNT_NUM": r["bank_account"],
            "BANK_ACCOUNT_NAME": r["account_holder"],
            "LAST_UPDATED_BY": r["changed_by"],
            "VERIFIED_FLAG": "Y" if r["is_verified"] else "N", **_cdc(c),
        }

    def po_headers(r: dict, c: RowCtx) -> dict:
        return {
            "PO_HEADER_ID": _surrogate(r["po_number"]), "SEGMENT1": r["po_number"],
            "VENDOR_ID": r["vendor_id"], "ORG_ID": r["company_code"],
            "CREATION_DATE": c.d("CREATION_DATE", r["po_date"], "ISO_TS"),
            "CURRENCY_CODE": r["currency"], "AGENT_ID": r["buyer"],
            "TYPE_LOOKUP_CODE": r["po_type"], "TERMS_NAME": r["terms_src"],
            "CREATED_BY": r["created_by"], **_cdc(c),
        }

    def po_lines(r: dict, c: RowCtx) -> dict:
        return {
            "PO_LINE_ID": f"{_surrogate(r['po_number'])}{r['po_line_number']:03d}",
            "SEGMENT1": r["po_number"], "LINE_NUM": r["po_line_number"],
            "ITEM_DESCRIPTION": r["material_desc"],
            "QUANTITY": c.a("QUANTITY", r["quantity"]),
            "UNIT_MEAS_LOOKUP_CODE": r["uom"],
            "UNIT_PRICE": c.a("UNIT_PRICE", r["unit_price"]),
            "AMOUNT": c.a("AMOUNT", r["net_value"]),
            "NEED_BY_DATE": c.d("NEED_BY_DATE", r["delivery_date"], "ISO"),
            "GL_ACCOUNT_SEGMENT": r["gl_account"],
            "COST_CENTER_SEGMENT": r["cost_center"],
            "TAX_CLASSIFICATION_CODE": r["tax_code"],
            "ORDER_TYPE_LOOKUP_CODE": "RATE" if r["is_service"] else "QUANTITY",
            "CURRENCY_CODE": r["currency"], **_cdc(c),
        }

    def receipts(r: dict, c: RowCtx) -> dict:
        return {
            "TRANSACTION_ID": r["receipt_doc_id"],
            "SEGMENT1": r["po_number"], "PO_LINE_NUMBER": r["po_line_number"],
            "TRANSACTION_TYPE": "RETURN TO VENDOR" if r["is_reversal"] else "RECEIVE",
            "TRANSACTION_DATE": c.d("TRANSACTION_DATE", r["receipt_date"], "ISO_TS"),
            "QUANTITY": c.a("QUANTITY", abs(r["quantity"])),
            "UNIT_OF_MEASURE": r["uom"],
            "AMOUNT": c.a("AMOUNT", abs(r["amount"])),
            "CURRENCY_CODE": r["currency"], "CREATED_BY": r["posted_by"], **_cdc(c),
        }

    def invoices(r: dict, c: RowCtx) -> dict:
        return {
            "INVOICE_ID": r["invoice_doc_id"], "ORG_ID": r["company_code"],
            "INVOICE_NUM": c.k("INVOICE_NUM", r["supplier_invoice_number"]),
            "VENDOR_ID": c.k("VENDOR_ID", r["vendor_id"]),
            "INVOICE_DATE": c.d("INVOICE_DATE", r["invoice_date"], "ISO"),
            "GL_DATE": c.d("GL_DATE", r["posting_date"], "ISO"),
            "CREATION_DATE": c.d("CREATION_DATE", r["entry_date"], "ISO_TS"),
            # Oracle signs the amount directly: no debit/credit indicator.
            "INVOICE_AMOUNT": c.a("INVOICE_AMOUNT", r["gross_amount"]),
            "TOTAL_TAX_AMOUNT": c.a("TOTAL_TAX_AMOUNT", r["tax_amount"]),
            "INVOICE_CURRENCY_CODE": c.c("INVOICE_CURRENCY_CODE", r["currency"]),
            "TERMS_NAME": r["terms_src"],
            "HOLD_FLAG": "Y" if r["payment_block"] else "N",
            "INVOICE_TYPE_LOOKUP_CODE": "STANDARD" if r["doc_type"] == "INVOICE" else "CREDIT",
            "PO_HEADER_ID": _surrogate(r["po_number"]),
            "SOURCE": r["capture_channel"],
            "CAPTURE_TIMESTAMP": c.d("CAPTURE_TIMESTAMP", r["capture_ts"], "ISO_TS"),
            **_cdc(c),
        }

    def invoice_lines(r: dict, c: RowCtx) -> dict:
        return {
            "INVOICE_LINE_ID": f"{r['invoice_doc_id']}-{r['line_number']:04d}",
            "INVOICE_ID": r["invoice_doc_id"], "ORG_ID": r["company_code"],
            "LINE_NUMBER": r["line_number"],
            "GL_DATE": c.d("GL_DATE", date(r["fiscal_year"], 6, 30), "ISO"),
            "PO_HEADER_ID": _surrogate(r["po_number"]),
            "PO_LINE_NUMBER": r["po_line_number"],
            "QUANTITY_INVOICED": c.a("QUANTITY_INVOICED", r["quantity"]),
            "UNIT_OF_MEAS_LOOKUP_CODE": r["uom"],
            "AMOUNT": c.a("AMOUNT", r["net_amount"]),
            "TAX_CLASSIFICATION_CODE": r["tax_code"],
            "TAX_AMOUNT": c.a("TAX_AMOUNT", r["tax_amount"]),
            "GL_ACCOUNT_SEGMENT": r["gl_account"],
            "COST_CENTER_SEGMENT": r["cost_center"],
            "DESCRIPTION": r["description"], **_cdc(c),
        }

    def distributions(r: dict, c: RowCtx) -> dict:
        return {
            "INVOICE_DISTRIBUTION_ID":
                f"{r['invoice_doc_id']}-{r['line_number']:04d}-{r['distribution_number']:02d}",
            "INVOICE_ID": r["invoice_doc_id"], "ORG_ID": r["company_code"],
            "INVOICE_LINE_NUMBER": r["line_number"],
            "DISTRIBUTION_LINE_NUMBER": r["distribution_number"],
            "AMOUNT": c.a("AMOUNT", r["amount"]),
            "GL_ACCOUNT_SEGMENT": r["gl_account"],
            "COST_CENTER_SEGMENT": r["cost_center"],
            "GL_DATE": c.d("GL_DATE", date(r["fiscal_year"], 6, 30), "ISO"), **_cdc(c),
        }

    def checks(r: dict, c: RowCtx) -> dict:
        return {
            "CHECK_ID": r["payment_doc_id"],
            "CHECK_NUMBER": r["payment_doc_id"][-8:],
            "ORG_ID": r["company_code"], "VENDOR_ID": r["vendor_id"],
            "CHECK_DATE": c.d("CHECK_DATE", r["payment_date"], "ISO"),
            "AMOUNT": c.a("AMOUNT", r["amount"]), "CURRENCY_CODE": r["currency"],
            "PAYMENT_METHOD_CODE": r["payment_method"],
            "PAYMENT_BATCH_ID": r["run_id"],
            # A void is a DATE here, not a flag.
            "VOID_DATE": c.d("VOID_DATE", r["payment_date"], "ISO") if r["is_void"] else None,
            **_cdc(c),
        }

    def invoice_payments(r: dict, c: RowCtx) -> dict:
        return {
            "INVOICE_PAYMENT_ID": f"{r['payment_doc_id']}-{r['invoice_doc_id']}",
            "CHECK_ID": r["payment_doc_id"], "INVOICE_ID": r["invoice_doc_id"],
            "ORG_ID": r["company_code"],
            "AMOUNT": c.a("AMOUNT", r["applied_amount"]),
            "DISCOUNT_TAKEN": c.a("DISCOUNT_TAKEN", r["discount_taken"]),
            "ACCOUNTING_DATE": c.d("ACCOUNTING_DATE", r["payment_date"], "ISO"),
            "CURRENCY_CODE": r["currency"], **_cdc(c),
        }

    def approvals(r: dict, c: RowCtx) -> dict:
        return {
            "APPROVAL_HISTORY_ID": f"{r['invoice_doc_id']}-{r['event_seq']:02d}",
            "INVOICE_ID": r["invoice_doc_id"], "ORG_ID": r["company_code"],
            "SEQUENCE_NUM": r["event_seq"],
            # Oracle's own vocabulary; value_domain_mapping reconciles it.
            "RESPONSE": "APPROVED" if r["event_type"] == "APPROVED" else "NO RESPONSE",
            "APPROVER_ID": r["actor"],
            "ACTION_DATE": c.d("ACTION_DATE", r["event_ts"], "ISO_TS"),
            "NOTE": r["comment"] or None, **_cdc(c),
        }

    cdc_cols = ("CDC_SEQ", "CDC_OP", "LAST_UPDATE_DATE")
    return [
        Layout("vendor_master", "suppliers", "vendors",
               ("VENDOR_ID", "SEGMENT1", "VENDOR_NAME", "COUNTRY", "CITY",
                "POSTAL_CODE", "NUM_1099", "VAT_REGISTRATION_NUM", "TERMS_NAME",
                "HOLD_FLAG", "ENABLED_FLAG", "CREATION_DATE", "CREATED_BY",
                "INVOICE_CURRENCY_CODE") + cdc_cols, suppliers, True),
        Layout("vendor_bank", "supplier_banks", "vendor_banks",
               ("EXT_BANK_ACCOUNT_ID", "VENDOR_ID", "BANK_COUNTRY",
                "BANK_BRANCH_NUM", "BANK_ACCOUNT_NUM", "BANK_ACCOUNT_NAME",
                "LAST_UPDATED_BY", "VERIFIED_FLAG") + cdc_cols, banks, True),
        Layout("po_header", "po_headers", "po_headers",
               ("PO_HEADER_ID", "SEGMENT1", "VENDOR_ID", "ORG_ID",
                "CREATION_DATE", "CURRENCY_CODE", "AGENT_ID", "TYPE_LOOKUP_CODE",
                "TERMS_NAME", "CREATED_BY") + cdc_cols, po_headers, True),
        Layout("po_line", "po_lines", "po_lines",
               ("PO_LINE_ID", "SEGMENT1", "LINE_NUM", "ITEM_DESCRIPTION",
                "QUANTITY", "UNIT_MEAS_LOOKUP_CODE", "UNIT_PRICE", "AMOUNT",
                "NEED_BY_DATE", "GL_ACCOUNT_SEGMENT", "COST_CENTER_SEGMENT",
                "TAX_CLASSIFICATION_CODE", "ORDER_TYPE_LOOKUP_CODE",
                "CURRENCY_CODE") + cdc_cols, po_lines, True),
        Layout("po_history", "rcv_transactions", "receipts",
               ("TRANSACTION_ID", "SEGMENT1", "PO_LINE_NUMBER",
                "TRANSACTION_TYPE", "TRANSACTION_DATE", "QUANTITY",
                "UNIT_OF_MEASURE", "AMOUNT", "CURRENCY_CODE",
                "CREATED_BY") + cdc_cols, receipts, True),
        Layout("invoice_header", "ap_invoices", "invoice_headers",
               ("INVOICE_ID", "ORG_ID", "INVOICE_NUM", "VENDOR_ID",
                "INVOICE_DATE", "GL_DATE", "CREATION_DATE", "INVOICE_AMOUNT",
                "TOTAL_TAX_AMOUNT", "INVOICE_CURRENCY_CODE", "TERMS_NAME",
                "HOLD_FLAG", "INVOICE_TYPE_LOOKUP_CODE", "PO_HEADER_ID",
                "SOURCE", "CAPTURE_TIMESTAMP") + cdc_cols, invoices, True,
               defect_targets={"date": ("INVOICE_DATE", "GL_DATE"),
                               "amount": ("INVOICE_AMOUNT", "TOTAL_TAX_AMOUNT"),
                               "ccy": ("INVOICE_CURRENCY_CODE",),
                               "key": ("INVOICE_NUM", "VENDOR_ID")}),
        Layout("invoice_line", "ap_invoice_lines", "invoice_lines",
               ("INVOICE_LINE_ID", "INVOICE_ID", "ORG_ID", "LINE_NUMBER",
                "GL_DATE", "PO_HEADER_ID", "PO_LINE_NUMBER", "QUANTITY_INVOICED",
                "UNIT_OF_MEAS_LOOKUP_CODE", "AMOUNT", "TAX_CLASSIFICATION_CODE",
                "TAX_AMOUNT", "GL_ACCOUNT_SEGMENT", "COST_CENTER_SEGMENT",
                "DESCRIPTION") + cdc_cols, invoice_lines, True),
        Layout("invoice_distribution", "ap_invoice_distributions",
               "invoice_distributions",
               ("INVOICE_DISTRIBUTION_ID", "INVOICE_ID", "ORG_ID",
                "INVOICE_LINE_NUMBER", "DISTRIBUTION_LINE_NUMBER", "AMOUNT",
                "GL_ACCOUNT_SEGMENT", "COST_CENTER_SEGMENT",
                "GL_DATE") + cdc_cols, distributions, True),
        Layout("payment_header", "ap_checks", "payments",
               ("CHECK_ID", "CHECK_NUMBER", "ORG_ID", "VENDOR_ID", "CHECK_DATE",
                "AMOUNT", "CURRENCY_CODE", "PAYMENT_METHOD_CODE",
                "PAYMENT_BATCH_ID", "VOID_DATE") + cdc_cols, checks, True),
        Layout("payment_application", "ap_invoice_payments",
               "payment_applications",
               ("INVOICE_PAYMENT_ID", "CHECK_ID", "INVOICE_ID", "ORG_ID",
                "AMOUNT", "DISCOUNT_TAKEN", "ACCOUNTING_DATE",
                "CURRENCY_CODE") + cdc_cols, invoice_payments, True),
        Layout("approval_event", "ap_approval_history", "approval_events",
               ("APPROVAL_HISTORY_ID", "INVOICE_ID", "ORG_ID", "SEQUENCE_NUM",
                "RESPONSE", "APPROVER_ID", "ACTION_DATE", "NOTE") + cdc_cols,
               approvals, True),
    ]


# --- TEN003: Coupa-style CSV ----------------------------------------------

def _t3_layouts() -> list[Layout]:
    def _lm(c: RowCtx) -> str:
        return fmt_date(c.extract_date, "ISO_TS")

    def suppliers(r: dict, c: RowCtx) -> dict:
        return {
            "supplier-number": r["vendor_id"], "supplier-name": r["vendor_name"],
            "country": r["country_code"], "city": r["city"],
            "postcode": r["postal_code"], "tax-registration": r["tax_id"] or "",
            "payment-terms": r["payment_terms_src"],
            "status": "on_hold" if r["is_blocked"] else "active",
            "created-at": c.d("created-at", r["created_date"], "ISO_TS"),
            "currency": r["currency"], "last-modified": _lm(c),
        }

    def banking(r: dict, c: RowCtx) -> dict:
        return {
            "supplier-number": r["vendor_id"], "bank-seq": r["version"],
            "bank-country": r["bank_country"], "bsb": r["bank_key"],
            "account-number": r["bank_account"], "account-name": r["account_holder"],
            "updated-at": c.d("updated-at", r["change_date"], "ISO_TS"),
            "updated-by": r["changed_by"], "last-modified": _lm(c),
        }

    def pos(r: dict, c: RowCtx) -> dict:
        return {
            "po-number": r["po_number"], "supplier-number": r["vendor_id"],
            "po-date": c.d("po-date", r["po_date"], "ISO"),
            "currency": r["currency"], "buyer": r["buyer"],
            "po-type": r["po_type"], "payment-terms": r["terms_src"],
            "created-by": r["created_by"], "last-modified": _lm(c),
        }

    def po_lines(r: dict, c: RowCtx) -> dict:
        return {
            "po-number": r["po_number"], "line-num": r["po_line_number"],
            "description": r["material_desc"],
            "quantity": c.a("quantity", r["quantity"]), "uom": r["uom"],
            "unit-price": c.a("unit-price", r["unit_price"]),
            "line-total": c.a("line-total", r["net_value"]),
            "need-by-date": c.d("need-by-date", r["delivery_date"], "ISO"),
            "account": f"{r['gl_account']}-{r['cost_center']}",
            "tax-code": r["tax_code"] or "", "last-modified": _lm(c),
        }

    def receipts(r: dict, c: RowCtx) -> dict:
        return {
            "receipt-id": r["receipt_doc_id"], "po-number": r["po_number"],
            "po-line-num": r["po_line_number"],
            "receipt-date": c.d("receipt-date", r["receipt_date"], "ISO"),
            "quantity": c.a("quantity", abs(r["quantity"])), "uom": r["uom"],
            "receipt-value": c.a("receipt-value", abs(r["amount"])),
            "currency": r["currency"], "received-by": r["posted_by"],
            "last-modified": _lm(c),
        }

    def invoices(r: dict, c: RowCtx) -> dict:
        return {
            "invoice-number": c.k("invoice-number", r["supplier_invoice_number"]
                                  or r["invoice_doc_id"]),
            "supplier-number": c.k("supplier-number", r["vendor_id"]),
            "invoice-date": c.d("invoice-date", r["invoice_date"], "ISO"),
            "accounting-date": c.d("accounting-date", r["posting_date"], "ISO"),
            "created-at": c.d("created-at", r["capture_ts"], "ISO_TS"),
            "total": c.a("total", r["gross_amount"]),
            "tax-amount": c.a("tax-amount", r["tax_amount"]),
            "currency": c.c("currency", r["currency"]),
            "payment-terms": r["terms_src"],
            "on-hold": "true" if r["payment_block"] else "false",
            "invoice-type": "invoice" if r["doc_type"] == "INVOICE" else "credit_note",
            "po-number": r["po_number"] or "",
            "source": r["capture_channel"], "last-modified": _lm(c),
        }

    def invoice_lines(r: dict, c: RowCtx) -> dict:
        return {
            "invoice-number": r.get("header_invoice_key") or r["invoice_doc_id"],
            "line-num": r["line_number"],
            "accounting-date": c.d("accounting-date",
                                   r.get("header_posting_date") or date(r["fiscal_year"], 1, 1), "ISO"),
            "description": r["description"],
            "quantity": c.a("quantity", r["quantity"]), "uom": r["uom"],
            "line-total": c.a("line-total", r["net_amount"]),
            "tax-code": r["tax_code"] or "",
            "tax-amount": c.a("tax-amount", r["tax_amount"]),
            "account": f"{r['gl_account']}-{r['cost_center']}",
            "po-number": r["po_number"] or "",
            "po-line-num": r["po_line_number"] or "",
            "last-modified": _lm(c),
        }

    def payments(r: dict, c: RowCtx) -> dict:
        return {
            "payment-id": r["payment_doc_id"], "supplier-number": r["vendor_id"],
            "payment-date": c.d("payment-date", r["payment_date"], "ISO"),
            "amount": c.a("amount", r["amount"]), "currency": r["currency"],
            "payment-method": r["payment_method"], "batch-id": r["run_id"],
            "status": "void" if r["is_void"] else "paid", "last-modified": _lm(c),
        }

    def payment_lines(r: dict, c: RowCtx) -> dict:
        return {
            "payment-id": r["payment_doc_id"],
            "invoice-number": r.get("header_invoice_key") or r["invoice_doc_id"],
            "applied-amount": c.a("applied-amount", r["applied_amount"]),
            "discount-taken": c.a("discount-taken", r["discount_taken"]),
            "currency": r["currency"], "last-modified": _lm(c),
        }

    return [
        Layout("vendor_master", "suppliers", "vendors",
               ("supplier-number", "supplier-name", "country", "city", "postcode",
                "tax-registration", "payment-terms", "status", "created-at",
                "currency", "last-modified"), suppliers),
        Layout("vendor_bank", "supplier_banking", "vendor_banks",
               ("supplier-number", "bank-seq", "bank-country", "bsb",
                "account-number", "account-name", "updated-at", "updated-by",
                "last-modified"), banking),
        Layout("po_header", "purchase_orders", "po_headers",
               ("po-number", "supplier-number", "po-date", "currency", "buyer",
                "po-type", "payment-terms", "created-by", "last-modified"), pos),
        Layout("po_line", "po_lines", "po_lines",
               ("po-number", "line-num", "description", "quantity", "uom",
                "unit-price", "line-total", "need-by-date", "account", "tax-code",
                "last-modified"), po_lines),
        Layout("po_history", "receipts", "receipts",
               ("receipt-id", "po-number", "po-line-num", "receipt-date",
                "quantity", "uom", "receipt-value", "currency", "received-by",
                "last-modified"), receipts),
        Layout("invoice_header", "invoices", "invoice_headers",
               ("invoice-number", "supplier-number", "invoice-date",
                "accounting-date", "created-at", "total", "tax-amount",
                "currency", "payment-terms", "on-hold", "invoice-type",
                "po-number", "source", "last-modified"), invoices,
               defect_targets={"date": ("invoice-date", "accounting-date"),
                               "amount": ("total", "tax-amount"),
                               "ccy": ("currency",),
                               "key": ("invoice-number", "supplier-number")}),
        Layout("invoice_line", "invoice_lines", "invoice_lines",
               ("invoice-number", "line-num", "accounting-date", "description",
                "quantity", "uom", "line-total", "tax-code", "tax-amount",
                "account", "po-number", "po-line-num", "last-modified"),
               invoice_lines),
        Layout("payment_header", "payments", "payments",
               ("payment-id", "supplier-number", "payment-date", "amount",
                "currency", "payment-method", "batch-id", "status",
                "last-modified"), payments),
        Layout("payment_application", "payment_lines", "payment_applications",
               ("payment-id", "invoice-number", "applied-amount",
                "discount-taken", "currency", "last-modified"), payment_lines),
    ]


LAYOUTS: dict[str, list[Layout]] = {
    "TEN001": _t1_layouts(),
    "TEN002": _t2_layouts(),
    "TEN003": _t3_layouts(),
}

# Which parent a child entity is held back with, so a released invoice always has
# its lines and a withheld invoice never leaks a line into the initial load.
_PARENT_OF: dict[str, str] = {
    "invoice_header": "invoice", "invoice_line": "invoice",
    "invoice_distribution": "invoice", "approval_event": "invoice",
    "po_header": "po", "po_line": "po", "po_history": "po",
    "payment_header": "payment", "payment_application": "payment",
}
_PARENT_KEY_FIELD = {"invoice": "invoice_doc_id", "po": "po_number",
                     "payment": "payment_doc_id"}


def _business_key(entity: str, r: dict) -> str:
    """Stable identity used for the defect ledger and for update targeting."""
    if entity == "vendor_master":
        return r["vendor_id"]
    if entity == "vendor_bank":
        return _pk(r["vendor_id"], r["bank_country"], r["bank_key"], r["bank_account"])
    if entity == "po_header":
        return r["po_number"]
    if entity == "po_line":
        return _pk(r["po_number"], r["po_line_number"])
    if entity == "po_history":
        return _pk(r["receipt_doc_id"], r["receipt_line"])
    if entity == "invoice_header":
        return r["invoice_doc_id"]
    if entity == "invoice_line":
        return _pk(r["invoice_doc_id"], r["line_number"])
    if entity == "invoice_distribution":
        return _pk(r["invoice_doc_id"], r["line_number"], r["distribution_number"])
    if entity == "payment_header":
        return r["payment_doc_id"]
    if entity == "payment_application":
        return _pk(r["payment_doc_id"], r["invoice_doc_id"])
    if entity == "approval_event":
        return _pk(r["invoice_doc_id"], r["event_seq"])
    raise ValueError(entity)


# ---------------------------------------------------------------------------
# Representational defect decisions, made once per record and then honoured on
# every emission. A record that is broken in the initial load stays broken in
# the snapshots, which is both realistic and what keeps the ledger a 1:1 index.
# ---------------------------------------------------------------------------


def _decide_repr(tenant: str, layout: Layout, records: list[dict],
                 wcfg: WriteConfig, ledger: dfx.DefectLedger) -> dict[str, dict[str, str]]:
    if not layout.defect_targets:
        return {}
    rng = random.Random(f"{wcfg.seed}|repr|{tenant}|{layout.entity}")
    mult = gen.TENANT_PROFILE[tenant]["defect_multiplier"]
    pools = _STYLE_POOL[tenant]
    out: dict[str, dict[str, str]] = {}

    for r in records:
        bk = _business_key(layout.entity, r)
        dec: dict[str, str] = {}
        for code, kind, pool in _REPR_PLAN:
            cols = layout.defect_targets.get(kind, ())
            if not cols:
                continue
            if not dfx.should_inject(rng, dfx.BY_CODE[code], mult):
                continue
            col = rng.choice(cols)
            if col in dec:
                continue                      # one defect per column; first wins
            style = rng.choice(pools[pool])
            dec[col] = style
            ledger.record(tenant_id=tenant, entity=layout.entity, code=code,
                          business_key=bk, detail=f"column {col} rendered as {style}",
                          injected_value=style)
        if dec:
            out[bk] = dec
    return out


# ---------------------------------------------------------------------------
# Delivery planning
# ---------------------------------------------------------------------------


@dataclass
class Event:
    row_id: int
    day: int
    op: str            # I insert, U update/restate, D delete
    seq: int
    rec: dict


def _bump_account(acct: str, delta: int) -> str:
    """Change an account number by `delta` while keeping its exact layout.

    Account numbers arrive formatted (IBAN groups, BSB "123-456 844-323",
    padded with spaces), so only the digits are changed and every separator
    stays where it was. Fixed after scale=0.2 crashed on a formatted value.
    """
    s = str(acct or "")
    digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        return s + str(delta)
    tail = digits[-8:]
    new_tail = str((int(tail) + delta) % (10 ** len(tail))).zfill(len(tail))
    new_digits = iter(digits[:-len(tail)] + new_tail)
    return "".join(next(new_digits) if ch.isdigit() else ch for ch in s)


def _mutate(entity: str, r: dict, rng: random.Random, extract: date,
            tenant: str, ledger: dfx.DefectLedger) -> dict:
    """What a delta day's restatement actually changes."""
    m = dict(r)
    if entity == "invoice_header":
        # The common real update: a payment block goes on or comes off.
        # (Changed in Step 4: this used to rescale the header amount by +/-2%
        # without touching the lines, which no ERP allows and which made ~5% of
        # invoices fail the header-equals-lines rule for a reason that was not
        # a planted defect. Amount corrections still arrive, as the planted
        # LATE_CORRECTION files, where they are meant to be caught.)
        m["payment_block"] = rng.choice(["A", "R", None])
    elif entity == "vendor_master":
        m["payment_terms_src"] = rng.choice(
            gen.TENANT_PROFILE[tenant]["terms"])
        m["is_blocked"] = rng.random() < 0.08
    elif entity == "vendor_bank":
        # A bank account change arriving mid-stream. Unverified changes followed
        # by a payment are the single highest-value signal in AP fraud control.
        m["bank_account"] = _bump_account(r["bank_account"], rng.randint(1, 99))
        m["version"] = r["version"] + 1
        m["change_date"] = extract
        m["is_verified"] = rng.random() < 0.7
        if not m["is_verified"]:
            ledger.record(tenant_id=tenant, entity="vendor_bank",
                          code="BANK_CHANGE_UNVERIFIED",
                          business_key=_business_key("vendor_bank", m),
                          original_value=r["bank_account"],
                          injected_value=m["bank_account"],
                          detail="bank account changed in a delta with no verification")
    elif entity == "po_line":
        m["unit_price"] = round(r["unit_price"] * rng.choice([1.0, 1.03]), 4)
        m["net_value"] = round(float(m["unit_price"]) * float(r["quantity"]), 2)
    return m


def _plan_entity(tenant: str, layout: Layout, records: list[dict],
                 holdback: dict[str, int], wcfg: WriteConfig,
                 seq: Iterator[int], ledger: dfx.DefectLedger,
                 allow_delete: bool) -> list[Event]:
    rng = random.Random(f"{wcfg.seed}|plan|{tenant}|{layout.entity}")
    parent = _PARENT_OF.get(layout.entity)
    events: list[Event] = []
    day0: list[Event] = []

    for i, r in enumerate(records):
        day = 0
        if parent:
            pkey = r.get(_PARENT_KEY_FIELD[parent])
            day = holdback.get(pkey, 0) if pkey else 0
        ev = Event(row_id=i, day=day, op="I", seq=next(seq), rec=r)
        events.append(ev)
        if day == 0:
            day0.append(ev)

    # Restatements and deletions apply only to rows already released on day 0;
    # updating a row that has not arrived yet would be incoherent.
    n_upd = max(0, int(len(day0) * wcfg.update_pct))
    n_del = max(0, int(len(day0) * wcfg.delete_pct)) if allow_delete else 0
    for day in range(1, wcfg.n_delta_days + 1):
        for ev in rng.sample(day0, min(n_upd, len(day0))):
            events.append(Event(ev.row_id, day, "U", next(seq),
                                _mutate(layout.entity, ev.rec, rng,
                                        wcfg.extract_date(day), tenant, ledger)))
        for ev in rng.sample(day0, min(n_del, len(day0))):
            events.append(Event(ev.row_id, day, "D", next(seq), ev.rec))

    events.sort(key=lambda e: (e.day, e.seq))
    return events


def _holdback(tenant: str, data: gen.TenantData, wcfg: WriteConfig) -> dict[str, int]:
    """Parent keys withheld from the initial load, mapped to their release day."""
    rng = random.Random(f"{wcfg.seed}|holdback|{tenant}")
    out: dict[str, int] = {}
    for keys, pct in (
        ([h["invoice_doc_id"] for h in data.invoice_headers], wcfg.holdback_invoice_pct),
        ([h["po_number"] for h in data.po_headers], wcfg.holdback_po_pct),
        ([p["payment_doc_id"] for p in data.payments], wcfg.holdback_payment_pct),
    ):
        uniq = sorted(set(k for k in keys if k))
        for k in rng.sample(uniq, min(int(len(uniq) * pct), len(uniq))):
            out[k] = rng.randint(1, wcfg.n_delta_days)
    return out


def _fold(events: list[Event], upto: int) -> list[dict]:
    """The full-snapshot view as at day `upto`."""
    state: dict[int, dict] = {}
    for e in events:
        if e.day > upto:
            break
        if e.op == "D":
            state.pop(e.row_id, None)
        else:
            state[e.row_id] = e.rec
    return list(state.values())


# ---------------------------------------------------------------------------
# Schema drift
# ---------------------------------------------------------------------------

_DRIFT_BY_TENANT: dict[str, list[dfx.DriftEvent]] = {}
for _d in dfx.DRIFT_EVENTS:
    _DRIFT_BY_TENANT.setdefault(_d.tenant_id, []).append(_d)


def _drift_for(tenant: str, entity: str, day: int) -> dfx.DriftEvent | None:
    for d in _DRIFT_BY_TENANT.get(tenant, []):
        if d.entity == entity and day >= d.effective_from_day:
            return d
    return None


def _drift_columns(drift: dfx.DriftEvent) -> tuple[str, str | None]:
    """A rename in the catalogue is written ``old -> new``; everything else names
    a single column. Parsing it here keeps the catalogue readable as prose."""
    if "->" in drift.column_name:
        old, new = (p.strip() for p in drift.column_name.split("->", 1))
        return old, new
    return drift.column_name.strip(), None


def _coerce_number(v) -> float | None:
    """Best-effort read of a number that may already have been damaged.

    Handles ``1,616,210.09``, ``1.616.210,09``, ``EUR 1616210.09`` and plain
    floats. Returns ``None`` when nothing numeric survives, which is itself a
    legitimate outcome: DATE/CURRENCY chaos includes values no reader can parse.
    """
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return None
    s = "".join(ch for ch in s if ch.isdigit() or ch in ".,-")
    if s.count(",") and s.count("."):
        # Whichever separator appears last is the decimal point.
        dec = "," if s.rfind(",") > s.rfind(".") else "."
        s = s.replace("," if dec == "." else ".", "").replace(dec, ".")
    elif s.count(","):
        # A single comma: decimal comma if it guards exactly two trailing digits.
        s = s.replace(",", "." if len(s) - s.rfind(",") == 3 else "")
    try:
        return float(s)
    except ValueError:
        return None


def _apply_drift(drift: dfx.DriftEvent | None, cols: list[str],
                 row: dict, rng: random.Random) -> tuple[list[str], dict]:
    """Each drift class expresses itself in the shape its format allows."""
    if drift is None:
        return cols, row
    col, new_name = _drift_columns(drift)

    if drift.drift_type == "COLUMN_ADDED":
        # Appended at the end, which is the only additive position a positional
        # layout can survive: every existing ordinal keeps its meaning.
        if col in row:
            return cols, row
        return cols + [col], {**row, col: f"ZU{rng.randint(10000, 99999)}"}

    if drift.drift_type == "TYPE_CHANGED":
        v = row.get(col)
        if v is None:
            return cols, row
        # A number becomes a string with a symbol and separators. try_cast returns
        # null, totals collapse quietly, and nothing errors. Hence BREAKING: a
        # silent null is worse than a failed batch.
        #
        # The value may already be a *string* here, because CURRENCY_FORMAT_CHAOS
        # runs before drift and may have given this same field thousands
        # separators, a decimal comma or a symbol. Two defects landing on one
        # field is realistic and must not crash the generator, so parse
        # defensively and fall back to prefixing whatever is already there.
        num = _coerce_number(v)
        if num is None:
            return cols, {**row, col: f"${v}"}
        return cols, {**row, col: f"${num:,.2f}"}

    if drift.drift_type == "COLUMN_RENAMED":
        if not new_name or col not in row:
            return cols, row
        # The old column does not linger. A header-based reader sees one column
        # vanish and another appear, and any mapping keyed on the old name
        # silently yields null for every row from this day forward.
        out_cols = [new_name if c == col else c for c in cols]
        out_row = {(new_name if k == col else k): v for k, v in row.items()}
        return out_cols, out_row

    return cols, row


# ---------------------------------------------------------------------------
# Serialisation
# ---------------------------------------------------------------------------


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _day_dir(wcfg: WriteConfig, tenant: str, entity: str, d: date) -> str:
    return os.path.join(wcfg.root, tenant, entity,
                        f"{d.year:04d}", f"{d.month:02d}", f"{d.day:02d}")


def _write_delimited(path: str, cols: list[str], rows: list[dict],
                     delimiter: str, encoding: str, header: bool) -> int:
    buf = io.StringIO()
    if header:
        buf.write(delimiter.join(cols) + "\n")
    for r in rows:
        buf.write(delimiter.join("" if r.get(c) is None else str(r.get(c))
                                 for c in cols) + "\n")
    data = buf.getvalue()
    with open(path, "w", encoding=encoding, errors="replace", newline="") as fh:
        fh.write(data)
    return len(data.encode(encoding, errors="replace"))


def _write_csv(path: str, cols: list[str], rows: list[dict]) -> int:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore",
                       quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
    w.writeheader()
    for r in rows:
        w.writerow({c: ("" if r.get(c) is None else r.get(c)) for c in cols})
    data = buf.getvalue()
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write(data)
    return len(data.encode("utf-8"))


def _write_ndjson(path: str, cols: list[str], rows: list[dict]) -> int:
    # Column order is preserved for readability; JSON readers do not care, but a
    # human diffing two days' files very much does.
    lines = [json.dumps({c: r.get(c) for c in cols}, separators=(",", ":"),
                        default=str) for r in rows]
    data = "\n".join(lines) + ("\n" if lines else "")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(data)
    return len(data.encode("utf-8"))


_FORMAT = {
    "TEN001": ("dat", "delimited"),
    "TEN002": ("json", "ndjson"),
    "TEN003": ("csv", "csv"),
}


def _emit(wcfg: WriteConfig, tenant: str, layout: Layout, day: int,
          rows: list[dict], cols: list[str], suffix: str = "") -> dict | None:
    if not rows:
        return None
    ext, kind = _FORMAT[tenant]
    d = wcfg.extract_date(day)
    folder = _day_dir(wcfg, tenant, layout.entity, d)
    _ensure_dir(folder)
    name = f"{layout.stem}{suffix}_{d:%Y%m%d}.{ext}"
    path = os.path.join(folder, name)
    if kind == "delimited":
        size = _write_delimited(path, cols, rows, "|", "ISO-8859-1", header=False)
    elif kind == "csv":
        size = _write_csv(path, cols, rows)
    else:
        size = _write_ndjson(path, cols, rows)
    return {"tenant_id": tenant, "entity": layout.entity, "load_day": day,
            "extract_date": d.isoformat(), "file_name": name, "file_path": path,
            "record_count": len(rows), "byte_count": size,
            "column_count": len(cols)}


# ---------------------------------------------------------------------------
# Top level
# ---------------------------------------------------------------------------


def _snapshot_days(tenant: str, wcfg: WriteConfig) -> list[int]:
    if wcfg.snapshot_days is None:
        return list(range(wcfg.n_delta_days + 1))
    if wcfg.snapshot_days:
        return sorted(set(wcfg.snapshot_days))
    # Auto: the first day, any drift day (so the drift is actually observable),
    # and the last day. See the Free Edition note in the module docstring.
    days = {0, wcfg.n_delta_days}
    for d in _DRIFT_BY_TENANT.get(tenant, []):
        if d.effective_from_day <= wcfg.n_delta_days:
            days.add(d.effective_from_day)
    return sorted(days)


def write_tenant(data: gen.TenantData, wcfg: WriteConfig,
                 ledger: dfx.DefectLedger) -> list[dict]:
    """Render and land one tenant's whole delivery series. Returns manifest rows."""
    tenant = data.tenant_id
    if wcfg.purge:
        shutil.rmtree(os.path.join(wcfg.root, tenant), ignore_errors=True)

    manifest: list[dict] = []
    seq = count(1)
    holdback = _holdback(tenant, data, wcfg)
    allow_delete = tenant == "TEN002"          # only CDC can state a deletion
    full_snapshot = tenant == "TEN001"
    snap_days = _snapshot_days(tenant, wcfg) if full_snapshot else None
    drift_rng = random.Random(f"{wcfg.seed}|drift|{tenant}")

    for layout in LAYOUTS[tenant]:
        records = getattr(data, layout.attr)
        if not records:
            continue
        decisions = _decide_repr(tenant, layout, records, wcfg, ledger)
        events = _plan_entity(tenant, layout, records, holdback, wcfg, seq,
                              ledger, allow_delete)

        days = snap_days if full_snapshot else range(wcfg.n_delta_days + 1)
        by_day: dict[int, list[Event]] = {}
        for e in events:
            by_day.setdefault(e.day, []).append(e)

        for day in days:
            if full_snapshot:
                batch = [(None, r) for r in _fold(events, day)]
            else:
                batch = [(e, e.rec) for e in by_day.get(day, [])]
            if not batch:
                continue

            drift = _drift_for(tenant, layout.entity, day)
            ctx_date = wcfg.extract_date(day)
            out_cols = list(layout.columns)
            rows: list[dict] = []
            for ev, rec in batch:
                dec = decisions.get(_business_key(layout.entity, rec), {})
                ctx = RowCtx(ctx_date, day, dec, layout.numeric_amounts,
                             seq=ev.seq if ev else 0, op=ev.op if ev else "I")
                row = layout.build(rec, ctx)
                cols2, row = _apply_drift(drift, list(layout.columns), row, drift_rng)
                out_cols = cols2
                rows.append(row)

            m = _emit(wcfg, tenant, layout, day, rows, out_cols)
            if m:
                m["drift_event_id"] = drift.event_id if drift else None
                m["load_type"] = ("FULL_SNAPSHOT" if full_snapshot
                                  else "CDC" if tenant == "TEN002" else "INCREMENTAL")
                m["op_counts"] = _op_counts(batch)
                manifest.append(m)

    manifest.extend(_write_late_corrections(data, wcfg, ledger))
    return manifest


def _op_counts(batch: list[tuple[Event | None, dict]]) -> str:
    counts: dict[str, int] = {}
    for ev, _ in batch:
        op = ev.op if ev else "S"
        counts[op] = counts.get(op, 0) + 1
    return ",".join(f"{k}={v}" for k, v in sorted(counts.items()))


def _write_late_corrections(data: gen.TenantData, wcfg: WriteConfig,
                            ledger: dfx.DefectLedger) -> list[dict]:
    """TEN003 post-close corrections, arriving past the 14-day lateness window.

    These are the rows decision D4 sends to the review queue instead of merging:
    a correction to an accounting period that is already closed and reported.
    They land in the same folder and match the same file glob, because that is
    how they really arrive — nothing in the file announces that it is late.
    """
    if data.tenant_id != "TEN003":
        return []
    layout = next(l for l in LAYOUTS["TEN003"] if l.entity == "invoice_header")
    rng = random.Random(f"{wcfg.seed}|late|{data.tenant_id}")
    spec = dfx.BY_CODE["LATE_CORRECTION"]
    mult = gen.TENANT_PROFILE["TEN003"]["defect_multiplier"]
    manifest: list[dict] = []

    pool = [h for h in data.invoice_headers if h.get("posting_date")]
    for day in range(wcfg.late_correction_from_day, wcfg.n_delta_days + 1, 3):
        extract = wcfg.extract_date(day)
        # Late corrections are an absolute volume, not a share of the ledger:
        # a handful of periods get reopened each week regardless of how big the
        # tenant is. Sampling a fixed-size window keeps that true at any scale.
        picks = [h for h in rng.sample(pool, min(len(pool), 200))
                 if dfx.should_inject(rng, spec, mult * 2)]
        if not picks:
            continue
        rows = []
        for h in picks:
            corrected = dict(h)
            corrected["gross_amount"] = round(h["gross_amount"] * rng.uniform(0.9, 1.1), 2)
            ctx = RowCtx(extract, day, {}, layout.numeric_amounts)
            rows.append(layout.build(corrected, ctx))
            ledger.record(
                tenant_id="TEN003", entity="invoice_header", code="LATE_CORRECTION",
                business_key=h["invoice_doc_id"],
                original_value=h["gross_amount"],
                injected_value=corrected["gross_amount"],
                detail=(f"correction to accounting date {h['posting_date']} "
                        f"arriving {extract}, past the 14-day window"),
            )
        m = _emit(wcfg, "TEN003", layout, day, rows, list(layout.columns),
                  suffix="_corrections")
        if m:
            m["drift_event_id"] = None
            m["load_type"] = "INCREMENTAL"
            m["op_counts"] = f"C={len(rows)}"
            manifest.append(m)
    return manifest


def write_all(datasets: Iterable[gen.TenantData], wcfg: WriteConfig,
              ledger: dfx.DefectLedger) -> list[dict]:
    manifest: list[dict] = []
    for data in datasets:
        manifest.extend(write_tenant(data, wcfg, ledger))
    return manifest
