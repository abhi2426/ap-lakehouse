"""
generators — build a coherent AP dataset in a neutral shape, then let the
writers render it into each ERP's native format.

Separation of concerns
----------------------
* **Semantic defects** (zero amounts, orphans, duplicates, over-receipts, SoD
  breaches) are injected here, because they are facts about the business data.
* **Representational defects** (date formats, whitespace, case, thousands
  separators, encodings) are injected in ``writers``, because they are facts
  about the file.

Getting this the wrong way round is how synthetic datasets end up with a
"duplicate" that only exists in the CSV rendering and vanishes in the Parquet one.

Determinism
-----------
Every random draw comes from a seeded ``random.Random`` keyed by tenant and
purpose. Re-running produces byte-identical output, which is what makes the
whole pipeline re-runnable and the exercises reproducible.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

from . import defects as dfx

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TENANT_SPLIT = {"TEN001": 0.45, "TEN002": 0.35, "TEN003": 0.20}

TENANT_PROFILE: dict[str, dict[str, Any]] = {
    "TEN001": {
        "company_codes": ["DE01", "DE01", "DE01", "DE02", "GB01"],   # weighted by repetition
        "currencies": ["EUR", "EUR", "EUR", "EUR", "GBP", "USD"],
        "countries": ["DE", "DE", "DE", "AT", "NL", "GB", "FR"],
        "terms": ["Z030", "Z030", "Z045", "Z060", "Z210", "Z000"],
        "tax_codes": ["V19", "V19", "V19", "V07", "V00", "RC0"],
        "gl_accounts": ["0000400000", "0000476000", "0000470000", "0000479900"],
        "channels": ["EDI", "EDI", "OCR", "MANUAL"],
        "po_rate": 0.82,                 # share of invoices with a PO
        "has_approval_events": True,
        "has_distributions": False,
        "defect_multiplier": 1.0,
    },
    "TEN002": {
        "company_codes": ["US01", "US01", "US02", "CA01"],
        "currencies": ["USD", "USD", "USD", "USD", "CAD"],
        "countries": ["US", "US", "US", "CA", "MX"],
        "terms": ["30 NET", "30 NET", "45 NET", "60 NET", "2/10 NET 30", "1/15 NET 45"],
        "tax_codes": ["ST0", "ST7", "ST7", "GST", "WH3"],
        "gl_accounts": ["0000500000", "0000610000", "0000620000"],
        "channels": ["EDI_810", "SUPPLIER_PORTAL", "SUPPLIER_PORTAL", "MANUAL"],
        "po_rate": 0.74,
        "has_approval_events": True,
        "has_distributions": True,
        "defect_multiplier": 0.9,
    },
    "TEN003": {
        "company_codes": ["MER01"],
        "currencies": ["AUD", "AUD", "AUD", "AUD", "USD", "NZD"],
        "countries": ["AU", "AU", "AU", "NZ", "SG"],
        "terms": ["NET30", "NET30", "NET14", "NET60", "3/5 NET 30"],
        "tax_codes": ["GST10", "GST10", "GST10", "FRE"],
        "gl_accounts": ["0000610100", "0000620100", "0000630100"],
        "channels": ["CXML", "PORTAL", "EMAIL", "EMAIL", "UNKNOWN"],
        "po_rate": 0.51,                 # services-heavy: lots of non-PO spend
        "has_approval_events": False,    # this tenant simply has no approval stream
        "has_distributions": False,
        "defect_multiplier": 1.6,        # least mature source, most defects
    },
}

VENDOR_STEMS = [
    "Atlas", "Borealis", "Cedar", "Delta", "Everest", "Foxglove", "Granite",
    "Harbour", "Ironwood", "Juniper", "Keystone", "Lattice", "Meridian",
    "Northgate", "Obsidian", "Pinnacle", "Quarry", "Redwood", "Sable",
    "Thornton", "Umbra", "Vantage", "Westfield", "Yarrow", "Zenith",
    "Kestrel", "Lumen", "Marlowe", "Nimbus", "Orchard",
]
VENDOR_SUFFIX = ["Industries", "Supply Co", "Logistics", "Services", "Holdings",
                 "Partners", "Systems", "Technologies", "Trading", "Solutions",
                 "Manufacturing", "Consulting", "Group", "Engineering"]
LEGAL_FORM = {"DE": "GmbH", "AT": "GmbH", "NL": "B.V.", "GB": "Ltd", "FR": "SARL",
              "US": "Inc", "CA": "ULC", "MX": "S.A. de C.V.", "AU": "Pty Ltd",
              "NZ": "Limited", "SG": "Pte Ltd"}

APPROVERS = [f"U{n:05d}" for n in range(1, 41)]
BUYERS = [f"B{n:04d}" for n in range(1, 26)]
UOMS = ["EA", "EA", "EA", "KG", "L", "M", "HR", "PCE"]


@dataclass
class GenConfig:
    """Volume and date envelope. Scale down freely; the shape does not change."""

    n_vendors: int = 2_000
    n_pos: int = 20_000
    n_invoices: int = 50_000
    n_invoice_lines: int = 180_000
    n_payments: int = 40_000
    start_date: date = date(2024, 7, 1)
    end_date: date = date(2026, 6, 30)
    n_cdc_days: int = 20
    seed: str = "ap-poc-v1"
    scale: float = 1.0                  # 0.1 gives a 10x smaller dataset for Free Edition

    def scaled(self, n: int) -> int:
        return max(1, int(n * self.scale))

    @property
    def span_days(self) -> int:
        return (self.end_date - self.start_date).days


@dataclass
class TenantData:
    """One tenant's generated entities, in the neutral shape."""

    tenant_id: str
    vendors: list[dict] = field(default_factory=list)
    vendor_banks: list[dict] = field(default_factory=list)
    po_headers: list[dict] = field(default_factory=list)
    po_lines: list[dict] = field(default_factory=list)
    receipts: list[dict] = field(default_factory=list)
    invoice_headers: list[dict] = field(default_factory=list)
    invoice_lines: list[dict] = field(default_factory=list)
    invoice_distributions: list[dict] = field(default_factory=list)
    payments: list[dict] = field(default_factory=list)
    payment_applications: list[dict] = field(default_factory=list)
    approval_events: list[dict] = field(default_factory=list)

    def summary(self) -> dict[str, int]:
        return {
            k: len(v) for k, v in self.__dict__.items() if isinstance(v, list)
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rng(cfg: GenConfig, tenant_id: str, purpose: str) -> random.Random:
    return random.Random(f"{cfg.seed}|{tenant_id}|{purpose}")


def _rand_date(rng: random.Random, cfg: GenConfig, skew_recent: bool = True) -> date:
    """Dates skewed towards recent months, as real invoice volume is."""
    u = rng.random()
    if skew_recent:
        u = u ** 0.7
    return cfg.start_date + timedelta(days=int(u * cfg.span_days))


def _money(rng: random.Random, low: float, high: float) -> float:
    """Log-uniform, so a few large invoices dominate value. Real AP looks like this."""
    import math

    v = math.exp(rng.uniform(math.log(low), math.log(high)))
    return round(v, 2)


# ---------------------------------------------------------------------------
# Entity generation
# ---------------------------------------------------------------------------

def gen_vendors(cfg: GenConfig, tenant_id: str, ledger: dfx.DefectLedger) -> tuple[list[dict], list[dict]]:
    prof = TENANT_PROFILE[tenant_id]
    rng = _rng(cfg, tenant_id, "vendors")
    mult = prof["defect_multiplier"]
    n = cfg.scaled(int(cfg.n_vendors * TENANT_SPLIT[tenant_id]))

    vendors, banks = [], []
    used_tax_ids: list[str] = []

    for i in range(n):
        vid = f"{tenant_id[-1]}V{i + 100000}"
        country = rng.choice(prof["countries"])
        name = f"{rng.choice(VENDOR_STEMS)} {rng.choice(VENDOR_SUFFIX)} {LEGAL_FORM.get(country,'Ltd')}"
        created = cfg.start_date - timedelta(days=rng.randint(30, 2000))

        tax_id = f"{country}{rng.randint(10**8, 10**9 - 1)}"
        # Vendor master duplication: reuse an existing tax id under a new vendor
        # id. This is how the same company gets paid twice under two numbers.
        if used_tax_ids and rng.random() < 0.025 * mult:
            tax_id = rng.choice(used_tax_ids)
            ledger.record(tenant_id=tenant_id, entity="vendor_master", code="NEAR_DUP_INVOICE",
                          business_key=vid, detail="duplicate vendor: tax id reused under a new vendor id",
                          injected_value=tax_id)
        else:
            used_tax_ids.append(tax_id)

        vendors.append({
            "vendor_id": vid,
            "vendor_name": name,
            "vendor_name_2": "",
            "country_code": country,
            "city": rng.choice(["Berlin", "Munich", "London", "Seattle", "Toronto",
                                "Sydney", "Melbourne", "Amsterdam", "Vienna", "Austin"]),
            "postal_code": f"{rng.randint(10000, 99999)}",
            "tax_id": tax_id,
            "vat_registration": f"{country}{rng.randint(10**8, 10**9-1)}" if country in ("DE","AT","NL","GB","FR") else "",
            "payment_terms_src": rng.choice(prof["terms"]),
            "is_blocked": rng.random() < 0.03,
            "is_marked_delete": rng.random() < 0.01,
            "created_date": created,
            "created_by": rng.choice(APPROVERS),
            "currency": rng.choice(prof["currencies"]),
            "spend_tier": rng.choices(["STRATEGIC", "PREFERRED", "TAIL"], weights=[5, 25, 70])[0],
        })

        # --- bank details, with change history -------------------------------
        n_versions = 1
        if rng.random() < 0.18:
            n_versions = 2
        if rng.random() < 0.03:
            n_versions = 3

        for v in range(n_versions):
            change_date = created if v == 0 else _rand_date(rng, cfg, skew_recent=False)
            acct = f"{rng.randint(10**9, 10**10 - 1)}"
            verified = True

            if v > 0:
                spec = dfx.BY_CODE["BANK_CHANGE_UNVERIFIED"]
                # A bank change with no verification. Paired with a payment inside
                # 14 days by the payment generator, this is the fraud signature.
                if dfx.should_inject(rng, spec, mult * 12):   # concentrated on changes only
                    verified = False
                    ledger.record(tenant_id=tenant_id, entity="vendor_bank",
                                  code="BANK_CHANGE_UNVERIFIED", business_key=f"{vid}|{acct}",
                                  detail=f"bank change on {change_date} with no verification event",
                                  injected_value=acct)

            spec = dfx.BY_CODE["BANK_ACCOUNT_MALFORMED"]
            if dfx.should_inject(rng, spec, mult):
                original = acct
                acct = f"{acct[:4]} {acct[4:7]}-{acct[7:]}"
                ledger.record(tenant_id=tenant_id, entity="vendor_bank",
                              code="BANK_ACCOUNT_MALFORMED", business_key=f"{vid}|{original}",
                              original_value=original, injected_value=acct)

            banks.append({
                "vendor_id": vid,
                "bank_country": country,
                "bank_key": f"{rng.randint(10**7, 10**8 - 1)}",
                "bank_account": acct,
                "bank_control_key": "",
                "account_holder": name[:35],
                "bank_type": "0001" if v == 0 else f"{v+1:04d}",
                "change_date": change_date,
                "changed_by": rng.choice(APPROVERS),
                "is_verified": verified,
                "version": v,
            })

    return vendors, banks


def gen_purchase_orders(cfg: GenConfig, tenant_id: str, vendors: list[dict],
                        ledger: dfx.DefectLedger) -> tuple[list[dict], list[dict], list[dict]]:
    prof = TENANT_PROFILE[tenant_id]
    rng = _rng(cfg, tenant_id, "po")
    mult = prof["defect_multiplier"]
    n = cfg.scaled(int(cfg.n_pos * TENANT_SPLIT[tenant_id]))

    headers, lines, receipts = [], [], []
    active_vendors = [v for v in vendors if not v["is_marked_delete"]]

    for i in range(n):
        po = f"{tenant_id[-1]}PO{i + 4500000}"
        vendor = rng.choice(active_vendors)
        po_date = _rand_date(rng, cfg)
        cc = rng.choice(prof["company_codes"])
        ccy = vendor["currency"]

        headers.append({
            "po_number": po,
            "vendor_id": vendor["vendor_id"],
            "company_code": cc,
            "po_date": po_date,
            "currency": ccy,
            "buyer": rng.choice(BUYERS),
            "po_type": rng.choices(["STANDARD", "BLANKET", "SERVICE"], weights=[70, 15, 15])[0],
            "terms_src": vendor["payment_terms_src"],
            "created_by": rng.choice(APPROVERS),
        })

        for ln in range(1, rng.choices([1, 2, 3, 5, 8], weights=[40, 25, 20, 10, 5])[0] + 1):
            qty = round(rng.uniform(1, 500), 3)
            price = _money(rng, 5, 5000)
            lines.append({
                "po_number": po, "po_line_number": ln * 10,
                "vendor_id": vendor["vendor_id"], "company_code": cc,
                "material_desc": f"{rng.choice(VENDOR_STEMS)} item {rng.randint(1000,9999)}",
                "quantity": qty, "unit_price": price,
                "net_value": round(qty * price, 2),
                "uom": rng.choice(UOMS), "currency": ccy,
                "delivery_date": po_date + timedelta(days=rng.randint(3, 60)),
                "gl_account": rng.choice(prof["gl_accounts"]),
                "cost_center": f"CC{rng.randint(1000, 1999)}",
                "tax_code": rng.choice(prof["tax_codes"]),
                "is_service": rng.random() < 0.2,
            })

    # --- receipts --------------------------------------------------------
    for pl in lines:
        # Some PO lines are never received: that is the invoiced-not-received
        # half of the GR/IR position, not an error.
        if rng.random() < 0.10:
            continue

        recv_qty = pl["quantity"]
        spec = dfx.BY_CODE["OVER_RECEIPT"]
        if dfx.should_inject(rng, spec, mult):
            recv_qty = round(pl["quantity"] * rng.uniform(1.06, 1.35), 3)
            ledger.record(tenant_id=tenant_id, entity="po_history", code="OVER_RECEIPT",
                          business_key=f"{pl['po_number']}|{pl['po_line_number']}",
                          original_value=pl["quantity"], injected_value=recv_qty,
                          detail="receipt quantity beyond configured tolerance")
        elif rng.random() < 0.08:
            recv_qty = round(pl["quantity"] * rng.uniform(0.6, 0.95), 3)   # partial, legitimate

        rdate = pl["delivery_date"] + timedelta(days=rng.randint(-3, 14))
        rdoc = f"{tenant_id[-1]}GR{rng.randint(5000000, 5999999)}"
        receipts.append({
            "receipt_doc_id": rdoc, "receipt_line": 1,
            "po_number": pl["po_number"], "po_line_number": pl["po_line_number"],
            "movement_type": "101", "receipt_date": rdate,
            "quantity": recv_qty, "uom": pl["uom"],
            "amount": round(recv_qty * pl["unit_price"], 2),
            "currency": pl["currency"], "posted_by": rng.choice(APPROVERS),
            "is_reversal": False,
        })

        # Reversals must net. Summing receipts without handling them
        # double-counts, which is the trap called out in Step 1.
        if rng.random() < 0.02:
            receipts.append({
                "receipt_doc_id": rdoc, "receipt_line": 2,
                "po_number": pl["po_number"], "po_line_number": pl["po_line_number"],
                "movement_type": "102", "receipt_date": rdate + timedelta(days=rng.randint(1, 20)),
                "quantity": -recv_qty, "uom": pl["uom"],
                "amount": round(-recv_qty * pl["unit_price"], 2),
                "currency": pl["currency"], "posted_by": rng.choice(APPROVERS),
                "is_reversal": True,
            })

        # Received but never invoiced: the GRNI half of GR/IR.
        spec = dfx.BY_CODE["GRIR_UNMATCHED_RECEIPT"]
        if dfx.should_inject(rng, spec, mult):
            pl["_never_invoiced"] = True
            ledger.record(tenant_id=tenant_id, entity="po_history", code="GRIR_UNMATCHED_RECEIPT",
                          business_key=f"{pl['po_number']}|{pl['po_line_number']}",
                          detail="goods received, no invoice will follow")

    return headers, lines, receipts


def gen_invoices(cfg: GenConfig, tenant_id: str, vendors: list[dict],
                 po_headers: list[dict], po_lines: list[dict],
                 ledger: dfx.DefectLedger) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    prof = TENANT_PROFILE[tenant_id]
    rng = _rng(cfg, tenant_id, "invoices")
    mult = prof["defect_multiplier"]
    n = cfg.scaled(int(cfg.n_invoices * TENANT_SPLIT[tenant_id]))
    target_lines = cfg.scaled(int(cfg.n_invoice_lines * TENANT_SPLIT[tenant_id]))
    avg_lines = max(1, round(target_lines / n))

    vendor_ids = [v["vendor_id"] for v in vendors]
    vendor_by_id = {v["vendor_id"]: v for v in vendors}
    po_by_number = {h["po_number"]: h for h in po_headers}
    lines_by_po: dict[str, list[dict]] = {}
    for pl in po_lines:
        lines_by_po.setdefault(pl["po_number"], []).append(pl)
    po_numbers = list(lines_by_po.keys())

    headers, lines, distributions, approvals = [], [], [], []
    seq = 0

    def _new_invoice(force: dict | None = None) -> dict:
        nonlocal seq
        seq += 1
        inv_date = _rand_date(rng, cfg)
        vendor = vendor_by_id[rng.choice(vendor_ids)]
        cc = rng.choice(prof["company_codes"])

        use_po = rng.random() < prof["po_rate"] and po_numbers
        po_number = rng.choice(po_numbers) if use_po else None
        if po_number:
            po_hdr = po_by_number[po_number]
            vendor = vendor_by_id.get(po_hdr["vendor_id"], vendor)
            cc = po_hdr["company_code"]
            inv_date = max(inv_date, po_hdr["po_date"] + timedelta(days=rng.randint(1, 45)))
            inv_date = min(inv_date, cfg.end_date)

        doc_type = "CREDIT_MEMO" if rng.random() < 0.055 else "INVOICE"
        posting = inv_date + timedelta(days=rng.randint(0, 12))
        channel = rng.choice(prof["channels"])
        capture = datetime.combine(
            inv_date + timedelta(days=rng.randint(0, 4)),
            datetime.min.time(),
        ) + timedelta(hours=rng.randint(6, 20), minutes=rng.randint(0, 59))

        inv = {
            "invoice_doc_id": f"{tenant_id[-1]}IV{seq + 1000000}",
            "supplier_invoice_number": f"INV-{rng.randint(100000, 999999)}",
            "company_code": cc,
            "fiscal_year": posting.year,
            "vendor_id": vendor["vendor_id"],
            "invoice_date": inv_date,
            "posting_date": posting,
            "entry_date": posting,
            "capture_ts": capture,
            "capture_channel": channel,
            "currency": vendor["currency"],
            "terms_src": vendor["payment_terms_src"],
            "doc_type": doc_type,
            "po_number": po_number,
            "payment_block": None,
            "gross_amount": 0.0,
            "tax_amount": 0.0,
            "_lines": [],
            "_touchless": channel in ("EDI", "EDI_810", "CXML"),
        }
        if force:
            inv.update(force)
        return inv

    for _ in range(n):
        inv = _new_invoice()
        n_lines = max(1, int(rng.gauss(avg_lines, avg_lines * 0.5)))
        po_line_pool = lines_by_po.get(inv["po_number"], []) if inv["po_number"] else []

        net_total, tax_total = 0.0, 0.0
        for ln in range(1, n_lines + 1):
            pl = po_line_pool[(ln - 1) % len(po_line_pool)] if po_line_pool else None
            if pl and pl.get("_never_invoiced"):
                pl = None

            if pl:
                qty = pl["quantity"]
                # Price and quantity variance: what the three-way match exists to find.
                price = pl["unit_price"] * (rng.uniform(0.97, 1.06) if rng.random() < 0.15 else 1.0)
                if rng.random() < 0.10:
                    qty = round(qty * rng.uniform(0.5, 1.0), 3)
                net = round(qty * price, 2)
                tax_code, gl, cctr = pl["tax_code"], pl["gl_account"], pl["cost_center"]
                uom = pl["uom"]
            else:
                qty = round(rng.uniform(1, 50), 3)
                net = _money(rng, 20, 20000)
                tax_code = rng.choice(prof["tax_codes"])
                gl = rng.choice(prof["gl_accounts"])
                cctr = f"CC{rng.randint(1000, 1999)}"
                uom = rng.choice(UOMS)

            if inv["doc_type"] == "CREDIT_MEMO":
                net = -abs(net)

            rate = {"V19": 19, "V07": 7, "V20": 20, "ST7": 7.25, "GST": 5,
                    "GST10": 10, "WH3": 24}.get(tax_code, 0)
            tax = round(net * rate / 100, 2)

            line = {
                "invoice_doc_id": inv["invoice_doc_id"], "company_code": inv["company_code"],
                "fiscal_year": inv["fiscal_year"], "line_number": ln,
                "po_number": pl["po_number"] if pl else None,
                "po_line_number": pl["po_line_number"] if pl else None,
                "quantity": qty, "uom": uom, "net_amount": net,
                "tax_code": tax_code, "tax_amount": tax,
                "gl_account": gl, "cost_center": cctr,
                "description": f"Line {ln} of {inv['invoice_doc_id']}",
            }

            # --- line-level defects -------------------------------------
            s = dfx.BY_CODE["MISSING_TAX_CODE"]
            if dfx.should_inject(rng, s, mult):
                ledger.record(tenant_id=tenant_id, entity="invoice_line", code="MISSING_TAX_CODE",
                              business_key=f"{inv['invoice_doc_id']}|{ln}", original_value=tax_code)
                line["tax_code"] = None

            s = dfx.BY_CODE["TAX_AMOUNT_INCONSISTENT"]
            if dfx.should_inject(rng, s, mult):
                bad = round(tax * rng.uniform(1.15, 1.9), 2)
                ledger.record(tenant_id=tenant_id, entity="invoice_line", code="TAX_AMOUNT_INCONSISTENT",
                              business_key=f"{inv['invoice_doc_id']}|{ln}",
                              original_value=tax, injected_value=bad)
                line["tax_amount"] = tax = bad

            s = dfx.BY_CODE["INACTIVE_GL_ACCOUNT"]
            if tenant_id == "TEN001" and dfx.should_inject(rng, s, mult):
                ledger.record(tenant_id=tenant_id, entity="invoice_line", code="INACTIVE_GL_ACCOUNT",
                              business_key=f"{inv['invoice_doc_id']}|{ln}",
                              original_value=gl, injected_value="0000479900")
                line["gl_account"] = "0000479900"

            s = dfx.BY_CODE["GRIR_UNMATCHED_INVOICE"]
            if pl and dfx.should_inject(rng, s, mult):
                ledger.record(tenant_id=tenant_id, entity="invoice_line", code="GRIR_UNMATCHED_INVOICE",
                              business_key=f"{inv['invoice_doc_id']}|{ln}",
                              detail="invoiced against a PO line with no receipt")
                line["_no_receipt"] = True

            inv["_lines"].append(line)
            net_total += line["net_amount"]
            tax_total += line["tax_amount"]

        inv["gross_amount"] = round(net_total + tax_total, 2)
        inv["tax_amount"] = round(tax_total, 2)

        # --- header-level defects ---------------------------------------
        s = dfx.BY_CODE["HEADER_LINE_MISMATCH"]
        if dfx.should_inject(rng, s, mult):
            bad = round(inv["gross_amount"] * rng.uniform(1.02, 1.15), 2)
            ledger.record(tenant_id=tenant_id, entity="invoice_header", code="HEADER_LINE_MISMATCH",
                          business_key=inv["invoice_doc_id"],
                          original_value=inv["gross_amount"], injected_value=bad)
            inv["gross_amount"] = bad

        s = dfx.BY_CODE["ZERO_AMOUNT"]
        if dfx.should_inject(rng, s, mult):
            ledger.record(tenant_id=tenant_id, entity="invoice_header", code="ZERO_AMOUNT",
                          business_key=inv["invoice_doc_id"], original_value=inv["gross_amount"])
            inv["gross_amount"] = 0.0

        s = dfx.BY_CODE["POSITIVE_CREDIT_MEMO"]
        if inv["doc_type"] == "CREDIT_MEMO" and dfx.should_inject(rng, s, mult * 8):
            ledger.record(tenant_id=tenant_id, entity="invoice_header", code="POSITIVE_CREDIT_MEMO",
                          business_key=inv["invoice_doc_id"],
                          detail="credit memo with a positive sign: debit/credit indicator lost")
            inv["gross_amount"] = abs(inv["gross_amount"])

        s = dfx.BY_CODE["NEGATIVE_NON_CREDIT"]
        if inv["doc_type"] == "INVOICE" and dfx.should_inject(rng, s, mult):
            ledger.record(tenant_id=tenant_id, entity="invoice_header", code="NEGATIVE_NON_CREDIT",
                          business_key=inv["invoice_doc_id"])
            inv["gross_amount"] = -abs(inv["gross_amount"])

        s = dfx.BY_CODE["NULL_MANDATORY_VENDOR"]
        if dfx.should_inject(rng, s, mult):
            ledger.record(tenant_id=tenant_id, entity="invoice_header", code="NULL_MANDATORY_VENDOR",
                          business_key=inv["invoice_doc_id"], original_value=inv["vendor_id"])
            inv["vendor_id"] = None

        s = dfx.BY_CODE["NULL_MANDATORY_SUPPLIER_INV"]
        if dfx.should_inject(rng, s, mult):
            ledger.record(tenant_id=tenant_id, entity="invoice_header", code="NULL_MANDATORY_SUPPLIER_INV",
                          business_key=inv["invoice_doc_id"], original_value=inv["supplier_invoice_number"])
            inv["supplier_invoice_number"] = None

        s = dfx.BY_CODE["NULL_CAPTURE_TS"]
        if dfx.should_inject(rng, s, mult):
            ledger.record(tenant_id=tenant_id, entity="invoice_header", code="NULL_CAPTURE_TS",
                          business_key=inv["invoice_doc_id"],
                          detail="cycle time and touchless rate cannot be computed for this record")
            inv["capture_ts"] = None

        s = dfx.BY_CODE["ORPHAN_INVOICE_VENDOR"]
        if dfx.should_inject(rng, s, mult):
            ghost = f"{tenant_id[-1]}V{rng.randint(900000, 999999)}"
            ledger.record(tenant_id=tenant_id, entity="invoice_header", code="ORPHAN_INVOICE_VENDOR",
                          business_key=inv["invoice_doc_id"],
                          original_value=inv["vendor_id"], injected_value=ghost)
            inv["vendor_id"] = ghost

        s = dfx.BY_CODE["FUTURE_POSTING_DATE"]
        if dfx.should_inject(rng, s, mult):
            future = cfg.end_date + timedelta(days=rng.randint(5, 120))
            ledger.record(tenant_id=tenant_id, entity="invoice_header", code="FUTURE_POSTING_DATE",
                          business_key=inv["invoice_doc_id"],
                          original_value=inv["posting_date"], injected_value=future)
            inv["posting_date"] = future

        s = dfx.BY_CODE["UNMAPPED_DOMAIN_VALUE"]
        if dfx.should_inject(rng, s, mult):
            bogus = rng.choice(["ZZ99", "NET-XX", "SPECIAL", "TBC"])
            ledger.record(tenant_id=tenant_id, entity="invoice_header", code="UNMAPPED_DOMAIN_VALUE",
                          business_key=inv["invoice_doc_id"],
                          original_value=inv["terms_src"], injected_value=bogus)
            inv["terms_src"] = bogus

        if rng.random() < 0.09:
            inv["payment_block"] = rng.choice(
                ["A", "M", "R"] if tenant_id == "TEN001" else ["HOLD"]
            )
            inv["_touchless"] = False

        headers.append(inv)

    # --- duplicates, added after the base population --------------------
    base = list(headers)
    for h in base:
        s = dfx.BY_CODE["NEAR_DUP_INVOICE"]
        if dfx.should_inject(rng, s, mult) and h["supplier_invoice_number"]:
            dup = dict(h)
            dup["_lines"] = [dict(l) for l in h["_lines"]]
            seq += 1
            dup["invoice_doc_id"] = f"{tenant_id[-1]}IV{seq + 1000000}"
            original = h["supplier_invoice_number"]
            style = rng.choice(["strip_dash", "leading_zeros", "spaces", "lower"])
            if style == "strip_dash":
                dup["supplier_invoice_number"] = original.replace("-", "")
            elif style == "leading_zeros":
                dup["supplier_invoice_number"] = "INV-" + original.split("-")[-1].zfill(9)
            elif style == "spaces":
                dup["supplier_invoice_number"] = original.replace("-", " - ")
            else:
                dup["supplier_invoice_number"] = original.lower()
            dup["posting_date"] = h["posting_date"] + timedelta(days=rng.randint(1, 25))
            for l in dup["_lines"]:
                l["invoice_doc_id"] = dup["invoice_doc_id"]
            ledger.record(tenant_id=tenant_id, entity="invoice_header", code="NEAR_DUP_INVOICE",
                          business_key=dup["invoice_doc_id"],
                          original_value=f"{h['invoice_doc_id']}|{original}",
                          injected_value=dup["supplier_invoice_number"],
                          detail=f"near-duplicate of {h['invoice_doc_id']} via {style}")
            headers.append(dup)

        s = dfx.BY_CODE["DUP_DIFFERENT_CHANNEL"]
        if dfx.should_inject(rng, s, mult) and h["supplier_invoice_number"]:
            dup = dict(h)
            dup["_lines"] = [dict(l) for l in h["_lines"]]
            seq += 1
            dup["invoice_doc_id"] = f"{tenant_id[-1]}IV{seq + 1000000}"
            dup["capture_channel"] = rng.choice([c for c in prof["channels"] if c != h["capture_channel"]])
            dup["posting_date"] = h["posting_date"] + timedelta(days=rng.randint(0, 6))
            for l in dup["_lines"]:
                l["invoice_doc_id"] = dup["invoice_doc_id"]
            ledger.record(tenant_id=tenant_id, entity="invoice_header", code="DUP_DIFFERENT_CHANNEL",
                          business_key=dup["invoice_doc_id"],
                          original_value=h["invoice_doc_id"],
                          detail=f"same invoice via {dup['capture_channel']} as well as {h['capture_channel']}")
            headers.append(dup)

    # Flatten lines, then inject orphans and exact-PK duplicates.
    for h in headers:
        lines.extend(h["_lines"])

    orphan_parents = set()
    for h in list(headers):
        s = dfx.BY_CODE["ORPHAN_INVOICE_LINE"]
        if dfx.should_inject(rng, s, mult):
            orphan_parents.add(h["invoice_doc_id"])
            ledger.record(tenant_id=tenant_id, entity="invoice_line", code="ORPHAN_INVOICE_LINE",
                          business_key=h["invoice_doc_id"],
                          detail="header withheld from the extract; its lines are orphans")
    headers = [h for h in headers if h["invoice_doc_id"] not in orphan_parents]

    for h in list(headers):
        s = dfx.BY_CODE["EXACT_DUP_PK"]
        if dfx.should_inject(rng, s, mult):
            ledger.record(tenant_id=tenant_id, entity="invoice_header", code="EXACT_DUP_PK",
                          business_key=h["invoice_doc_id"],
                          detail="identical primary key emitted twice in one extract")
            headers.append(dict(h))

    # --- Oracle distribution grain --------------------------------------
    if prof["has_distributions"]:
        for l in lines:
            n_dist = rng.choices([1, 2, 3], weights=[75, 18, 7])[0]
            remaining = l["net_amount"]
            for d in range(1, n_dist + 1):
                amt = round(remaining if d == n_dist else remaining * rng.uniform(0.3, 0.7), 2)
                remaining = round(remaining - amt, 2)
                distributions.append({
                    "invoice_doc_id": l["invoice_doc_id"], "company_code": l["company_code"],
                    "fiscal_year": l["fiscal_year"], "line_number": l["line_number"],
                    "distribution_number": d, "amount": amt,
                    "gl_account": l["gl_account"], "cost_center": l["cost_center"],
                })

    # --- approval events -------------------------------------------------
    if prof["has_approval_events"]:
        vendor_creator = {v["vendor_id"]: v["created_by"] for v in vendors}
        for h in headers:
            if h["gross_amount"] == 0:
                continue
            approver = rng.choice(APPROVERS)
            s = dfx.BY_CODE["SOD_BREACH"]
            creator = vendor_creator.get(h["vendor_id"])
            if creator and dfx.should_inject(rng, s, mult):
                approver = creator
                ledger.record(tenant_id=tenant_id, entity="approval_event", code="SOD_BREACH",
                              business_key=h["invoice_doc_id"],
                              detail=f"actor {approver} created the vendor and approved its invoice")
            base_ts = h["capture_ts"] or datetime.combine(h["invoice_date"], datetime.min.time())
            n_steps = 1 if abs(h["gross_amount"]) < 5000 else rng.choice([2, 2, 3])
            for step in range(1, n_steps + 1):
                approvals.append({
                    "invoice_doc_id": h["invoice_doc_id"], "company_code": h["company_code"],
                    "fiscal_year": h["fiscal_year"], "event_seq": step,
                    "event_type": "APPROVED" if step == n_steps else "ROUTED",
                    "actor": approver if step == n_steps else rng.choice(APPROVERS),
                    "event_ts": base_ts + timedelta(hours=rng.randint(2, 96) * step),
                    "comment": "",
                })

    return headers, lines, distributions, approvals


def gen_payments(cfg: GenConfig, tenant_id: str, invoices: list[dict],
                 vendor_banks: list[dict], ledger: dfx.DefectLedger) -> tuple[list[dict], list[dict]]:
    prof = TENANT_PROFILE[tenant_id]
    rng = _rng(cfg, tenant_id, "payments")
    n = cfg.scaled(int(cfg.n_payments * TENANT_SPLIT[tenant_id]))

    payable = [i for i in invoices
               if i["gross_amount"] > 0 and not i["payment_block"] and i["vendor_id"]]
    rng.shuffle(payable)
    payable = payable[:n]

    # Unverified bank changes, by vendor and date: used to place a payment
    # shortly after the change, which is the fraud pattern KPI 34 looks for.
    risky = {}
    for b in vendor_banks:
        if not b["is_verified"] and b["version"] > 0:
            risky.setdefault(b["vendor_id"], []).append(b["change_date"])

    payments, applications = [], []
    terms_days = {"Z030": 30, "Z045": 45, "Z060": 60, "Z210": 30, "Z000": 0,
                  "30 NET": 30, "45 NET": 45, "60 NET": 60, "2/10 NET 30": 30,
                  "1/15 NET 45": 45, "NET30": 30, "NET14": 14, "NET60": 60,
                  "3/5 NET 30": 30}

    for i, inv in enumerate(payable):
        net_days = terms_days.get(inv["terms_src"], 30)
        due = inv["invoice_date"] + timedelta(days=net_days)

        # Pay behaviour: mostly near due, a long tail of late, some early.
        r = rng.random()
        if r < 0.10:
            pay_date = due - timedelta(days=rng.randint(5, 25))     # early, often no discount taken
        elif r < 0.72:
            pay_date = due + timedelta(days=rng.randint(-2, 3))
        elif r < 0.93:
            pay_date = due + timedelta(days=rng.randint(4, 30))
        else:
            pay_date = due + timedelta(days=rng.randint(31, 120))

        # Place a payment inside the 14-day window after an unverified bank change.
        for cd in risky.get(inv["vendor_id"], []):
            if rng.random() < 0.45:
                pay_date = cd + timedelta(days=rng.randint(1, 13))
                break

        pay_date = min(max(pay_date, inv["invoice_date"]), cfg.end_date + timedelta(days=60))

        amount = inv["gross_amount"]
        partial = rng.random() < 0.06
        if partial:
            amount = round(amount * rng.uniform(0.3, 0.8), 2)

        pdoc = f"{tenant_id[-1]}PY{i + 7000000}"
        payments.append({
            "payment_doc_id": pdoc, "fiscal_year": pay_date.year,
            "company_code": inv["company_code"], "vendor_id": inv["vendor_id"],
            "payment_date": pay_date, "amount": amount, "currency": inv["currency"],
            "payment_method": rng.choices(["ACH", "WIRE", "CHECK", "SEPA"], weights=[55, 15, 10, 20])[0],
            "run_id": f"RUN{pay_date.strftime('%Y%m%d')}",
            "is_void": rng.random() < 0.008,
        })
        applications.append({
            "payment_doc_id": pdoc, "fiscal_year": pay_date.year,
            "invoice_doc_id": inv["invoice_doc_id"], "invoice_fiscal_year": inv["fiscal_year"],
            "company_code": inv["company_code"], "applied_amount": amount,
            "discount_taken": round(amount * 0.02, 2) if (
                inv["terms_src"] in ("Z210", "2/10 NET 30", "3/5 NET 30")
                and (pay_date - inv["invoice_date"]).days <= 10
            ) else 0.0,
            "payment_date": pay_date, "currency": inv["currency"],
        })

    return payments, applications


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def generate_tenant(cfg: GenConfig, tenant_id: str, ledger: dfx.DefectLedger) -> TenantData:
    data = TenantData(tenant_id=tenant_id)
    data.vendors, data.vendor_banks = gen_vendors(cfg, tenant_id, ledger)
    data.po_headers, data.po_lines, data.receipts = gen_purchase_orders(
        cfg, tenant_id, data.vendors, ledger
    )
    (data.invoice_headers, data.invoice_lines,
     data.invoice_distributions, data.approval_events) = gen_invoices(
        cfg, tenant_id, data.vendors, data.po_headers, data.po_lines, ledger
    )
    data.payments, data.payment_applications = gen_payments(
        cfg, tenant_id, data.invoice_headers, data.vendor_banks, ledger
    )
    _link_children_to_headers(data)
    return data


def _link_children_to_headers(data: TenantData) -> None:
    """Give every invoice line and payment application its header's FINAL
    external key and accounting date, after all header defects are applied.

    Sources that identify an invoice by the supplier's own number (TEN003)
    must emit that number on the lines and applications too, otherwise the
    children cannot be joined to their header at all. Taken at the end so a
    defect that changes the header key is reflected consistently. (Fixed in
    Step 4: lines previously carried the internal doc id, and a synthetic
    accounting date one fiscal year off, so no TEN003 line ever matched.)
    """
    by_doc = {h["invoice_doc_id"]: h for h in data.invoice_headers}
    for rec in list(data.invoice_lines) + list(data.payment_applications):
        h = by_doc.get(rec.get("invoice_doc_id"))
        if h is not None:
            rec["header_invoice_key"] = h.get("supplier_invoice_number") or h["invoice_doc_id"]
            rec["header_posting_date"] = h.get("posting_date")
