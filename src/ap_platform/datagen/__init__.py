"""datagen — synthetic, deliberately messy AP source data.

Three layers, deliberately separated:

* ``defects``    — the catalogue of injected defects and the ledger that records
                   every injection, so detection can be measured rather than
                   assumed.
* ``generators`` — a coherent business dataset in a neutral shape, carrying the
                   *semantic* defects (duplicates, orphans, over-receipts).
* ``writers``    — renders that dataset into each ERP's native file format,
                   carrying the *representational* defects (date and number
                   formats, whitespace, encodings) plus delivery: full snapshots,
                   CDC deltas, incremental loads, and schema drift.

Typical use, from notebook 03:

    from ap_platform.datagen import defects, generators as gen, writers as wr

    ledger = defects.DefectLedger()
    cfg    = gen.GenConfig(scale=0.2)
    wcfg   = wr.WriteConfig(root="/Volumes/ap_bronze/landing/inbox")
    data   = [gen.generate_tenant(cfg, t, ledger) for t in gen.TENANT_SPLIT]
    files  = wr.write_all(data, wcfg, ledger)
"""

from . import defects, generators, writers  # noqa: F401

__all__ = ["defects", "generators", "writers"]
