"""Documentation for analysts: one definition, two outputs.

Job ad: "clear communication with analysts".
GOLD describes every gold table and column once. The pipeline writes it into Unity Catalog as
table and column comments (shown in Catalog Explorer and by DESCRIBE TABLE), and
scripts/make_data_dictionary.py renders the same text as docs/data_dictionary.md. A unit test
fails if the two drift apart or a gold column has no description.
"""

from pyspark.sql import SparkSession

PROTECTED = "Disclosure-controlled: counts 1-9 are null (suppressed), others rounded to the nearest 5."
RATIO = ("Registered patients per resident, from exact counts, 3 decimals. Null if either count is "
         "suppressed or there are no residents.")
LINEAGE = {
    "nhspd_release": "NHS Postcode Directory release used to place postcodes (e.g. 2026-08).",
    "_run_id": "Databricks job run that wrote the row; its input and output Delta versions are in "
               "silver.reconciliation (step gold_aggregate).",
}


def _area_table(level: str, code: str, name: str, area: str) -> tuple[str, dict]:
    return (
        f"Synthetic GP registrations vs Census 2021 residents per {level} (2021 boundaries), for the "
        f"study districts. One row per {level}. {PROTECTED}",
        {
            code: f"{level} 2021 GSS code ({area}).",
            name: f"{level} 2021 name.",
            "lad_code": "Local authority district (GSS code) of the residents. Null for areas that only "
                        "have registrations: homes outside the study districts, mostly postcode typos "
                        "that are real postcodes elsewhere.",
            "residents": f"Census 2021 usual residents. {PROTECTED}",
            "registered": f"Registered patients (synthetic), after validation and deduplication. {PROTECTED}",
            "registered_per_resident": RATIO,
            "any_suppressed": "True if residents or registered was suppressed in this row.",
            **LINEAGE,
        },
    )


GOLD: dict[str, tuple[str, dict[str, str]]] = {
    "lsoa_registered_vs_resident": _area_table(
        "LSOA", "lsoa21_code", "lsoa21_name", "E01..., about 1,500 residents each"
    ),
    "msoa_registered_vs_resident": _area_table(
        "MSOA", "msoa21_code", "msoa21_name", "E02..., about 8,000 residents each"
    ),
    "practice_profile": (
        "Registered patients (synthetic) per GP practice and 5-year age band, for the latest ODS "
        f"snapshot. One row per practice and age band with at least one patient. {PROTECTED}",
        {
            "practice_code": "ODS practice code.",
            "practice_name": "Practice name from ODS (epraccur).",
            "postcode": "Practice postcode (ODS, public).",
            "lsoa21_code": "LSOA of the practice postcode, from the NHSPD release in force on the ODS "
                           "snapshot date.",
            "age_band": "Age band on the snapshot date: 00-04, 05-09, ..., 85-89, 90+.",
            "registered": f"Registered patients in the age band. {PROTECTED}",
            "suppressed": "True if registered was suppressed.",
            "list_size": f"All registered patients at the practice. {PROTECTED}",
            **LINEAGE,
        },
    ),
    "icb_summary": (
        "Registered patients (synthetic) vs Census 2021 residents per integrated care board, using "
        "the ICB edition in force for the NHSPD release. Residents are counted only in the study "
        f"districts. {PROTECTED}",
        {
            "icb_gss": "ICB GSS code (E54...).",
            "icb_name": "ICB name from the current ICB code list.",
            "residents": f"Census 2021 usual residents in the study districts. {PROTECTED}",
            "registered": f"Registered patients (synthetic) whose home is in the ICB. {PROTECTED}",
            "registered_per_resident": RATIO,
            **LINEAGE,
        },
    ),
    "lsoa_calibration": (
        "Calibration per LSOA in the study districts: Census 2021 residents, synthetic registered "
        "patients and real registered patients from NHS England's published 'Patients Registered at a "
        "GP Practice' (patients by LSOA of residence, all practices). The real ratio also carries "
        f"population change since the Census; see postcodes_since_census. {PROTECTED}",
        {
            "lsoa21_code": "LSOA 2021 GSS code.",
            "lsoa21_name": "LSOA 2021 name.",
            "lad_code": "Local authority district (GSS code).",
            "residents": f"Census 2021 usual residents. {PROTECTED}",
            "registered_synthetic": f"Registered patients in the synthetic list. {PROTECTED}",
            "registered_nhs": f"Real registered patients living in the LSOA (NHS England). {PROTECTED}",
            "synthetic_per_resident": RATIO,
            "nhs_per_resident": RATIO,
            "postcodes_since_census": "Postcodes in the LSOA introduced after Census day (21 March 2021), "
                                      "a sign of new housing the Census could not count.",
            "any_suppressed": "True if any of the three counts was suppressed in this row.",
            "nhs_extract": "NHS England extract used (YYYY-MM).",
            **LINEAGE,
        },
    ),
    "practice_calibration": (
        "Calibration per practice: patients living in the study LSOAs, synthetic vs real (NHS England). "
        "Tests the synthetic nearest-practice choice. Includes practices only one side uses. "
        f"{PROTECTED}",
        {
            "practice_code": "ODS code.",
            "practice_name": "Name from ODS (epraccur); null if not in the snapshot.",
            "prescribing_setting": "ODS prescribing setting; RO76 = GP practice. Other settings (minor "
                                   "injury units, prisons, pharmacies) hold no registered list.",
            "registered_synthetic": f"Synthetic registered patients from the study LSOAs. {PROTECTED}",
            "registered_nhs": f"Real registered patients from the study LSOAs. {PROTECTED}",
            "any_suppressed": "True if either count was suppressed in this row.",
            "nhs_extract": "NHS England extract used (YYYY-MM).",
            **LINEAGE,
        },
    ),
    "lsoa_points": (
        "One map point per study LSOA: the centroid of its 2021 boundary in WGS84, for dashboard point "
        "maps. Public ONS geography, no patient data.",
        {
            "lsoa21_code": "LSOA 2021 GSS code.",
            "lat": "Latitude of the LSOA centroid (WGS84, 6 decimals).",
            "lon": "Longitude of the LSOA centroid (WGS84, 6 decimals).",
            **LINEAGE,
        },
    ),
    "registered_monthly": (
        "Registered patients per district at each month end, read from the effective-dated patient "
        "history (silver.patient_history) that monthly update files are merged into. "
        f"{PROTECTED}",
        {
            "month_end": "Last day of the month the count applies to.",
            "lad_code": "Local authority district (GSS code) of the patient's address on that date.",
            "registered": f"Patients registered and active on that date. {PROTECTED}",
            "suppressed": "True if registered was suppressed.",
            "_run_id": "Databricks job run that wrote the row (gbdemo_incremental).",
        },
    ),
    "update_summary": (
        "Monthly registration update files: events received, applied to the history, and quarantined "
        "by reason. Counts distinct event ids, so re-delivered files and reruns are not double counted. "
        f"{PROTECTED}",
        {
            "extract_month": "Month of the update file (YYYY-MM).",
            "measure": "received, applied or quarantined.",
            "item": "Event type (received, applied), 'all' (received total) or the quarantine reason.",
            "events": f"Number of distinct events. {PROTECTED}",
            "_run_id": "Databricks job run that wrote the row (gbdemo_incremental).",
        },
    ),
}


def _sql_string(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def apply(spark: SparkSession, table: str, doc: tuple[str, dict[str, str]]) -> None:
    """Write the table comment and every column comment into the catalog."""
    table_comment, columns = doc
    spark.sql(f"COMMENT ON TABLE {table} IS {_sql_string(table_comment)}")
    for column, comment in columns.items():
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {column} COMMENT {_sql_string(comment)}")


def markdown(docs: dict[str, tuple[str, dict[str, str]]] = GOLD) -> str:
    out = [
        "# Data dictionary: gold tables",
        "",
        "Generated from `src/gbdemo/catalog_docs.py` by `scripts/make_data_dictionary.py`; the same text",
        "is stored as Unity Catalog table and column comments. Gold is the only layer analysts can read",
        "(see the bundle grants); every count in it is disclosure-controlled.",
        "",
    ]
    for table, (comment, columns) in docs.items():
        out += [f"## `gold.{table}`", "", comment, "", "| Column | Description |", "|---|---|"]
        out += [f"| `{c}` | {d} |" for c, d in columns.items()]
        out.append("")
    return "\n".join(out)
