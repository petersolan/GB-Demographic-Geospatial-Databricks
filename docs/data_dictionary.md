# Data dictionary: gold tables

Generated from `src/gbdemo/catalog_docs.py` by `scripts/make_data_dictionary.py`; the same text
is stored as Unity Catalog table and column comments. Gold is the only layer analysts can read
(see the bundle grants); every count in it is disclosure-controlled.

## `gold.lsoa_registered_vs_resident`

Synthetic GP registrations vs Census 2021 residents per LSOA (2021 boundaries), for the study districts. One row per LSOA. Disclosure-controlled: counts 1-9 are null (suppressed), others rounded to the nearest 5.

| Column | Description |
|---|---|
| `lsoa21_code` | LSOA 2021 GSS code (E01..., about 1,500 residents each). |
| `lsoa21_name` | LSOA 2021 name. |
| `lad_code` | Local authority district (GSS code) of the residents. Null for areas that only have registrations: homes outside the study districts, mostly postcode typos that are real postcodes elsewhere. |
| `residents` | Census 2021 usual residents. Disclosure-controlled: counts 1-9 are null (suppressed), others rounded to the nearest 5. |
| `registered` | Registered patients (synthetic), after validation and deduplication. Disclosure-controlled: counts 1-9 are null (suppressed), others rounded to the nearest 5. |
| `registered_per_resident` | Registered patients per resident, from exact counts, 3 decimals. Null if either count is suppressed or there are no residents. |
| `any_suppressed` | True if residents or registered was suppressed in this row. |
| `nhspd_release` | NHS Postcode Directory release used to place postcodes (e.g. 2026-08). |
| `_run_id` | Databricks job run that wrote the row; its input and output Delta versions are in silver.reconciliation (step gold_aggregate). |

## `gold.msoa_registered_vs_resident`

Synthetic GP registrations vs Census 2021 residents per MSOA (2021 boundaries), for the study districts. One row per MSOA. Disclosure-controlled: counts 1-9 are null (suppressed), others rounded to the nearest 5.

| Column | Description |
|---|---|
| `msoa21_code` | MSOA 2021 GSS code (E02..., about 8,000 residents each). |
| `msoa21_name` | MSOA 2021 name. |
| `lad_code` | Local authority district (GSS code) of the residents. Null for areas that only have registrations: homes outside the study districts, mostly postcode typos that are real postcodes elsewhere. |
| `residents` | Census 2021 usual residents. Disclosure-controlled: counts 1-9 are null (suppressed), others rounded to the nearest 5. |
| `registered` | Registered patients (synthetic), after validation and deduplication. Disclosure-controlled: counts 1-9 are null (suppressed), others rounded to the nearest 5. |
| `registered_per_resident` | Registered patients per resident, from exact counts, 3 decimals. Null if either count is suppressed or there are no residents. |
| `any_suppressed` | True if residents or registered was suppressed in this row. |
| `nhspd_release` | NHS Postcode Directory release used to place postcodes (e.g. 2026-08). |
| `_run_id` | Databricks job run that wrote the row; its input and output Delta versions are in silver.reconciliation (step gold_aggregate). |

## `gold.practice_profile`

Registered patients (synthetic) per GP practice and 5-year age band, for the latest ODS snapshot. One row per practice and age band with at least one patient. Disclosure-controlled: counts 1-9 are null (suppressed), others rounded to the nearest 5.

| Column | Description |
|---|---|
| `practice_code` | ODS practice code. |
| `practice_name` | Practice name from ODS (epraccur). |
| `postcode` | Practice postcode (ODS, public). |
| `lsoa21_code` | LSOA of the practice postcode, from the NHSPD release in force on the ODS snapshot date. |
| `age_band` | Age band on the snapshot date: 00-04, 05-09, ..., 85-89, 90+. |
| `registered` | Registered patients in the age band. Disclosure-controlled: counts 1-9 are null (suppressed), others rounded to the nearest 5. |
| `suppressed` | True if registered was suppressed. |
| `list_size` | All registered patients at the practice. Disclosure-controlled: counts 1-9 are null (suppressed), others rounded to the nearest 5. |
| `nhspd_release` | NHS Postcode Directory release used to place postcodes (e.g. 2026-08). |
| `_run_id` | Databricks job run that wrote the row; its input and output Delta versions are in silver.reconciliation (step gold_aggregate). |

## `gold.icb_summary`

Registered patients (synthetic) vs Census 2021 residents per integrated care board, using the ICB edition in force for the NHSPD release. Residents are counted only in the study districts. Disclosure-controlled: counts 1-9 are null (suppressed), others rounded to the nearest 5.

| Column | Description |
|---|---|
| `icb_gss` | ICB GSS code (E54...). |
| `icb_name` | ICB name from the current ICB code list. |
| `residents` | Census 2021 usual residents in the study districts. Disclosure-controlled: counts 1-9 are null (suppressed), others rounded to the nearest 5. |
| `registered` | Registered patients (synthetic) whose home is in the ICB. Disclosure-controlled: counts 1-9 are null (suppressed), others rounded to the nearest 5. |
| `registered_per_resident` | Registered patients per resident, from exact counts, 3 decimals. Null if either count is suppressed or there are no residents. |
| `nhspd_release` | NHS Postcode Directory release used to place postcodes (e.g. 2026-08). |
| `_run_id` | Databricks job run that wrote the row; its input and output Delta versions are in silver.reconciliation (step gold_aggregate). |
