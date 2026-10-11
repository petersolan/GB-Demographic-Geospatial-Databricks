"""Calibration: the synthetic registration list against real NHS England published counts.

Job ad (desirable): "NHS demographic data". The real data is NHS England's "Patients Registered at
a GP Practice" (GP Payments system), quarterly at LSOA 2021 level: patients per practice per LSOA
of residence. It is compared with the synthetic list and Census 2021 residents over the same LSOAs:

- per LSOA: real registered per resident vs the synthetic ratio (the synthetic model inflates
  every list uniformly; the real ratio also carries population change since the Census, which
  is why new-build postcodes are counted alongside)
- per practice: real patients living in the study area vs synthetic, which tests the
  nearest-practice choice model

Outputs are protected like every gold table, although the NHS source is already public.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gbdemo.disclosure import is_suppressed, protect, protect_ratio
from gbdemo.registrations import CENSUS_DAY

# rows in the NHS file whose patients have no usable LSOA (no postcode / postcode not matched)
NO_LSOA = ("EMPTY", "CLOSED")


def nhs_lsoa_patients(nhs_lsoa: DataFrame) -> DataFrame:
    """Bronze NHS rows -> (practice_code, lsoa21_code, patients) for patients with a usable LSOA."""
    return nhs_lsoa.where(~F.col("lsoa_code").isin(*NO_LSOA)).select(
        F.col("practice_code"),
        F.col("lsoa_code").alias("lsoa21_code"),
        F.col("number_of_patients").cast("long").alias("patients"),
    )


def nhs_registered_by_lsoa(patients: DataFrame) -> DataFrame:
    return patients.groupBy("lsoa21_code").agg(F.sum("patients").alias("registered_nhs"))


def postcodes_since_census(postcodes: DataFrame) -> DataFrame:
    """Postcodes introduced after Census day per LSOA: new housing the Census could not count."""
    return postcodes.groupBy("lsoa21_code").agg(
        F.count_if(F.col("date_introduced") > F.lit(CENSUS_DAY).cast("date")).alias("postcodes_since_census")
    )


def lsoa_calibration(
    residents: DataFrame, synthetic: DataFrame, nhs: DataFrame, new_postcodes: DataFrame, names: DataFrame
) -> DataFrame:
    """Per study-area LSOA: residents, synthetic and real registered, both ratios; protected.

    Driven by the residents (the study area): real patients living elsewhere are not compared.
    """
    exact = (
        residents.join(
            synthetic.withColumnRenamed("registered", "registered_synthetic"), "lsoa21_code", "left"
        )
        .join(nhs, "lsoa21_code", "left")
        .join(new_postcodes, "lsoa21_code", "left")
        .fillna(0, ["registered_synthetic", "registered_nhs", "postcodes_since_census"])
    )
    s = [is_suppressed(F.col(c)) for c in ("residents", "registered_synthetic", "registered_nhs")]
    return exact.join(names, "lsoa21_code", "left").select(
        "lsoa21_code",
        "lsoa21_name",
        "lad_code",
        protect(F.col("residents")).alias("residents"),
        protect(F.col("registered_synthetic")).alias("registered_synthetic"),
        protect(F.col("registered_nhs")).alias("registered_nhs"),
        protect_ratio(F.col("registered_synthetic"), F.col("residents")).alias("synthetic_per_resident"),
        protect_ratio(F.col("registered_nhs"), F.col("residents")).alias("nhs_per_resident"),
        F.col("postcodes_since_census"),  # a count of postcodes, not people: not disclosive
        (s[0] | s[1] | s[2]).alias("any_suppressed"),
    )


def practice_calibration(
    synthetic_regs: DataFrame, nhs_patients: DataFrame, study_lsoas: DataFrame, practices: DataFrame
) -> DataFrame:
    """Per practice: patients living in the study LSOAs, synthetic vs real; protected.

    A full join, so practices only the model uses (e.g. non-GP services) and practices only real
    patients use (further away, or chosen for other reasons) both show up.
    """
    syn = (
        synthetic_regs.join(study_lsoas, "lsoa21_code")
        .groupBy("practice_code")
        .agg(F.count("*").alias("registered_synthetic"))
    )
    real = (
        nhs_patients.join(study_lsoas, "lsoa21_code")
        .groupBy("practice_code")
        .agg(F.sum("patients").alias("registered_nhs"))
    )
    exact = syn.join(real, "practice_code", "full").fillna(0, ["registered_synthetic", "registered_nhs"])
    info = practices.select("practice_code", "practice_name", "prescribing_setting")
    return exact.join(info, "practice_code", "left").select(
        "practice_code",
        "practice_name",
        "prescribing_setting",
        protect(F.col("registered_synthetic")).alias("registered_synthetic"),
        protect(F.col("registered_nhs")).alias("registered_nhs"),
        (is_suppressed(F.col("registered_synthetic")) | is_suppressed(F.col("registered_nhs"))).alias(
            "any_suppressed"
        ),
    )


def overlap_share(synthetic_regs: DataFrame, nhs_patients: DataFrame, study_lsoas: DataFrame) -> float:
    """Share of real study-area patients registered in an (LSOA, practice) pair the model also uses."""
    pairs = synthetic_regs.select("lsoa21_code", "practice_code").distinct()
    real = nhs_patients.join(study_lsoas, "lsoa21_code")
    total = real.agg(F.sum("patients")).first()[0] or 0
    covered = real.join(pairs, ["lsoa21_code", "practice_code"]).agg(F.sum("patients")).first()[0] or 0
    return round(covered / total, 4) if total else 0.0
