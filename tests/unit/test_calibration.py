"""Calibration against NHS England counts: unusable LSOAs dropped, both sides kept, protected output."""
import datetime as dt

from gbdemo import calibration

NHS_COLS = "practice_code string, lsoa_code string, number_of_patients string"  # bronze: all strings


def nhs(spark):
    return calibration.nhs_lsoa_patients(spark.createDataFrame([
        ("P1", "A", "1200"), ("P1", "B", "4"), ("P2", "A", "300"),
        ("P2", "EMPTY", "7"), ("P1", "CLOSED", "2"),           # no usable LSOA: excluded
        ("P3", "Z", "500"),                                     # lives outside the study area
    ], NHS_COLS))


def test_nhs_patients_drop_rows_without_lsoa(spark):
    rows = {(r.practice_code, r.lsoa21_code): r.patients for r in nhs(spark).collect()}
    assert rows == {("P1", "A"): 1200, ("P1", "B"): 4, ("P2", "A"): 300, ("P3", "Z"): 500}


def test_postcodes_since_census(spark):
    pcs = spark.createDataFrame([
        ("A", dt.date(2020, 1, 1)), ("A", dt.date(2021, 3, 21)),   # on Census day: counted by it
        ("A", dt.date(2023, 6, 1)), ("B", None),
    ], "lsoa21_code string, date_introduced date")
    out = {r.lsoa21_code: r.postcodes_since_census for r in calibration.postcodes_since_census(pcs).collect()}
    assert out == {"A": 1, "B": 0}


def test_lsoa_calibration_protected_and_study_area_only(spark):
    residents = spark.createDataFrame([("A", "L", 1003), ("B", "L", 50)],
                                      "lsoa21_code string, lad_code string, residents long")
    synthetic = spark.createDataFrame([("A", 1053), ("B", 52)], "lsoa21_code string, registered long")
    new_pcs = spark.createDataFrame([("A", 3)], "lsoa21_code string, postcodes_since_census long")
    names = spark.createDataFrame([("A", "Exeter 001A"), ("B", "Exeter 001B")],
                                  "lsoa21_code string, lsoa21_name string")
    out = {r.lsoa21_code: r for r in calibration.lsoa_calibration(
        residents, synthetic, calibration.nhs_registered_by_lsoa(nhs(spark)), new_pcs, names).collect()}

    assert set(out) == {"A", "B"}                                  # Z (outside the study) not compared
    a = out["A"]  # exact 1003 / 1053 / 1500 -> rounded to 5; ratios from exact counts
    assert (a.residents, a.registered_synthetic, a.registered_nhs) == (1005, 1055, 1500)
    assert (a.synthetic_per_resident, a.nhs_per_resident, a.postcodes_since_census) == (1.05, 1.496, 3)
    b = out["B"]  # 4 real patients: suppressed, and so is its ratio
    assert (b.registered_nhs, b.nhs_per_resident, b.any_suppressed, b.postcodes_since_census) == (
        None, None, True, 0)


def test_practice_calibration_keeps_both_sides(spark):
    regs = spark.createDataFrame([("A", "P1")] * 900 + [("A", "Y1")] * 400 + [("Z", "P3")] * 20,
                                 "lsoa21_code string, practice_code string")
    study = spark.createDataFrame([("A",), ("B",)], "lsoa21_code string")
    practices = spark.createDataFrame([
        ("P1", "Practice One", "RO76"), ("P2", "Practice Two", "RO76"), ("Y1", "Minor Injury Unit", "RO72"),
    ], "practice_code string, practice_name string, prescribing_setting string")
    out = calibration.practice_calibration(regs, nhs(spark), study, practices).collect()
    out = {r.practice_code: r for r in out}

    assert set(out) == {"P1", "P2", "Y1"}                          # P3 only serves Z: outside the study
    assert (out["P1"].registered_synthetic, out["P1"].registered_nhs) == (900, 1205)
    assert (out["P2"].registered_synthetic, out["P2"].registered_nhs) == (0, 300)   # only real patients
    y = out["Y1"]                                                  # only the model uses it: not a GP list
    assert (y.registered_synthetic, y.registered_nhs, y.prescribing_setting) == (400, 0, "RO72")


def test_overlap_share(spark):
    regs = spark.createDataFrame([("A", "P1"), ("B", "P1")], "lsoa21_code string, practice_code string")
    study = spark.createDataFrame([("A",), ("B",)], "lsoa21_code string")
    # real study patients: (P1,A) 1200 + (P1,B) 4 + (P2,A) 300 = 1504; the model uses (A,P1) and (B,P1)
    assert calibration.overlap_share(regs, nhs(spark), study) == round(1204 / 1504, 4)
