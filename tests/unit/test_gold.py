"""Gold aggregates: exact aggregation first, protection last; only protected values out."""

from gbdemo import gold


def test_lsoa_registered_vs_resident(spark):
    census = spark.createDataFrame(
        [("EX1 1HS", 1000), ("ex1 1ab", 3), ("EX2 4NF", 6)], "postcode string, residents int"
    )
    postcodes = spark.createDataFrame(
        [
            ("EX1 1HS", "A", "E07000041"),
            ("EX1 1AB", "A", "E07000041"),  # terminated still resolves
            ("EX2 4NF", "B", "E07000041"),
        ],
        "postcode string, lsoa21_code string, lad_code string",
    )
    regs = spark.createDataFrame([("A",)] * 1053 + [("B",)] * 4 + [("C",)] * 12, "lsoa21_code string")
    names = spark.createDataFrame(
        [("A", "Exeter 001A"), ("B", "Exeter 001B"), ("C", "Exeter 001C")],
        "lsoa21_code string, lsoa21_name string",
    )

    res = gold.residents_by_lsoa(census, postcodes, ["E07000041"])
    out = {
        r.lsoa21_code: r
        for r in gold.lsoa_registered_vs_resident(res, gold.registered_by_lsoa(regs), names).collect()
    }

    a = out["A"]  # residents 1003 -> 1005, registered 1053 -> 1055, ratio from exact counts
    assert (a.residents, a.registered, a.registered_per_resident, a.any_suppressed) == (
        1005,
        1055,
        1.05,
        False,
    )
    b = out["B"]  # 6 residents, 4 registered: both suppressed, ratio too
    assert (b.residents, b.registered, b.registered_per_resident, b.any_suppressed) == (
        None,
        None,
        None,
        True,
    )
    c = out["C"]  # registered but no census residents (full outer join keeps it)
    assert (c.residents, c.registered, c.registered_per_resident) == (0, 10, None)


def test_msoa_registered_vs_resident(spark):
    """Same rules one level up: LSOAs A and B both sit in MSOA M, so their counts add before protection."""
    census = spark.createDataFrame([("EX1 1HS", 6), ("EX2 4NF", 7)], "postcode string, residents int")
    postcodes = spark.createDataFrame(
        [("EX1 1HS", "M", "E07000041"), ("EX2 4NF", "M", "E07000041")],
        "postcode string, msoa21_code string, lad_code string",
    )
    regs = spark.createDataFrame([("M",)] * 14, "msoa21_code string")
    names = spark.createDataFrame([("M", "Exeter 001")], "msoa21_code string, msoa21_name string")

    res = gold.residents_by_area(census, postcodes, ["E07000041"], "msoa21_code")
    registered = gold.registered_by_area(regs, "msoa21_code")
    (m,) = gold.msoa_registered_vs_resident(res, registered, names).collect()
    # 6 + 7 = 13 residents: safe once added up (each LSOA alone would be suppressed)
    assert (m.msoa21_code, m.msoa21_name, m.residents, m.registered, m.registered_per_resident) == (
        "M",
        "Exeter 001",
        15,
        15,
        1.077,
    )
