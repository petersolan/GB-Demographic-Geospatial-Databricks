"""Silver typing, vintage-aligned ICB enrichment, flags, and GP practice matching."""
import datetime as dt

from gbdemo import silver

NHSPD_COLS = ["pcds", "dointr", "doterm", "oseast1m", "osnrth1m", "osgrdind", "usertype", "ctry", "oslaua",
              "nhser", "sicbl", "icb", "oa21", "lsoa21", "msoa21", "_version", "_run_id"]
ODS_COLS = ("organisation_code string, name string, postcode string, status_code string, open_date string, "
            "close_date string, high_level_health_geography string, commissioner string, "
            "prescribing_setting string, _version string")


def nhspd_row(pcds, version, icb="QJK", doterm=None, east="292057", north="0092517", grid="1",
              dointr="198001"):
    return (pcds, dointr, doterm, east, north, grid, "0", "E92000001", "E07000041",
            "Y58", "15N", icb, "E00000001", "E01034630", "E02000001", version, "run-1")


def lookup(spark):
    codes = spark.createDataFrame([
        ("QJK", "E54000037", "NHS Devon", "2023-04"), ("OLD", "E54000011", "Old merged ICB", "2023-04"),
        ("QJK", "E54000037", "NHS Devon", "2026-04"), ("NEW01", "E54000070", "New merged ICB", "2026-04"),
    ], "icb_cdh string, icb_cd string, icb_nm string, _version string")
    return silver.icb_lookup(codes)


def build(spark, rows):
    bronze = spark.createDataFrame(rows, ", ".join(f"{c} string" for c in NHSPD_COLS))
    return silver.flag_nhspd(silver.enrich_icb(silver.typed_nhspd(bronze), lookup(spark)))


def test_typing_and_vintage_aligned_icb(spark):
    out = {(r.postcode, r.nhspd_release): r for r in build(spark, [
        nhspd_row("EX1 1HS", "2025-08"),
        nhspd_row("EX1 1HS", "2026-08"),
        nhspd_row("CB1 1AA", "2025-08", icb="OLD"),     # old code, valid for the 2023 edition
        nhspd_row("CB1 1AA", "2026-08", icb="NEW01"),   # merged ICB, new 5-character code
        nhspd_row("CB1 2AA", "2026-08", icb="OLD"),     # old code in a 2026 release: no match
    ]).collect()}

    ex = out[("EX1 1HS", "2025-08")]
    assert (ex.easting, ex.northing, ex.grid_quality) == (292057, 92517, 1)  # leading zero gone once typed
    assert ex.date_introduced == dt.date(1980, 1, 1) and ex.is_live
    assert ex.icb_gss == "E54000037" and ex.icb_edition == "2023-04"
    assert out[("EX1 1HS", "2026-08")].icb_edition == "2026-04"
    assert out[("CB1 1AA", "2025-08")].icb_gss == "E54000011"
    assert out[("CB1 1AA", "2026-08")].icb_gss == "E54000070"
    assert out[("CB1 2AA", "2026-08")].dq_flags == ["icb_code_unmatched"]
    assert ex.dq_flags == []


def test_bad_values_are_flagged_not_fatal(spark):
    # ANSI mode would raise on CAST('12AB' AS INT); try_cast flags it instead
    out = {r.postcode: r for r in build(spark, [
        nhspd_row("TQ1 1AA", "2026-08", east="12AB"),
        nhspd_row("TQ1 2AA", "2026-08", dointr="19X801"),
        nhspd_row("TQ1 3AA", "2026-08", grid="9", east="", north=""),
        nhspd_row("AB1 0AA", "2026-08", doterm="199606"),
    ]).collect()}
    assert out["TQ1 1AA"].dq_flags == ["no_grid_reference", "unparseable_coordinate"]
    assert out["TQ1 2AA"].dq_flags == ["unparseable_date"]
    assert out["TQ1 3AA"].dq_flags == ["no_grid_reference"]
    assert out["AB1 0AA"].is_live is False and out["AB1 0AA"].date_terminated == dt.date(1996, 6, 1)


def test_practices_matched_to_release_in_force(spark):
    postcodes = build(spark, [
        nhspd_row("EX1 1HS", "2025-08"), nhspd_row("EX1 1HS", "2026-08"),
        nhspd_row("EX2 4NF", "2026-08", doterm="202001"),          # terminated postcode
        nhspd_row("EX4 4QJ", "2026-08", grid="9", east="", north=""),
    ])
    ods = spark.createDataFrame([
        ("L83001", "A", "ex11hs", "ACTIVE", "19740401", "", "QJK", "15N", "RO76", "2026-10-10"),
        ("L83002", "B", "EX2 4NF", "ACTIVE", "19740401", "", "QJK", "15N", "RO76", "2026-10-10"),
        ("L83003", "C", "ZZ99 9ZZ", "ACTIVE", "19740401", "", "QJK", "15N", "RO76", "2026-10-10"),
        ("L83004", "D", "BAD", "INACTIVE", "19740401", "20171031", "QJK", "15N", "RO76", "2026-10-10"),
        ("L83005", "E", "EX4 4QJ", "ACTIVE", "19740401", "", "QJK", "15N", "RO76", "2026-10-10"),
    ], ODS_COLS)
    matched = silver.match_practices(silver.typed_practices(ods), postcodes)
    out = {r.practice_code: r for r in matched.collect()}

    a = out["L83001"]
    assert a.postcode == "EX1 1HS" and a.nhspd_release == "2026-08" and a.lsoa21_code == "E01034630"
    assert a.dq_flags == [] and not a.is_blocked
    assert out["L83002"].dq_flags == ["postcode_terminated"] and not out["L83002"].is_blocked
    assert out["L83003"].dq_flags == ["postcode_not_in_nhspd"] and out["L83003"].is_blocked
    assert out["L83004"].dq_flags == ["invalid_format"] and out["L83004"].close_date == dt.date(2017, 10, 31)
    assert out["L83005"].dq_flags == ["no_grid_reference"] and out["L83005"].is_blocked
