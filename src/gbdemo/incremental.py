"""Incremental registration updates: Auto Loader in, MERGE into an effective-dated history (SCD2).

Job ad: "batch and incremental ingestion, schema management, merge logic, deduplication and
maintaining historical records without losing the meaning of effective dates"; "reliable
scheduling, incremental processing and safe reprocessing".

GP systems send monthly update files: changes of address, practice transfers, deregistrations
and new registrations, each with an event id and an effective date. Bronze ingests new files only
(Auto Loader, pipelines/bronze_updates.py). Silver (pipelines/silver_updates.py) applies each
micro-batch to silver.patient_history, one row per patient per period:

    patient_id, postcode .. lad_code, practice_code, is_active, valid_from, valid_to, is_current

Rules (all counted, nothing silently dropped):
- duplicate event ids within a batch are removed; event ids already in the history are skipped,
  so re-delivered files and reruns change nothing (idempotent)
- events that can't be applied safely are quarantined with a reason: invalid NHS number,
  unknown event type, bad date, unusable postcode or practice, unknown patient, already
  registered, or late (effective before the patient's current version: needs a backfill)
- an event that changes nothing (e.g. a move to the same postcode) is a no-op
- several events for one patient in one batch are applied in effective-date order
- one MERGE per batch closes the current rows and inserts the new versions atomically
Reconciliation per batch: events in = duplicates + quarantined + already applied + no-ops + applied.
"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from gbdemo import nhs_number
from gbdemo.postcodes import is_valid_postcode, normalise_postcode
from gbdemo.silver import flags, try_date
from gbdemo.synthetic import stable_uniform

NEW, MOVE, TRANSFER, LEAVE = "NEW_REGISTRATION", "CHANGE_OF_ADDRESS", "PRACTICE_TRANSFER", "DEREGISTRATION"
EVENT_TYPES = (NEW, MOVE, TRANSFER, LEAVE)
# as received in the CSV files: all strings, schema fixed so a changed file fails loudly
EVENT_SCHEMA = (
    "event_id STRING, nhs_number STRING, event_type STRING, effective_date STRING, "
    "postcode STRING, practice_code STRING, extract_month STRING"
)
ADDRESS = ("postcode", "lsoa21_code", "icb_gss", "lad_code")
HISTORY_COLS = (
    "patient_id",
    *ADDRESS,
    "practice_code",
    "is_active",
    "valid_from",
    "valid_to",
    "is_current",
    "event_id",
    "event_type",
)
HISTORY_TYPES = [
    ("patient_id", "string"),
    ("postcode", "string"),
    ("lsoa21_code", "string"),
    ("icb_gss", "string"),
    ("lad_code", "string"),
    ("practice_code", "string"),
    ("is_active", "boolean"),
    ("valid_from", "date"),
    ("valid_to", "date"),
    ("is_current", "boolean"),
    ("event_id", "string"),
    ("event_type", "string"),
]
SALT = "gbdemo-updates-v1"
# monthly event rates per registered person (cumulative bands), then injected problems
RATES = {MOVE: 0.010, TRANSFER: 0.003, LEAVE: 0.002}
NEW_RATE = 0.003
DUPLICATE_RATE = 0.005  # events delivered twice in the same file
BAD_POSTCODE_RATE = 0.02  # of moves: typo'd postcode
LATE_RATE = 0.05  # of last month's movers: an earlier-dated move arrives a month late


# ---------------------------------------------------------------- synthetic monthly files


def _month_start(month: str) -> Column:
    return F.to_date(F.lit(month + "-01"))


def _pick(df: DataFrame, u: Column, choices: DataFrame, value_cols: list[str], tag: str) -> DataFrame:
    """Attach one row of `choices` to each row of df, chosen by the hash u (deterministic)."""
    n = choices.count()
    idx = choices.withColumn(f"_i_{tag}", F.row_number().over(Window.orderBy(*value_cols)) - 1)
    return df.withColumn(f"_i_{tag}", F.floor(u * n).cast("int")).join(F.broadcast(idx), f"_i_{tag}", "left")


def _events_for(people: DataFrame, month: str, postcodes: DataFrame, practices: DataFrame) -> DataFrame:
    """Moves, transfers and deregistrations for existing people in one month (no injected problems)."""
    u = stable_uniform(F.col("nhs_number"), F.lit(month), salt=SALT)
    band, edge = None, 0.0
    for kind, rate in RATES.items():  # cumulative bands: [0, 1%) move, [1%, 1.3%) transfer, ...
        edge += rate
        band = F.when(u < edge, F.lit(kind)) if band is None else band.when(u < edge, F.lit(kind))
    e = people.withColumn("event_type", band).where("event_type IS NOT NULL")
    day = F.floor(stable_uniform(F.col("nhs_number"), F.lit(month), salt=SALT + "day") * 28).cast("int")
    e = e.withColumn("effective", F.date_add(_month_start(month), day))
    e = _pick(
        e,
        stable_uniform(F.col("nhs_number"), F.lit(month), salt=SALT + "pc"),
        postcodes,
        ["new_postcode"],
        "pc",
    )
    e = _pick(
        e,
        stable_uniform(F.col("nhs_number"), F.lit(month), salt=SALT + "pr"),
        practices,
        ["new_practice"],
        "pr",
    )
    return e.select(
        "nhs_number",
        "event_type",
        "effective",
        F.when(F.col("event_type") == MOVE, F.col("new_postcode")).alias("postcode"),
        F.when(F.col("event_type") == TRANSFER, F.col("new_practice")).alias("practice_code"),
    )


def generate_updates(
    people: DataFrame, postcodes: DataFrame, practices: DataFrame, month: str, first_new_index: int
) -> DataFrame:
    """One month's update file (as received): events with ids, effective dates and problems.

    people: nhs_number of registered people; postcodes: new_postcode values to move to;
    practices: new_practice codes; first_new_index: test-range index for new registrations.
    Deterministic: the same month always gives the same file.
    """
    y, m = (int(x) for x in month.split("-"))
    prev = f"{y - (m == 1)}-{12 if m == 1 else m - 1:02d}"
    events = _events_for(people, month, postcodes, practices)

    # typo'd postcodes on a few moves: the inward code's digit typed as the letter O (always invalid)
    typo = stable_uniform(F.col("nhs_number"), F.lit(month), salt=SALT + "typo") < BAD_POSTCODE_RATE
    events = events.withColumn(
        "postcode",
        F.when((F.col("event_type") == MOVE) & typo, F.regexp_replace("postcode", " [0-9]", " O")).otherwise(
            F.col("postcode")
        ),
    )
    # late arrivals: a move of last month's movers, dated before their last-month move
    late = (
        _events_for(people, prev, postcodes, practices)
        .where(F.col("event_type") == MOVE)
        .where(stable_uniform(F.col("nhs_number"), F.lit(month), salt=SALT + "late") < LATE_RATE)
        .withColumn("effective", F.date_sub("effective", 1))
    )
    # new registrations: fresh test-range NHS numbers
    n_new = max(1, int(people.count() * NEW_RATE))
    last = first_new_index + int(n_new * 1.2)  # ~9% of bodies have no valid check digit
    if not (0 <= first_new_index and last <= nhs_number.TEST_BODIES):
        raise ValueError(
            f"new-registration indices {first_new_index:,}-{last:,} fall outside the "
            f"{nhs_number.TEST_BODIES:,} test-range bodies: numbers would repeat"
        )
    spark = people.sparkSession
    new = (
        spark.range(first_new_index, first_new_index + int(n_new * 1.2))
        .withColumn("nhs_number", nhs_number.synthetic_col(F.col("id")))
        .where("nhs_number IS NOT NULL")
        .limit(n_new)
        .withColumn("event_type", F.lit(NEW))
        .withColumn(
            "effective",
            F.date_add(
                _month_start(month),
                F.floor(stable_uniform(F.col("nhs_number"), salt=SALT + "nd") * 28).cast("int"),
            ),
        )
    )
    new = _pick(
        new, stable_uniform(F.col("nhs_number"), salt=SALT + "npc"), postcodes, ["new_postcode"], "pc"
    )
    new = _pick(
        new, stable_uniform(F.col("nhs_number"), salt=SALT + "npr"), practices, ["new_practice"], "pr"
    )
    new = new.select(
        "nhs_number",
        "event_type",
        "effective",
        F.col("new_postcode").alias("postcode"),
        F.col("new_practice").alias("practice_code"),
    )

    out = (
        events.unionByName(late)
        .unionByName(new)
        .withColumn(
            "event_id",
            F.substring(F.sha2(F.concat_ws("|", "nhs_number", "event_type", "effective"), 256), 1, 16),
        )
    )
    dup = out.where(stable_uniform(F.col("event_id"), salt=SALT + "dup") < DUPLICATE_RATE)
    return out.unionByName(dup).select(
        "event_id",
        "nhs_number",
        "event_type",
        F.date_format("effective", "yyyy-MM-dd").alias("effective_date"),
        "postcode",
        "practice_code",
        F.lit(month).alias("extract_month"),
    )


# ---------------------------------------------------------------- silver: stage, then apply


def seed_history(registrations: DataFrame) -> DataFrame:
    """The starting history: one current, active row per clean registration."""
    return registrations.select(
        "patient_id",
        *ADDRESS,
        "practice_code",
        F.lit(True).alias("is_active"),
        F.col("registration_date").alias("valid_from"),
        F.lit(None).cast("date").alias("valid_to"),
        F.lit(True).alias("is_current"),
        F.lit("baseline").alias("event_id"),
        F.lit("BASELINE").alias("event_type"),
    )


def stage(
    events: DataFrame, postcodes: DataFrame, practice_codes: DataFrame, key: str
) -> tuple[DataFrame, DataFrame, dict]:
    """Validate, deduplicate and pseudonymise one batch of raw events.

    Returns (valid events, quarantine, counts). postcodes: one NHSPD release (silver.postcode);
    practice_codes: practice_code of practices that hold a list.
    """
    pc = postcodes.select(
        F.col("postcode").alias("postcode_norm"), *ADDRESS[1:], "is_live", "easting", "grid_quality"
    )
    known = practice_codes.select("practice_code", F.lit(True).alias("practice_known")).distinct()
    w = Window.partitionBy("event_id").orderBy(F.col("_source_file").asc_nulls_last(), "nhs_number")
    ranked = events.withColumn("_n", F.row_number().over(w))
    e = (
        ranked.where("_n = 1")
        .withColumn("postcode_norm", normalise_postcode(F.col("postcode")))
        .withColumn("effective", try_date(F.col("effective_date"), "yyyy-MM-dd"))
        .withColumn("patient_id", nhs_number.pseudonymise_col(F.col("nhs_number"), key))
        .join(F.broadcast(pc), "postcode_norm", "left")
        .join(F.broadcast(known), "practice_code", "left")
    )
    needs_address = F.col("event_type").isin(NEW, MOVE)
    needs_practice = F.col("event_type").isin(NEW, TRANSFER)
    usable = F.col("is_live").isNotNull() & F.col("easting").isNotNull() & (F.col("grid_quality") != 9)
    e = e.withColumn(
        "dq_flags",
        flags(
            (~nhs_number.is_valid_col(F.col("nhs_number")), "invalid_nhs_number"),
            (~F.col("event_type").isin(*EVENT_TYPES) | F.col("event_type").isNull(), "unknown_event_type"),
            (F.col("effective").isNull(), "invalid_effective_date"),
            (needs_address & ~is_valid_postcode(F.col("postcode_norm")), "invalid_postcode"),
            (
                needs_address & is_valid_postcode(F.col("postcode_norm")) & ~F.coalesce(usable, F.lit(False)),
                "postcode_not_usable",
            ),
            (needs_practice & F.col("practice_known").isNull(), "unknown_practice"),
        ),
    )
    staged = e.where(F.size("dq_flags") == 0).select(
        "event_id",
        "patient_id",
        "event_type",
        "effective",
        F.col("postcode_norm").alias("postcode"),
        *ADDRESS[1:],
        "practice_code",
    )
    quarantine = e.where(F.size("dq_flags") > 0).select(
        "event_id",
        "patient_id",
        "event_type",
        "effective_date",
        F.col("postcode").alias("postcode_recorded"),
        "practice_code",
        "dq_flags",
    )
    counts = {
        "events_in": events.count(),
        "duplicates_removed": ranked.where("_n > 1").count(),
        "quarantined_validation": quarantine.count(),
        "staged": staged.count(),
    }
    return staged, quarantine, counts


def plan(history: DataFrame, staged: DataFrame) -> tuple[DataFrame, DataFrame, dict]:
    """Work out the MERGE source for one batch: rows to close and versions to insert.

    Returns (merge source, quarantine, counts). The source has merge_key + close_date for rows to
    close (matched on patient_id and is_current) and merge_key = null for rows to insert.
    """
    done = history.select("event_id").distinct()
    fresh = staged.join(done, "event_id", "left_anti")
    already = staged.join(done, "event_id", "left_semi").count()

    cur = history.where("is_current").select(
        "patient_id",
        F.struct(*ADDRESS).alias("c_address"),
        F.col("practice_code").alias("c_practice"),
        F.col("is_active").alias("c_active"),
        F.col("valid_from").alias("c_from"),
    )
    has_new = (
        fresh.where(F.col("event_type") == NEW)
        .select("patient_id")
        .distinct()
        .withColumn("_has_new", F.lit(True))
    )
    e = fresh.join(cur, "patient_id", "left").join(has_new, "patient_id", "left")
    e = e.withColumn(
        "dq_flags",
        flags(
            (F.col("c_from").isNull() & F.col("_has_new").isNull(), "unknown_patient"),
            (
                (F.col("event_type") == NEW) & F.coalesce(F.col("c_active"), F.lit(False)),
                "already_registered",
            ),
            (F.col("c_from").isNotNull() & (F.col("effective") < F.col("c_from")), "late_event"),
        ),
    )
    quarantine = e.where(F.size("dq_flags") > 0).select(
        "event_id",
        "patient_id",
        "event_type",
        F.date_format("effective", "yyyy-MM-dd").alias("effective_date"),
        F.col("postcode").alias("postcode_recorded"),
        "practice_code",
        "dq_flags",
    )
    ok = e.where(F.size("dq_flags") == 0)

    # one timeline per patient: the current row first (seq 0), then events in effective-date order
    events = ok.select(
        "patient_id",
        "event_id",
        "event_type",
        F.col("effective").alias("eff"),
        F.lit(1).alias("seq"),
        F.when(F.col("event_type").isin(NEW, MOVE), F.struct(*ADDRESS)).alias("address"),
        F.when(F.col("event_type").isin(NEW, TRANSFER), F.col("practice_code")).alias("practice"),
        F.when(F.col("event_type") == NEW, F.lit(True))
        .when(F.col("event_type") == LEAVE, F.lit(False))
        .alias("active"),
    )
    seeds = cur.join(ok.select("patient_id").distinct(), "patient_id").select(
        "patient_id",
        F.lit("seed").alias("event_id"),
        F.lit("SEED").alias("event_type"),
        F.col("c_from").alias("eff"),
        F.lit(0).alias("seq"),
        F.col("c_address").alias("address"),
        F.col("c_practice").alias("practice"),
        F.col("c_active").alias("active"),
    )
    order = ["eff", "seq", "event_id"]
    upto = Window.partitionBy("patient_id").orderBy(*order).rowsBetween(Window.unboundedPreceding, 0)
    t = seeds.unionByName(events)
    for c in ("address", "practice", "active"):
        t = t.withColumn(c, F.last(c, ignorenulls=True).over(upto))
    w = Window.partitionBy("patient_id").orderBy(*order)
    same = (
        F.lag("address").over(w).eqNullSafe(F.col("address"))
        & F.lag("practice").over(w).eqNullSafe(F.col("practice"))
        & F.lag("active").over(w).eqNullSafe(F.col("active"))
    )
    t = t.withColumn("_no_change", (F.col("seq") == 1) & same)
    no_change = t.where("_no_change").count()
    kept = t.where("NOT _no_change").withColumn("_next", F.lead("eff").over(w))

    close = kept.where((F.col("seq") == 0) & F.col("_next").isNotNull()).select(
        F.col("patient_id").alias("merge_key"),
        F.col("_next").alias("close_date"),
        *[F.lit(None).cast(dt).alias(c) for c, dt in HISTORY_TYPES],
    )
    versions = kept.where("seq = 1").select(
        F.lit(None).cast("string").alias("merge_key"),
        F.lit(None).cast("date").alias("close_date"),
        "patient_id",
        *[F.col("address")[c].alias(c) for c in ADDRESS],
        F.col("practice").alias("practice_code"),
        F.col("active").alias("is_active"),
        F.col("eff").alias("valid_from"),
        F.col("_next").alias("valid_to"),
        F.col("_next").isNull().alias("is_current"),
        "event_id",
        "event_type",
    )
    counts = {
        "already_applied": already,
        "quarantined_apply": quarantine.count(),
        "no_change": no_change,
        "applied": versions.count(),
        "rows_closed": close.count(),
    }
    return close.unionByName(versions), quarantine, counts


def merge(spark, history_table: str, source: DataFrame) -> None:
    """One atomic MERGE: close current rows (matched) and insert new versions (merge_key null)."""
    source.createOrReplaceTempView("_patient_history_changes")
    cols = ", ".join(c for c, _ in HISTORY_TYPES)
    vals = ", ".join(f"s.{c}" for c, _ in HISTORY_TYPES)
    spark.sql(f"""
        MERGE INTO {history_table} t
        USING _patient_history_changes s
        ON t.patient_id = s.merge_key AND t.is_current
        WHEN MATCHED THEN UPDATE SET t.valid_to = s.close_date, t.is_current = false
        WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})
    """)


def registered_as_of(history: DataFrame, day: str) -> DataFrame:
    """Patients registered on a date: the version valid then, if active (point-in-time view)."""
    d = F.lit(day).cast("date")
    return history.where(
        (F.col("valid_from") <= d)
        & (F.col("valid_to").isNull() | (F.col("valid_to") > d))
        & F.col("is_active")
    )


def check_history(history: DataFrame) -> dict:
    """Invariants: one current row per patient and no gaps or overlaps between versions."""
    w = Window.partitionBy("patient_id").orderBy("valid_from", "event_id")
    chain = history.withColumn("_next_from", F.lead("valid_from").over(w))
    per = history.groupBy("patient_id").agg(F.count_if("is_current").alias("n_current"))
    return {
        "history_rows": history.count(),
        "patients": per.count(),
        "patients_without_one_current_row": per.where("n_current <> 1").count(),
        "broken_chain_rows": chain.where(
            F.col("_next_from").isNotNull() & ~F.col("valid_to").eqNullSafe(F.col("_next_from"))
        ).count(),
    }
