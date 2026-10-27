"""Profile Crime IDs that repeat within a bronze file, and compare candidate dedupe rules.

Read-only analysis (not part of the pipeline). Reads the bronze Delta table and the kept
2026-08.zip (outcomes CSVs are copied to a temp dir that is deleted on exit) and prints
findings to stdout. Writes nothing under data/.

    uv run python analyses/profile_duplicate_crime_ids.py [--archive 2026-08]

A "group" is the rows sharing (_force, _month, crime_id) in one archive. A "variant" is a
distinct combination of all 12 source columns within a group.
"""

import argparse
import re
import shutil
import tempfile
import zipfile
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from uk_crime import config
from uk_crime.bronze import SOURCE_BRONZE_NAMES
from uk_crime.spark import get_spark

KEY = ["_force", "_month", "crime_id"]
OUTCOME = "last_outcome_category"
LOCATION_COLS = ["longitude", "latitude", "location", "lsoa_code", "lsoa_name"]
OTHER_COLS = ["month", "reported_by", "falls_within", "context"]
NON_FINAL = ["Under investigation", "Status update unavailable"]  # plus NULL
# Street files say "Awaiting court outcome" / "Court result unavailable" where the
# outcomes files say "Suspect charged" (assumption, checked in the output of section 6).
STREET_TO_OUTCOMES_FILE = {
    "Awaiting court outcome": "Suspect charged",
    "Court result unavailable": "Suspect charged",
}
OUTCOMES_COLUMNS = [
    "Crime ID", "Month", "Reported by", "Falls within", "Longitude", "Latitude",
    "Location", "LSOA code", "LSOA name", "Outcome type",
]  # fmt: skip
NULL = "<NULL>"


def heading(text: str) -> None:
    print(f"\n{'=' * 100}\n{text}\n{'=' * 100}")


def show(df: DataFrame, total: int | None = None, limit: int = 50) -> None:
    """Print a small result DataFrame; add a % column when total is given."""
    rows = df.limit(limit).collect()
    cols = df.columns
    print(" | ".join(cols + (["pct"] if total else [])))
    for r in rows:
        extra = [f"{100 * r[cols[-1]] / total:.2f}%"] if total else []
        print(" | ".join(str(v) for v in [*r, *extra]))


def pct(part: int, whole: int) -> str:
    return f"{part:,} ({100 * part / whole:.2f}%)" if whole else f"{part:,}"


def nn(col: str) -> F.Column:
    """Column with NULL made explicit so collect_set keeps it."""
    return F.coalesce(F.col(col), F.lit(NULL))


# ------------------------------------------------------------------------------ build


def build_variants(spark: SparkSession, archive: str) -> DataFrame:
    """One row per distinct 12-column variant within each (force, month, crime_id)."""
    bronze = spark.read.format("delta").load(str(config.BRONZE_STREET_TABLE))
    rows = bronze.where(
        (F.col("_archive_month") == F.lit(f"{archive}-01").cast("date"))
        & F.col("crime_id").isNotNull()
    )
    return rows.groupBy(*KEY, *[c for c in SOURCE_BRONZE_NAMES if c != "crime_id"]).agg(
        F.count(F.lit(1)).alias("copies")
    )


def build_groups(variants: DataFrame) -> DataFrame:
    """One row per group with variant/row counts and which column families differ."""

    def distinct_count(cols: list[str]) -> F.Column:
        return F.size(F.collect_set(F.struct(*[nn(c) for c in cols])))

    return variants.groupBy(*KEY).agg(
        F.sum("copies").alias("n_rows"),
        F.count(F.lit(1)).alias("n_variants"),
        (distinct_count([OUTCOME]) > 1).alias("d_outcome"),
        (distinct_count(["crime_type"]) > 1).alias("d_crime_type"),
        (distinct_count(LOCATION_COLS) > 1).alias("d_location"),
        (distinct_count(OTHER_COLS) > 1).alias("d_other"),
        F.sort_array(F.collect_set(nn(OUTCOME))).alias("outcomes"),
        F.sort_array(F.collect_set(nn("crime_type"))).alias("crime_types"),
        (distinct_count(["lsoa_code"]) > 1).alias("d_lsoa"),
    )


def classify(groups: DataFrame) -> DataFrame:
    """Add a readable class label to each group."""
    parts = F.concat_ws(
        " + ",
        F.when(F.col("d_outcome"), F.lit("outcome")),
        F.when(F.col("d_crime_type"), F.lit("crime_type")),
        F.when(F.col("d_location"), F.lit("location")),
        F.when(F.col("d_other"), F.lit("other")),
    )
    label = F.when(F.col("n_variants") == 1, F.lit("identical")).otherwise(
        F.concat(parts, F.lit(" differs"))
    )
    return groups.withColumn("class", label)


def is_final(col: str) -> F.Column:
    return F.col(col).isNotNull() & ~F.col(col).isin(NON_FINAL)


# --------------------------------------------------------------------------- outcomes


def read_outcome_events(
    spark: SparkSession, zip_path: Path, workdir: Path
) -> DataFrame:
    """Copy the configured forces' outcomes CSVs out of the zip and read them."""
    pattern = re.compile(
        r"^(\d{4}-\d{2})/\1-("
        + "|".join(map(re.escape, config.FORCES))
        + r")-outcomes\.csv$"
    )
    with zipfile.ZipFile(zip_path) as archive:
        for name in archive.namelist():
            if pattern.match(name):
                target = workdir / Path(name).name
                with archive.open(name) as src, target.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
    schema = StructType([StructField(c, StringType(), True) for c in OUTCOMES_COLUMNS])
    return (
        spark.read.format("csv")
        .schema(schema)
        .option("header", "true")
        .option("enforceSchema", "false")
        .load(str(workdir))
        .select(
            F.col("Crime ID").alias("crime_id"),
            F.col("Month").alias("event_month"),
            F.col("Outcome type").alias("outcome_type"),
        )
    )


def latest_events(events: DataFrame) -> DataFrame:
    """Per Crime ID: latest event month and the set of outcome types in that month."""
    w = Window.partitionBy("crime_id")
    latest = events.withColumn("m", F.max("event_month").over(w)).where(
        F.col("event_month") == F.col("m")
    )
    return latest.groupBy("crime_id").agg(
        F.first("m").alias("latest_month"),
        F.sort_array(F.collect_set("outcome_type")).alias("latest_outcomes"),
        F.count(F.lit(1)).alias("events_in_latest_month"),
    )


def mapped_outcome(col: str) -> F.Column:
    mapping = F.create_map(
        *[F.lit(x) for kv in STREET_TO_OUTCOMES_FILE.items() for x in kv]
    )
    return F.coalesce(mapping[F.col(col)], F.col(col))


# ------------------------------------------------------------------------------- main


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", default="2026-08")
    args = parser.parse_args()
    zip_path = config.DOWNLOADS_DIR / f"{args.archive}.zip"

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "16")

    variants = build_variants(spark, args.archive).cache()
    groups = classify(build_groups(variants)).cache()

    bronze = (
        spark.read.format("delta")
        .load(str(config.BRONZE_STREET_TABLE))
        .where(F.col("_archive_month") == F.lit(f"{args.archive}-01").cast("date"))
    )
    total_rows = bronze.count()
    asb_rows = bronze.where(F.col("crime_id").isNull()).count()
    id_rows = total_rows - asb_rows
    n_groups = groups.count()
    dup = groups.where(F.col("n_rows") > 1).cache()
    n_dup = dup.count()
    dup_rows = dup.agg(F.sum("n_rows")).collect()[0][0]

    heading(f"0. Scope: archive {args.archive}")
    print(f"bronze rows            : {total_rows:,}")
    print(f"  without Crime ID(ASB): {pct(asb_rows, total_rows)}")
    print(f"  with Crime ID        : {pct(id_rows, total_rows)}")
    print(f"groups (force,month,id): {n_groups:,}")
    print(
        f"groups with >1 row     : {pct(n_dup, n_groups)}  holding {pct(dup_rows, id_rows)} rows"
    )

    # ---- 1
    heading("1. Classification of every duplicate group (groups with >1 row)")
    show(
        dup.groupBy("class")
        .agg(F.count(F.lit(1)).alias("groups"), F.sum("n_rows").alias("rows"))
        .orderBy(F.desc("groups"))
        .select("class", "rows", "groups"),
        n_dup,
    )
    print("(pct is of duplicate groups; columns: class | rows | groups)")
    conflicting = dup.where(F.col("n_variants") > 1).cache()
    n_conf = conflicting.count()
    print(f"conflicting groups (>1 variant): {pct(n_conf, n_dup)} of duplicate groups")
    print("group size distribution (rows per group):")
    show(dup.groupBy("n_rows").agg(F.count(F.lit(1)).alias("groups")).orderBy("n_rows"))

    # ---- 2
    outcome_only = conflicting.where("class = 'outcome differs'").cache()
    n_oo = outcome_only.count()
    heading(f"2. Outcome-only groups: {pct(n_oo, n_conf)} of conflicting groups")
    show(
        outcome_only.withColumn("combo", F.concat_ws("  ||  ", "outcomes"))
        .groupBy("combo")
        .agg(F.count(F.lit(1)).alias("groups"))
        .orderBy(F.desc("groups")),
        n_oo,
        20,
    )
    finals = F.expr(
        f"size(filter(outcomes, x -> x != '{NULL}' and x not in ('Under investigation',"
        " 'Status update unavailable')))"
    )
    nonfinals = F.size(F.col("outcomes")) - finals
    cat = (
        F.when(finals == 0, "all non-final (no final value)")
        .when((finals == 1) & (nonfinals >= 1), "one final + non-final")
        .when((finals == 1) & (nonfinals == 0), "n/a")
        .when(
            finals >= 2,
            F.when(nonfinals >= 1, "two+ different finals (+ non-final)").otherwise(
                "two+ different finals only"
            ),
        )
    )
    print("\nfinal / non-final structure of outcome-only groups:")
    show(
        outcome_only.withColumn("structure", cat)
        .groupBy("structure")
        .agg(F.count(F.lit(1)).alias("groups"))
        .orderBy(F.desc("groups")),
        n_oo,
    )

    # ---- 3
    ct = conflicting.where("d_crime_type").cache()
    n_ct = ct.count()
    heading(
        f"3. Groups where crime_type differs: {pct(n_ct, n_conf)} of conflicting groups"
    )
    show(
        ct.withColumn("combo", F.concat_ws("  ||  ", "crime_types"))
        .groupBy("combo")
        .agg(F.count(F.lit(1)).alias("groups"))
        .orderBy(F.desc("groups")),
        n_ct,
        20,
    )
    print("\nDoes the outcome also differ in crime_type groups?")
    show(ct.groupBy("d_outcome").agg(F.count(F.lit(1)).alias("groups")), n_ct)

    # ---- 4
    loc = conflicting.where("d_location").select(*KEY, "d_lsoa", "class").cache()
    n_loc = loc.count()
    heading(
        f"4. Groups where location differs: {pct(n_loc, n_conf)} of conflicting groups"
    )
    pts = (
        variants.join(loc.select(*KEY), KEY)
        .select(
            *KEY,
            F.col("latitude").cast("double").alias("lat"),
            F.col("longitude").cast("double").alias("lon"),
        )
        .distinct()
    )
    pa, pb = pts.alias("a"), pts.alias("b")
    cond = [F.col(f"a.{k}") == F.col(f"b.{k}") for k in KEY] + [
        F.concat_ws(",", F.col("a.lat"), F.col("a.lon"))
        < F.concat_ws(",", F.col("b.lat"), F.col("b.lon"))
    ]
    rad = F.lit(3.141592653589793 / 180)
    dlat = (F.col("b.lat") - F.col("a.lat")) * rad
    dlon = (F.col("b.lon") - F.col("a.lon")) * rad
    hav = F.sin(dlat / 2) ** 2 + (
        F.cos(F.col("a.lat") * rad) * F.cos(F.col("b.lat") * rad) * F.sin(dlon / 2) ** 2
    )
    dist = pa.join(pb, cond).select(
        *[F.col(f"a.{k}") for k in KEY], (2 * 6371000 * F.asin(F.sqrt(hav))).alias("m")
    )
    per_group = dist.groupBy(*KEY).agg(F.max("m").alias("max_m"))
    gdist = loc.join(per_group, KEY, "left")
    stats = gdist.agg(
        F.count(F.lit(1)).alias("groups"),
        F.sum(F.col("max_m").isNull().cast("int")).alias("no_coordinate_change"),
        F.percentile_approx("max_m", 0.5).alias("median_m"),
        F.percentile_approx("max_m", 0.9).alias("p90_m"),
        F.max("max_m").alias("max_m"),
        F.sum(F.col("d_lsoa").cast("int")).alias("lsoa_changes"),
    ).collect()[0]
    print(dict(stats.asDict()))
    print(
        "(max_m = largest pairwise distance between distinct coordinates in the group;"
        " no_coordinate_change = location text/LSOA differs but coordinates are equal or NULL)"
    )
    show(
        loc.groupBy("class")
        .agg(F.count(F.lit(1)).alias("groups"))
        .orderBy(F.desc("groups")),
        n_loc,
    )

    # ---- 5
    heading(
        "5. Cross-file: Crime IDs in more than one (force, month) file of this archive"
    )
    ids = groups.groupBy("crime_id").agg(F.count(F.lit(1)).alias("files"))
    multi = ids.where("files > 1")
    n_multi = multi.count()
    print(
        f"distinct Crime IDs: {ids.count():,}; appearing in >1 file: {pct(n_multi, ids.count())}"
    )
    show(multi.groupBy("files").agg(F.count(F.lit(1)).alias("ids")).orderBy("files"))
    cross_force = (
        groups.join(multi, "crime_id")
        .groupBy("crime_id")
        .agg(F.countDistinct("_force").alias("forces"))
        .where("forces > 1")
        .count()
    )
    print(f"of those, IDs spanning more than one force: {cross_force:,}")

    # ---- 6
    heading("6. Outcomes-file cross-check (outcome-only groups)")
    with tempfile.TemporaryDirectory(prefix="outcomes_") as tmp:
        events = read_outcome_events(spark, zip_path, Path(tmp)).cache()
        n_events = events.count()
        latest = latest_events(events).cache()
        print(
            f"outcome events read: {n_events:,}; Crime IDs with events: {latest.count():,}"
        )
        print(
            "events in latest month per Crime ID (>1 means a tie within the latest month):"
        )
        show(
            latest.groupBy("events_in_latest_month")
            .agg(F.count(F.lit(1)).alias("ids"))
            .orderBy("events_in_latest_month")
        )
        print("\nstreet vocabulary vs outcomes-file vocabulary (assumed equivalence):")
        print(STREET_TO_OUTCOMES_FILE)

        oo_variants = variants.join(outcome_only.select(*KEY), KEY)
        oo_variants = oo_variants.withColumn("mapped", mapped_outcome(OUTCOME))
        joined = oo_variants.join(latest, "crime_id", "left").withColumn(
            "match",
            F.coalesce(F.expr("array_contains(latest_outcomes, mapped)"), F.lit(False)),
        )
        per = joined.groupBy(*KEY).agg(
            F.max(F.col("latest_month").isNotNull().cast("int")).alias("has_event"),
            F.sum(F.when(F.col("match"), F.col("copies")).otherwise(0)).alias(
                "matching_rows"
            ),
            F.sum(F.col("match").cast("int")).alias("matching_variants"),
            F.sum("copies").alias("rows"),
        )
        print(
            "\noutcome-only groups by number of matching ROWS vs the latest outcomes event:"
        )
        show(
            per.withColumn(
                "situation",
                F.when(F.col("has_event") == 0, "no outcomes-file event")
                .when(F.col("matching_variants") == 0, "no variant matches")
                .when(F.col("matching_variants") == 1, "exactly one variant matches")
                .otherwise("more than one variant matches"),
            )
            .groupBy("situation")
            .agg(F.count(F.lit(1)).alias("groups"))
            .orderBy(F.desc("groups")),
            n_oo,
        )
        print(
            "\n...and for the 'no outcomes-file event' groups, the outcome structure:"
        )
        show(
            per.where("has_event = 0")
            .join(outcome_only, KEY)
            .withColumn("combo", F.concat_ws("  ||  ", "outcomes"))
            .groupBy("combo")
            .agg(F.count(F.lit(1)).alias("groups"))
            .orderBy(F.desc("groups")),
            None,
            10,
        )

        # ---- 7
        heading("7. Largest group, in full")
        top = groups.orderBy(F.desc("n_rows")).first()
        print({k: top[k] for k in (*KEY, "n_rows", "n_variants", "class")})
        bronze_rows = (
            bronze.where(
                (F.col("_force") == top["_force"])
                & (F.col("_month") == top["_month"])
                & (F.col("crime_id") == top["crime_id"])
            )
            .select(*SOURCE_BRONZE_NAMES)
            .collect()
        )
        for r in bronze_rows:
            print("  ", r.asDict())

        top_c = conflicting.orderBy(F.desc("n_rows"), *KEY).first()
        print(
            "\nLargest CONFLICTING group:",
            {k: top_c[k] for k in (*KEY, "n_rows", "n_variants", "class")},
        )
        for r in (
            bronze.where(
                (F.col("_force") == top_c["_force"])
                & (F.col("_month") == top_c["_month"])
                & (F.col("crime_id") == top_c["crime_id"])
            )
            .select(*SOURCE_BRONZE_NAMES)
            .collect()
        ):
            print("  ", r.asDict())

        # ---- 8
        heading("8. Candidate rules: rows kept / dropped / quarantined (this archive)")
        grp = conflicting.select(
            *KEY,
            "n_rows",
            "n_variants",
            "class",
            "d_outcome",
            "d_crime_type",
            "d_location",
            "d_other",
            "outcomes",
        )
        ident = dup.where("n_variants = 1")
        ident_dropped = ident.agg(F.sum(F.col("n_rows") - 1)).collect()[0][0]
        n_conf_rows = conflicting.agg(F.sum("n_rows")).collect()[0][0]
        print(
            f"identical duplicate groups: {ident.count():,}; rows collapsed (all rules): {ident_dropped:,}"
        )
        print(f"conflicting groups: {n_conf:,}; rows in them: {n_conf_rows:,}")

        # R2
        f_cnt = F.expr(
            "size(filter(outcomes, x -> x != '<NULL>' and x not in ('Under investigation',"
            " 'Status update unavailable')))"
        )
        r2 = grp.withColumn(
            "decision",
            F.when(
                F.col("class") != "outcome differs",
                "quarantine: non-outcome column differs",
            )
            .when(f_cnt == 1, "keep the one final")
            .when(f_cnt >= 2, "quarantine: different finals")
            .otherwise("quarantine: no final value"),
        )
        # R3
        v3 = (
            variants.join(grp.select(*KEY), KEY)
            .withColumn("mapped", mapped_outcome(OUTCOME))
            .join(latest, "crime_id", "left")
            .withColumn(
                "match",
                F.coalesce(
                    F.expr("array_contains(latest_outcomes, mapped)"), F.lit(False)
                ),
            )
        )
        m3 = v3.groupBy(*KEY).agg(
            F.sum(F.col("match").cast("int")).alias("matching_variants"),
            F.max(F.col("latest_month").isNotNull().cast("int")).alias("has_event"),
        )
        r3 = grp.join(m3, KEY).withColumn(
            "decision",
            F.when(F.col("matching_variants") == 1, "keep the matching variant")
            .when(F.col("has_event") == 0, "quarantine: no outcomes-file event")
            .when(F.col("matching_variants") == 0, "quarantine: no variant matches")
            .otherwise("quarantine: several variants match"),
        )

        def rows_by(df: DataFrame, rule: str) -> None:
            res = (
                df.groupBy("decision")
                .agg(F.count(F.lit(1)).alias("groups"), F.sum("n_rows").alias("rows"))
                .orderBy("decision")
                .collect()
            )
            kept_rows = sum(
                r["groups"] for r in res if r["decision"].startswith("keep")
            )
            quar_rows = sum(
                r["rows"] for r in res if r["decision"].startswith("quarantine")
            )
            quar_groups = sum(
                r["groups"] for r in res if r["decision"].startswith("quarantine")
            )
            dropped = sum(
                r["rows"] - r["groups"] for r in res if r["decision"].startswith("keep")
            )
            print(f"\n{rule}")
            for r in res:
                print(
                    f"  {r['decision']:<45} groups={r['groups']:>7,} rows={r['rows']:>8,}"
                )
            print(
                f"  => silver keeps {kept_rows:,} rows from conflicting groups, drops {dropped:,} as superseded,"
                f" quarantines {quar_rows:,} rows ({quar_groups:,} Crime IDs not in silver)"
            )
            print(
                f"  => plus {ident_dropped:,} identical extras collapsed; "
                f"{pct(quar_groups, n_groups)} of all groups lost"
            )

        r1 = grp.withColumn("decision", F.lit("quarantine all"))
        rows_by(r1, "R1 quarantine-all on conflict (current rule)")
        rows_by(r2, "R2 outcome precedence")
        rows_by(r3, "R3 outcomes-file authority")
        print(
            f"\nR4 keep all rows flagged: keeps {n_conf_rows:,} rows from conflicting groups "
            f"(all {n_conf:,} Crime IDs retained, flagged is_conflicting_duplicate); "
            f"identical extras collapsed: {ident_dropped:,}; quarantined 0."
        )
        print(
            "   Gold must pick one row per Crime ID or count distinct crime_id; "
            "per-row measures (e.g. outcome counts) would double count."
        )

        # R2 vs R3 agreement where both keep
        both = r2.select(*KEY, F.col("decision").alias("d2")).join(
            r3.select(*KEY, F.col("decision").alias("d3")), KEY
        )
        print("\nR2 vs R3 decisions (conflicting groups):")
        show(
            both.groupBy("d2", "d3")
            .agg(F.count(F.lit(1)).alias("groups"))
            .orderBy(F.desc("groups")),
            n_conf,
            30,
        )
        # when both keep, do they keep the same variant? (outcome-only groups only)
        agree = (
            variants.join(r2.where("decision = 'keep the one final'").select(*KEY), KEY)
            .withColumn("mapped", mapped_outcome(OUTCOME))
            .join(latest, "crime_id", "left")
            .withColumn(
                "match",
                F.coalesce(
                    F.expr("array_contains(latest_outcomes, mapped)"), F.lit(False)
                ),
            )
            .withColumn("is_final", is_final(OUTCOME))
            .groupBy(*KEY)
            .agg(
                F.max(F.when(F.col("is_final") & F.col("match"), 1).otherwise(0)).alias(
                    "final_row_matches"
                ),
                F.max(F.col("latest_month").isNotNull().cast("int")).alias("has_event"),
            )
        )
        print(
            "\nWhere R2 keeps the final row: does that row match the outcomes-file latest event?"
        )
        show(
            agree.groupBy("has_event", "final_row_matches")
            .agg(F.count(F.lit(1)).alias("groups"))
            .orderBy("has_event", "final_row_matches")
        )

        # R5 (proposed): exactly one variant has a final outcome -> keep it, whatever else differs
        fv = (
            variants.join(grp.select(*KEY), KEY)
            .withColumn("is_final", is_final(OUTCOME))
            .groupBy(*KEY)
            .agg(F.sum(F.col("is_final").cast("int")).alias("final_variants"))
        )
        r5 = grp.join(fv, KEY).withColumn(
            "decision",
            F.when(
                F.col("final_variants") == 1,
                "keep the one variant with a final outcome",
            )
            .when(F.col("final_variants") == 0, "quarantine: no final outcome")
            .otherwise("quarantine: several variants with a final outcome"),
        )
        rows_by(
            r5, "R5 (proposed) single-final precedence, regardless of other columns"
        )
        v5 = (
            variants.join(r5.where("decision LIKE 'keep%'").select(*KEY), KEY)
            .where(is_final(OUTCOME))
            .withColumn("mapped", mapped_outcome(OUTCOME))
            .join(latest, "crime_id", "left")
            .withColumn(
                "match",
                F.coalesce(
                    F.expr("array_contains(latest_outcomes, mapped)"), F.lit(False)
                ),
            )
        )
        print("\nR5 kept variant vs latest outcomes-file event:")
        show(
            v5.groupBy(F.col("latest_month").isNotNull().alias("has_event"), "match")
            .agg(F.count(F.lit(1)).alias("groups"))
            .orderBy("has_event", "match"),
            r5.where("decision LIKE 'keep%'").count(),
        )
        print("\nR5 vs R3 decisions:")
        show(
            r5.select(*KEY, F.col("decision").alias("d5"))
            .join(r3.select(*KEY, F.col("decision").alias("d3")), KEY)
            .groupBy("d5", "d3")
            .agg(F.count(F.lit(1)).alias("groups"))
            .orderBy(F.desc("groups")),
            n_conf,
            30,
        )
        print(
            "\nDirection of crime_type changes in R5-kept crime_type groups"
            " (type on the final-outcome row  <-  type on the non-final row):"
        )
        ft = variants.join(
            r5.where("decision LIKE 'keep%' AND d_crime_type").select(*KEY), KEY
        )
        final_t = ft.where(is_final(OUTCOME)).select(
            *KEY, F.col("crime_type").alias("final_type")
        )
        other_t = ft.where(~is_final(OUTCOME)).select(
            *KEY, F.col("crime_type").alias("other_type")
        )
        show(
            final_t.join(other_t, KEY)
            .where("final_type != other_type")
            .withColumn("pair", F.concat_ws("  <-  ", "final_type", "other_type"))
            .groupBy("pair")
            .agg(F.count(F.lit(1)).alias("groups"))
            .orderBy(F.desc("groups")),
            None,
            12,
        )
        print("\nR5 quarantined groups, by structure:")
        show(
            r5.where("decision LIKE 'quarantine%'")
            .groupBy("decision", "class")
            .agg(F.count(F.lit(1)).alias("groups"), F.sum("n_rows").alias("rows"))
            .orderBy(F.desc("groups")),
            None,
            20,
        )

    spark.stop()


if __name__ == "__main__":
    main()
