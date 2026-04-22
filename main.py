"""
Delaware North: Take-Home Assignment

Technical Prerequisites:
  - Python 3+
  - Spark: pip install delta-spark==2.4.0 pyspark==3.4.0
  - Java Runtime: https://java.com/en/download/manual.jsp

Assignment Background:
    - You are a freelance analytics consultant who has partnered with the TTPD (Tiny Town Police Department)
      to analyze speeding tickets that have been given to the adult citizens of Tiny Town over the 2020-2023 period.
    - Inside the folder "ttpd_data" you will find a directory of data for Tiny Town. This dataset will need to be "ingested" for analysis.
    - The solutions must use the Dataframes API.
    - You will need to ingest this data into a PySpark environment and answer the following three questions for the TTPD.

Assignment Deliverable:
    - You will create a Bronze/Silver/Gold lakehouse data model used to answer the Questions below.
    - During your session to review your work, we will ask questions around enhancements required if this were a production-ready pipeline.
    - Bronze Layer: Load the Tiny Town dataset to Delta tables using PySpark.
    - Silver Layer: Clean and prepare the data for Gold modelling. This may include null handling, parsing timestamps, ensuring correct joins, etc.
    - Gold Layer: Create one or more tables that aggregate the speeding ticket data for analytics. This table should be designed for the analytical questions below.
    - Provide correct answers to the 3 questions.


Questions:
    1. Which police officer was handed the most speeding tickets?
        - Police officers are recorded as citizens. Find in the data what differentiates an officer from a non-officer.
    2. What 3 months (year + month) had the most speeding tickets?
        - Bonus: What overall month-by-month or year-by-year trends, if any, do you see?
    3. Using the ticket fee table below, who are the top 10 people who have spent the most money paying speeding tickets overall?

Ticket Fee Table:
    - Ticket (base): $30
    - Ticket (base + school zone): $60
    - Ticket (base + construction work zone): $60
    - Ticket (base + school zone + construction work zone): $120
"""

# ─────────────────────────────────────────────────────────────────────────────
# COLAB SETUP  (run this cell first in Google Colab before anything else)
# ─────────────────────────────────────────────────────────────────────────────
# Colab ships PySpark 4.0 — use delta-spark 4.0.0 to match.
# !pip install delta-spark==4.0.0 --quiet
#
# Also make sure your ttpd_data folder is uploaded to Colab.
# You can upload via the Files panel on the left, or run:
#   from google.colab import files
#   files.upload()   # then unzip manually
# ─────────────────────────────────────────────────────────────────────────────

import os
import glob

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

# Path to the unzipped ttpd_data folder (adjust if your folder is somewhere else)
DATA_DIR = "./ttpd_data"

# Where Delta tables will be stored on disk
DELTA_BASE = "./delta_tables"

BRONZE_PEOPLE_PATH    = f"{DELTA_BASE}/bronze/people"
BRONZE_AUTOS_PATH     = f"{DELTA_BASE}/bronze/automobiles"
BRONZE_TICKETS_PATH   = f"{DELTA_BASE}/bronze/speeding_tickets"

SILVER_PEOPLE_PATH    = f"{DELTA_BASE}/silver/people"
SILVER_AUTOS_PATH     = f"{DELTA_BASE}/silver/automobiles"
SILVER_TICKETS_PATH   = f"{DELTA_BASE}/silver/speeding_tickets"

GOLD_OFFICER_PATH     = f"{DELTA_BASE}/gold/officer_ticket_counts"
GOLD_MONTHLY_PATH     = f"{DELTA_BASE}/gold/monthly_ticket_counts"
GOLD_SPENDERS_PATH    = f"{DELTA_BASE}/gold/top_spenders"


# ─────────────────────────────────────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────────────────────────────────────

def get_spark_session() -> SparkSession:
    """Retrieves or creates an active Spark Session configured for Delta Lake.

    Delta Lake is an open-source storage layer that adds ACID transactions and
    versioning on top of plain Parquet files. We need two config keys to enable it:
      - spark.sql.extensions  : registers Delta's SQL commands (MERGE, RESTORE, etc.)
      - spark.sql.catalog.*   : tells Spark to use Delta's table catalog so it knows
                                 how to read/write the Delta format.

    Returns:
        spark (SparkSession): the active Spark Session
    """
    builder = (
        SparkSession
        .builder
        .appName("ttpd_takehome")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Keep Spark log noise low so our printed answers are easy to read
        .config("spark.sql.shuffle.partitions", "4")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ─────────────────────────────────────────────────────────────────────────────
# BRONZE LAYER  –  Raw ingestion, no transformations
# ─────────────────────────────────────────────────────────────────────────────
# Think of Bronze as a digital copy of the source files.  We load them exactly
# as-is and immediately save to Delta so every downstream layer has a stable,
# queryable starting point.  If the source ever changes, we re-run Bronze only.
# ─────────────────────────────────────────────────────────────────────────────

def ingest_bronze_people(spark: SparkSession) -> None:
    """Read all people CSV files and write to a single Bronze Delta table.

    The files use pipe ('|') as the delimiter and wrap values in double-quotes.
    We let Spark infer the schema automatically since the raw layer intentionally
    preserves the original shape of the data.
    """
    print("\n[BRONZE] Ingesting people CSVs ...")

    # Find every people CSV across all batch files
    people_files = glob.glob(os.path.join(DATA_DIR, "*_people_*.csv"))

    # spark.read.csv can accept a Python list of file paths and merge them all
    # into one DataFrame automatically.
    df_people = (
        spark.read
        .option("header", "true")       # first row = column names
        .option("sep", "|")             # pipe-delimited
        .option("quote", '"')           # values are quoted with double-quotes
        .option("inferSchema", "true")  # let Spark guess data types
        .csv(people_files)
    )

    # Save as a Delta table (overwrites if it already exists so the script is re-runnable)
    df_people.write.format("delta").mode("overwrite").save(BRONZE_PEOPLE_PATH)
    print(f"  → Wrote {df_people.count()} people records to Bronze.")


def ingest_bronze_automobiles(spark: SparkSession) -> None:
    """Read all automobile XML files and write to a single Bronze Delta table.

    Spark's built-in XML reader is not included by default, so we parse the XML
    ourselves with Python's standard ElementTree library, collect all records into
    a plain Python list, and then create a Spark DataFrame from that list.
    This is a common pattern when dealing with non-standard file formats.
    """
    print("\n[BRONZE] Ingesting automobile XMLs ...")

    from xml.etree import ElementTree as ET

    auto_files = glob.glob(os.path.join(DATA_DIR, "*_automobiles_*.xml"))

    # Walk every XML file and collect each <automobile> element as a dict
    records = []
    for filepath in auto_files:
        tree = ET.parse(filepath)
        root = tree.getroot()                       # <automobiles>
        for auto in root.findall("automobile"):     # each <automobile> child
            records.append({
                "person_id":     auto.findtext("person_id"),
                "license_plate": auto.findtext("license_plate"),
                "vin":           auto.findtext("vin"),
                "color":         auto.findtext("color"),
                "year":          auto.findtext("year"),   # keep as string for now; Silver will cast
            })

    # Convert the Python list of dicts into a Spark DataFrame
    df_autos = spark.createDataFrame(records)

    df_autos.write.format("delta").mode("overwrite").save(BRONZE_AUTOS_PATH)
    print(f"  → Wrote {df_autos.count()} automobile records to Bronze.")


def ingest_bronze_tickets(spark: SparkSession) -> None:
    """Read all speeding-ticket JSON files and write to a single Bronze Delta table.

    Each JSON file has the shape:  { "speeding_tickets": [ {...}, {...}, ... ] }
    We use Spark's native JSON reader with multiLine mode off (each file is one
    JSON object), then explode the nested array into individual rows.
    """
    print("\n[BRONZE] Ingesting speeding ticket JSONs ...")

    ticket_files = glob.glob(os.path.join(DATA_DIR, "*_speeding_tickets_*.json"))

    # Read all JSON files at once. Spark sees each file as one row with a
    # "speeding_tickets" column that contains an array of structs.
    df_raw = (
        spark.read
        .option("multiLine", "true")    # each file is a single multi-line JSON object
        .json(ticket_files)
    )

    # F.explode() turns the array column into individual rows — one row per ticket
    df_tickets = df_raw.select(F.explode("speeding_tickets").alias("ticket"))

    # Flatten the nested struct so each field becomes its own column
    df_tickets = df_tickets.select("ticket.*")

    df_tickets.write.format("delta").mode("overwrite").save(BRONZE_TICKETS_PATH)
    print(f"  → Wrote {df_tickets.count()} ticket records to Bronze.")


def run_bronze(spark: SparkSession) -> None:
    """Entry point for the full Bronze ingestion pass."""
    print("=" * 60)
    print("BRONZE LAYER — Raw Ingestion")
    print("=" * 60)
    ingest_bronze_people(spark)
    ingest_bronze_automobiles(spark)
    ingest_bronze_tickets(spark)


# ─────────────────────────────────────────────────────────────────────────────
# SILVER LAYER  –  Cleaning and type-safe preparation
# ─────────────────────────────────────────────────────────────────────────────
# Silver reads from Bronze and applies:
#   • Correct data types (timestamps, integers, booleans)
#   • Null handling (drop rows that are un-joinable or analytically useless)
#   • Normalisation (e.g. profession casing so "Police officer" == "Police Officer")
#   • No business logic yet — that lives in Gold.
# ─────────────────────────────────────────────────────────────────────────────

def clean_silver_people(spark: SparkSession) -> None:
    """Clean the people Bronze table into Silver.

    Key changes:
      - Trim whitespace and standardise the profession column to UPPER CASE
        so 'Police officer' and 'Police Officer' are treated identically.
      - Cast date_of_birth to a proper DateType.
      - Drop rows with a null 'id' — without it we cannot join to tickets.
    """
    print("\n[SILVER] Cleaning people ...")

    df = spark.read.format("delta").load(BRONZE_PEOPLE_PATH)

    df_clean = (
        df
        # Drop any row where the primary key is missing — it cannot be joined
        .filter(F.col("id").isNotNull())

        # Standardise profession to UPPER so "Police officer" = "Police Officer"
        .withColumn("profession", F.upper(F.trim(F.col("profession"))))

        # Parse date_of_birth string (e.g. "1973-06-22") into a real DateType
        .withColumn("date_of_birth", F.to_date(F.col("date_of_birth"), "yyyy-MM-dd"))

        # Trim whitespace from name fields
        .withColumn("first_name", F.trim(F.col("first_name")))
        .withColumn("last_name",  F.trim(F.col("last_name")))

        # Create a convenient full_name column for display in Gold results
        .withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))

        # Flag whether this person is a police officer (used in Gold Q1)
        # After uppercasing, both variants become "POLICE OFFICER"
        .withColumn("is_officer", F.col("profession") == "POLICE OFFICER")
    )

    df_clean.write.format("delta").mode("overwrite").save(SILVER_PEOPLE_PATH)
    officer_count = df_clean.filter(F.col("is_officer")).count()
    print(f"  → Wrote {df_clean.count()} people records to Silver.")
    print(f"  → Police officers identified: {officer_count}")


def clean_silver_automobiles(spark: SparkSession) -> None:
    """Clean the automobiles Bronze table into Silver.

    Key changes:
      - Cast year from string to IntegerType.
      - Drop rows where license_plate or person_id is null
        (both are needed for joins).
    """
    print("\n[SILVER] Cleaning automobiles ...")

    df = spark.read.format("delta").load(BRONZE_AUTOS_PATH)

    df_clean = (
        df
        .filter(F.col("license_plate").isNotNull() & F.col("person_id").isNotNull())
        .withColumn("year", F.col("year").cast(IntegerType()))
        .withColumn("license_plate", F.trim(F.col("license_plate")))
    )

    df_clean.write.format("delta").mode("overwrite").save(SILVER_AUTOS_PATH)
    print(f"  → Wrote {df_clean.count()} automobile records to Silver.")


def clean_silver_tickets(spark: SparkSession) -> None:
    """Clean the speeding tickets Bronze table into Silver.

    Key changes:
      - Parse ticket_time string into a proper TimestampType.
      - Extract ticket_year and ticket_month_str (YYYY-MM) for aggregations.
      - Cast boolean flags from JSON (already boolean, but we confirm the type).
      - Calculate the ticket_fee using the fee schedule from the assignment.
      - Drop rows missing license_plate or officer_id (un-joinable).
    """
    print("\n[SILVER] Cleaning speeding tickets ...")

    df = spark.read.format("delta").load(BRONZE_TICKETS_PATH)

    df_clean = (
        df
        .filter(F.col("license_plate").isNotNull() & F.col("officer_id").isNotNull())

        # Parse the timestamp string "2021-11-30 13:16:53" into a real timestamp
        .withColumn("ticket_time", F.to_timestamp(F.col("ticket_time"), "yyyy-MM-dd HH:mm:ss"))

        # Derive year and YYYY-MM string for monthly aggregation (used in Q2)
        .withColumn("ticket_year",       F.year(F.col("ticket_time")))
        .withColumn("ticket_month_str",  F.date_format(F.col("ticket_time"), "yyyy-MM"))

        # Ensure boolean columns are cast correctly (JSON booleans are already bool in Spark)
        .withColumn("school_zone_ind", F.col("school_zone_ind").cast(BooleanType()))
        .withColumn("work_zone_ind",   F.col("work_zone_ind").cast(BooleanType()))

        # Apply the fee schedule from the assignment brief:
        #   school + work zone  → $120
        #   school OR work zone → $60
        #   base only           → $30
        .withColumn(
            "ticket_fee",
            F.when(F.col("school_zone_ind") & F.col("work_zone_ind"),  120)
             .when(F.col("school_zone_ind") | F.col("work_zone_ind"),   60)
             .otherwise(30)
        )
    )

    df_clean.write.format("delta").mode("overwrite").save(SILVER_TICKETS_PATH)
    print(f"  → Wrote {df_clean.count()} ticket records to Silver.")
    print(f"  → Ticket years present: {[r.ticket_year for r in df_clean.select('ticket_year').distinct().orderBy('ticket_year').collect()]}")


def run_silver(spark: SparkSession) -> None:
    """Entry point for the full Silver cleaning pass."""
    print("=" * 60)
    print("SILVER LAYER — Cleaning & Preparation")
    print("=" * 60)
    clean_silver_people(spark)
    clean_silver_automobiles(spark)
    clean_silver_tickets(spark)


# ─────────────────────────────────────────────────────────────────────────────
# GOLD LAYER  –  Aggregations that answer the three business questions
# ─────────────────────────────────────────────────────────────────────────────
# Gold tables are purpose-built for analytics. Each one directly serves one of
# the three questions. A downstream BI tool or report could query these tables
# without needing to know anything about the raw source data.
# ─────────────────────────────────────────────────────────────────────────────

def build_gold_officer_ticket_counts(spark: SparkSession) -> None:
    """Gold table for Question 1: which officer issued the most tickets?

    Join path:
        silver_tickets.officer_id  →  silver_people.id
    Filter: only rows where is_officer = True (profession = 'POLICE OFFICER')
    Aggregate: count tickets per officer, rank descending.

    Why officer_id points to a person:
        Officers are citizens too — they exist in the people table.
        The ticket records them via officer_id, which is their person ID.
        The only field that distinguishes them from civilians is profession.
    """
    print("\n[GOLD] Building officer ticket counts ...")

    df_tickets = spark.read.format("delta").load(SILVER_TICKETS_PATH)
    df_people  = spark.read.format("delta").load(SILVER_PEOPLE_PATH)

    # Join tickets to people on the officer_id = id link
    df_officer_tickets = (
        df_tickets
        .join(
            df_people.filter(F.col("is_officer")),   # only join to officers
            df_tickets["officer_id"] == df_people["id"],
            how="inner"
        )
        .groupBy("officer_id", "full_name")
        .agg(F.count("*").alias("tickets_issued"))
        .orderBy(F.col("tickets_issued").desc())
    )

    df_officer_tickets.write.format("delta").mode("overwrite").save(GOLD_OFFICER_PATH)
    print(f"  → Wrote {df_officer_tickets.count()} officer rows to Gold.")


def build_gold_monthly_ticket_counts(spark: SparkSession) -> None:
    """Gold table for Question 2: which 3 months had the most tickets?

    Aggregates ticket count by ticket_month_str (YYYY-MM) and ticket_year.
    Includes yearly totals for the bonus trend analysis.
    """
    print("\n[GOLD] Building monthly ticket counts ...")

    df_tickets = spark.read.format("delta").load(SILVER_TICKETS_PATH)

    # Monthly counts
    df_monthly = (
        df_tickets
        .groupBy("ticket_year", "ticket_month_str")
        .agg(F.count("*").alias("ticket_count"))
        .orderBy(F.col("ticket_count").desc())
    )

    df_monthly.write.format("delta").mode("overwrite").save(GOLD_MONTHLY_PATH)
    print(f"  → Wrote {df_monthly.count()} month rows to Gold.")


def build_gold_top_spenders(spark: SparkSession) -> None:
    """Gold table for Question 3: top 10 people by total ticket fees paid.

    Join path:
        silver_tickets.license_plate  →  silver_autos.license_plate
        silver_autos.person_id        →  silver_people.id

    Aggregate: sum of ticket_fee per person, rank descending, take top 10.
    """
    print("\n[GOLD] Building top spenders ...")

    df_tickets = spark.read.format("delta").load(SILVER_TICKETS_PATH)
    df_autos   = spark.read.format("delta").load(SILVER_AUTOS_PATH)
    df_people  = spark.read.format("delta").load(SILVER_PEOPLE_PATH)

    # Step 1: link each ticket to the automobile's owner via license plate
    df_with_owner = (
        df_tickets
        .join(df_autos, on="license_plate", how="inner")
    )

    # Step 2: link automobile owner to their full name in the people table
    df_with_person = (
        df_with_owner
        .join(df_people, df_with_owner["person_id"] == df_people["id"], how="inner")
    )

    # Step 3: sum fees per person and take the top 10
    df_spenders = (
        df_with_person
        .groupBy("person_id", "full_name")
        .agg(F.sum("ticket_fee").alias("total_fees_paid"))
        .orderBy(F.col("total_fees_paid").desc())
        .limit(10)
    )

    df_spenders.write.format("delta").mode("overwrite").save(GOLD_SPENDERS_PATH)
    print(f"  → Wrote top {df_spenders.count()} spenders to Gold.")


def run_gold(spark: SparkSession) -> None:
    """Entry point for the full Gold aggregation pass."""
    print("=" * 60)
    print("GOLD LAYER — Analytics Aggregations")
    print("=" * 60)
    build_gold_officer_ticket_counts(spark)
    build_gold_monthly_ticket_counts(spark)
    build_gold_top_spenders(spark)


# ─────────────────────────────────────────────────────────────────────────────
# ANSWERS  –  Print the final results for each question
# ─────────────────────────────────────────────────────────────────────────────

def print_answers(spark: SparkSession) -> None:
    """Read the Gold tables and print clear, formatted answers to all 3 questions."""

    print("\n")
    print("=" * 60)
    print("ANSWERS")
    print("=" * 60)

    # ── Question 1 ────────────────────────────────────────────────────────────
    print("\nQUESTION 1: Which police officer issued the most speeding tickets?")
    print("-" * 60)
    print("(Officers are identified by profession = 'POLICE OFFICER' in the people table)")
    print()

    df_officers = spark.read.format("delta").load(GOLD_OFFICER_PATH)
    top_officer = df_officers.limit(1).collect()[0]
    print(f"  Officer Name  : {top_officer['full_name']}")
    print(f"  Tickets Issued: {top_officer['tickets_issued']}")
    print()
    print("  Top 5 officers for reference:")
    df_officers.show(5, truncate=False)

    # ── Question 2 ────────────────────────────────────────────────────────────
    print("\nQUESTION 2: What 3 months (year + month) had the most speeding tickets?")
    print("-" * 60)

    df_monthly = spark.read.format("delta").load(GOLD_MONTHLY_PATH)
    top3 = df_monthly.limit(3).collect()
    for i, row in enumerate(top3, 1):
        print(f"  #{i}: {row['ticket_month_str']}  →  {row['ticket_count']} tickets")

    print()
    print("  BONUS — Year-by-year totals (trend analysis):")
    df_monthly.groupBy("ticket_year").agg(
        F.sum("ticket_count").alias("total_tickets")
    ).orderBy("ticket_year").show()

    print("  BONUS — All months ranked (top 20):")
    df_monthly.show(20, truncate=False)

    # ── Question 3 ────────────────────────────────────────────────────────────
    print("\nQUESTION 3: Top 10 people who spent the most on speeding tickets")
    print("-" * 60)
    print("  Fee schedule: base=$30 | +school zone=$60 | +work zone=$60 | both=$120")
    print()

    df_spenders = spark.read.format("delta").load(GOLD_SPENDERS_PATH)
    spenders = df_spenders.collect()
    for i, row in enumerate(spenders, 1):
        print(f"  #{i:2d}  {row['full_name']:<25}  ${row['total_fees_paid']:.0f}")

    print()
    df_spenders.show(10, truncate=False)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    spark: SparkSession = get_spark_session()

    # Suppress verbose Spark/Delta log output so answers are clearly visible
    spark.sparkContext.setLogLevel("ERROR")

    # Run each layer in sequence: Bronze → Silver → Gold → Answers
    run_bronze(spark)
    run_silver(spark)
    run_gold(spark)
    print_answers(spark)

    print("\n✓ Pipeline complete.")
    spark.stop()


if __name__ == "__main__":
    main()
