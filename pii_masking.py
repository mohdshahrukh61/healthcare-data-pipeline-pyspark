# =============================================================================
# SILVER LAYER — Patient Data Normalization & PII Masking
# Project  : Healthcare Patient Data Pipeline
# Author   : Mohd Shahrukh
# GitHub   : https://github.com/mohdshahrukh61
# =============================================================================
# Description:
#   Reads Bronze patient data, applies normalization, deduplication,
#   and HIPAA-compliant PII masking. Writes to Silver Delta Table.
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, when, lit,
    sha2, concat, regexp_replace, current_timestamp,
    row_number, datediff, to_date, coalesce
)
from pyspark.sql.window import Window

# ── Spark Session ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Silver_Patient_Normalization_PII") \
    .getOrCreate()

# ── Config ────────────────────────────────────────────────────────────────────
BRONZE_TABLE  = "bronze.patient_raw"
SILVER_PATH   = "abfss://silver@<your_storage>.dfs.core.windows.net/patient_clean/"
SILVER_TABLE  = "silver.patient_clean"

# ── Read Bronze ───────────────────────────────────────────────────────────────
print(">>> [SILVER] Reading from Bronze layer...")
df = spark.read.format("delta").table(BRONZE_TABLE)
print(f">>> [SILVER] Bronze record count: {df.count()}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: Drop Nulls in Critical Columns
# ─────────────────────────────────────────────────────────────────────────────
print(">>> [SILVER] Dropping null critical records...")
df = df.dropna(subset=["patient_id", "admission_date", "department"])

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: Standardize & Normalize Fields
# ─────────────────────────────────────────────────────────────────────────────
print(">>> [SILVER] Normalizing fields...")
df = df \
    .withColumn("gender",       upper(trim(col("gender")))) \
    .withColumn("department",   upper(trim(col("department")))) \
    .withColumn("diagnosis_code", trim(col("diagnosis_code"))) \
    .withColumn("readmission_flag",
        when(upper(trim(col("readmission_flag"))).isin("YES", "Y", "1"), lit("Y"))
        .otherwise(lit("N")))

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: Compute Length of Stay (LOS)
# ─────────────────────────────────────────────────────────────────────────────
print(">>> [SILVER] Computing Length of Stay...")
df = df.withColumn("length_of_stay_days",
    datediff(col("discharge_date"), col("admission_date")))

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: HIPAA-Compliant PII Masking
# Mask: first_name, last_name, date_of_birth, contact_number, email
# Keep : patient_id (hashed), diagnosis, admission data for analytics
# ─────────────────────────────────────────────────────────────────────────────
print(">>> [SILVER] Applying HIPAA PII masking...")

df_masked = df \
    .withColumn("patient_id_hashed",
        sha2(col("patient_id"), 256)) \
    .withColumn("first_name",
        lit("***MASKED***")) \
    .withColumn("last_name",
        lit("***MASKED***")) \
    .withColumn("date_of_birth",
        lit(None).cast("date")) \
    .withColumn("contact_number",
        regexp_replace(col("contact_number"), r"\d", "*")) \
    .withColumn("email",
        lit("***MASKED***")) \
    .drop("patient_id") \
    .withColumnRenamed("patient_id_hashed", "patient_id")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: Deduplication (keep latest per patient_id + admission_date)
# ─────────────────────────────────────────────────────────────────────────────
print(">>> [SILVER] Deduplicating records...")
window_spec = Window \
    .partitionBy("patient_id", "admission_date") \
    .orderBy(col("ingestion_timestamp").desc())

df_deduped = df_masked \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num", "source_file")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 6: Add Silver Metadata + Audit Log
# ─────────────────────────────────────────────────────────────────────────────
df_silver = df_deduped \
    .withColumn("transformed_timestamp", current_timestamp()) \
    .withColumn("layer",                 lit("silver")) \
    .withColumn("pii_masked",            lit(True))

# ── Quality Report ────────────────────────────────────────────────────────────
silver_count = df_silver.count()
print(f">>> [SILVER] Final Silver record count : {silver_count}")
print(f">>> [SILVER] PII fields masked         : first_name, last_name, dob, contact, email")

# ── Write to Silver Delta Table ───────────────────────────────────────────────
print(">>> [SILVER] Writing to Silver Delta Table...")

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("department") \
    .save(SILVER_PATH)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_TABLE}
    USING DELTA
    LOCATION '{SILVER_PATH}'
""")

print(f">>> [SILVER] Successfully written to: {SILVER_TABLE}")
print(">>> [SILVER] Normalization & PII masking complete.")
