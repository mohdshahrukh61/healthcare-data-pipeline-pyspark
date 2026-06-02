# =============================================================================
# BRONZE LAYER — Raw EHR Data Ingestion
# Project  : Healthcare Patient Data Pipeline
# Author   : Mohd Shahrukh
# GitHub   : https://github.com/mohdshahrukh61
# =============================================================================
# Description:
#   Ingests raw HL7/CSV patient data from hospital EHR systems into
#   ADLS Gen2 and stores as Delta Tables with schema validation.
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, input_file_name
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DateType, TimestampType
)

# ── Spark Session ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Bronze_Ingest_Healthcare_EHR") \
    .getOrCreate()

# ── Config ────────────────────────────────────────────────────────────────────
ADLS_RAW_PATH     = "abfss://raw@<your_storage>.dfs.core.windows.net/healthcare/ehr/"
BRONZE_DELTA_PATH = "abfss://bronze@<your_storage>.dfs.core.windows.net/patient_raw/"
BRONZE_TABLE      = "bronze.patient_raw"

# ── Schema Definition ─────────────────────────────────────────────────────────
patient_schema = StructType([
    StructField("patient_id",        StringType(),    True),
    StructField("first_name",        StringType(),    True),
    StructField("last_name",         StringType(),    True),
    StructField("date_of_birth",     DateType(),      True),
    StructField("gender",            StringType(),    True),
    StructField("contact_number",    StringType(),    True),
    StructField("email",             StringType(),    True),
    StructField("admission_date",    DateType(),      True),
    StructField("discharge_date",    DateType(),      True),
    StructField("department",        StringType(),    True),
    StructField("diagnosis_code",    StringType(),    True),
    StructField("diagnosis_desc",    StringType(),    True),
    StructField("attending_doctor",  StringType(),    True),
    StructField("hospital_id",       StringType(),    True),
    StructField("readmission_flag",  StringType(),    True),
])

# ── Ingest Raw CSV from ADLS Gen2 ─────────────────────────────────────────────
print(">>> [BRONZE] Reading raw EHR patient data from ADLS Gen2...")

df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .schema(patient_schema) \
    .csv(ADLS_RAW_PATH)

# ── Schema Validation — Check Critical Fields ─────────────────────────────────
print(">>> [BRONZE] Running schema validation...")
required_cols = ["patient_id", "admission_date", "department", "hospital_id"]
missing_cols  = [c for c in required_cols if c not in df_raw.columns]

if missing_cols:
    raise ValueError(f"[BRONZE] ERROR: Missing critical columns: {missing_cols}")

print(">>> [BRONZE] Schema validation passed.")

# ── Add Metadata Columns ──────────────────────────────────────────────────────
df_bronze = df_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file",         input_file_name()) \
    .withColumn("layer",               lit("bronze")) \
    .withColumn("data_source",         lit("EHR_SYSTEM"))

# ── Row Count Check ───────────────────────────────────────────────────────────
row_count = df_bronze.count()
print(f">>> [BRONZE] Patient records ingested: {row_count}")

if row_count == 0:
    raise ValueError("[BRONZE] ERROR: No records found in EHR source. Aborting.")

# ── Write to Bronze Delta Table (Append Mode) ─────────────────────────────────
print(">>> [BRONZE] Writing to Bronze Delta Table...")

df_bronze.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("hospital_id") \
    .save(BRONZE_DELTA_PATH)

# ── Register in Metastore ─────────────────────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_TABLE}
    USING DELTA
    LOCATION '{BRONZE_DELTA_PATH}'
""")

print(f">>> [BRONZE] Successfully written to: {BRONZE_TABLE}")
print(">>> [BRONZE] EHR Ingestion complete.")
