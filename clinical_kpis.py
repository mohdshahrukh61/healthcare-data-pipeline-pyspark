# =============================================================================
# GOLD LAYER — Clinical KPI Aggregations
# Project  : Healthcare Patient Data Pipeline
# Author   : Mohd Shahrukh
# GitHub   : https://github.com/mohdshahrukh61
# =============================================================================
# Description:
#   Reads Silver Delta Table and computes clinical KPIs:
#   - Patient Admission Trends (daily/monthly)
#   - 30-Day Readmission Rates by Department
#   - Treatment Outcomes by Diagnosis Category
#   Writes optimized Delta Tables for clinical dashboards.
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, round as _round,
    month, year, when, lit, current_timestamp,
    countDistinct, lag
)
from pyspark.sql.window import Window

# ── Spark Session ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Gold_Clinical_KPIs") \
    .getOrCreate()

# ── Config ────────────────────────────────────────────────────────────────────
SILVER_TABLE           = "silver.patient_clean"
GOLD_PATH_ADMISSION    = "abfss://gold@<your_storage>.dfs.core.windows.net/admission_trends/"
GOLD_PATH_READMISSION  = "abfss://gold@<your_storage>.dfs.core.windows.net/readmission_rates/"
GOLD_PATH_OUTCOMES     = "abfss://gold@<your_storage>.dfs.core.windows.net/treatment_outcomes/"

# ── Read Silver ───────────────────────────────────────────────────────────────
print(">>> [GOLD] Reading Silver layer...")
df = spark.read.format("delta").table(SILVER_TABLE)

# ─────────────────────────────────────────────────────────────────────────────
# KPI 1: Patient Admission Trends (Monthly by Department)
# ─────────────────────────────────────────────────────────────────────────────
print(">>> [GOLD] Computing Patient Admission Trends...")

df_admissions = df \
    .withColumn("admission_month", month(col("admission_date"))) \
    .withColumn("admission_year",  year(col("admission_date"))) \
    .groupBy("hospital_id", "department", "admission_year", "admission_month") \
    .agg(
        count("patient_id").alias("total_admissions"),
        _round(avg("length_of_stay_days"), 2).alias("avg_length_of_stay"),
        countDistinct("diagnosis_code").alias("unique_diagnoses")
    )

# Month-over-Month growth
window_spec = Window.partitionBy("hospital_id", "department") \
    .orderBy("admission_year", "admission_month")

df_admissions = df_admissions \
    .withColumn("prev_month_admissions",
        lag("total_admissions", 1).over(window_spec)) \
    .withColumn("mom_growth_pct",
        _round(
            (col("total_admissions") - col("prev_month_admissions"))
            / col("prev_month_admissions") * 100,
        2)) \
    .withColumn("gold_timestamp", current_timestamp())

df_admissions.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("hospital_id") \
    .save(GOLD_PATH_ADMISSION)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold.admission_trends
    USING DELTA LOCATION '{GOLD_PATH_ADMISSION}'
""")
print(">>> [GOLD] Admission Trends table written.")

# ─────────────────────────────────────────────────────────────────────────────
# KPI 2: 30-Day Readmission Rate by Department
# ─────────────────────────────────────────────────────────────────────────────
print(">>> [GOLD] Computing 30-Day Readmission Rates...")

df_readmission = df \
    .groupBy("hospital_id", "department") \
    .agg(
        count("patient_id").alias("total_patients"),
        _sum(when(col("readmission_flag") == "Y", 1).otherwise(0))
            .alias("readmitted_patients")
    ) \
    .withColumn("readmission_rate_pct",
        _round(
            col("readmitted_patients") / col("total_patients") * 100,
        2)) \
    .withColumn("risk_category",
        when(col("readmission_rate_pct") >= 20, lit("HIGH"))
        .when(col("readmission_rate_pct") >= 10, lit("MEDIUM"))
        .otherwise(lit("LOW"))) \
    .withColumn("gold_timestamp", current_timestamp()) \
    .orderBy(col("readmission_rate_pct").desc())

df_readmission.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(GOLD_PATH_READMISSION)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold.readmission_rates
    USING DELTA LOCATION '{GOLD_PATH_READMISSION}'
""")
print(">>> [GOLD] Readmission Rates table written.")

# ─────────────────────────────────────────────────────────────────────────────
# KPI 3: Treatment Outcomes by Diagnosis Category
# ─────────────────────────────────────────────────────────────────────────────
print(">>> [GOLD] Computing Treatment Outcomes...")

df_outcomes = df \
    .groupBy("diagnosis_code", "diagnosis_desc", "department") \
    .agg(
        count("patient_id").alias("total_cases"),
        _round(avg("length_of_stay_days"), 2).alias("avg_los_days"),
        _sum(when(col("readmission_flag") == "Y", 1).otherwise(0))
            .alias("readmissions"),
    ) \
    .withColumn("outcome_score",
        _round(
            (lit(1) - col("readmissions") / col("total_cases")) * 100,
        2)) \
    .withColumn("outcome_category",
        when(col("outcome_score") >= 90, lit("EXCELLENT"))
        .when(col("outcome_score") >= 75, lit("GOOD"))
        .when(col("outcome_score") >= 60, lit("FAIR"))
        .otherwise(lit("NEEDS_IMPROVEMENT"))) \
    .withColumn("gold_timestamp", current_timestamp()) \
    .orderBy(col("total_cases").desc())

df_outcomes.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("department") \
    .save(GOLD_PATH_OUTCOMES)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold.treatment_outcomes
    USING DELTA LOCATION '{GOLD_PATH_OUTCOMES}'
""")
print(">>> [GOLD] Treatment Outcomes table written.")

# ── Optimize All Gold Tables ──────────────────────────────────────────────────
print(">>> [GOLD] Optimizing Delta Tables with Z-Ordering...")
spark.sql("OPTIMIZE gold.admission_trends    ZORDER BY (hospital_id, department)")
spark.sql("OPTIMIZE gold.readmission_rates   ZORDER BY (hospital_id, department)")
spark.sql("OPTIMIZE gold.treatment_outcomes  ZORDER BY (diagnosis_code, department)")

print(">>> [GOLD] All clinical KPI tables ready for dashboard consumption.")
print(">>> [GOLD] Gold layer complete.")
