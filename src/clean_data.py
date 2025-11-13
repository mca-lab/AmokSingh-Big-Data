from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
import os
from pathlib import Path

RAW_DIR = Path("/app/data/raw")
PROC_DIR = Path("/app/data/processed")
PROC_DIR.mkdir(parents=True, exist_ok=True)


def start_spark(app_name="big_data_clean"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()
    return spark


def read_climate(spark):
    
    p = RAW_DIR / "climate_data.csv"
    if not p.exists():
        raise FileNotFoundError(f"{p} not found. Run fetch_data first.")
    df = spark.read.option("header", True).csv(str(p))
    return df


def read_agriculture(spark):
    p = RAW_DIR / "agriculture_data.csv"
    if not p.exists():
        raise FileNotFoundError(f"{p} not found. Run fetch_data first.")
    df = spark.read.option("header", True).csv(str(p))
    return df


def coerce_types_climate(df):
    # convert year and value to numeric where possible
    df = df.withColumn("year", F.col("year").cast(IntegerType()))
    df = df.withColumn("value", F.col("value").cast(DoubleType()))
    
    df = df.withColumn("country", F.trim(F.col("country")))
    return df


def coerce_types_ag(df):
    df = df.withColumn("year", F.col("year").cast(IntegerType()))
    df = df.withColumn("country", F.trim(F.col("country")))
    # try to cast known numeric columns (if present)
    numeric_cols = [c for c in df.columns if c not in ("country", "year")]
    for c in numeric_cols:
        df = df.withColumn(c, F.col(c).cast(DoubleType()))
    return df


def pivot_climate(df):
    df_wide = df.groupBy("country", "year").pivot("indicator").agg(F.avg("value"))
    return df_wide


def impute_median_per_country(spark, df, numeric_cols):
    for col in numeric_cols:
        med = df.groupBy("country").agg(F.expr(f"percentile_approx({col}, 0.5)").alias("median"))
        med = med.withColumnRenamed("median", f"{col}_median")
        df = df.join(med, on="country", how="left")
        df = df.withColumn(col,
                           F.when(F.col(col).isNull(), F.col(f"{col}_median")).otherwise(F.col(col)))
        df = df.drop(f"{col}_median")
    return df


def flag_outliers_iqr(df, numeric_cols):
    for col in numeric_cols:
        q1 = df.approxQuantile(col, [0.25], 0.0)[0]
        q3 = df.approxQuantile(col, [0.75], 0.0)[0]
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        df = df.withColumn(f"{col}_outlier", F.when((F.col(col) < lower) | (F.col(col) > upper), True).otherwise(False))
    return df


def main():
    spark = start_spark()
    print("Reading raw files...")
    climate = read_climate(spark)
    agriculture = read_agriculture(spark)

    print("Coercing types and dropping duplicates...")
    climate = coerce_types_climate(climate).dropDuplicates()
    agriculture = coerce_types_ag(agriculture).dropDuplicates()

    print("Pivoting climate data (long -> wide)...")
    climate_wide = pivot_climate(climate)

    print("Merging agriculture and climate data...")
    merged = agriculture.join(climate_wide, on=["country", "year"], how="left")

    numeric_cols = [c for c, t in merged.dtypes if t in ("double", "int", "bigint", "float") and c not in ("year",)]
    numeric_cols = [c for c in numeric_cols if c != "country"]

    if numeric_cols:
        print("Imputing missing numeric values (median per country)...")
        merged = impute_median_per_country(spark, merged, numeric_cols)

        print("Flagging outliers (IQR global)...")
        merged = flag_outliers_iqr(merged, numeric_cols)

    out_csv = str(PROC_DIR / "merged_climate_agriculture.csv")
    out_parquet = str(PROC_DIR / "merged_climate_agriculture.parquet")

    print("Writing processed outputs...")
    merged.coalesce(1).write.mode("overwrite").option("header", True).csv(out_csv + ".tmp")
    
    merged.write.mode("overwrite").parquet(out_parquet)

    print("Cleaning complete. Outputs written to:", PROC_DIR)
    spark.stop()


if __name__ == "__main__":
    main()