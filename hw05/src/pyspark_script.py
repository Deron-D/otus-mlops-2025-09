"""
Script: pyspark_script.py
Description: PySpark script for testing.
"""

from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    DoubleType, IntegerType, TimestampType
)

def cleanup_data(spark, src_path: str, dst_path: str) -> None:
    schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("tx_datetime", TimestampType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("terminal_id", IntegerType(), True),
        StructField("tx_amount", DoubleType(), True),
        StructField("tx_time_seconds", IntegerType(), True),
        StructField("tx_time_days", IntegerType(), True),
        StructField("tx_fraud", IntegerType(), True),
        StructField("tx_fraud_scenario", IntegerType(), True),
    ])

    df = (
        spark.read
        .option("header", "false")
        .option("pathGlobFilter", "*.txt")
        .option("comment", "#")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(src_path)
    )

    df_clean = (
        df.filter(df.terminal_id.isNotNull())
        .filter(df.customer_id > 0)
        .filter(df.tx_amount > 0)
        .filter(df.tx_time_seconds > 0)
        .filter(df.transaction_id > 0)
        .filter((df.tx_fraud == 0) | (df.tx_fraud == 1))
        .filter(df.tx_fraud_scenario.isin(0, 1, 2, 3))
        .dropDuplicates(["transaction_id"])
    )

    (
        df_clean
        .repartition(5)
        .write
        .mode("overwrite")
        .parquet(f"{dst_path}/dataset.parquet")
    )

def main():
    parser = ArgumentParser()
    parser.add_argument("--bucket", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("hw05").getOrCreate()

    input_path = f"s3a://{args.bucket}/input_data"
    output_path = f"s3a://{args.bucket}/output_data"

    print(f"Input: {input_path}")
    print(f"Output: {output_path}")

    cleanup_data(spark, input_path, output_path)
    spark.stop()

if __name__ == "__main__":
    main()
