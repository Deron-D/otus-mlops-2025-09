#!/usr/bin/env python3

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType


spark = (
    SparkSession
        .builder
        .appName("hw03")
        .config("spark.hadoop.fs.s3a.committer.name", "directory")
        .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
        .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")     # 8 ГБ для экзекутора
        .config("spark.executor.cores", "2")       # 2 ядра на экзекутор
        .config("spark.executor.instances", "6")   # 6 экзекуторов
        .getOrCreate()
)

spark.conf.set("spark.hadoop.parquet.block.size", 128 * 1024 * 1024)    # 128 МБ
spark.conf.set("spark.sql.files.maxPartitionBytes", 512 * 1024 * 1024) # 512 МБ

schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("tx_datetime", TimestampType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("terminal_id", IntegerType(), True),
    StructField("tx_amount", DoubleType(), True),
    StructField("tx_time_seconds", IntegerType(), True),
    StructField("tx_time_days", IntegerType(), True),
    StructField("tx_fraud", IntegerType(), True),
    StructField("tx_fraud_scenario", IntegerType(), True)
])


df = spark.read.csv(
    "/user/ubuntu/data",
    header=False,
    pathGlobFilter="*.txt",
    schema=schema,
    comment="#",
    mode="PERMISSIVE"
)

df_clean = df.filter(
    df.terminal_id.isNotNull() &
    (df.customer_id > 0) &
    (df.tx_amount > 0) &
    (df.tx_time_seconds > 0) &
    (df.transaction_id > 0) &
    df.tx_fraud.isin([0, 1]) &
    df.tx_fraud_scenario.isin([0, 1, 2, 3])
).dropDuplicates(['transaction_id'])

df_clean.write.parquet("s3a://otus-bucket-d96c6f8b8b8aa500/dataset.parquet")

spark.stop()