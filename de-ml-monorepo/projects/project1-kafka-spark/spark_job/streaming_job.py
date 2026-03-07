"""PySpark Structured Streaming job: Kafka → MinIO (parquet) + Postgres (agg)."""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── Config ───────────────────────────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "namazbek")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "bekzhanov")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw-events")
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_USER = os.getenv("POSTGRES_USER", "namazbek")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "bekzhanov")
PG_DB = os.getenv("POSTGRES_DB", "dedb")

# Expected event schema
EVENT_SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("ts", TimestampType()),
        StructField("value", StringType()),
        StructField("user_id", IntegerType()),
    ]
)

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {"user": PG_USER, "password": PG_PASS, "driver": "org.postgresql.Driver"}


def build_spark() -> SparkSession:
    """Create SparkSession with S3/MinIO and Kafka packages."""
    return (
        SparkSession.builder.appName("KafkaSparkStreaming")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def write_to_postgres(batch_df, batch_id: int) -> None:
    """foreachBatch sink: compute windowed aggregates and upsert to Postgres."""
    agg = (
        batch_df.groupBy(
            F.window("ts", "1 minute").alias("w"),
        )
        .count()
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            F.col("count").alias("event_count"),
        )
    )
    agg.write.jdbc(
        url=JDBC_URL, table="event_agg", mode="append", properties=JDBC_PROPS
    )


def run(spark: SparkSession) -> None:
    """Read from Kafka, write raw to MinIO and aggregates to Postgres."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # Parse JSON payload
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("data")
    ).select("data.*")

    # Sink 1: raw parquet partitioned by date → MinIO
    raw_query = (
        parsed.withColumn("date", F.to_date("ts"))
        .writeStream.format("parquet")
        .option("path", f"s3a://{MINIO_BUCKET}/events/")
        .option("checkpointLocation", f"s3a://{MINIO_BUCKET}/checkpoints/raw/")
        .partitionBy("date")
        .outputMode("append")
        .start()
    )

    # Sink 2: aggregates → Postgres via foreachBatch
    agg_query = (
        parsed.writeStream.foreachBatch(write_to_postgres)
        .option("checkpointLocation", f"s3a://{MINIO_BUCKET}/checkpoints/agg/")
        .outputMode("update")
        .start()
    )

    raw_query.awaitTermination()
    agg_query.awaitTermination()


if __name__ == "__main__":
    spark = build_spark()
    run(spark)
