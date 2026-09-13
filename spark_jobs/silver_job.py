from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

spark = SparkSession.builder.appName("silverJob").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

## json parse schema
json_schema = StructType(
    [
        StructField("id", StringType()),
        StructField("meta", StructType([StructField("domain", StringType())])),
        StructField("timestamp", LongType()),
        StructField("title", StringType()),
        StructField("user", StringType()),
        StructField("bot", StringType()),
        StructField("namespace", IntegerType()),
        StructField("comment", StringType()),
        StructField(
            "length",
            StructType(
                [StructField("old", IntegerType()), StructField("new", IntegerType())]
            ),
        ),
        StructField("server_name", StringType()),
    ]
)

bronze_schema = StructType(
    [
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
        StructField("ingestion_timestamp", TimestampType()),
        StructField("key", StringType()),
        StructField("raw_json_value", StringType()),
    ]
)

bronze_layer = (
    spark.readStream.format("parquet").schema(bronze_schema).load("output/bronze")
)

kafka_payload = bronze_layer.select(F.col("raw_json_value").cast("string"))

parsed_df = kafka_payload.select(F.from_json(F.col("raw_json_value"), json_schema).alias("data"))

## transformation and filtering
silver_df = (
    parsed_df.filter(
        F.col("data.timestamp").isNotNull() & F.col("data.meta.domain").isNotNull()
    )
    .withColumn(
        "timestamp",
        F.when(
            F.col("data.timestamp").cast("long") > 0,
            F.timestamp_seconds(F.col("data.timestamp")),
        ).otherwise(F.lit(None)),
    )
    .withColumn(
        "old_length",
        F.when(
            F.col("data.length.old").cast("long") >= 0, F.col("data.length.old")
        ).otherwise(F.lit(None)),
    )
    .withColumn(
        "new_length",
        F.when(
            F.col("data.length.new").cast("long") >= 0, F.col("data.length.new")
        ).otherwise(F.lit(None)),
    )
    .withColumn(
        "domain",
        F.col("data.meta.domain")
    )
    .select(
        F.col("data.id").alias("event_id"),
        F.col("timestamp"),
        F.col("domain"),
        F.col("data.title").alias("title"),
        F.col("data.user").alias("username"),
        F.col("data.bot").cast("boolean").alias("bot_flag"),
        F.col("data.namespace").alias("namespace"),
        F.col("data.comment").alias("comment"),
        F.col("old_length"),
        F.col("new_length"),
        (F.col("data.length.new") - F.col("data.length.old"))
        .cast("int")
        .alias("delta"),
        F.col("data.server_name").alias("server_name")
    )
)

query = (
    silver_df.writeStream.format("parquet")
    .outputMode("append")
    .option("truncate", "false")
    .option("path", "output/silver")
    .option("checkpointLocation", "checkpoints/silver")
    .trigger(processingTime="1 minute")
    .start()
)

spark.streams.awaitAnyTermination()