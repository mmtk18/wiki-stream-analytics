from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

spark = SparkSession.builder.appName("goldJob").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

SILVER_LAYER_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("domain", StringType()),
        StructField("title", StringType()),
        StructField("username", StringType()),
        StructField("bot_flag", BooleanType()),
        StructField("namespace", StringType()),
        StructField("comment", StringType()),
        StructField("old_length", StringType()),
        StructField("new_length", StringType()),
        StructField("delta", IntegerType()),
        StructField("server_name", StringType()),
    ]
)

silver_layer = (
    spark.readStream.format("parquet").schema(SILVER_LAYER_SCHEMA).load("output/silver")
)

## streaming agg
# edits per 1-minute tumbling window,
# bot vs human counts per 5-minute window,
# top domains by edit count per 5-minute window,
# average and max absolute edit delta per 5-minute window.

edits_per_m_df = (
    silver_layer.withWatermark("timestamp", "2 minutes")
    .groupBy(F.window("timestamp", "1 minute"))
    .count()
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("count").alias("edit_count"),
    )
)

edits_per_m_df.writeStream.format("parquet").outputMode("append").option(
    "checkpointLocation", "checkpoints/gold/q1"
).option("path", "output/gold/edits_per_minute").start()

bot_vs_human_cnt_df = (
    silver_layer.withWatermark("timestamp", "2 minutes")
    .groupBy(F.window("timestamp", "5 minutes"))
    .agg(
        F.sum(F.when(F.col("bot_flag"), 1).otherwise(0)).alias("bot_count"),
        F.sum(F.when(F.col("bot_flag"), 0).otherwise(1)).alias("human_count"),
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("bot_count"),
        F.col("human_count")        
    )
)

bot_vs_human_cnt_df.writeStream.format("parquet").outputMode("append").option(
    "path", "output/gold/bot_vs_human_cnts"
).option("checkpointLocation", "checkpoints/gold/q2").start()

domains_edit_cnts_df = (
    silver_layer.withWatermark("timestamp", "2 minutes")
    .groupBy(F.window("timestamp", "5 minutes"), F.col("domain"))
    .agg(F.count("*").alias("edits_count"))
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("domain"),
        F.col("edits_count"),
    )
)

domains_edit_cnts_df.writeStream.format("parquet").outputMode("append").option(
    "path", "output/gold/domain_edit_cnts"
).option("checkpointLocation", "checkpoints/gold/q3").start()

edits_max_avg_df = (
    silver_layer.withWatermark("timestamp", "2 minutes")
    .groupBy(F.window("timestamp", "5 minutes"))
    .agg(
        F.avg(F.abs(F.col("delta"))).alias("avg_delta"),
        F.max(F.abs(F.col("delta"))).alias("max_delta")
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("avg_delta"),
        F.col("max_delta")  
    )
)

edits_max_avg_df.writeStream.format("parquet").outputMode("append").option(
    "path", "output/gold/edits_max_avg"
).option("checkpointLocation", "checkpoints/gold/q4").start()

spark.streams.awaitAnyTermination()
