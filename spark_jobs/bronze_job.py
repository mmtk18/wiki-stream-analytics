from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("bronzeJob").master("local[4]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Spark master:", spark.sparkContext.master)
print("Default parallelism:", spark.sparkContext.defaultParallelism)

## Reading kafka topic
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "wiki_events")
    .option("startingOffsets", "earliest")
    .load()
)

bronze_layer_df = kafka_df.withColumn(
    "ingestion_timestamp", F.current_timestamp()
).select(
    F.col("topic"),
    F.col("partition"),
    F.col("offset"),
    F.col("timestamp"),
    F.col("ingestion_timestamp"),
    F.col("key"),
    F.col("value").alias("raw_json_value"),
)

query = (
    bronze_layer_df.writeStream.format("parquet")
    .outputMode("append")
    .option("path", "output/bronze")
    .option("checkpointLocation", "checkpoints/bronze")
    .trigger(processingTime="1 minute")
    .start()
)

spark.streams.awaitAnyTermination()
