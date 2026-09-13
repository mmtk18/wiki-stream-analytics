from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("GoldCsvExport")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

GOLD_PATH = "C:/projects/DE_Proj/output/gold"
CSV_PATH = "C:/projects/DE_Proj/output/gold_csv"

datasets = [
    "bot_vs_human_cnts",
    "domain_edit_cnts",
    "edits_max_avg",
    "edits_per_minute",
]

for dataset in datasets:
    input_path = f"{GOLD_PATH}/{dataset}"
    output_path = f"{CSV_PATH}/{dataset}"

    print(f"Exporting {dataset}...")

    df = spark.read.parquet(input_path)

    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)

spark.stop()