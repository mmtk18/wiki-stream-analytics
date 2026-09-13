import duckdb

SILVER_PATH = "output/silver"
GOLD_PATH = "output/gold"

datasets = [
    "bot_vs_human_cnts",
    "domain_edit_cnts",
    "edits_max_avg",
    "edits_per_minute",
]

for dataset in datasets:
    print(f"writing dataset {dataset} as csv")
    query = f"""
    COPY (
        SELECT * 
        FROM '{GOLD_PATH}/{dataset}/*.parquet'
        ORDER BY WINDOW_START
    )
    TO 'csv_output/gold/{dataset}.csv'
    (HEADER, DELIMITER ',')
    """

    df = duckdb.sql(query)


duckdb.sql(f"""
    COPY (
        SELECT * 
        FROM '{SILVER_PATH}/*.parquet'
        ORDER BY TIMESTAMP
    )
    TO 'csv_output/silver/data.csv'
    (HEADER, DELIMITER ',')
    """)
