"""
A Spark Streaming job (running on Databricks) continuously reads raw messages from these Kafka topics.
It takes the raw Kafka message (including the Debezium JSON payload as a string).
It adds ingestion metadata (e.g., Kafka timestamp, ingestion timestamp).
It writes this raw, unstructured data directly into designated Bronze Layer tables in Delta Lake (within Unity Catalog). 
Each Kafka topic usually corresponds to a Bronze table (e.g., mcq_quiz_bronze.student_test_submission).
"""

# Assuming 'spark' session is available and Kafka config is set

# Read stream from Kafka
df_raw_kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "your_kafka_broker:9092") \
    .option("subscribe", "dbserver1.mcq_app.your_table_name") \
    .load()

# Select raw value and add ingestion metadata
df_bronze = df_raw_kafka_stream.selectExpr(
    "CAST(key AS STRING) as key",
    "CAST(value AS STRING) as value", # Raw Debezium JSON string
    "topic",
    "partition",
    "offset",
    "timestamp as kafka_timestamp",
    "current_timestamp() as ingestion_timestamp" # When it landed in Bronze
)

# Write to Bronze Delta table
query_bronze = df_bronze.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/delta/checkpoints/mcq_quiz_bronze/your_table_name") \
    .toTable("mcq_quiz_bronze.your_table_name")

# This stream will run continuously to ingest new data.
