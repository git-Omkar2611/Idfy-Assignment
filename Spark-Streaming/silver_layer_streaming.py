"""
This will be the flow for Silver Layer Transformation

1. Spark Streaming job continuously reads new data arriving in the Bronze Layer tables.
2. Parse the Debezium JSON value to extract before, after, and op (operation type).
3. Apply schema to the after (and before) data, performing basic type casting and initial data quality checks.
4. Use MERGE INTO to apply CDC changes (upserts and deletes) to Silver Delta tables.
    - If op is 'c' (create): The new record is inserted into the Silver table.
    - If op is 'u' (update): The existing record (identified by its primary key) in the Silver table is updated with the new values from the after image.
    - If op is 'd' (delete): The corresponding record is removed from the Silver table (or marked as is_deleted for soft deletes).

    
This ensures the Silver tables always reflect the latest, cleaned, and structured state of the operational data.

"""

from delta.tables import DeltaTable
from pyspark.sql.functions import from_json, col, current_timestamp

spark = SparkSession.builder.getOrCreate()

TABLE_NAME = "student_test_submission" # Example table
PRIMARY_KEYS = ["id"]

# Read stream from Bronze Delta table
df_bronze_stream = spark.readStream \
    .format("delta") \
    .table(f"mcq_quiz_bronze.{TABLE_NAME}")

def process_silver_batch(microBatchOutputDF, batchId):
    # Parse the Debezium payload from the 'value' column
    df_cdc_events = microBatchOutputDF.withColumn("debezium", from_json(col("value"), debezium_payload_schema)) \
                                     .select(
                                         col("debezium.op").alias("op"),
                                         from_json(col("debezium.after"), student_test_submission_schema).alias("data"),
                                         from_json(col("debezium.before"), student_test_submission_schema).alias("old_data")
                                     ) \
                                     .withColumn("silver_processed_at", current_timestamp())

    # Get target Delta table (create if not exists, for initial load)
    # This logic would be more robust in a real implementation
    if not spark._jsparkSession.catalog().tableExists(f"mcq_quiz_silver.{TABLE_NAME}"):
        df_cdc_events.filter(col("op") == "c") \
                     .select(col("data.*"), col("silver_processed_at")) \
                     .write.format("delta").mode("append").saveAsTable(f"mcq_quiz_silver.{TABLE_NAME}")
        return # Skip merge for the initial creation batch

    silver_table = DeltaTable.forName(spark, f"mcq_quiz_silver.{TABLE_NAME}")

    # Merge inserts and updates
    silver_table.alias("target") \
        .merge(
            df_cdc_events.filter(col("op").isin("c", "u")).alias("source"),
            " AND ".join([f"target.{pk} = source.data.{pk}" for pk in PRIMARY_KEYS])
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

    df_deletes = df_cdc_events.filter(col("op") == "d")
    if df_deletes.count() > 0:
        silver_table.alias("target").delete(
             condition=col(f"target.{PRIMARY_KEYS[0]}").isin(
                 df_deletes.select(f"old_data.{PRIMARY_KEYS[0]}").rdd.flatMap(lambda x: x).collect()
             )
        )

# Start the Silver layer streaming query
query_silver = df_bronze_stream.writeStream \
    .foreachBatch(process_silver_batch) \
    .outputMode("update") \
    .option("checkpointLocation", f"/mnt/delta/checkpoints/mcq_quiz_silver/{TABLE_NAME}") \
    .start()