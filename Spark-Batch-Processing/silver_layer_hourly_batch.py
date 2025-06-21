"""
1. Read New Changes
2. Parse & Clean the data from bronze
3. Apply SCD Type 2

"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, lit, sha2, concat_ws, monotonically_increasing_id, row_number
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType, BooleanType
from delta.tables import DeltaTable
from pyspark.sql.window import Window 

BRONZE_SCHEMA_NAME = "mcq_quiz_bronze"
SILVER_SCHEMA_NAME = "mcq_quiz_silver"

debezium_payload_schema = StructType([
    StructField("before", StringType(), True),
    StructField("after", StringType(), True),
    StructField("op", StringType(), True), 
    StructField("ts_ms", LongType(), True), 
    StructField("source", StructType([
        StructField("db", StringType(), True),
        StructField("table", StringType(), True)
    ]), True)
])


teacher_source_schema = StructType([
    StructField("teacher_id", IntegerType(), False),
    StructField("teacher_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Schema for the Silver 'dim_teacher' table, including SCD Type 2 columns
dim_teacher_silver_schema = StructType([
    StructField("teacher_id", IntegerType(), False),
    StructField("teacher_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("original_created_at", TimestampType(), True),
    StructField("original_updated_at", TimestampType(), True),
    StructField("start_date", TimestampType(), False),
    StructField("end_date", TimestampType(), True),
    StructField("is_current", BooleanType(), False),
    StructField("silver_processed_at", TimestampType(), False)
])

student_source_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("student_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("enrollment_date", TimestampType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

dim_student_silver_schema = StructType([
    StructField("student_id", IntegerType(), False),
    StructField("student_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("enrollment_date", TimestampType(), True),
    StructField("original_created_at", TimestampType(), True),
    StructField("original_updated_at", TimestampType(), True),
    StructField("start_date", TimestampType(), False),
    StructField("end_date", TimestampType(), True),
    StructField("is_current", BooleanType(), False),
    StructField("silver_processed_at", TimestampType(), False)
])


def process_dimension_scd2_silver_batch(
    source_bronze_table_name: str,       
    target_silver_dim_table_name: str,   
    natural_id_column: str,             
    source_record_schema: StructType,    
    target_silver_dim_schema: StructType,
):
    """
    High-level PySpark function to process a dimension table from Bronze to Silver
    with SCD Type 2 logic using batch processing.
    """
    print(f"Starting high-level Silver SCD2 batch job for '{source_bronze_table_name}' into '{target_silver_dim_table_name}'")

    bronze_table_full_path = f"{BRONZE_SCHEMA_NAME}.{source_bronze_table_name}"
    silver_table_full_path = f"{SILVER_SCHEMA_NAME}.{target_silver_dim_table_name}"

    current_processing_timestamp = current_timestamp()

    df_bronze_data = spark.read.format("delta").table(bronze_table_full_path)

    df_parsed_cdc = df_bronze_data.withColumn("debezium", from_json(col("value"), debezium_payload_schema)) \
                                 .select(
                                     col("debezium.op").alias("op_type"),
                                     from_json(col("debezium.after"), source_record_schema).alias("new_data"),
                                     from_json(col("debezium.before"), source_record_schema).alias("old_data"),
                                     col("ingestion_timestamp") 
                                 ) \
                                 .filter(col("new_data").isNotNull() | col("old_data").isNotNull()) 

    window_spec = Window.partitionBy(col(f"new_data.{natural_id_column}")).orderBy(col("ingestion_timestamp").desc(), col("op_type").desc())
    df_latest_changes = df_parsed_cdc.withColumn("rn", row_number().over(window_spec)).filter(col("rn") == 1).drop("rn")

    df_source_for_merge = df_latest_changes.select(
        col(f"new_data.{natural_id_column}").alias(natural_id_column),
        col("new_data.*"), # All columns from the 'after' image
        col("op_type"),
        col("ingestion_timestamp").alias("event_timestamp") # For start_date/end_date
    )

    if not spark._jsparkSession.catalog().tableExists(silver_table_full_path):
        print(f"Creating initial Silver dimension table: {silver_table_full_path}")
        initial_records = df_source_for_merge.filter(col("op_type").isin("c", "r")).select(
            monotonically_increasing_id().alias(target_silver_dim_schema.names[0]), # Surrogate key
            col(natural_id_column), # Natural key from source
            col("original_columns_from_source.*"), # Conceptual placeholder for actual columns
            col("event_timestamp").alias("start_date"),
            lit(None).cast("timestamp").alias("end_date"),
            lit(True).alias("is_current"),
            current_processing_timestamp.alias("silver_processed_at")
        )
        initial_records.write.format("delta").mode("append").saveAsTable(silver_table_full_path)
        return 

    silver_delta_table = DeltaTable.forName(spark, silver_table_full_path)


    silver_delta_table.alias("target") \
        .merge(
            df_source_for_merge.alias("source"),
            f"target.{natural_id_column} = source.{natural_id_column} AND target.is_current = true"
        ) \
        .whenMatchedAnd(
            "target.content_hash != source.content_hash OR source.op_type = 'd'"
        ).update(set = {
            "is_current": lit(False),
            "end_date": col("source.event_timestamp")
        }) \
        .whenNotMatchedInsert(values = {
            "surrogate_id": monotonically_increasing_id(), # New surrogate key
            natural_id_column: col(natural_id_column),
            "other_descriptive_columns": col("source.other_descriptive_columns"), # Conceptual
            "start_date": col("source.event_timestamp"),
            "end_date": lit(None).cast("timestamp"),
            "is_current": lit(True),
            "silver_processed_at": current_processing_timestamp
        }) \
        .execute()

    print(f"High-level Silver SCD2 batch job for '{source_bronze_table_name}' completed.")


TEACHER_SRC_SCHEMA_CONCEPT = "STRUCT<id INT, teacher_name STRING, email STRING, created_at TIMESTAMP, updated_at TIMESTAMP>"
STUDENT_SRC_SCHEMA_CONCEPT = "STRUCT<id INT, student_name STRING, email STRING, enrollment_date TIMESTAMP, created_at TIMESTAMP, updated_at TIMESTAMP>"


TEACHER_SCD2_COLS = ["teacher_name", "email"]
STUDENT_SCD2_COLS = ["student_name", "email", "enrollment_date"]


process_dimension_scd2_silver_batch(
    source_bronze_table_name="teachers",
    target_silver_dim_table_name="dim_teacher",
    natural_id_column="id",
    source_record_schema=teacher_source_schema, 
    target_silver_dim_schema=dim_teacher_silver_schema, 
    descriptive_columns_for_hash=TEACHER_SCD2_COLS 
)

process_dimension_scd2_silver_batch(
    source_bronze_table_name="students",
    target_silver_dim_table="dim_student",
    natural_id_column="id",
    source_record_schema=student_source_schema, 
    target_silver_dim_schema=dim_student_silver_schema, 
    descriptive_columns_for_hash=STUDENT_SCD2_COLS 
)

