"""

1. Read from Silver: 
    -Scheduled or streaming jobs read from the clean, structured Silver Layer tables.

2. Create Dimension Tables:
    -Data from Silver tables representing entities (e.g., mcq_quiz_silver.student, mcq_quiz_silver.test, mcq_quiz_silver.questions, mcq_quiz_silver.teacher, mcq_quiz_silver.status) is extracted, potentially de-duplicated, and loaded into Dimension Tables in the Gold Layer.
    -These tables (Dim_Student, Dim_Test, Dim_Question, Dim_Teacher, Dim_Status, Dim_Date) contain descriptive attributes for analysis and act as lookup tables. Slowly Changing Dimensions (SCD) might be applied here if history of attribute changes is needed.

3. Create Fact Tables:
    -Data from Silver tables representing events or measurements (e.g., mcq_quiz_silver.student_test_submission, mcq_quiz_silver.student_test_answer_details, mcq_quiz_silver.session) is transformed and aggregated into Fact Tables in the Gold Layer.
    -These tables (Fact_StudentTestAttempt, Fact_QuestionAnswer, Fact_Session) contain foreign keys linking to the dimension tables and the numerical measures (metrics) required for dashboards (e.g., total_score, time_taken_for_question, is_correct_flag, session_duration).
    -Joins between Silver tables may occur here to enrich the fact data before loading.

4. Optimization: Gold Layer tables are typically highly optimized (e.g., Z-ordering, partitioning) for analytical query performance.
"""

# --- Example: Creating a Dim_Test table (Dimension) ---
# This would typically be a batch job or a streaming job for SCD Type 1/2

# SQL approach (common in Databricks SQL Endpoints/Notebooks)
spark.sql(f"""
CREATE OR REPLACE TABLE mcq_quiz_gold.dim_test
(
  test_id INT NOT NULL COMMENT "Unique identifier for the test",
  test_name STRING,
  description STRING,
  total_questions INT,
  deadline TIMESTAMP,
  teacher_id INT, -- Assuming teacher_id is also a FK to Dim_Teacher
  test_created_at TIMESTAMP,
  test_updated_at TIMESTAMP,
  silver_processed_at TIMESTAMP COMMENT "When this record was processed in Silver"
)
USING DELTA
LOCATION '/mnt/gold/dim_test'; -- Or managed table within Unity Catalog

INSERT OVERWRITE INTO mcq_quiz_gold.dim_test
SELECT
  id as test_id,
  test_name,
  description,
  total_questions,
  deadline,
  teacher_id,
  created_at as test_created_at,
  updated_at as test_updated_at,
  silver_processed_at
FROM mcq_quiz_silver.test;
""")

# --- Example: Creating a Fact_StudentTestAttempt table (Fact) ---
# This would often be a daily/hourly batch job calculating aggregates

# PySpark (DataFrame API) approach
df_fact_test_attempt = spark.table("mcq_quiz_silver.student_test_submission") \
    .join(spark.table("mcq_quiz_silver.session"),
          on=(col("student_test_submission.student_id") == col("session.student_id")) &
             (col("student_test_submission.test_id") == col("session.test_id")) &
             (col("student_test_submission.start_time") == col("session.start_time")), # Need a good join key for session
          how="left") \
    .join(spark.table("mcq_quiz_silver.test"),
          on=col("student_test_submission.test_id") == col("test.id"),
          how="left") \
    .select(
        col("student_test_submission.id").alias("submission_id"),
        col("student_test_submission.student_id").alias("student_id"), # Foreign Key to Dim_Student
        col("student_test_submission.test_id").alias("test_id"),       # Foreign Key to Dim_Test
        col("session.id").alias("session_id"),                           # Foreign Key to Dim_Session
        col("student_test_submission.start_time").alias("test_start_time"),
        col("student_test_submission.submission_time").alias("test_submission_time"),
        col("student_test_submission.is_completed").alias("is_completed_flag"),
        col("student_test_submission.is_submitted").alias("is_submitted_flag"),
        col("student_test_submission.total_score").alias("total_score"),
        ((unix_timestamp(col("student_test_submission.submission_time")) - unix_timestamp(col("student_test_submission.start_time")))/60).alias("test_duration_minutes"), # Calculated metric
        col("student_test_submission.silver_processed_at").alias("data_processed_at")
        # Add more calculated metrics as needed for dashboards
    )

# Write to Gold Delta table
df_fact_test_attempt.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "/mnt/gold/fact_student_test_attempt") \
    .saveAsTable("mcq_quiz_gold.fact_student_test_attempt")

#repeat this pattern for other dimensions (Dim_Student, Dim_Question, Dim_Teacher, Dim_Date, etc.) and other facts (Fact_QuestionAnswer) using data from relevant Silver tables.