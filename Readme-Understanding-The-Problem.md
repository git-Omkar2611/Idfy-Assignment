MCQ Test Application Data Pipeline
This repository outlines the design and architecture of a robust, scalable data pipeline for an MCQ (Multiple Choice Question) Test Application. The goal is to provide comprehensive analytical insights into test performance, student engagement, and application health, leveraging a modern data lakehouse architecture.

1. Problem Statement
The core problem, as defined in the provided assessment, is to build an end-to-end data pipeline solution for an MCQ-based question paper application. Key aspects of the application and data requirements include:

Teachers create MCQ papers, rolled out to enrolled students.

Tests have fixed, varying deadlines.

Students must complete within the deadline to submit.

Tests are multi-page (one MCQ per page); questions must be submitted to move forward, but already submitted questions can be revisited/resubmitted.

Tests can be attended in multiple sessions (30-minute TTL per session); re-login redirects to the last active page.

Error cases: Failed store calls during submission, and potential bugs allowing submissions after session expiry.

The pipeline must support a high volume (1 Crore tests attempted per day) and enable a dashboard to answer specific questions:

How many tests were started?

How many tests were completed?

How many tests were not completed and reasons for it?

Timeline for each action (question) taken by the student (time taken per question, time to reach current question, time from test start).

Funnel view for test performance (not student performance).

Session-level stats.

Question-level stats (most time-consuming, mostly answered correctly/wrongly, mostly revisited).

2. Solution Overview
The proposed solution employs a Medallion Architecture (Bronze, Silver, Gold layers) built on Delta Lake within the Databricks platform, combined with Change Data Capture (CDC) via Debezium and Kafka for real-time data ingestion. This design ensures data reliability, scalability, and optimal performance for analytical workloads.

High-Level Architecture:

MCQ Test App <--> Transactional DB --(Debezium CDC)--> Kafka --(Spark Streaming - Bronze)--> Delta Lake (Bronze) --(Spark Streaming - Silver)--> Delta Lake (Silver) --(Spark Batch/Incremental Stream - Gold)--> Delta Lake (Gold - Fact & Dim) <-- Databricks SQL / Dashboards

3. Data Pipeline Architecture
3.1. Bronze Layer (Raw Data Ingestion)
Purpose: To ingest raw, immutable, and untransformed data directly from the source system. It serves as a resilient, comprehensive archive of all source changes.

Data Source: Kafka topics, populated by Debezium CDC events.

Format: Raw JSON strings (Debezium payload) for each change event (insert, update, delete).

Key Operations:

Real-time Stream Ingestion: Spark Structured Streaming continuously reads raw messages from Kafka topics.

Metadata Addition: Adds Kafka message metadata (topic, partition, offset, kafka_timestamp) and pipeline ingestion timestamp (ingestion_timestamp).

Immutable Storage: Writes the raw key and value (Debezium JSON) directly into append-only Delta tables.

Technology Stack: Debezium, Apache Kafka, Spark Structured Streaming, Delta Lake, Databricks Unity Catalog.

Tables: mcq_quiz_bronze.<source_table_name> (e.g., mcq_quiz_bronze.student_test_submission, mcq_quiz_bronze.session).

3.2. Silver Layer (Cleaned & Conformed Data)
Purpose: To transform raw Bronze data into a clean, structured, and validated format. This layer reflects the current state (or a managed history) of the source system, making data more usable.

Data Source: Bronze Delta tables (streaming).

Format: Structured Delta tables with enforced schemas and correct data types.

Key Operations:

CDC Event Parsing: Parses the raw Debezium JSON value to extract the before image, after image, and op (operation type: 'c' for create, 'u' for update, 'd' for delete).

Schema Enforcement & Type Casting: Applies predefined schemas to the after image, converting JSON fields into strong data types (e.g., INT, TIMESTAMP, BOOLEAN).

CDC Application with MERGE INTO: Utilizes Delta Lake's MERGE INTO statement to efficiently apply inserts, updates, and deletes to the Silver tables, ensuring they reflect the latest state of the source records.

Basic Data Quality Checks: Implicit through schema enforcement (e.g., nulls for type mismatches), and potential explicit filtering of malformed records.

Technology Stack: Spark Structured Streaming, Delta Lake, Databricks Unity Catalog.

Tables: mcq_quiz_silver.<source_table_name> (e.g., mcq_quiz_silver.student_test_submission, mcq_quiz_silver.questions).

3.3. Gold Layer (Curated & Optimized for Analytics)
Purpose: To provide highly curated, aggregated, and business-ready data, optimized specifically for analytical queries, reporting, and dashboards. This layer follows a dimensional modeling approach.

Data Source: Silver Delta tables (typically batch-processed for daily/hourly updates).

Format: Dimensional model (Fact and Dimension tables) in Delta Lake.

Key Operations:

Dimensional Modeling: Data is transformed into well-defined Fact tables (containing measures/metrics) and Dimension tables (containing descriptive attributes).

Business Logic & Aggregation: Complex joins across multiple Silver tables, calculation of derived metrics (e.g., test duration), and pre-aggregation for common dashboard queries (e.g., total completed tests per day).

Performance Optimization: Gold tables are optimized for read performance using techniques like Z-ordering and partitioning (e.g., by date, test ID).

Technology Stack: Databricks Jobs (running Spark batch jobs), Delta Lake, Databricks Unity Catalog.

Tables:

Dimension Tables (e.g., mcq_quiz_gold.dim_test, mcq_quiz_gold.dim_student, mcq_quiz_gold.dim_question, mcq_quiz_gold.dim_session_context, mcq_quiz_gold.dim_status, mcq_quiz_gold.dim_date):

Dim_Test: test_key, test_name, description, total_questions, deadline, teacher_id.

Dim_Student: student_key, student_name, email, enrollment_date.

Dim_Question: question_key, test_key, question_text, options, correct_answer, points.

Dim_Session_Context: session_key, session_start_time, session_end_time, session_ttl_minutes.

Dim_Status: status_key, status_name.

Dim_Date: Standard date dimension (date_key, full_date, day_of_week, month, year, etc.).

Fact Tables (e.g., mcq_quiz_gold.fact_student_test_attempt, mcq_quiz_gold.fact_question_answer):

Fact_StudentTestAttempt: submission_key (PK), student_key (FK), test_key (FK), session_key (FK), status_key (FK), test_start_timestamp, test_submission_timestamp, is_test_completed (measure), is_test_submitted (measure), total_score (measure), test_duration_seconds (measure).

Fact_QuestionAnswer: answer_key (PK), submission_key (FK), student_key (FK), question_key (FK), is_correct (measure), is_revisited (measure), time_taken_for_question_seconds (measure), question_submission_timestamp.

4. Key Metrics & Dashboard Insights
The Gold layer's dimensional model directly supports the required dashboard metrics:

Tests Started/Completed: Count submission_key from Fact_StudentTestAttempt filtered by test_start_timestamp and is_test_completed flags.

Tests Not Completed & Reasons: Count submission_key from Fact_StudentTestAttempt where is_test_completed is false, joined with Dim_Status for reasons.

Timeline per Action (Question):

Time taken for current question: time_taken_for_question_seconds from Fact_QuestionAnswer.

Time to reach question from last: Calculate using question_submission_timestamp and sequence in Fact_QuestionAnswer.

Time from test start: question_submission_timestamp from Fact_QuestionAnswer minus test_start_timestamp from Fact_StudentTestAttempt.

Funnel View (Test Performance): Track progression rates from Fact_QuestionAnswer (e.g., count distinct students who answered question 1, then question 2, etc.) joined with Fact_StudentTestAttempt for overall submission.

Session Level Stats: Analyze test_duration_seconds and session counts from Fact_StudentTestAttempt and Dim_Session_Context.

Question Level Stats:

Most time-consuming: Aggregate time_taken_for_question_seconds from Fact_QuestionAnswer grouped by question_key (joined with Dim_Question for text).

Mostly answered correctly/wrongly: Aggregate is_correct from Fact_QuestionAnswer grouped by question_key.

Mostly revisited: Aggregate is_revisited from Fact_QuestionAnswer grouped by question_key.

4.1. Refresh Frequency
Bronze Layer: Real-time (as data arrives from Kafka).

Silver Layer: Near real-time (micro-batches every 30-60 seconds), ensuring current state is maintained quickly.

Gold Layer:

Hourly Batch Refresh: For Teachers and Students Ingestion.

Near Real-time (Incremental Stream): Potentially for highly operational metrics like "Currently Active Tests" or "Live Student Count," if extreme freshness is a strict requirement for a specific dashboard panel (this would involve a small, dedicated streaming Gold pipeline).

5. Error Handling Considerations
Data Ingestion (Bronze): Kafka and Spark's checkpointing ensure exactly-once semantics, preventing data loss or duplication even during failures. Dead Letter Queues (DLQs) can capture messages that fail to be processed by Debezium.

Data Transformation (Silver):

Schema Evolution: Delta Lake's schema evolution features help handle minor schema changes from the source.

Data Quality/Validation: Records failing parsing or validation rules (e.g., critical nulls) can be diverted to "quarantine" Delta tables for investigation and reprocessing.

Idempotency: MERGE INTO operations are inherently idempotent, ensuring correct state even if a batch is reprocessed.

Application-Level Errors: For "store call fails" or "post-expiry submissions," the pipeline relies on the application recording these events/states in the database. The pipeline then captures and exposes these as metrics on the dashboard, enabling monitoring and bug detection.

6. Scalability Considerations
Debezium: Scales horizontally by adding more Kafka Connect workers.

Kafka: Highly scalable, distributed streaming platform.

Spark Structured Streaming: Built for distributed processing, scales horizontally by adding more cluster resources.

Delta Lake: Provides scalable metadata handling and efficient storage for petabytes of data, optimized for reads and writes.

Databricks Platform: Designed for elastic scalability of compute resources to handle varying workloads (from 1 to 1 Crore tests per day).

Dimensional Model (Gold): Optimized for analytical queries, making dashboards performant even with massive data volumes.

7. Technical Stack
Source Database: MySQL / PostgreSQL (or similar RDBMS)

Change Data Capture (CDC): Debezium

Message Broker: Apache Kafka

Data Lakehouse Storage: Delta Lake

Data Processing & Orchestration: Apache Spark (PySpark), Databricks (Spark Structured Streaming, Databricks Jobs)

Data Governance: Databricks Unity Catalog

Dashboarding & Visualization: Databricks SQL (native dashboards) or Power BI

8. Future Enhancements / Next Steps
Delta Live Tables (DLT): Transitioning Bronze-to-Silver-to-Gold transformations to DLT for declarative, simplified pipeline development, automated error handling, and managed data quality expectations.

SCD Type 2 for Dimensions: Implement Slowly Changing Dimension Type 2 logic in the Gold layer for dimensions like Dim_Student or Dim_Teacher if historical changes to their attributes need to be tracked.

Detailed Data Quality Framework: Implement a more explicit data quality framework with rules defined for each layer, and mechanisms for alerting and quarantining non-conforming data.

Monitoring & Alerting: Set up end-to-end monitoring for pipeline health, data freshness, and data quality using Databricks monitoring tools, integrating with alerting systems.

Cost Optimization: Implement auto-scaling policies, monitor cluster utilization, and optimize Delta table storage (e.g., compaction, vacuum) for cost efficiency.

Security: Deep dive into fine-grained access control with Unity Catalog for all tables and data operations.