-- Test Timeliness: Tests Submitted Before/After Deadline (and percentage)
SELECT
    d_t.test_id,
    d_t.test_name,
    COUNT(fsta.submission_id) AS total_submitted_attempts,
    SUM(CASE WHEN fsta.test_submission_timestamp <= d_t.deadline THEN 1 ELSE 0 END) AS submitted_on_time_count,
    SUM(CASE WHEN fsta.test_submission_timestamp > d_t.deadline THEN 1 ELSE 0 END) AS submitted_late_count,
    (SUM(CASE WHEN fsta.test_submission_timestamp <= d_t.deadline THEN 1 ELSE 0 END) * 100.0) / COUNT(fsta.submission_id) AS percentage_submitted_on_time
FROM
    mcq_quiz_gold.fact_student_test_attempt AS fsta
JOIN
    mcq_quiz_gold.dim_test AS d_t ON fsta.test_id = d_t.test_id
WHERE
    fsta.is_test_submitted = TRUE 
GROUP BY
    d_t.test_id, d_t.test_name
HAVING COUNT(fsta.submission_id) > 0
ORDER BY
    percentage_submitted_on_time DESC;

-- Student Engagement: How many distinct sessions, on average, are used to complete a single test.
SELECT
    fsta.test_id,
    d_t.test_name,
    COUNT(DISTINCT fsta.student_id) AS unique_students_who_completed,
    COUNT(DISTINCT dsess.session_id) AS total_sessions_for_test,
    CAST(COUNT(DISTINCT dsess.session_id) AS DOUBLE) / COUNT(DISTINCT fsta.student_id) AS avg_sessions_per_student_per_test
FROM
    mcq_quiz_gold.fact_student_test_attempt AS fsta
JOIN
    mcq_quiz_gold.dim_test AS d_t ON fsta.test_id = d_t.test_id
LEFT JOIN 
    mcq_quiz_gold.dim_session_context AS dsess ON fsta.session_id = dsess.session_id 
WHERE
    fsta.is_test_completed = TRUE
GROUP BY
    fsta.test_id, d_t.test_name
HAVING COUNT(DISTINCT fsta.student_id) > 0 
ORDER BY
    avg_sessions_per_student_per_test DESC;

-- Failed Submission Rate (for tests, if failure is tracked in status_id)
SELECT
    ds.status_name,
    COUNT(fsta.submission_id) AS count_of_failed_submissions,
    (COUNT(fsta.submission_id) * 100.0) / (SELECT COUNT(submission_id) FROM mcq_quiz_gold.fact_student_test_attempt WHERE is_test_submitted = TRUE) AS percentage_of_total_submitted
FROM
    mcq_quiz_gold.fact_student_test_attempt AS fsta
JOIN
    mcq_quiz_gold.dim_status AS ds ON fsta.status_id = ds.status_id
WHERE
    fsta.is_test_submitted = TRUE 
    AND (ds.status_name LIKE '%fail%' OR ds.status_name LIKE '%error%' OR ds.status_name = 'Expired') 
GROUP BY
    ds.status_name
ORDER BY
    count_of_failed_submissions DESC;

--Average Time per Question Category/Difficulty 

SELECT
    d_q.question_category,
    d_q.difficulty_level,
    AVG(fqa.time_taken_for_question_seconds) AS avg_time_per_category_difficulty
FROM
    mcq_quiz_gold.fact_question_answer AS fqa
JOIN
    mcq_quiz_gold.dim_question AS d_q ON fqa.question_id = d_q.question_id
GROUP BY
    d_q.question_category, d_q.difficulty_level
ORDER BY
    avg_time_per_category_difficulty DESC;