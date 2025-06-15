SELECT
    ds.session_key,
    ds.student_key,
    ds.test_key,
    d_t.test_name,
    ds.session_start_time,
    ds.session_end_time,
    ds.session_ttl_minutes,
    (UNIX_TIMESTAMP(ds.session_end_time) - UNIX_TIMESTAMP(ds.session_start_time)) AS actual_session_duration_seconds,
    ds.last_active_page,
    ds.is_active,
    COUNT(DISTINCT fqa.question_key) AS questions_answered_in_session -- How many distinct questions were answered in this session
FROM
    mcq_quiz_gold.dim_session_context AS ds
LEFT JOIN -- Use LEFT JOIN to include sessions even if no questions were answered
    mcq_quiz_gold.fact_question_answer AS fqa ON ds.session_key = fqa.session_key
LEFT JOIN
    mcq_quiz_gold.dim_test AS d_t ON ds.test_key = d_t.test_key
GROUP BY
    ds.session_key, ds.student_key, ds.test_key, d_t.test_name, ds.session_start_time, ds.session_end_time,
    ds.session_ttl_minutes, ds.last_active_page, ds.is_active
ORDER BY
    ds.session_start_time DESC;

-- Overall Averages for Session Stats:
SELECT
    AVG(UNIX_TIMESTAMP(ds.session_end_time) - UNIX_TIMESTAMP(ds.session_start_time)) AS avg_session_duration_seconds,
    COUNT(DISTINCT ds.session_key) AS total_sessions,
    COUNT(DISTINCT ds.test_key) AS total_tests_engaged,
    CAST(COUNT(DISTINCT ds.session_key) AS DOUBLE) / COUNT(DISTINCT ds.student_key) AS avg_sessions_per_student_overall
FROM
    mcq_quiz_gold.dim_session_context AS ds;