SELECT
    ds.session_id,
    ds.student_id,
    ds.test_id,
    d_t.test_name,
    ds.session_start_time,
    ds.session_end_time,
    ds.session_ttl_minutes,
    (UNIX_TIMESTAMP(ds.session_end_time) - UNIX_TIMESTAMP(ds.session_start_time)) AS actual_session_duration_seconds,
    ds.last_active_page,
    ds.is_active,
    COUNT(DISTINCT fqa.question_id) AS questions_answered_in_session 
FROM
    mcq_quiz_gold.dim_session_context AS ds
LEFT JOIN 
    mcq_quiz_gold.fact_question_answer AS fqa ON ds.session_id = fqa.session_id
LEFT JOIN
    mcq_quiz_gold.dim_test AS d_t ON ds.test_id = d_t.test_id
GROUP BY
    ds.session_id, ds.student_id, ds.test_id, d_t.test_name, ds.session_start_time, ds.session_end_time,
    ds.session_ttl_minutes, ds.last_active_page, ds.is_active
ORDER BY
    ds.session_start_time DESC;

-- Overall Averages for Session Stats:
SELECT
    AVG(UNIX_TIMESTAMP(ds.session_end_time) - UNIX_TIMESTAMP(ds.session_start_time)) AS avg_session_duration_seconds,
    COUNT(DISTINCT ds.session_id) AS total_sessions,
    COUNT(DISTINCT ds.test_id) AS total_tests_engaged,
    CAST(COUNT(DISTINCT ds.session_id) AS DOUBLE) / COUNT(DISTINCT ds.student_id) AS avg_sessions_per_student_overall
FROM
    mcq_quiz_gold.dim_session_context AS ds;