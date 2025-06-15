SELECT
    ds.status_name AS completion_reason,
    COUNT(DISTINCT fsta.submission_key) AS num_tests_not_completed
FROM
    mcq_quiz_gold.fact_student_test_attempt AS fsta
JOIN
    mcq_quiz_gold.dim_status AS ds ON fsta.status_key = ds.status_key
WHERE
    fsta.is_test_completed = FALSE
GROUP BY
    ds.status_name
ORDER BY
    num_tests_not_completed DESC;