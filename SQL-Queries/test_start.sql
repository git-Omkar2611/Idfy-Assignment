SELECT
    COUNT(DISTINCT fsta.submission_id) AS total_tests_started
FROM
    mcq_quiz_gold.fact_student_test_attempt AS fsta;