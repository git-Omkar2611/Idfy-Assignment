-- 4. Timeline for each action (question) taken by the student
-- Provides a detailed timeline for a specific student's journey through a specific test submission.
-- Includes time on current question, time from last question, and time from test start.
-- PARAMETERS: REPLACE {your_submission_key} and {your_student_key} with actual values or dashboard filters.
SELECT
    fqa.submission_key,
    fqa.student_key,
    d_q.question_text,
    fqa.question_submission_timestamp,
    fqa.time_taken_for_question_seconds AS time_on_current_question_seconds,
    -- Time taken to reach this question from the last question (using LAG window function)
    COALESCE(
        (UNIX_TIMESTAMP(fqa.question_submission_timestamp) -
         UNIX_TIMESTAMP(LAG(fqa.question_submission_timestamp, 1, fsta.test_start_timestamp) OVER (PARTITION BY fqa.submission_key ORDER BY fqa.question_submission_timestamp))),
        0
    ) AS time_since_last_action_seconds, -- Can be from last question or test start if first question
    -- Time from the start of the test
    UNIX_TIMESTAMP(fqa.question_submission_timestamp) - UNIX_TIMESTAMP(fsta.test_start_timestamp) AS time_from_test_start_seconds
FROM
    mcq_quiz_gold.fact_question_answer AS fqa
JOIN
    mcq_quiz_gold.dim_question AS d_q ON fqa.question_key = d_q.question_key
JOIN
    mcq_quiz_gold.fact_student_test_attempt AS fsta ON fqa.submission_key = fsta.submission_key
WHERE
    fqa.submission_key = {your_submission_key} -- e.g., 101
    AND fqa.student_key = {your_student_key}   -- e.g., 201
ORDER BY
    fqa.question_submission_timestamp;