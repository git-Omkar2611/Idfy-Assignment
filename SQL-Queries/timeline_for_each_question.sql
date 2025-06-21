-- Timeline for each action (question) taken by the student
-- Provides a detailed timeline for a specific student's journey through a specific test submission.
-- Includes time on current question, time from last question, and time from test start.
-- PARAMETERS: REPLACE {your_submission_id} and {your_student_id} with actual values or dashboard filters.
SELECT
    fqa.submission_id,
    fqa.student_id,
    d_q.question_text,
    fqa.question_submission_timestamp,
    fqa.time_taken_for_question_seconds AS time_on_current_question_seconds,
    COALESCE(
        (UNIX_TIMESTAMP(fqa.question_submission_timestamp) -
         UNIX_TIMESTAMP(LAG(fqa.question_submission_timestamp, 1, fsta.test_start_timestamp) OVER (PARTITION BY fqa.submission_id ORDER BY fqa.question_submission_timestamp))),
        0
    ) AS time_since_last_action_seconds, 
    UNIX_TIMESTAMP(fqa.question_submission_timestamp) - UNIX_TIMESTAMP(fsta.test_start_timestamp) AS time_from_test_start_seconds
FROM
    mcq_quiz_gold.fact_question_answer AS fqa
JOIN
    mcq_quiz_gold.dim_question AS d_q ON fqa.question_id = d_q.question_id
JOIN
    mcq_quiz_gold.fact_student_test_attempt AS fsta ON fqa.submission_id = fsta.submission_id
WHERE
    fqa.submission_id = {your_submission_id} 
    AND fqa.student_id = {your_student_id}  
ORDER BY
    fqa.question_submission_timestamp;