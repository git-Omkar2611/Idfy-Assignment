WITH QuestionOrder AS (
    SELECT
        question_key,
        test_key,
        ROW_NUMBER() OVER (PARTITION BY test_key ORDER BY question_key) AS question_sequence_num
    FROM
        mcq_quiz_gold.dim_question
    WHERE
        test_key = {your_test_key} 
),
StudentsAtEachStep AS (
    SELECT
        fqa.submission_key,
        fqa.student_key,
        qo.question_sequence_num
    FROM
        mcq_quiz_gold.fact_question_answer AS fqa
    JOIN
        QuestionOrder AS qo ON fqa.question_key = qo.question_key AND fqa.test_key = qo.test_key
    WHERE
        fqa.test_key = {your_test_key}
    GROUP BY 
        fqa.submission_key, fqa.student_key, qo.question_sequence_num
),
FunnelCounts AS (
    SELECT
        question_sequence_num,
        COUNT(DISTINCT student_key) AS students_at_step
    FROM
        StudentsAtEachStep
    GROUP BY
        question_sequence_num
),
InitialStarters AS (
    SELECT
        COUNT(DISTINCT student_key) AS total_starters
    FROM
        mcq_quiz_gold.fact_student_test_attempt
    WHERE
        test_key = {your_test_key}
)
SELECT
    fc.question_sequence_num,
    d_q.question_text,
    fc.students_at_step,
    (CAST(fc.students_at_step AS DOUBLE) / (SELECT total_starters FROM InitialStarters)) * 100 AS percentage_from_start,
    
    (CAST(fc.students_at_step AS DOUBLE) / LAG(fc.students_at_step, 1, fc.students_at_step) OVER (ORDER BY fc.question_sequence_num)) * 100 AS percentage_from_previous_step
FROM
    FunnelCounts AS fc
JOIN
    QuestionOrder AS d_q ON fc.question_sequence_num = d_q.question_sequence_num 
ORDER BY
    fc.question_sequence_num;