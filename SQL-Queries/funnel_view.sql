WITH QuestionOrder AS (
    SELECT
        question_id,
        test_id,
        ROW_NUMBER() OVER (PARTITION BY test_id ORDER BY question_id) AS question_sequence_num
    FROM
        mcq_quiz_gold.dim_question
    WHERE
        test_id = {your_test_id} 
),
StudentsAtEachStep AS (
    SELECT
        fqa.submission_id,
        fqa.student_id,
        qo.question_sequence_num
    FROM
        mcq_quiz_gold.fact_question_answer AS fqa
    JOIN
        QuestionOrder AS qo ON fqa.question_id = qo.question_id AND fqa.test_id = qo.test_id
    WHERE
        fqa.test_id = {your_test_id}
    GROUP BY 
        fqa.submission_id, fqa.student_id, qo.question_sequence_num
),
FunnelCounts AS (
    SELECT
        question_sequence_num,
        COUNT(DISTINCT student_id) AS students_at_step
    FROM
        StudentsAtEachStep
    GROUP BY
        question_sequence_num
),
InitialStarters AS (
    SELECT
        COUNT(DISTINCT student_id) AS total_starters
    FROM
        mcq_quiz_gold.fact_student_test_attempt
    WHERE
        test_id = {your_test_id}
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