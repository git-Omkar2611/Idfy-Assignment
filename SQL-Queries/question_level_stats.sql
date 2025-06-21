-- Most Time Consuming
SELECT
    d_q.question_id,
    d_q.question_text,
    AVG(fqa.time_taken_for_question_seconds) AS average_time_spent_seconds,
    COUNT(fqa.answer_id) AS total_answer_submissions 
FROM
    mcq_quiz_gold.fact_question_answer AS fqa
JOIN
    mcq_quiz_gold.dim_question AS d_q ON fqa.question_id = d_q.question_id
GROUP BY
    d_q.question_id, d_q.question_text
ORDER BY
    average_time_spent_seconds DESC
LIMIT 10;


-- Mostly answered correctly questions

SELECT
    d_q.question_id,
    d_q.question_text,
    COUNT(fqa.answer_id) AS total_answers_for_question,
    SUM(CASE WHEN fqa.is_correct = TRUE THEN 1 ELSE 0 END) AS total_correct_answers,
    (SUM(CASE WHEN fqa.is_correct = TRUE THEN 1 ELSE 0 END) * 100.0) / COUNT(fqa.answer_id) AS percentage_correct
FROM
    mcq_quiz_gold.fact_question_answer AS fqa
JOIN
    mcq_quiz_gold.dim_question AS d_q ON fqa.question_id = d_q.question_id
GROUP BY
    d_q.question_id, d_q.question_text
HAVING COUNT(fqa.answer_id) > 0 
ORDER BY
    percentage_correct DESC
LIMIT 10;

--  Mostly answered wrong question 
SELECT
    d_q.question_id,
    d_q.question_text,
    COUNT(fqa.answer_id) AS total_answers_for_question,
    SUM(CASE WHEN fqa.is_correct = FALSE THEN 1 ELSE 0 END) AS total_incorrect_answers,
    (SUM(CASE WHEN fqa.is_correct = FALSE THEN 1 ELSE 0 END) * 100.0) / COUNT(fqa.answer_id) AS percentage_incorrect
FROM
    mcq_quiz_gold.fact_question_answer AS fqa
JOIN
    mcq_quiz_gold.dim_question AS d_q ON fqa.question_id = d_q.question_id
GROUP BY
    d_q.question_id, d_q.question_text
HAVING COUNT(fqa.answer_id) > 0
ORDER BY
    percentage_incorrect DESC
LIMIT 10;

-- Mostly revisited question 
SELECT
    d_q.question_id,
    d_q.question_text,
    SUM(CASE WHEN fqa.is_revisited = TRUE THEN 1 ELSE 0 END) AS total_revisits_count
FROM
    mcq_quiz_gold.fact_question_answer AS fqa
JOIN
    mcq_quiz_gold.dim_question AS d_q ON fqa.question_id = d_q.question_id
GROUP BY
    d_q.question_id, d_q.question_text
ORDER BY
    total_revisits_count DESC
LIMIT 10;

