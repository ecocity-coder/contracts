WITH CTE AS (
    SELECT
        customer_id,
        contract_id,
        contract_serial_number,
        issue_dt,
        loan_amount,
        EXTRACT(YEAR FROM issue_dt) AS year,
        EXTRACT(MONTH FROM issue_dt) AS month,
        FIRST_VALUE(loan_amount) OVER (PARTITION BY customer_id ORDER BY issue_dt) AS first_loan_amount,
        LAST_VALUE(loan_amount) OVER (PARTITION BY customer_id ORDER BY issue_dt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_loan_amount,
        RANK() OVER (PARTITION BY EXTRACT(YEAR FROM issue_dt), EXTRACT(MONTH FROM issue_dt) ORDER BY contract_serial_number DESC) AS rank
    FROM final_data
    WHERE EXTRACT(YEAR FROM issue_dt) = 2019
)

SELECT
    year,
    month,
    customer_id,
    MAX(contract_serial_number) AS max_contract_serial_number,
    (last_loan_amount / NULLIF(first_loan_amount, 0)) AS loan_growth_factor
FROM CTE
WHERE rank = 1
GROUP BY year, month, customer_id, first_loan_amount, last_loan_amount
ORDER BY year, month, customer_id;

