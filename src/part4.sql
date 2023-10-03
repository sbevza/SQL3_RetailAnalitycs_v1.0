CREATE OR REPLACE FUNCTION form_offer_for_average_check(
    IN calc_method INT,
    IN start_date DATE,
    IN end_date DATE,
    IN transaction_count INT,
    IN increase_coeff DECIMAL(10, 2),
    IN max_churn_index DECIMAL(10, 2),
    IN max_discount_percentage DECIMAL(10, 2),
    IN max_margin_percentage DECIMAL(10, 2)
)
    RETURNS TABLE
            (
                Customer_ID            INT,
                Required_Check_Measure DECIMAL(10, 2),
                Group_Name             VARCHAR(255),
                Offer_Discount_Depth   DECIMAL(10, 2)
            )
AS
$$
BEGIN
    CREATE TEMP TABLE temp_result_table
    (
        Customer_ID            INT,
        Required_Check_Measure DECIMAL(10, 2),
        Group_Name             VARCHAR(255),
        Offer_Discount_Depth   DECIMAL(10, 2)
    ) ON COMMIT DROP;

    IF calc_method = 1 THEN
        -- Расчет по периоду
        INSERT INTO temp_result_table
        SELECT c.customer_id,
               ROUND(AVG(t.transaction_summ) * increase_coeff, 2) AS required_check_measure,
               NULL::VARCHAR(255)                                 AS Group_Name
        FROM cards c
                 JOIN transactions t ON t.customer_card_id = c.customer_card_id
        WHERE t.transaction_datetime BETWEEN start_date AND end_date
        GROUP BY c.customer_id;

    ELSIF calc_method = 2 THEN
        -- Расчет по количеству транзакций
        INSERT INTO temp_result_table
        SELECT c.customer_id,
               ROUND(AVG(c.transaction_summ) * increase_coeff, 2) AS required_check_measure,
               NULL::VARCHAR(255)                                 AS Group_Name
        FROM (SELECT c.customer_id,
                     t.transaction_summ
              FROM cards c
                       JOIN transactions t ON t.customer_card_id = c.customer_card_id
              ORDER BY t.transaction_id DESC
              LIMIT transaction_count) c
        GROUP BY c.customer_id;

    END IF;

-- Определяем Group_Name для каждого Customer_ID в таблице temp_result_table
    UPDATE temp_result_table AS trt
    SET Group_Name = (SELECT DISTINCT ON (pd.customer_id) g.group_name
                      FROM Periods pd
                               JOIN Customers cu ON pd.customer_id = cu.customer_id
                               JOIN groups_sku g ON pd.group_id = g.group_id
                               JOIN groups gv on gv.customer_id = cu.customer_id
                          AND gv.group_id = g.group_id
                      WHERE pd.customer_id = trt.Customer_ID
                        AND gv.Group_Churn_Rate <= max_churn_index
                        AND gv.Group_Discount_Share <= max_discount_percentage
                      ORDER BY pd.customer_id, pd.group_frequency DESC)
    WHERE EXISTS (SELECT 1
                  FROM Periods pd
                           JOIN Customers cu ON pd.customer_id = cu.customer_id
                  WHERE pd.customer_id = trt.Customer_ID);

    RETURN QUERY
        SELECT * FROM temp_result_table;
END;
$$ LANGUAGE plpgsql;



SELECT *
FROM form_offer_for_average_check(
        calc_method := 1,
        start_date := '2021-07-01',
        end_date := '2023-12-31',
        transaction_count := 1,
        increase_coeff := 1.5,
        max_churn_index := 2,
        max_discount_percentage := 2,
        max_margin_percentage := 0.3
    );



SELECT *
FROM form_offer_for_average_check(
        calc_method := 2, -- Здесь укажите нужные значения для ваших параметров
        start_date := NULL,
        end_date := NULL,
        transaction_count := 10,
        increase_coeff := 3,
        max_churn_index := 1,
        max_discount_percentage := 2,
        max_margin_percentage := 0.3
    );



CREATE OR REPLACE FUNCTION form_offer_for_average_check(
    IN calc_method INT,
    IN start_date DATE,
    IN end_date DATE,
    IN transaction_count INT,
    IN increase_coeff DECIMAL(10,2),
    IN max_churn_index DECIMAL(10,2),
    IN max_discount_percentage DECIMAL(10,2),
    IN max_margin_percentage DECIMAL(10,2)
)
    RETURNS TABLE (
                      customer_id INT,
                      required_check_measure DECIMAL(10,2),
                      group_name VARCHAR(255),
                      offer_discount_depth DECIMAL(10,2)
                  ) AS $$
DECLARE
    curr_avg_check DECIMAL(10,2);
    target_avg_check DECIMAL(10,2);
BEGIN

    CREATE TEMP TABLE temp_result AS (
        SELECT
            c.customer_id,
            0 AS required_check_measure,

            '' AS group_name,
            0 AS group_minimum_discount,
            0 AS group_margin,
            0 AS group_id,

            0 AS offer_discount_depth
        FROM cards c
    );

    CREATE INDEX idx_temp_result_cust_id ON temp_result(customer_id);

    IF calc_method = 1 THEN

        UPDATE temp_result
        SET
            curr_avg_check = t.current_avg_check,
            required_check_measure = t.required_check_measure
        FROM (
                 SELECT
                     c.customer_id,
                     AVG(t.transaction_summ) AS current_avg_check,
                     ROUND(AVG(t.transaction_summ) * increase_coeff, 2) AS required_check_measure
                 FROM cards c
                          JOIN transactions t ON t.customer_card_id = c.customer_card_id
                 WHERE t.transaction_datetime BETWEEN start_date AND end_date
                 GROUP BY c.customer_id
             ) t
        WHERE t.customer_id = temp_result.customer_id;

    ELSIF calc_method = 2 THEN

        UPDATE temp_result
        SET
            curr_avg_check = t.current_avg_check,
            required_check_measure = t.required_check_measure
        FROM (
                 SELECT
                     c.customer_id,
                     AVG(t.transaction_summ) AS current_avg_check,
                     ROUND(AVG(t.transaction_summ) * increase_coeff, 2) AS required_check_measure
                 FROM (
                          SELECT c.customer_id, t.transaction_summ
                          FROM cards c
                                   JOIN transactions t ON t.customer_card_id = c.customer_card_id
                          ORDER BY t.transaction_id DESC
                          LIMIT transaction_count
                      ) c
                 GROUP BY c.customer_id
             ) t
        WHERE t.customer_id = temp_result.customer_id;

    END IF;

    UPDATE temp_result tr
    SET
        group_name = g.group_name,
        group_minimum_discount = g.group_minimum_discount,
        group_margin = g.group_margin,
        group_id = g.group_id
    FROM groups g
    WHERE g.customer_id = tr.customer_id
      AND g.group_churn_rate <= max_churn_index
      AND g.group_discount_share <= max_discount_percentage
        ORDER BY tr.customer_id, g.group_affinity_index DESC
    LIMIT 1;

    UPDATE temp_result
    SET offer_discount_depth = ceil(
                                           least(
                                                   temp_result.group_minimum_discount,
                                                   max_margin_percentage / 100 * temp_result.group_margin
                                               ) / 5
                                   ) * 5;

    DELETE FROM temp_result
    WHERE offer_discount_depth IS NULL;

    UPDATE temp_result tr
    SET group_name = g.group_name
    FROM (
             SELECT customer_id, group_name
             FROM groups g
                      JOIN periods p ON p.group_id = g.group_id
             WHERE p.group_min_discount <= max_discount_percentage
             ORDER BY customer_id, p.group_frequency DESC
             LIMIT 1
         ) g
    WHERE g.customer_id = tr.customer_id;

    RETURN QUERY SELECT * FROM temp_result;

END;
$$ LANGUAGE plpgsql;