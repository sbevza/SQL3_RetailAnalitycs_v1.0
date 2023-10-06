CREATE OR REPLACE FUNCTION form_offer_for_average_check(
    IN calc_method INT,
    IN start_date DATE,
    IN end_date DATE,
    IN transaction_count INT,
    IN increase_coeff DECIMAL,
    IN max_churn_index DECIMAL,
    IN max_discount_percentage DECIMAL,
    IN max_margin_percentage DECIMAL
)
    RETURNS TABLE
            (
                Customer_ID            INT,
                Required_Check_Measure DECIMAL,
                Group_Name             VARCHAR(255),
                Offer_Discount_Depth   DECIMAL
            )
AS
$$
BEGIN
    CREATE TEMP TABLE temp_result_table
    (
        Customer_ID            INT,
        Required_Check_Measure DECIMAL,
        Group_Name             VARCHAR(255),
        Offer_Discount_Depth   DECIMAL
    ) ON COMMIT DROP;

    IF calc_method = 1 THEN
        -- Расчет по периоду и вставка данных во временную таблицу
        INSERT INTO temp_result_table (Customer_ID, Required_Check_Measure, Group_Name)
        SELECT c.customer_id,
               round(SUM(t.transaction_summ) / COUNT(t.transaction_id) * increase_coeff, 2),
               NULL::VARCHAR(255)
        FROM cards c
                 JOIN transactions t ON t.customer_card_id = c.customer_card_id
        WHERE t.transaction_datetime BETWEEN start_date AND end_date
        GROUP BY c.customer_id;
    ELSIF calc_method = 2 THEN
        -- Расчет по количеству транзакций и вставка данных во временную таблицу
        INSERT INTO temp_result_table (Customer_ID, Required_Check_Measure, Group_Name)
        SELECT c.customer_id,
               round(SUM(t.transaction_summ) / COUNT(t.transaction_id) * 1.15, 2),
               NULL::VARCHAR(255)
        FROM cards c
                 JOIN transactions t ON t.customer_card_id = c.customer_card_id
        GROUP BY c.customer_id
        ORDER BY MAX(t.transaction_id) DESC
        LIMIT transaction_count;
    END IF;

    -- Определяем Group_Name для каждого Customer_ID в таблице temp_result_table
    UPDATE temp_result_table AS trt
    SET Group_Name = (
        SELECT DISTINCT ON (pd.customer_id) g.group_name
        FROM Periods pd
                 JOIN groups_sku g ON pd.group_id = g.group_id
                 JOIN groups gv ON gv.customer_id = trt.customer_id AND gv.group_id = g.group_id
        WHERE pd.customer_id = trt.Customer_ID
          AND gv.Group_Churn_Rate <= max_churn_index
          AND gv.Group_Discount_Share <= max_discount_percentage
          AND ((trt.Required_Check_Measure/increase_coeff) * max_margin_percentage)  > CEIL(gv.group_minimum_discount * 100 / 5) * 5
        ORDER BY pd.customer_id, pd.group_frequency DESC
    )
    WHERE EXISTS (
        SELECT 1
        FROM Periods pd
                 JOIN Customers cu ON pd.customer_id = cu.customer_id
        WHERE pd.customer_id = trt.Customer_ID
    );

    UPDATE temp_result_table AS trt
    SET Offer_Discount_Depth = CEIL(g.group_minimum_discount * 100 / 5) * 5
    FROM groups g
             JOIN groups_sku gs ON g.group_id = gs.group_id
    WHERE gs.group_name = trt.Group_Name
    AND trt.Customer_ID = G.customer_id;


    RETURN QUERY
        SELECT * FROM temp_result_table;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM form_offer_for_average_check(
        calc_method := 2, -- Здесь укажите нужные значения для ваших параметров
        start_date := NULL,
        end_date := NULL,
        transaction_count := 100,
        increase_coeff := 1.15,
        max_churn_index := 3,
        max_discount_percentage := 70,
        max_margin_percentage := 30
    );


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






-- Функция формирования предложения для повышения среднего чека
SELECT c.customer_id,
       p.group_id,
       SUM(t.transaction_summ) / COUNT(t.transaction_id) as average,
       (SELECT group_margin FROM calculate_average_margin(2, 0,100) WHERE customer_id = c.customer_id and group_id = p.group_id) * 0.3 as p5,
       p.group_min_discount * 100 as p6
FROM cards c
         JOIN transactions t ON t.customer_card_id = c.customer_card_id
         JOIN periods p ON c.customer_id = p.customer_id
GROUP BY c.customer_id, p.group_id, p.group_min_discount, p.group_frequency
ORDER BY p.group_frequency DESC
LIMIT 100;


CREATE OR REPLACE FUNCTION calculate_average_margin(
    p_mode INT,
    p_period_days INT DEFAULT 0,
    p_transactions_count INT DEFAULT 0,
    start_date DATE DEFAULT NULL,
    end_date DATE DEFAULT NULL
)
    RETURNS TABLE (
                      customer_id INT,
                      group_id INT,
                      group_margin NUMERIC
                  ) AS $$
BEGIN
    CASE
        WHEN p_mode = 0 THEN
            -- Режим 0: Выборка по всем данным
            RETURN QUERY (
                SELECT
                    ph.customer_id,
                    ph.group_id,
                    SUM(group_summ_paid - group_cost)::numeric AS group_margin
                FROM purchase_history ph
                GROUP BY ph.customer_id, ph.group_id
                ORDER BY ph.customer_id, ph.group_id
            );

        WHEN p_mode = 1 THEN
            -- Режим 1: Выборка за последние p_period_days дней
            RETURN QUERY (
                SELECT
                    ph.customer_id,
                    ph.group_id,
                    SUM(group_summ_paid - group_cost)::numeric AS group_margin
                FROM purchase_history ph, analysis_date ad
                WHERE transaction_datetime <= ad.analysis_formation - INTERVAL '1 day' * p_period_days
                GROUP BY ph.customer_id, ph.group_id
                ORDER BY ph.customer_id, ph.group_id
            );

        WHEN p_mode = 2 THEN
            -- Режим 2: Выборка последних p_transactions_count транзакций
            RETURN QUERY (
                SELECT
                    ph.customer_id,
                    ph.group_id,
                    SUM(group_summ_paid - group_cost)::numeric AS group_margin
                FROM purchase_history ph
                GROUP BY ph.customer_id, ph.group_id
                ORDER BY MAX(transaction_id) DESC
                LIMIT p_transactions_count
            );

        WHEN p_mode = 3 THEN
            -- Режим 3: Выборка за период с start_date по end_date
            RETURN QUERY (
                SELECT
                    ph.customer_id,
                    ph.group_id,
                    SUM(group_summ_paid - group_cost)::numeric AS group_margin
                FROM purchase_history ph
                WHERE transaction_datetime BETWEEN start_date AND end_date
                GROUP BY ph.customer_id, ph.group_id
                ORDER BY ph.customer_id, ph.group_id
            );

        ELSE
            -- Неизвестный режим, вернуть NULL или другое значение по умолчанию
            RETURN QUERY (
                SELECT NULL::INT, NULL::INT, NULL::NUMERIC
            );
        END CASE;

END;
$$ LANGUAGE plpgsql;





-- Выборка всех данных
SELECT * FROM calculate_average_margin(0);

-- Выборка данных за последние 7 дней
SELECT * FROM calculate_average_margin(1, 100);

-- Выборка последних 10 транзакций
SELECT * FROM calculate_average_margin(2, 0, 100);

-- Выборка данных за период с 2023-01-01 по 2023-02-01
SELECT * FROM calculate_average_margin(3, 0, 0, '2021-07-01', '2023-02-01');


SELECT customer_id,
       group_id,
       SUM(group_summ_paid - group_cost)::numeric AS group_margin
FROM purchase_history
GROUP BY customer_id, group_id
ORDER BY customer_id, group_id-- Вызов функции с режимом 0 (все данные)

