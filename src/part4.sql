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
               SUM(t.transaction_summ) / COUNT(t.transaction_id) * increase_coeff,
               NULL::VARCHAR(255)
        FROM cards c
                 JOIN transactions t ON t.customer_card_id = c.customer_card_id
        WHERE t.transaction_datetime BETWEEN start_date AND end_date
        GROUP BY c.customer_id;
    ELSIF calc_method = 2 THEN
        -- Расчет по количеству транзакций и вставка данных во временную таблицу
        INSERT INTO temp_result_table (Customer_ID, Required_Check_Measure, Group_Name)
        SELECT c.customer_id,
               SUM(t.transaction_summ) / COUNT(t.transaction_id) * 1.15,
               NULL::VARCHAR(255)
        FROM cards c
                 JOIN transactions t ON t.customer_card_id = c.customer_card_id
        GROUP BY c.customer_id
        ORDER BY MAX(t.transaction_id) DESC
        LIMIT transaction_count;
    END IF;

    -- Определяем Group_Name для каждого Customer_ID в таблице temp_result_table
    UPDATE temp_result_table AS trt
    SET Group_Name = (SELECT DISTINCT ON (c.customer_id) gs.group_name
                      FROM cards c

                               JOIN transactions t ON t.customer_card_id = c.customer_card_id
                               JOIN groups g ON c.customer_id = g.customer_id AND g.group_id = group_id
                               JOIN periods p ON g.customer_id = p.customer_id AND g.group_id = p.group_id
                               JOIN groups_sku gs on gs.group_id = p.group_id
                      where g.group_churn_rate < 3
                        and g.group_discount_share <= 0.7

                      ORDER BY c.customer_id, p.group_frequency DESC)
    WHERE EXISTS (SELECT 1
                  FROM Periods pd
                           JOIN Customers cu ON pd.customer_id = cu.customer_id
                  WHERE pd.customer_id = trt.Customer_ID);

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


-- SELECT *
-- FROM form_offer_for_average_check(
--         calc_method := 1,
--         start_date := '2021-07-01',
--         end_date := '2023-12-31',
--         transaction_count := 1,
--         increase_coeff := 1.5,
--         max_churn_index := 2,
--         max_discount_percentage := 2,
--         max_margin_percentage := 0.3
--     );

-- формирование предложения
SELECT c.customer_id,
       SUM(t.transaction_summ) / COUNT(t.transaction_id) * 1.15 AS "целевой чек",
       g.group_id,
       0.3 *  (SELECT group_margin FROM calculate_average_margin(2, 0, 100)
               WHERE customer_id = c.customer_id AND group_id = g.group_id) as p5,
       CEIL(g.group_minimum_discount * 1.05 / 0.5) * 5 as p6,
       p.group_frequency,
       g.group_churn_rate,
       g.group_discount_share,
       g.group_minimum_discount

FROM cards c
         join groups_sku gs on gs.group_id = group_id
         join checks on customer_id = c.customer_id
         JOIN transactions t ON t.customer_card_id = c.customer_card_id

         JOIN groups g ON c.customer_id = g.customer_id AND g.group_id = gs.group_id
         JOIN periods p ON g.customer_id = p.customer_id AND g.group_id = p.group_id
where g.group_churn_rate < 3
and g.group_discount_share < 0.7

GROUP BY c.customer_id, g.group_id, g.group_discount_share, g.group_churn_rate, p.group_frequency, g.group_minimum_discount
ORDER BY c.customer_id, p.group_frequency DESC
LIMIT 100;

-- -- Формирование маржи по группам
-- SELECT customer_id,
--        group_id,
--        SUM(group_summ_paid - group_cost) AS group_margin
-- FROM purchase_history
-- GROUP BY customer_id, group_id
-- ORDER BY customer_id, group_id;
--
--
-- -- Выборка всех данных
-- SELECT *
-- FROM calculate_average_margin(0);
--
-- -- Выборка данных за последние 7 дней
-- SELECT *
-- FROM calculate_average_margin(1, 100);
--
-- -- Выборка последних 10 транзакций
-- SELECT *
-- FROM calculate_average_margin(2, 0, 100);
--
-- -- Выборка данных за период с 2023-01-01 по 2023-02-01
-- SELECT *
-- FROM calculate_average_margin(3, 0, 0, '2021-07-01', '2023-02-01');
