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
    -- Общая часть запроса
    RETURN QUERY (WITH aver_mar_calc
                           AS (SELECT c.customer_id,
                                      c.group_id,
                                      gs.group_name,
                                      c.group_margin
                               FROM calculate_average_margin(
                                            CASE
                                                WHEN calc_method = 1 THEN 5
                                                WHEN calc_method = 2 THEN 4
                                                ELSE 0
                                                END,
                                            0,
                                            CASE
                                                WHEN calc_method = 2 THEN transaction_count
                                                ELSE 0
                                                END,
                                            start_date,
                                            end_date
                                        ) c
                                        LEFT JOIN groups_sku gs ON gs.group_id = c.group_id
                                        LEFT JOIN groups g on g.customer_id = c.customer_id and g.group_id = c.group_id
                               WHERE group_churn_rate < max_churn_index
                                 AND group_discount_share < (max_discount_percentage / 100)
                               ORDER BY c.customer_id, gs.group_name),

                       Required_Check AS (SELECT cards.customer_id,
                                                 ROUND(SUM(t.transaction_summ) / COUNT(t.transaction_id) *
                                                       increase_coeff, 2) AS Required_Check_Measure
                                          FROM cards
                                                   JOIN transactions t ON cards.customer_card_id = t.customer_card_id
--                                           WHERE
--                                               (calc_method = 1 AND t.transaction_datetime BETWEEN start_date AND end_date) OR
--                                               (calc_method = 2 AND t.transaction_id >= (SELECT MAX(transaction_id) - transaction_count FROM transactions))
                                          GROUP BY cards.customer_id),

                       RankedGroups AS (SELECT p1.customer_id,
                                               rc.Required_Check_Measure,
                                               p1.group_name,
                                               CEIL(g.group_minimum_discount / 0.05) * 5                          AS Offer_Discount_Depth,
                                               ROW_NUMBER()
                                               OVER (PARTITION BY p1.customer_id ORDER BY p.group_frequency DESC) AS rn
                                        FROM aver_mar_calc p1
                                                 LEFT JOIN groups g ON g.customer_id = p1.customer_id AND g.group_id = p1.group_id
                                                 LEFT JOIN periods p ON p.customer_id = p1.customer_id AND p.group_id = p1.group_id
                                                 LEFT JOIN Required_Check rc ON rc.customer_id = p1.customer_id
                                        WHERE CEIL(g.group_minimum_discount / 0.05) * 5 <=
                                              p1.group_margin * (max_margin_percentage / 100)
                                        ORDER BY customer_id, p.group_frequency DESC)
                  SELECT rg.customer_id,
                         rg.Required_Check_Measure,
                         rg.group_name,
                         rg.Offer_Discount_Depth
                  FROM RankedGroups rg
                  WHERE rn = 1
                  ORDER BY customer_id);
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
        start_date := '2021-01-01',
        end_date := '2022-07-31',
        transaction_count := 0,
        increase_coeff := 1.15,
        max_churn_index := 3,
        max_discount_percentage := 70,
        max_margin_percentage := 30
    );

