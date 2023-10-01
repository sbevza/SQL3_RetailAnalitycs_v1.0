CREATE OR REPLACE FUNCTION form_offer_for_visit_frequency(
    p_start_date timestamp without time zone,
    p_end_date timestamp without time zone,
    p_added_transactions numeric,
    p_max_churn numeric,
    p_max_discount numeric,
    p_max_margin numeric
)
    RETURNS TABLE (
                      Customer_ID INT,
                      Start_Date timestamp without time zone,
                      End_Date timestamp without time zone,
                      Required_Transactions_Count numeric,
                      Group_Name varchar(255),
                      Offer_Discount_Depth numeric
                  )
AS $$
DECLARE
    selected_group varchar(255);
BEGIN
    -- Расчет Group_Demand_Index
    SELECT INTO selected_group
        g.Group_Name
    FROM Groups_SKU g
             JOIN (
        SELECT s.Group_ID
        FROM SKU s
        WHERE s.SKU_ID IN (
            SELECT c.SKU_ID
            FROM Checks c
                     JOIN SKU s ON c.SKU_ID = s.SKU_ID
            GROUP BY s.Group_ID, c.SKU_ID
            HAVING COUNT(c.SKU_ID) >= ALL (
                SELECT COUNT(c2.SKU_ID)
                FROM Checks c2
                WHERE c2.SKU_ID = c.SKU_ID
                GROUP BY c2.SKU_ID
            )
        )
    ) AS top_group ON g.Group_ID = top_group.Group_ID
    LIMIT 1;

    RETURN QUERY
        SELECT
            c.Customer_ID,
            p_start_date AS Start_Date,
            p_end_date AS End_Date,
            p_added_transactions AS Required_Transactions_Count,
            selected_group AS Group_Name,
            p_max_margin AS Offer_Discount_Depth
        FROM Personal_Data c
                 JOIN Cards card ON c.Customer_ID = card.Customer_ID
                 LEFT JOIN (
            SELECT
                t.Customer_Card_ID,
                COUNT(DISTINCT t.Transaction_ID) /
                DATE_PART('day', p_end_date - p_start_date) AS Customer_Frequency
            FROM Transactions t
            WHERE t.Transaction_DateTime >= p_start_date
              AND t.Transaction_DateTime <= p_end_date
            GROUP BY t.Customer_Card_ID
        ) AS freq ON card.Customer_Card_ID = freq.Customer_Card_ID
        WHERE freq.Customer_Frequency IS NULL OR freq.Customer_Frequency <= p_max_churn;

    RETURN;
END;
$$ LANGUAGE plpgsql;



SELECT *
FROM form_offer_for_visit_frequency(
        '2023-09-01'::TIMESTAMP, -- Начальная дата периода
        '2023-09-30'::TIMESTAMP, -- Конечная дата периода
        3,                      -- Добавляемое число транзакций
        10,                    -- Максимальный индекс оттока
        15,                    -- Максимальная доля транзакций со скидкой
        0.4                     -- Допустимая доля маржи
    );

SELECT *
FROM form_offer_for_visit_frequency(
        '2018-09-01'::TIMESTAMP, -- Начальная дата периода
        '2023-09-30'::TIMESTAMP, -- Конечная дата периода
        2,                      -- Добавляемое число транзакций (просто для начала)
        50,                    -- Максимальный индекс оттока (пусть будет выше)
        14,                    -- Максимальная доля транзакций со скидкой (пусть будет выше)
        30                    -- Допустимая доля маржи (пусть будет выше)
    );
