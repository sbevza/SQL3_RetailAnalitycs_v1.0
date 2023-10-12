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
    IF calc_method = 1 THEN

    ELSIF calc_method = 2 THEN
        -- Расчет по количеству транзакций и вставка данных во временную таблицу
        return query (
            with part1 as (SELECT c.customer_id,
                                  c.group_id,
                                  gs.group_name,
                                  c.group_margin

                           FROM calculate_average_margin(4, 0, transaction_count) c
                                    left join groups_sku gs on gs.group_id = c.group_id

                           order by c.customer_id, gs.group_name),

                 groups as (select *

                            from groups
                            where group_churn_rate < max_churn_index
                              and group_discount_share < (max_discount_percentage/100)),
                 Required_Check as (select cards.customer_id,
                                           round(SUM(t.transaction_summ) / COUNT(t.transaction_id) * increase_coeff, 2) as Required_Check_Measure
                                    from cards
                                             join transactions t on cards.customer_card_id = t.customer_card_id
                                    group by cards.customer_id),

                 RankedGroups AS (select p1.customer_id,
                                         rc.Required_Check_Measure,
                                         p1.group_name,
                                         ceil(g.group_minimum_discount / 0.05) * 5                                       as Offer_Discount_Depth,
                                         ROW_NUMBER() OVER (PARTITION BY p1.customer_id ORDER BY p.group_frequency DESC) AS rn


                                  from part1 p1
                                           left join groups g on g.customer_id = p1.customer_id and g.group_id = p1.group_id
                                           left join periods p on p.customer_id = p1.customer_id and p.group_id = p1.group_id
                                           left join Required_Check rc on rc.customer_id = p1.customer_id
                                  where ceil(g.group_minimum_discount / 0.05) * 5 <= p1.group_margin * (max_margin_percentage/100)
                                  order by customer_id, p.group_frequency desc)


            SELECT RankedGroups.customer_id,
                   RankedGroups.Required_Check_Measure,
                   RankedGroups.group_name,
                   RankedGroups.Offer_Discount_Depth
            FROM RankedGroups
            WHERE rn = 1
            ORDER BY customer_id);
    END IF;



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
       round(SUM(t.transaction_summ) / COUNT(t.transaction_id) * 1.15, 2) AS "целевой чек",
       g.group_id,
       gs.group_name,
       (SELECT group_margin
        FROM calculate_average_margin(4, 0, 0)
        WHERE customer_id = c.customer_id
          AND group_id = g.group_id) * 0.3                                as p5,
       ceil(g.group_minimum_discount / 0.05) * 5                          as p6,
       g.group_minimum_discount,
       p.group_frequency,
       g.group_churn_rate,
       g.group_discount_share
FROM cards c
         left join groups_sku gs on gs.group_id = group_id
         left join checks on customer_id = c.customer_id
         left JOIN transactions t ON t.customer_card_id = c.customer_card_id
         left JOIN groups g ON c.customer_id = g.customer_id AND g.group_id = gs.group_id and g.group_churn_rate < 3 and
                               g.group_discount_share <= 0.7 and g.group_minimum_discount != 0
         JOIN periods p ON g.customer_id = p.customer_id AND g.group_id = p.group_id
where ceil(g.group_minimum_discount / 0.05) * 5 < 0.3 * (SELECT group_margin
                                                         FROM calculate_average_margin(4, 0, 0)
                                                         WHERE customer_id = c.customer_id
                                                           AND group_id = gs.group_id)

GROUP BY c.customer_id, g.group_id, gs.group_name, g.group_discount_share, g.group_churn_rate, p.group_frequency,
         g.group_minimum_discount
ORDER BY c.customer_id, p.group_frequency DESC



with part1 as (SELECT c.customer_id,
                      c.group_id,
                      gs.group_name,
                      c.group_margin

               FROM calculate_average_margin(4, 0, 100) c
                        left join groups_sku gs on gs.group_id = c.group_id

               order by c.customer_id, gs.group_name),

     groups as (select *

                from groups
                where group_churn_rate < 3
                  and group_discount_share < 0.7),
     Required_Check as (select cards.customer_id,
                               round(SUM(t.transaction_summ) / COUNT(t.transaction_id) * 1.15, 2) as Required_Check_Measure
                        from cards
                                 join transactions t on cards.customer_card_id = t.customer_card_id
                        group by cards.customer_id),

     RankedGroups AS (select p1.customer_id,
                             rc.Required_Check_Measure,
                             p1.group_name,
                             ceil(g.group_minimum_discount / 0.05) * 5                                       as Offer_Discount_Depth,
                             ROW_NUMBER() OVER (PARTITION BY p1.customer_id ORDER BY p.group_frequency DESC) AS rn


                      from part1 p1
                               left join groups g on g.customer_id = p1.customer_id and g.group_id = p1.group_id
                               left join periods p on p.customer_id = p1.customer_id and p.group_id = p1.group_id
                               left join Required_Check rc on rc.customer_id = p1.customer_id
                      where ceil(g.group_minimum_discount / 0.05) * 5 <= p1.group_margin * 0.3
                      order by customer_id, p.group_frequency desc)


SELECT customer_id,
       Required_Check_Measure,
       group_name,
       Offer_Discount_Depth
FROM RankedGroups
WHERE rn = 1
ORDER BY customer_id;


-- Выборка всех данных
SELECT *
FROM calculate_average_margin(0);

-- Выборка данных за последние 7 дней
SELECT *
FROM calculate_average_margin(1, 100);

-- Выборка последних 10 транзакций
SELECT *
FROM calculate_average_margin(2, 0, 100);

-- Выборка данных за период с 2023-01-01 по 2023-02-01
SELECT *
FROM calculate_average_margin(3, 0, 0, '2021-07-01', '2023-02-01');







