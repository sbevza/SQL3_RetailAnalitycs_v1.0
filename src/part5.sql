CREATE OR REPLACE FUNCTION generate_offers(
    p_start_date DATE,
    p_end_date DATE,
    added_transactions INT,
    max_churn_index NUMERIC,
    max_discount_share_percent NUMERIC,
    acceptable_margin_percent NUMERIC
)
    RETURNS TABLE
            (
                Customer_ID                 INT,
                Start_Date                  TIMESTAMP,
                End_Date                    TIMESTAMP,
                Required_Transactions_Count INT,
                Group_Name                  VARCHAR,
                Offer_Discount_Depth        NUMERIC
            )
AS
$$
BEGIN

    IF p_start_date > p_end_date THEN
        RAISE EXCEPTION 'ERROR: Start date have to less then end date';
    END IF;

    RETURN QUERY
        WITH Required_Transactions
                 AS (SELECT c.Customer_ID,
                            p_start_date::TIMESTAMP,
                            p_end_date::TIMESTAMP,
                            ROUND((p_end_date - p_start_date) / c.customer_frequency)::INTEGER
                                + added_transactions AS Required_Transactions_Count
                     FROM Customers c),

             Rewards
                 AS (SELECT g.customer_id,
                            g.group_id,
                            ceil(g.group_minimum_discount / 0.05) * 5                                          AS Offer_Discount_Depth,
                            row_number()
                            OVER (PARTITION BY g.customer_id, g.group_id ORDER BY g.group_affinity_index DESC) as rn
                     FROM groups g
                              LEFT JOIN purchase_history ph
                                        ON g.customer_id = ph.customer_id AND g.group_id = ph.group_id
                     WHERE g.group_churn_rate <= max_churn_index
                       AND g.group_discount_share * 100 < max_discount_share_percent
                     GROUP BY g.customer_id, g.group_id, ceil(g.group_minimum_discount / 0.05) * 5, g.group_affinity_index
                     HAVING (acceptable_margin_percent / 100) *
                            AVG((ph.group_summ_paid - ph.group_cost) / (ph.group_summ_paid / 100))
                                > ceil(g.group_minimum_discount / 0.05) * 5
                     ORDER BY g.customer_id, g.group_affinity_index DESC, rn DESC)

        SELECT rt.customer_id,
               rt.p_start_date AS Start_Date,
               rt.p_end_date   AS End_Date,
               rt.Required_Transactions_Count,
               gs.group_name,
               r.Offer_Discount_Depth
        FROM Required_Transactions rt
                 LEFT JOIN Rewards r ON rt.customer_id = r.customer_id
                    LEFT JOIN groups_sku gs ON r.group_id = gs.group_id
        WHERE r.rn = 1;
END;
$$ LANGUAGE plpgsql;


Select *
FROM generate_offers('2022-08-18', '2022-08-18', 1, 3, 70, 30);
