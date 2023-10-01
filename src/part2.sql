---------- Customers View ----------
CREATE VIEW Customers AS
WITH Customer_Average_Check
         AS (SELECT pd.customer_id,
                    ROUND(SUM(t.transaction_summ) / COUNT(t.transaction_id), 2) AS Customer_Average_Check
             FROM personal_data pd
                      JOIN public.cards c ON pd.customer_id = c.customer_id
                      JOIN public.transactions t ON c.customer_card_id = t.customer_card_id
             GROUP BY pd.customer_id
             ORDER BY Customer_Average_Check DESC),

     Customer_Frequency
         AS (SELECT pd.customer_id,
                    ROUND((MAX(t.transaction_datetime)::date - MIN(t.transaction_datetime)::date)::numeric
                              / COUNT(t.transaction_id), 2) AS Customer_Frequency
             FROM personal_data pd
                      JOIN public.cards c ON pd.customer_id = c.customer_id
                      JOIN public.transactions t ON c.customer_card_id = t.customer_card_id
             WHERE t.transaction_datetime <= (SELECT max(analysis_formation) FROM analysis_date)
             GROUP BY pd.customer_id
             ORDER BY Customer_Frequency),

     Churn_Probability
         AS (SELECT pd.customer_id,
                    ROUND(EXTRACT(EPOCH FROM ((SELECT MAX(analysis_formation) FROM analysis_date)
                        - MAX(t.transaction_datetime))) / 86400, 2)                          AS Customer_Inactive_Period,
                    ROUND((EXTRACT(EPOCH FROM ((SELECT MAX(analysis_formation) FROM analysis_date)
                        - MAX(t.transaction_datetime))) / 86400) / cf.Customer_Frequency, 2) AS Customer_Churn_Rate
             FROM personal_data pd
                      JOIN public.cards c ON pd.customer_id = c.customer_id
                      JOIN public.transactions t ON c.customer_card_id = t.customer_card_id
                      JOIN Customer_Frequency cf ON pd.customer_id = cf.customer_id
             WHERE t.transaction_datetime <= (SELECT MAX(analysis_formation) FROM analysis_date)
             GROUP BY pd.customer_id, cf.Customer_Frequency),

     Stors_Visits
         AS (SELECT customer_id,
                    transaction_store_id,
                    last_visit,
                    COUNT(*)::numeric / totat_transaction AS transactions_part
             FROM (SELECT pd.customer_id,
                          t.transaction_store_id,
                          MAX(t.transaction_datetime)
                          OVER (PARTITION BY pd.customer_id, t.transaction_store_id)      AS last_visit,
                          SUM(COUNT(t.transaction_id)) OVER (PARTITION BY pd.customer_id) AS totat_transaction
                   FROM personal_data pd
                            JOIN public.cards c ON pd.customer_id = c.customer_id
                            JOIN public.transactions t ON c.customer_card_id = t.customer_card_id
                   WHERE t.transaction_datetime <= (SELECT MAX(analysis_formation) FROM analysis_date)
                   GROUP BY pd.customer_id, t.transaction_store_id, t.transaction_datetime) s
             GROUP BY customer_id, transaction_store_id, last_visit, totat_transaction
             ORDER BY customer_id, transactions_part DESC, last_visit DESC),

     Last_Three_Transactions
         AS (SELECT *
             FROM (SELECT pd.customer_id,
                          t.transaction_store_id,
                          t.transaction_datetime,
                          ROW_NUMBER()
                          OVER (PARTITION BY pd.customer_id ORDER BY t.transaction_datetime DESC) AS rn
                   FROM personal_data pd
                            JOIN public.cards c ON pd.customer_id = c.customer_id
                            JOIN public.transactions t ON c.customer_card_id = t.customer_card_id
                   WHERE t.transaction_datetime <= (SELECT MAX(analysis_formation) FROM analysis_date)) s
             WHERE rn <= 3),

     Primary_Store
         AS (SELECT ltt.customer_id,
                    CASE
                        WHEN COUNT(DISTINCT ltt.transaction_store_id) = 1 THEN
                            MAX(ltt.transaction_store_id)
                        ELSE
                            (SELECT sv.transaction_store_id
                             FROM Stors_Visits sv
                             WHERE sv.customer_id = ltt.customer_id
                             LIMIT 1)
                        END AS primary_store
             FROM Last_Three_Transactions ltt
             GROUP BY ltt.customer_id),

     Customer_Unassignment
         AS (SELECT ca.customer_id,
                    ca.Customer_Average_Check,
                    CASE
                        WHEN RANK() OVER (ORDER BY Customer_Average_Check DESC) <=
                             CEIL(0.1 * (SELECT COUNT(*) FROM Customer_Average_Check)) THEN 'High'
                        WHEN RANK() OVER (ORDER BY Customer_Average_Check DESC) <=
                             CEIL(0.35 * (SELECT COUNT(*) FROM Customer_Average_Check)) THEN 'Medium'
                        ELSE 'Low'
                        END AS Customer_Average_Check_Segment,
                    cf.Customer_Frequency,
                    CASE
                        WHEN RANK() OVER (ORDER BY cf.Customer_Frequency) <=
                             CEIL(0.1 * (SELECT COUNT(*) FROM Customer_Frequency)) THEN 'Often'
                        WHEN RANK() OVER (ORDER BY cf.Customer_Frequency) <=
                             CEIL(0.35 * (SELECT COUNT(*) FROM Customer_Frequency)) THEN 'Occasionally'
                        ELSE 'Rarely'
                        END
                            AS Customer_Frequency_Segment,
                    cp.Customer_Inactive_Period,
                    cp.Customer_Churn_Rate,
                    CASE
                        WHEN cp.Customer_Churn_Rate < 2 THEN 'Low'
                        WHEN cp.Customer_Churn_Rate < 5 THEN 'Medium'
                        ELSE 'High'
                        END AS Customer_Churn_Segment,
                    ps.primary_store
             FROM Customer_Average_Check ca
                      JOIN Customer_Frequency cf ON ca.customer_id = cf.customer_id
                      JOIN Churn_Probability cp ON cp.customer_id = ca.customer_id
                      JOIN Primary_Store ps ON ca.customer_id = ps.customer_id
             GROUP BY ca.customer_id,
                      ca.Customer_Average_Check,
                      cf.Customer_Frequency,
                      cp.Customer_Inactive_Period,
                      cp.Customer_Churn_Rate,
                      ps.primary_store)

SELECT customer_id,
       Customer_Average_Check,
       Customer_Average_Check_Segment,
       Customer_Frequency,
       Customer_Frequency_Segment,
       Customer_Inactive_Period,
       Customer_Churn_Rate,
       Customer_Churn_Segment,
       CASE Customer_Average_Check_Segment
           WHEN 'Low' THEN 0
           WHEN 'Medium' THEN 9
           ELSE 18 END
           +
       CASE Customer_Frequency_Segment
           WHEN 'Rarely' THEN 0
           WHEN 'Occasionally' THEN 3
           ELSE 6 END
           +
       CASE Customer_Churn_Segment
           WHEN 'Low' THEN 1
           WHEN 'Medium' THEN 2
           ELSE 3 END
           AS Customer_Segment,
       Primary_Store
FROM Customer_Unassignment;

---------- Groups View ----------
WITH demanded_groups
         AS (SELECT pd.customer_id,
                    gs.group_id
             FROM personal_data pd
                      JOIN public.cards c ON pd.customer_id = c.customer_id
                      JOIN public.transactions t ON c.customer_card_id = t.customer_card_id
                      JOIN public.checks ch on t.transaction_id = ch.transaction_id
                      JOIN public.sku s on ch.sku_id = s.sku_id
                      JOIN public.groups_sku gs on s.group_id = gs.group_id
             WHERE t.transaction_datetime <= (SELECT max(analysis_formation) FROM analysis_date)
             GROUP BY pd.customer_id, gs.group_id
             ORDER BY pd.customer_id)

SELECT dg.customer_id,
       group_id
FROM demanded_groups dg;

---------- Periods View ----------
CREATE VIEW Periods AS
WITH CommonData AS (SELECT pd.customer_id,
                           gs.group_id,
                           t.transaction_id,
                           t.transaction_datetime,
                           ch.SKU_Discount / ch.SKU_Summ AS SKU_Discount_Ratio
                    FROM personal_data pd
                             JOIN public.cards c ON pd.customer_id = c.customer_id
                             JOIN public.transactions t ON c.customer_card_id = t.customer_card_id
                             JOIN public.checks ch ON t.transaction_id = ch.transaction_id
                             JOIN public.sku s ON ch.sku_id = s.sku_id
                             JOIN public.groups_sku gs ON s.group_id = gs.group_id
                    WHERE t.transaction_datetime <= (SELECT max(analysis_formation) FROM analysis_date)),
     GroupPurchaseInfo AS (SELECT customer_id,
                                  group_id,
                                  MIN(transaction_datetime)         AS First_Group_Purchase_Date,
                                  MAX(transaction_datetime)         AS Last_Group_Purchase_Date,
                                  COUNT(*)                          AS Group_Purchase,
                                  ROUND(MIN(SKU_Discount_Ratio), 2) AS Group_Min_Discount
                           FROM CommonData
                           GROUP BY customer_id, group_id)

SELECT gd.customer_id,
       gd.group_id,
       gpi.First_Group_Purchase_Date,
       gpi.Last_Group_Purchase_Date,
       gpi.Group_Purchase,
       (gpi.Last_Group_Purchase_Date::date - gpi.First_Group_Purchase_Date::date + 1) /
       gpi.Group_Purchase AS Group_Frequency,
       gpi.Group_Min_Discount
FROM CommonData gd
         JOIN GroupPurchaseInfo gpi ON gd.customer_id = gpi.customer_id AND gd.group_id = gpi.group_id
GROUP BY gd.customer_id, gd.group_id, gpi.First_Group_Purchase_Date, gpi.Last_Group_Purchase_Date,
         gpi.Group_Purchase, gpi.Group_Min_Discount
ORDER BY gd.customer_id, gd.group_id;


-------------------

CREATE VIEW CustomerGroupStatistics AS
WITH PurchaseIntervals AS (SELECT c.Customer_ID,
                                  s.Group_ID,
                                  EXTRACT(EPOCH FROM MAX(t.Transaction_DateTime) - MIN(t.Transaction_DateTime)) /
                                  86400 AS PurchaseInterval
                           FROM Cards c
                                    INNER JOIN Transactions t ON c.Customer_Card_ID = t.Customer_Card_ID
                                    INNER JOIN Checks ch ON t.Transaction_ID = ch.Transaction_ID
                                    INNER JOIN SKU s ON ch.SKU_ID = s.SKU_ID
                           GROUP BY c.Customer_ID, s.Group_ID),
     GroupStability AS (SELECT Customer_ID,
                               Group_ID,
                               round(AVG(PurchaseInterval), 2) AS Group_Stability_Index
                        FROM PurchaseIntervals
                        GROUP BY Customer_ID, Group_ID),
     GroupMargin AS (SELECT c.Customer_ID,
                            ch.SKU_ID                                                                        AS Group_ID,
                            round(SUM(ch.SKU_Summ_Paid - ch.SKU_Summ) / COUNT(DISTINCT t.Transaction_ID),
                                  2)                                                                         AS Group_Margin
                     FROM Transactions t
                              INNER JOIN Checks ch ON t.Transaction_ID = ch.Transaction_ID
                              INNER JOIN Cards c ON t.Customer_Card_ID = c.Customer_Card_ID
                     GROUP BY c.Customer_ID, ch.SKU_ID),
     GroupDiscountAnalysis AS (SELECT c.Customer_ID,
                                      ch.SKU_ID                                                             AS Group_ID,
                                      COUNT(DISTINCT ch.Transaction_ID)                                     AS DiscountedTransactions,
                                      NULLIF(SUM(CASE WHEN ch.SKU_Discount > 0 THEN 1 ELSE 0 END), 0) /
                                      NULLIF(COUNT(DISTINCT ch.Transaction_ID), 0)                          AS Group_Discount_Share,
                                      MIN(CASE WHEN ch.SKU_Discount > 0 THEN ch.SKU_Discount ELSE NULL END) AS Group_Minimum_Discount,
                                      SUM(CASE WHEN ch.SKU_Discount > 0 THEN ch.SKU_Discount ELSE 0 END) /
                                      NULLIF(SUM(CASE WHEN ch.SKU_Discount > 0 THEN 1 ELSE 0 END), 0)       AS Group_Average_Discount
                               FROM Checks ch
                                        INNER JOIN Transactions t ON ch.Transaction_ID = t.Transaction_ID
                                        INNER JOIN Cards c ON t.Customer_Card_ID = c.Customer_Card_ID
                               GROUP BY c.Customer_ID, ch.SKU_ID)

SELECT gs.Customer_ID,
       gs.Group_ID,
       gs.Group_Stability_Index,
       gm.Group_Margin,
       gda.DiscountedTransactions,
       gda.Group_Discount_Share,
       gda.Group_Minimum_Discount,
       gda.Group_Average_Discount
FROM GroupStability gs
         INNER JOIN GroupMargin gm ON gs.Customer_ID = gm.Customer_ID AND gs.Group_ID = gm.Group_ID
         INNER JOIN GroupDiscountAnalysis gda ON gs.Customer_ID = gda.Customer_ID AND gs.Group_ID = gda.Group_ID;


CREATE VIEW purchase_history AS
with CTE1 as (SELECT c.Customer_ID,
                     t.transaction_id,
                     t.transaction_datetime,
                     s.sku_id                              as S,
                     gs.group_name,
                     st.sku_purchase_price * ch.sku_amount as Group_Cost
              from cards c
                       join transactions t on t.customer_card_id = c.customer_id
                       join checks ch on ch.transaction_id = t.transaction_id
                       join stores st on ch.sku_id = st.sku_id
                       join sku s on ch.sku_id = s.sku_id
                       join groups_sku gs on s.group_id = gs.group_id
--               group by t.transaction_id, gs.group_name, c.customer_id
              order by t.transaction_id)
select ch.sku_id
from stores st,
     checks ch
         join CTE1 on CTE1.S = ch.sku_id

where ch.sku_id = st.sku_id
group by ch.transaction_id, ch.sku_id, st.sku_purchase_price, ch.sku_amount


--     Group_Cost,
--     Group_Summ,
--     Group_Summ_Paid



CREATE VIEW purchase_history AS
WITH CTE1 AS (
    SELECT
        c.Customer_ID,
        t.transaction_id,
        t.transaction_datetime,
        ch.sku_id AS SKU_ID,
        gs.group_name,
        st.sku_purchase_price * ch.sku_amount AS Group_Cost
    FROM
        cards c
            JOIN
        transactions t ON t.customer_card_id = c.customer_id
            JOIN
        checks ch ON ch.transaction_id = t.transaction_id
            JOIN
        stores st ON ch.sku_id = st.sku_id
            JOIN
        sku s ON ch.sku_id = s.sku_id
            JOIN
        groups_sku gs ON s.group_id = gs.group_id
    ORDER BY
        t.transaction_id
)
SELECT
    CTE1.Customer_ID,
    CTE1.transaction_id AS Transaction_ID,
    CTE1.transaction_datetime AS Transaction_DateTime,
    CTE1.SKU_ID,
    CTE1.group_name AS Group_Name,
    SUM(CTE1.Group_Cost) AS Group_Cost,
    SUM(ch.sku_summ) AS Group_Summ,
    SUM(ch.sku_summ_paid) AS Group_Summ_Paid
FROM
    CTE1
        JOIN
    checks ch ON CTE1.SKU_ID = ch.sku_id
        JOIN
    stores st ON ch.sku_id = st.sku_id
GROUP BY
    CTE1.Customer_ID,
    CTE1.transaction_id,
    CTE1.transaction_datetime,
    CTE1.SKU_ID,
    CTE1.group_name;

