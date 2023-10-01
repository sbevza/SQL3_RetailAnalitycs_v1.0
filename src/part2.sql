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




---------- Purchase history ----------
CREATE or replace VIEW Purchase_history AS
SELECT pd.customer_id,
       t.transaction_id,
       t.transaction_datetime,
       gs.group_id,
       SUM(st.sku_purchase_price * ch.sku_amount) AS Group_Cost,
       SUM(ch.sku_summ) AS Group_Summ,
       SUM(ch.sku_summ_paid) AS Group_Summ_Paid
FROM personal_data pd
         JOIN public.cards c ON pd.customer_id = c.customer_id
         JOIN public.transactions t ON c.customer_card_id = t.customer_card_id
         JOIN public.checks ch on t.transaction_id = ch.transaction_id
         JOIN public.sku s on ch.sku_id = s.sku_id
         JOIN public.groups_sku gs on s.group_id = gs.group_id
         JOIN stores st ON s.sku_id = st.sku_id AND st.transaction_store_id = t.transaction_store_id
WHERE t.transaction_datetime <= (SELECT max(analysis_formation) FROM analysis_date)
GROUP BY pd.customer_id, t.transaction_id, gs.group_id
ORDER BY pd.customer_id;




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
WITH CommonData AS (
    SELECT pd.customer_id,
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
    WHERE t.transaction_datetime <= (SELECT max(analysis_formation) FROM analysis_date)
),
     GroupPurchaseInfo AS (
         SELECT customer_id,
                group_id,
                MIN(transaction_datetime) AS First_Group_Purchase_Date,
                MAX(transaction_datetime) AS Last_Group_Purchase_Date,
                COUNT(*) AS Group_Purchase,
                ROUND(MIN(SKU_Discount_Ratio), 2) AS Group_Min_Discount
         FROM CommonData
         GROUP BY customer_id, group_id
     )

SELECT gd.customer_id,
       gd.group_id,
       gpi.First_Group_Purchase_Date,
       gpi.Last_Group_Purchase_Date,
       gpi.Group_Purchase,
       (gpi.Last_Group_Purchase_Date::date - gpi.First_Group_Purchase_Date::date + 1) / gpi.Group_Purchase AS Group_Frequency,
       gpi.Group_Min_Discount
FROM CommonData gd
         JOIN GroupPurchaseInfo gpi ON gd.customer_id = gpi.customer_id AND gd.group_id = gpi.group_id
GROUP BY gd.customer_id, gd.group_id, gpi.First_Group_Purchase_Date, gpi.Last_Group_Purchase_Date,
         gpi.Group_Purchase, gpi.Group_Min_Discount
ORDER BY gd.customer_id, gd.group_id;

