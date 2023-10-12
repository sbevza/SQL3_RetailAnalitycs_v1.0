-- DROP FUNCTION IF EXISTS offers_aimed_at_cross_selling(number_of_group INTEGER,
--                                     churn_index NUMERIC, stability_index NUMERIC,
--                                     SKU NUMERIC, margin NUMERIC);

CREATE OR REPLACE FUNCTION offers_aimed_at_cross_selling(number_of_group INTEGER,
                                    churn_index NUMERIC, stability_index NUMERIC,
                                    SKU NUMERIC, margin NUMERIC)
RETURNS TABLE(Customer_ID INTEGER,
			 SKU_Name VARCHAR,
 			 Offer_Discount_Depth NUMERIC)
LANGUAGE plpgsql
AS $$
BEGIN
RETURN QUERY

    /*-----без костыля но с неточностью-------*/

--Выбор групп
    WITH select_groups
	AS (SELECT g.customer_id, g.group_id, g.group_affinity_index, g.group_minimum_discount
	FROM groups g
	WHERE g.Group_Churn_Rate <= churn_index AND g.group_stability_index < stability_index
	GROUP BY g.group_affinity_index, g.customer_id, g.group_id, g.group_minimum_discount
	ORDER BY g.group_id, g.customer_id),

--Определение SKU с максимальной маржой
    sku_margin_max AS
    (SELECT sg.*, sku.sku_id, sku.sku_name, c.primary_store, s.sku_retail_price, MAX(s.sku_retail_price - s.sku_purchase_price) AS margin_max
	FROM select_groups sg
	JOIN sku ON sku.group_id = sg.group_id
	JOIN stores s ON sku.sku_id = s.sku_id
	JOIN customers c ON s.transaction_store_id = c.primary_store AND c.customer_average_check_segment = 'Low'
	GROUP BY s.sku_id, sg.customer_id, sg.group_id, sg.group_affinity_index, sg.group_minimum_discount,
	         sku.sku_id, sku.sku_name, c.primary_store, s.sku_retail_price, s.sku_purchase_price
	ORDER BY sg.customer_id),

 	sku_margin_max_distinct AS
	(SELECT DISTINCT ranked.*
    FROM (SELECT sku_margin_max.*,
                 RANK() OVER(PARTITION BY group_id ORDER BY margin_max DESC) AS group_count
          FROM sku_margin_max) ranked
    JOIN select_groups sg ON ranked.group_id = sg.group_id
    WHERE group_count = 1
    ORDER BY group_id),

--Определение доли SKU в группе
	count_share_sku AS
    (SELECT mg.*,
             (SELECT count(*) FROM checks c WHERE c.sku_id = mg.sku_id) /
             (SELECT count(*) FROM checks c
              JOIN sku ON c.sku_id = sku.sku_id
              WHERE sku.group_id = mg.group_id)::NUMERIC AS sku_share
              FROM sku_margin_max_distinct mg
              WHERE mg.group_count <= number_of_group),

 	count_discount_offer AS
	(SELECT cs.*,
             (CEIL(cs.group_minimum_discount * 100 / 5) * 5)::NUMERIC AS discount,
             (CEIL(((((margin / 100) * cs.margin_max) / cs.sku_retail_price) * 100) / 5) * 5)::NUMERIC AS discount_offer
     FROM count_share_sku cs
	 WHERE cs.sku_share <= (SKU / 100)),

    offer AS
	(SELECT cdo.customer_id, cdo.sku_name,
            (CASE WHEN cdo.discount_offer >= cdo.discount THEN
                  CASE WHEN cdo.discount > 0 THEN
                            cdo.discount
                  END
             END) AS discount
     FROM count_discount_offer cdo)

SELECT DISTINCT * FROM offer
WHERE discount IS NOT NULL;

/*-----с костылем, но как в чеке-------*/

-- WITH common_table_margin AS
--      (SELECT DISTINCT g.customer_id, g.group_id, g.group_affinity_index, g.group_minimum_discount,
--                      sku.sku_id, sku.sku_name,
-- 			         c.primary_store, c.customer_average_check_segment,
--                      s.transaction_store_id,  s.sku_retail_price,
-- 			        (s.sku_retail_price - s.sku_purchase_price) AS marg,
--                     MAX(s.sku_retail_price - s.sku_purchase_price) OVER (PARTITION BY g.group_id) AS max_marg
--      FROM groups g
--      LEFT JOIN sku ON sku.group_id = g.group_id
--      LEFT JOIN stores s ON sku.sku_id = s.sku_id
--      LEFT JOIN customers c ON c.primary_store = s.transaction_store_id
--      WHERE g.group_churn_rate <= churn_index
-- 	 AND g.group_stability_index < stability_index
-- 	 --AND c.customer_average_check_segment = 'Low'
--      ORDER BY g.customer_id),

-- 	 common_table_margin_max AS
-- 	 (SELECT DISTINCT *
-- 	  FROM common_table_margin ct
--       WHERE marg = max_marg),

-- 	 margin_group_count AS
-- 	 (SELECT ct.*,
-- 	         row_number() OVER (PARTITION BY ct.customer_id ORDER BY ct.group_affinity_index) AS group_count
--              FROM common_table_margin_max ct),

-- 	 count_sku_share AS
-- 	 (SELECT mg.*,
--              (SELECT count(*) FROM checks c WHERE c.sku_id = mg.sku_id) /
--              (SELECT count(*) FROM checks c
--               JOIN sku ON c.sku_id = sku.sku_id
--               WHERE sku.group_id = mg.group_id)::NUMERIC AS sku_share
--               FROM margin_group_count mg
--               WHERE mg.group_count <= number_of_group),

--  	count_discount_offer AS
-- 	(SELECT cs.*,
--              (FLOOR((cs.group_minimum_discount * 100) / 5) * 5)::NUMERIC AS discount,
--              CEIL(((((margin  / 100) * cs.max_marg) / cs.sku_retail_price) * 100) / 5) * 5 AS discount_offer
--              FROM count_sku_share cs
--              WHERE cs.sku_share <= (SKU / 100)),

-- 	offer AS
-- 	(SELECT cdo.customer_id, cdo.sku_name,
--             (CASE WHEN cdo.discount_offer >= cdo.discount THEN
--                   CASE WHEN cdo.discount > 0 THEN
--                             cdo.discount
--                   ELSE '5'::INT
--                   END
--              END) AS discount
--      FROM count_discount_offer cdo)

-- 	SELECT DISTINCT * FROM offer
--     WHERE discount IS NOT NULL;


END
$$;

SELECT * FROM offers_aimed_at_cross_selling(5,3,0.5,100,30);
SELECT * FROM offers_aimed_at_cross_selling(5,3,0.5,100,10);
SELECT * FROM offers_aimed_at_cross_selling(0,3,0.5,100,30);

