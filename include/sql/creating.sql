SET search_path TO raw, silver, gold;

-- Create delivered_v view
CREATE VIEW silver.delivered_v AS
WITH delivered_v AS (
    SELECT d.*,
        o.order_amount,
        o.order_created_day,
        o.order_created_month,
        o.order_created_year,
        s.store_id,
        s.store_name,
        s.store_segment,
        s.store_plan_price,
        h.hub_name,
        h.hub_city,
        h.hub_state
    FROM raw.deliveries d
    JOIN raw.orders o ON d.delivery_order_id = o.delivery_order_id
    JOIN raw.stores s ON o.store_id = s.store_id
    JOIN raw.hubs h ON s.hub_id = h.hub_id
    WHERE d.delivery_status = UPPER('delivered')
),
delivered_notnull AS (
    SELECT dd.*
    FROM delivered_v dd
    WHERE dd.driver_id IS NOT NULL
)
SELECT *
FROM delivered_notnull;

-- Outliers in distance
CREATE VIEW silver.delivered_no_distance_outliers_v AS
WITH quartiles AS (
    SELECT
        percentile_cont(0.25) WITHIN GROUP (ORDER BY delivery_distance_meters) AS Q1,
        percentile_cont(0.75) WITHIN GROUP (ORDER BY delivery_distance_meters) AS Q3
    FROM silver.delivered_v
),
iqr AS (
    SELECT Q3 - Q1 AS IQR
    FROM quartiles
),
bounds AS (
    SELECT Q3 + 1.5 * IQR AS upper_bound
    FROM quartiles, iqr
),
delivered_no_outliers AS (
    SELECT *
    FROM silver.delivered_v dv
    JOIN bounds ON dv.delivery_distance_meters <= bounds.upper_bound
)
SELECT *
FROM delivered_no_outliers;

-- Outliers in order amount
CREATE MATERIALIZED VIEW silver.clean_delivered_mv AS
WITH quartiles AS (
    SELECT
        percentile_cont(0.25) WITHIN GROUP (ORDER BY order_amount) AS Q1,
        percentile_cont(0.75) WITHIN GROUP (ORDER BY order_amount) AS Q3
    FROM silver.delivered_no_distance_outliers_v
),
iqr AS (
    SELECT Q3 - Q1 AS IQR
    FROM quartiles
),
bounds AS (
    SELECT Q3 + 1.5 * IQR AS upper_bound
    FROM quartiles, iqr
),
delivered_no_outliers AS (
    SELECT dv.*
    FROM silver.delivered_no_distance_outliers_v dv
    JOIN bounds ON dv.order_amount <= bounds.upper_bound
)
SELECT *
FROM delivered_no_outliers;

--outliers in order amount 
-- CREATE MATERIALIZED VIEW silver.clean_delivered_mv AS
-- with quartiles as (
--     select  
--         percentile_cont(0.25) WITHIN GROUP (ORDER BY order_amount) AS Q1,
--         percentile_cont(0.75) WITHIN GROUP (ORDER BY order_amount) AS Q3
--     FROM silver.delivered_no_distance_outliers_v
-- 	),
	
-- 	iqr as (
--     select quartiles.Q3 - quartiles.Q1 AS IQR
--     from quartiles
-- 	),
	
-- 	bounds AS (
--     SELECT quartiles.Q3 + 1.5 *iqr.IQR AS upper_bound
--     from quartiles, iqr
-- 	),
	
-- 	delivered_no_outliers as (
-- 	SELECT dv.* FROM 
--     silver.delivered_no_distance_outliers_v dv
-- 	JOIN 
--     bounds
--     ON dv.delivery_distance_meters <= bounds.upper_bound
-- 	)

-- select * from delivered_no_outliers
-- ;

CREATE MATERIALIZED VIEW silver.ranking_all_mv AS
	with all_rank_drivers as (
		SELECT 
		cd.driver_id,
		d.driver_modal,
		d.driver_type,
        SUM(cd.order_amount) as sum_of_amount_of_orders,
		SUM(cd.delivery_distance_meters) as sum_of_distance,
        MAX(cd.delivery_distance_meters) as max_distance,
    	RANK() OVER (ORDER BY SUM(delivery_distance_meters) DESC) AS ranking
		FROM 
    	silver.clean_delivered_mv cd
    	join raw.drivers d on cd.driver_id = d.driver_id
		GROUP BY cd.driver_id, d.driver_modal, d.driver_type
	)
	select * from all_rank_drivers
;

DO $$
BEGIN
    EXECUTE 'CREATE TABLE IF NOT EXISTS gold.top20_ranking_stratified_' || to_char(current_date, 'YYYY_MM_DD') || ' AS
        WITH clean_drivers AS (
            SELECT DISTINCT
                cd.driver_id,
                d.driver_modal,
                d.driver_type
            FROM 
                silver.clean_delivered_mv cd
            JOIN 
                raw.drivers d ON cd.driver_id = d.driver_id
        ),
        proportion AS (
            SELECT 
                driver_modal,
                driver_type,
                COUNT(driver_id) AS counting,
                ROUND(COUNT(driver_id)::numeric / sum(COUNT(driver_id)) OVER () * 16) AS proportion
            FROM 
                clean_drivers
            GROUP BY 
                driver_modal, driver_type
        ),
        rank_drivers_biker_free AS (
            SELECT 
                r.driver_id,
                r.driver_modal,
                r.driver_type,
                r.sum_of_amount_of_orders,
		        r.sum_of_distance,
                r.max_distance,
                r.ranking
            FROM 
                silver.ranking_all_mv r 	
            WHERE
                r.driver_type = UPPER(''freelance'')
            AND
                r.driver_modal = UPPER(''biker'')
            ORDER BY 
                r.ranking
            LIMIT (
                SELECT 
                    ROUND(p.proportion) 
                FROM 
                    proportion p 
                WHERE 
                    p.driver_modal = UPPER(''biker'') AND p.driver_type = UPPER(''freelance'')
            ) + 1
        ),
        rank_drivers_biker_logo AS (
            SELECT 
                r.driver_id,
                r.driver_modal,
                r.driver_type,
                r.sum_of_amount_of_orders,
		        r.sum_of_distance,
                r.max_distance,
                r.ranking		
            FROM 
                silver.ranking_all_mv r 
            WHERE
                r.driver_type = UPPER(''logistic operator'')
            AND
                r.driver_modal = UPPER(''biker'')
            ORDER BY 
                r.ranking
            LIMIT (
                SELECT 
                    ROUND(p.proportion) 
                FROM 
                    proportion p 
                WHERE 
                    p.driver_modal = UPPER(''biker'') AND p.driver_type = UPPER(''logistic operator'')
            ) + 1
        ),
        rank_drivers_moto_free AS (
            SELECT 
                r.driver_id,
                r.driver_modal,
                r.driver_type,
                r.sum_of_amount_of_orders,
		        r.sum_of_distance,
                r.max_distance,
                r.ranking  		
            FROM 
                silver.ranking_all_mv r 	
            WHERE
                r.driver_type = UPPER(''freelance'')
            AND
                r.driver_modal = UPPER(''motoboy'')
            ORDER BY 
                r.ranking
            LIMIT (
                SELECT 
                    ROUND(p.proportion) 
                FROM 
                    proportion p 
                WHERE 
                    p.driver_modal = UPPER(''motoboy'') AND p.driver_type = UPPER(''freelance'')
            ) + 1
        ),
        rank_drivers_moto_logo AS (
            SELECT 
                r.driver_id,
                r.driver_modal,
                r.driver_type,
                r.sum_of_amount_of_orders,
		        r.sum_of_distance,
                r.max_distance,
                r.ranking		
            FROM 
                silver.ranking_all_mv r  	
            WHERE
                r.driver_type = UPPER(''logistic operator'')
            AND
                r.driver_modal = UPPER(''motoboy'')
            ORDER BY 
                r.ranking
            LIMIT (
                SELECT 
                    ROUND(p.proportion) 
                FROM 
                    proportion p 
                WHERE 
                    p.driver_modal = UPPER(''motoboy'') AND p.driver_type = UPPER(''logistic operator'')
            ) + 1
        ),
        concating AS (
            SELECT * FROM rank_drivers_moto_logo
            UNION ALL
            SELECT * FROM rank_drivers_moto_free
            UNION ALL
            SELECT * FROM rank_drivers_biker_logo
            UNION ALL
            SELECT * FROM rank_drivers_biker_free
        )
        SELECT * FROM concating;';
END $$;

DO $$
BEGIN
    EXECUTE 'CREATE TABLE IF NOT EXISTS gold.distance_mean_state_' || to_char(current_date, 'YYYY_MM_DD') || ' AS
select 
CAST(AVG(cdm.delivery_distance_meters) AS DECIMAL(10,2)) AS mean,
cdm.hub_state 
from silver.clean_delivered_mv cdm 
join raw.drivers d on cdm.driver_id = d.driver_id 
where d.driver_modal = upper(''motoboy'') 
group by hub_state '
;
END $$;

DO $$
BEGIN
    EXECUTE 'CREATE TABLE IF NOT EXISTS gold.revenue_segment_' || to_char(current_date, 'YYYY_MM_DD') || ' AS
    SELECT 
        cdm.store_segment, 
        CAST(AVG(cdm.order_amount) AS DECIMAL(10,2)) AS average_revenue, 
        CAST(SUM(cdm.order_amount) AS DECIMAL(10,2)) AS total_revenue
    FROM 
        silver.clean_delivered_mv cdm 
    GROUP BY 
        cdm.store_segment';
END $$;

DO $$
BEGIN
    EXECUTE 'CREATE TABLE IF NOT EXISTS gold.revenue_state_' || to_char(current_date, 'YYYY_MM_DD') || ' AS
    SELECT 
        cdm.hub_state,
        CAST(AVG(cdm.order_amount) AS DECIMAL(10,2)) AS average_revenue, 
        CAST(SUM(cdm.order_amount) AS DECIMAL(10,2)) AS total_revenue
    FROM 
        silver.clean_delivered_mv cdm 
    GROUP BY 
        cdm.hub_state
    order by total_revenue desc ';
END $$;