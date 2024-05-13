SET search_path TO raw, silver, gold;

CREATE VIEW silver.delivered_v AS
WITH delivered AS (select d.*,
	o.order_amount ,
	o.order_created_day ,
	o.order_created_month ,
	o.order_created_year ,
	s.store_id ,
	s.store_name ,
	s.store_segment ,
	s.store_plan_price ,
	h.hub_name ,
	h.hub_city ,
	h.hub_state
	from 
	raw.deliveries d  
	join raw.orders o on d.delivery_order_id = o.delivery_order_id 
	join raw.stores s on o.store_id = s.store_id 
	join raw.hubs h on s.hub_id = h.hub_id
	WHERE d.delivery_status = UPPER('delivered')
),

delivered_notnull AS (
    SELECT dd.*
    FROM delivered dd
    WHERE dd.driver_id IS NOT NULL
)
SELECT *
FROM delivered_notnull;

--outliers in distance
CREATE VIEW silver.delivered_no_out_v AS
with quartiles as (
    select  
        percentile_cont(0.25) WITHIN GROUP (ORDER BY delivery_distance_meters) AS Q1,
        percentile_cont(0.75) WITHIN GROUP (ORDER BY delivery_distance_meters) AS Q3
    FROM silver.delivered_v
	),
	
	iqr as (
    select quartiles.Q3 - quartiles.Q1 AS IQR
    from quartiles
	),
	
	bounds AS (
    SELECT quartiles.Q3 + 1.5 *iqr.IQR AS upper_bound
    from quartiles, iqr
	),
	
	delivered_no_outliers as (
	SELECT * FROM 
    silver.delivered_v
	JOIN 
    bounds
	ON 
    silver.delivered_v.delivery_distance_meters <= bounds.upper_bound
	)
	select * from delivered_no_outliers
;
--outliers in order amount 
CREATE TABLE silver.delivered_clean AS
with quartiles as (
    select  
        percentile_cont(0.25) WITHIN GROUP (ORDER BY order_amount) AS Q1,
        percentile_cont(0.75) WITHIN GROUP (ORDER BY order_amount) AS Q3
    FROM silver.delivered_no_out_v
	),
	
	iqr as (
    select quartiles.Q3 - quartiles.Q1 AS IQR
    from quartiles
	),
	
	bounds AS (
    SELECT quartiles.Q3 + 1.5 *iqr.IQR AS upper_bound
    from quartiles, iqr
	),
	
	delivered_no_outliers as (
	SELECT * FROM 
    silver.delivered_v
	JOIN 
    bounds
	ON 
    silver.delivered_v.delivery_distance_meters <= bounds.upper_bound
	)
	select * from delivered_no_outliers
;

CREATE TABLE silver.ranking_all AS
	with all_rank_drivers as (
		SELECT 
		dno.driver_id,
		d.driver_modal,
		d.driver_type,
        SUM(dno.order_amount) as sum_of_amount_of_orders,
		SUM(dno.delivery_distance_meters) as sum_of_distance,
        MAX(dno.delivery_distance_meters) as max_distance,
    	RANK() OVER (ORDER BY SUM(delivery_distance_meters) DESC) AS ranking
		FROM 
    	silver.delivered_clean dno
    	join raw.drivers d on dno.driver_id = d.driver_id
		GROUP BY dno.driver_id, d.driver_modal, d.driver_type
	)
	select * from all_rank_drivers
;

DO $$
BEGIN
    EXECUTE 'CREATE TABLE gold.ranking_20_stratified_' || to_char(current_date, 'YYYY_MM_DD') || ' AS
        WITH clean_drivers AS (
            SELECT DISTINCT
                dno.driver_id,
                d.driver_modal,
                d.driver_type
            FROM 
                silver.delivered_no_out dno
            JOIN 
                raw.drivers d ON dno.driver_id = d.driver_id
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
                r.max_distance
                r.ranking
            FROM 
                silver.ranking_all r 	
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
                r.max_distance
                r.ranking		
            FROM 
                silver.ranking_all r 
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
                r.max_distance
                r.ranking  		
            FROM 
                silver.ranking_all r 	
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
                r.max_distance
                r.ranking		
            FROM 
                silver.ranking_all r  	
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
