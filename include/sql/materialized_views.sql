SET search_path TO raw, silver, gold;
CREATE MATERIALIZED VIEW silver.delivered_mv AS
WITH delivered AS (
    SELECT * 
    FROM deliveries d
    WHERE d.delivery_status = UPPER('delivered')
),
delivered_notnull AS (
    SELECT * 
    FROM delivered dd
    WHERE dd.driver_id IS NOT NULL
)
SELECT *
FROM delivered_notnull
;

CREATE MATERIALIZED VIEW silver.delivered_clean_mv AS
with quartiles as (
    select  
        percentile_cont(0.25) WITHIN GROUP (ORDER BY delivery_distance_meters) AS Q1,
        percentile_cont(0.75) WITHIN GROUP (ORDER BY delivery_distance_meters) AS Q3
    FROM silver.delivered_mv
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
    silver.delivered_mv
	JOIN 
    bounds
	ON 
    silver.delivered_mv.delivery_distance_meters <= bounds.upper_bound
	)
	select * from delivered_no_outliers
;

    CREATE MATERIALIZED VIEW silver.ranking_all AS
	with all_rank_drivers as (
		SELECT 
		dno.driver_id,
		d.driver_modal,
		d.driver_type,
		SUM(delivery_distance_meters) as sum_of_distance,
    	RANK() OVER (ORDER BY SUM(delivery_distance_meters) DESC) AS ranking
		FROM 
    	silver.delivered_no_out_mv dno
    	join raw.drives d on dno.driver_id = d.driver_id
		GROUP BY dno.driver_id, d.driver_modal, d.driver_type
	)
	select * from all_rank_drivers
;

DO $$
BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW gold.ranking_20_stratified_mv_' || to_char(current_date, 'YYYY_MM_DD') || ' AS
        WITH clean_drivers AS (
            SELECT DISTINCT
                dno.driver_id,
                d.driver_modal,
                d.driver_type
            FROM 
                silver.delivered_no_out_mv dno
            JOIN 
                raw.drives d ON dno.driver_id = d.driver_id
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
                r.sum_of_distance,
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
                r.sum_of_distance,
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
                r.sum_of_distance,
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
                r.sum_of_distance,
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
