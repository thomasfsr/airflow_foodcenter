REFRESH MATERIALIZED VIEW WITHOUT CONCURRENTLY silver.delivered_mv;
REFRESH MATERIALIZED VIEW WITHOUT CONCURRENTLY silver.delivered_no_out_mv;
REFRESH MATERIALIZED VIEW WITHOUT CONCURRENTLY silver.ranking_all_mv
--REFRESH MATERIALIZED VIEW WITHOUT CONCURRENTLY gold.ranking_20_stratified_mv;
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
