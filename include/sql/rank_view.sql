--ctes mas precisa ser materialized view na silver.
-- view task 1
with delivered as (
	select * from deliveries d
	where d.delivery_status = upper('delivered')
	),

	delivered_notnull as (
	select * from delivered dd
	where dd.driver_id is not null
	),
	
	quartiles as (
    select  
        percentile_cont(0.25) WITHIN GROUP (ORDER BY delivery_distance_meters) AS Q1,
        percentile_cont(0.75) WITHIN GROUP (ORDER BY delivery_distance_meters) AS Q3
    FROM delivered_notnull
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
    delivered_notnull
	JOIN 
    bounds
	ON 
    delivered_notnull.delivery_distance_meters <= bounds.upper_bound
	),
	
	all_rank_drivers as (
		SELECT 
		dno.driver_id,
		d.driver_modal,
		d.driver_type,
		SUM(delivery_distance_meters) as sum_of_distance,
    	RANK() OVER (ORDER BY SUM(delivery_distance_meters) DESC) AS ranking
		FROM 
    	delivered_no_outliers dno
    	join drives d on dno.driver_id = d.driver_id
		GROUP BY dno.driver_id, d.driver_modal, d.driver_type
	),
	
	clean_drivers AS (
    	SELECT DISTINCT
        dno.driver_id,
        d.driver_modal,
        d.driver_type
    	FROM 
        delivered_no_outliers dno
        join drives d on dno.driver_id = d.driver_id
        ),
	
	proportion AS (
    SELECT 
        driver_modal,
        driver_type,
        COUNT(driver_id) AS counting,
        ROUND(COUNT(driver_id)::numeric / sum(COUNT(driver_id)) OVER () * 16) as proportion
    FROM 
        clean_drivers
    GROUP BY 
        driver_modal, driver_type
	),
	
	rank_drivers_biker_free as (
		SELECT 
		r.driver_id,
		r.driver_modal,
		r.driver_type,
		r.sum_of_distance,
		r.ranking   		
		FROM 
    	all_rank_drivers r 	
		where
    	r.driver_type = upper('freelance')
    	and
    	r.driver_modal = upper('biker')
    	order by 
    	r.ranking
    	LIMIT (
        SELECT 
            ROUND(p.proportion) 
        FROM 
            proportion p 
        WHERE 
            p.driver_modal = UPPER('biker') AND p.driver_type = UPPER('freelance')
    	) +1
    	),
    	
	rank_drivers_biker_logo as (
		SELECT 
		r.driver_id,
		r.driver_modal,
		r.driver_type,
		r.sum_of_distance,
		r.ranking   		
		FROM 
    	all_rank_drivers r 	
		where
    	r.driver_type = upper('logistic operator')
    	and
    	r.driver_modal = upper('biker')
    	order by 
    	r.ranking
    	LIMIT (
        SELECT 
            ROUND(p.proportion) 
        FROM 
            proportion p 
        WHERE 
            p.driver_modal = UPPER('biker') AND p.driver_type = UPPER('logistic operator')
    	) +1
    	),
    	
	rank_drivers_moto_free as (
		SELECT 
		r.driver_id,
		r.driver_modal,
		r.driver_type,
		r.sum_of_distance,
		r.ranking   		
		FROM 
    	all_rank_drivers r 	
		where
    	r.driver_type = upper('freelance')
    	and
    	r.driver_modal = upper('motoboy')
    	order by 
    	r.ranking
    	LIMIT (
        SELECT 
            ROUND(p.proportion) 
        FROM 
            proportion p 
        WHERE 
            p.driver_modal = UPPER('motoboy') AND p.driver_type = UPPER('freelance')
    	) +1
    	),
    	
	rank_drivers_moto_logo as (
		SELECT 
		r.driver_id,
		r.driver_modal,
		r.driver_type,
		r.sum_of_distance,
		r.ranking   		
		FROM 
    	all_rank_drivers r 	
		where
    	r.driver_type = upper('logistic operator')
    	and
    	r.driver_modal = upper('motoboy')
    	order by 
    	r.ranking
    	LIMIT (
        SELECT 
            ROUND(p.proportion) 
        FROM 
            proportion p 
        WHERE 
            p.driver_modal = UPPER('motoboy') AND p.driver_type = UPPER('logistic operator')
    	) +1
    	),
    	
    	concating as (
    	select * from rank_drivers_moto_logo
		union all
		select * from rank_drivers_moto_free
		union all
		select * from rank_drivers_biker_logo
		union all
		select * from rank_drivers_biker_free
		order by ranking asc)
		
		select * from concating	