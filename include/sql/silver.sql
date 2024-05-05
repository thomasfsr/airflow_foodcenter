--ctes mas precisa ser materialized view na silver.
with delivered as (
	select * from deliveries d
	where d.delivery_status = upper('delivered')
	),

	delivered_notnull as (
	select * from delivered dd
	where dd.driver_id is not null
	)

select * from delivered_notnull
