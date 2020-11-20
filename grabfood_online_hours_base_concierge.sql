SELECT 
bk_metrics.driver_id,
bk_metrics.dedicated_fleet,
bk_metrics.country_name,
bk_metrics.city_name,
bk_metrics.completed_orders_gf,
bk_metrics.total_online_seconds,
bk_metrics.total_transit_seconds,
COALESCE(job_metrics.jobs_bid_gf,0) AS jobs_bid_gf,
COALESCE(job_metrics.jobs_received_gf,0) AS jobs_received_gf,
bk_metrics.date_local

FROM
(SELECT 
jd.driver_id,
jd.dedicated_fleet,
jd.country_name,
jd.city_name,
jd.completed_orders_gf,
os.total_online_seconds,
os.total_transit_seconds,
jd.date_local

FROM 

(SELECT 
bookings.date_local,
bookings.city_id,
ct.city_name,
ct.country_name,
bookings.driver_id,
CASE WHEN driver_fleet.driver_id IS NOT NULL OR ct.country_name IN ('Singapore','Malaysia') THEN 1 ELSE 0 END AS dedicated_fleet,
sum(CASE WHEN is_completed_state = TRUE AND is_unique_booking = TRUE THEN 1 ELSE 0 END) AS completed_orders_gf

FROM public.prejoin_grabfood bookings

LEFT JOIN
(
SELECT
			driver_id
			,fleet_name
			,snapshot_date 
		FROM (
			SELECT 
				*
				,ROW_NUMBER() OVER (PARTITION BY snapshot_date, driver_id ORDER BY created_at DESC) AS ranking
			FROM (
				SELECT
					driver_taxi_types.driver_id
					,fleets.name AS fleet_name
					,driver_taxi_types.created_at 
					,date_add('day', -1, date(driver_taxi_types.snapshot_date)) AS snapshot_date
				FROM
					snapshots.driver_taxi_types 
				INNER JOIN public.fleets on fleets.id = driver_taxi_types.fleet_id
				INNER JOIN datamart.dim_taxi_types ON dim_taxi_types.id = driver_taxi_types.taxi_type_id
				WHERE
					dim_taxi_types.taxi_type_simple = 'GrabFood'
					AND driver_taxi_types.snapshot_date >= date_format(date([[inc_start_date]]) - INTERVAL '2' DAY, '%Y-%m-%d')
					AND driver_taxi_types.snapshot_date <= [[inc_end_date]]
				GROUP BY
					1,2,3,4
			)
		)
		WHERE
			ranking = 1
			AND regexp_like(lower(fleet_name), 'gf dedicated|grabfood mnl exclusive|grabfood ceb exclusive fleet')
) driver_fleet 

ON bookings.driver_id = driver_fleet.driver_id
AND bookings.date_local = driver_fleet.snapshot_date


LEFT JOIN datamart.dim_cities_countries ct ON bookings.city_id = ct.city_id

WHERE
is_test_booking = FALSE
AND bookings.date_local >= date([[inc_start_date]])
AND bookings.date_local <= DATE([[inc_end_date]])
AND DATE(bookings.partition_date) >= date([[inc_start_date]]) - interval '1' day
AND DATE(bookings.partition_date) <= DATE([[inc_end_date]]) + interval '1' day

GROUP BY 1,2,3,4,5,6) jd

INNER JOIN 
(SELECT 
city_id,
date(date_local) AS date_local,
driver_id,
sum(online_seconds) AS total_online_seconds,
sum(transit_seconds) AS total_transit_seconds
FROM
slide.agg_vertical_driver_online_hours
WHERE date_local >= [[inc_start_date]]
AND date_local <= [[inc_end_date]]
AND vertical = 'GrabFood'
GROUP BY 1,2,3) os

ON jd.driver_id = os.driver_id
AND jd.city_id = os.city_id
AND jd.date_local = os.date_local) bk_metrics

LEFT JOIN 

(SELECT 
driver_id,
city_name,
date_local,
sum(jobs_bid) AS jobs_bid_gf,
sum(jobs_received) AS jobs_received_gf
from
datamart.agg_driver_bookings bk
INNER JOIN datamart.dim_taxi_types tt 
ON bk.taxi_type_id = tt.id 
INNER JOIN datamart.dim_cities_countries ct 
ON bk.city_id = ct.city_id
WHERE 
bk.date_local >= [[inc_start_date]]
AND bk.date_local <= [[inc_end_date]]
AND tt.taxi_type_simple = 'GrabFood'
AND date(bk.date_local) >= date(tt.start_at) 
AND date(bk.date_local) < date(tt.end_at) 
GROUP BY 1,2,3) job_metrics 

ON bk_metrics.driver_id = job_metrics.driver_id 
AND bk_metrics.city_name = job_metrics.city_name
AND bk_metrics.date_local = date(job_metrics.date_local)
