/*
    Name: slide.gf_dash_weekly_rolling_metrics_country
    Refresh Time: Daily, 21:00 UTC
    Aggregation Mode: Incremental, lookback window 20 days
    Lighthouse Dependancy Tables:
        slide.gf_daily_city_base_metrics_agg_v2 21:00
*/

WITH latest_booking_past_5weeks AS
(SELECT
passenger_id,
date_local,
country_name AS country,
city_name AS city
from
slide.gf_dash_pax_metrics_agg 

WHERE (unique_bookings_gf>0 OR completed_orders_gf>0 )
 AND DATE(date_local)>= date_trunc('week',date_add('week',-4, date([[inc_start_date]]) ) )
 AND  DATE(date_local) <= date([[inc_end_date]])
 GROUP BY 1,2,3,4
 )

,Dax_first_bid AS 
   (
   SELECT 
   country_name,
   driver_id,
   min(first_bid_date) AS first_bid_date
   FROM slide.gf_dash_dax_first_last_bid_date
   GROUP BY 1,2
	)

,latest_bidding_past_5weeks AS
(    
  SELECT 
  country
  ,city
  ,driver_id
  ,date_local
  ,sum(jobs_bid_gf) AS jobs_bid_gf
  
  FROM slide.gf_dash_dax_metrics_aggregated
  
 
 WHERE date_local >= date_trunc('week',date_add('week',-4, date([[inc_start_date]]) ) )
 AND date_local <= DATE([[inc_end_date]])
 GROUP BY 1,2,3,4
 HAVING sum(jobs_bid_gf)>0
   )


/********** Weekly + Country ************/ 
(SELECT 
'By Week' AS By_day_week_month
,'By Country' AS By_city_country
,base.week_of AS time_period
,base.country AS country_name
,'All' AS city_name
,COALESCE(pax_conversion.gf_orders_past4week,0) AS gf_orders_past4week
,COALESCE(pax_conversion.gf_gmv_past4week,0) AS gf_gmv_past4week
,COALESCE(pax_conversion.gf_mau_past4week,0) AS gf_mau_past4week
,COALESCE(pax_conversion.gf_mtu_past4week,0) AS gf_mtu_past4week

,COALESCE(pax_conversion.gf_orders_past4week_new,0) AS gf_orders_past4week_new
,COALESCE(pax_conversion.gf_gmv_past4week_new,0) AS gf_gmv_past4week_new
,COALESCE(pax_conversion.gf_mau_past4week_new,0) AS gf_mau_past4week_new

,COALESCE(pax_conversion.gf_orders_past4week_new_mtu,0) AS gf_orders_past4week_new_mtu
,COALESCE(pax_conversion.gf_gmv_past4week_new_mtu,0) AS gf_gmv_past4week_new_mtu
,COALESCE(pax_conversion.gf_mau_past4week_new_mtu,0) AS gf_mau_past4week_new_mtu

,COALESCE(pax_conversion.transport_mau_past4week,0) AS transport_mau_past4week
,COALESCE(pax_conversion.transport_mtu_past4week,0) AS transport_mtu_past4week

,COALESCE(pax_conversion.transport_gf_mau_past4week,0) AS transport_gf_mau_past4week
,COALESCE(pax_conversion.transport_gf_mtu_past4week,0) AS transport_gf_mtu_past4week

,COALESCE(pax_conversion.transport_mau_who_used_gf,0) AS transport_mau_who_used_gf
,COALESCE(pax_conversion.transport_mtu_who_ordered_gf,0) AS transport_mtu_who_ordered_gf

,COALESCE(churn_reactivation_pax.churn_users,0) AS churn_users
,COALESCE(churn_reactivation_pax.reactivated_users,0) AS reactivated_users
,COALESCE(churn_reactivation_driver.churn_drivers,0) AS churn_drivers
,COALESCE(churn_reactivation_driver.reactivated_drivers,0) AS reactivated_drivers

from
(SELECT
    time_period AS week_of
	,country
	FROM slide.gf_daily_city_base_metrics_agg_v2
	WHERE by_city_country='By Country'
	AND by_day_week_month = 'By Week'
        AND business_model = 'All'
        AND cashless_status = 'All'
	AND time_period >= date_trunc('week',date([[inc_start_date]]))
    AND time_period <= DATE([[inc_end_date]])
    GROUP BY 1,2
) base

LEFT JOIN

(
  SELECT
  week_of,
  country,
  sum(gf_orders_past4week) as gf_orders_past4week,
  sum(gf_gmv_past4week) as gf_gmv_past4week,
  count(CASE WHEN gf_ub_past4week>0 THEN passenger_id ELSE NULL END) AS gf_mau_past4week,
  count(CASE WHEN Grab_ub_past4week>0 THEN passenger_id ELSE NULL END) as grab_mau_past4week,
  
  count(CASE WHEN gf_orders_past4week>0 THEN passenger_id ELSE NULL END) AS gf_mtu_past4week,
  count(CASE WHEN Grab_rides_past4week>0 THEN passenger_id ELSE NULL END) as grab_mtu_past4week, 

  sum(CASE WHEN gf_mau_new=TRUE then gf_orders_past4week else 0 end) as gf_orders_past4week_new,
  sum(CASE WHEN gf_mau_new=TRUE then gf_gmv_past4week else 0 end) as gf_gmv_past4week_new,
  count(CASE WHEN gf_mau_new=TRUE AND gf_ub_past4week>0 THEN passenger_id ELSE NULL END) AS gf_mau_past4week_new,

  sum(CASE WHEN gf_mtu_new=TRUE then gf_orders_past4week else 0 end) as gf_orders_past4week_new_mtu,
  sum(CASE WHEN gf_mtu_new=TRUE then gf_gmv_past4week else 0 end) as gf_gmv_past4week_new_mtu,
  count(CASE WHEN gf_mtu_new=TRUE AND gf_ub_past4week>0 THEN passenger_id ELSE NULL END) AS gf_mau_past4week_new_mtu,  
  
  count(CASE WHEN transport_ub_past4week>0 THEN passenger_id ELSE NULL END) AS transport_mau_past4week,
  count(CASE WHEN transport_rides_past4week>0 THEN passenger_id ELSE NULL END) AS transport_mtu_past4week,
  
  count(CASE WHEN transport_ub_past4week>0 AND gf_ub_past4week>0 THEN passenger_id ELSE NULL END) AS transport_gf_mau_past4week,
  count(CASE WHEN transport_rides_past4week>0 AND gf_orders_past4week>0 THEN passenger_id ELSE NULL END) AS transport_gf_mtu_past4week,
   
  count(CASE WHEN transport_ub_past4week>0 AND existing_gf_user = TRUE THEN passenger_id ELSE NULL END) AS transport_mau_who_used_gf,
  count(CASE WHEN transport_rides_past4week>0 AND existing_gf_eater = TRUE THEN passenger_id ELSE NULL END) AS transport_mtu_who_ordered_gf
  
  FROM
  (
	  SELECT 
	  evaluation_week.week_of
	  ,mau_info.passenger_id
	  ,mau_info.country
	  ,CASE WHEN first_bk_country.first_bk_week IS NOT NULL AND  first_bk_country.first_bk_week <= evaluation_week.week_of THEN TRUE ELSE FALSE END AS existing_gf_user
	  ,CASE WHEN first_bk_country.first_order_week IS NOT NULL AND  first_bk_country.first_order_week <= evaluation_week.week_of THEN TRUE ELSE FALSE END AS existing_gf_eater
	  ,CASE WHEN first_bk_country.first_bk_week >= date_add('week',-3,evaluation_week.week_of) and first_bk_country.first_bk_week <= evaluation_week.week_of THEN TRUE ELSE FALSE END AS gf_mau_new
	  ,CASE WHEN first_bk_country.first_order_week >= date_add('week',-3,evaluation_week.week_of) and first_bk_country.first_order_week <= evaluation_week.week_of THEN TRUE ELSE FALSE END AS gf_mtu_new
	  ,sum(unique_bookings_gf) as gf_ub_past4week
	  ,sum(completed_orders_gf) as gf_orders_past4week
	  ,sum(gmv_usd_gf) as gf_gmv_past4week
	  ,sum(unique_bookings_transport) as transport_ub_past4week
	  ,sum(unique_bookings) as Grab_ub_past4week
	  ,sum(rides_transport) as transport_rides_past4week
	  ,sum(rides) as Grab_rides_past4week  
	  FROM
	
	  (
		  SELECT
		  passenger_id,
		  country_name AS country,
		  date_trunc('week',date(date_local)) as week_local,
		  sum(unique_bookings) as unique_bookings,
		  sum(rides) as rides,
		  sum(unique_bookings_gf) AS unique_bookings_gf,
		  sum(completed_orders_gf) AS completed_orders_gf,
		  sum(unique_bookings_transport) AS unique_bookings_transport,
		  sum(rides_transport) AS rides_transport,
	      sum(gmv_usd_gf) as gmv_usd_gf
		  from
		  slide.gf_dash_pax_metrics_agg
		  WHERE date(date_local)>=DATE_TRUNC('week', DATE_ADD('week', -4, date([[inc_start_date]])))
		  AND date(date_local) <= date([[inc_end_date]])
		  AND country_name IN ('Indonesia','Vietnam','Thailand','Philippines','Singapore','Malaysia')
		  AND unique_bookings>0
		  group by 1,2,3
	  ) mau_info
	
	  
	  LEFT JOIN
	  (
		  SELECT 
		  passenger_id
		  ,country_name AS country
		  ,min(date_trunc('week',first_bookings_on_taxi_type_simple)) AS first_bk_week
		  ,min(date_trunc('week',first_ride_on_taxi_type_simple)) AS first_order_week
		  FROM 
		  slide.gf_dash_pax_first_booking_order 
		  GROUP BY 1,2
	  ) first_bk_country
	  
	  ON mau_info.country = first_bk_country.country
	  and mau_info.passenger_id = first_bk_country.passenger_id
	
	 
	 
	  RIGHT JOIN
	
	  (
		  SELECT DISTINCT date_trunc('week',date(l_date)) AS week_of from public.date_dim WHERE
		  date(l_date)>=DATE_TRUNC('week', date([[inc_start_date]]))
		  AND l_date<= [[inc_end_date]]
	  ) evaluation_week
	
	  ON mau_info.week_local >= date_add('week',-3,evaluation_week.week_of)
	  AND mau_info.week_local <= evaluation_week.week_of
	
	  GROUP BY 1,2,3,4,5,6,7 )

 GROUP BY 1,2
) pax_conversion

ON base.week_of = pax_conversion.week_of
AND base.country = pax_conversion.country


LEFT JOIN

(SELECT
	churn_reactivated_user.country,
	churn_reactivated_user.week_of,
	SUM(CASE WHEN active_first_1_week = 1 AND (active_last_4_weeks = 0 OR active_last_4_weeks IS NULL) THEN 1 ELSE 0 end) AS churn_users,
	SUM(CASE WHEN active_last_1_week = 1 AND (active_first_4_weeks = 0 OR active_first_4_weeks IS NULL) AND new_users.passenger_id IS NULL THEN 1 ELSE 0 end) AS reactivated_users
FROM

(
	SELECT
	DISTINCT
	week_of
	,passenger_id
	,city
	,country
	,min_order_time
	,max_order_time
	,CASE WHEN date(min_order_time)>=date(week_of) - INTERVAL '28' DAY AND date(min_order_time)<week_of - INTERVAL '21' DAY THEN 1 ELSE 0 END AS active_first_1_week
	,CASE WHEN date(max_order_time)>=date(week_of) - INTERVAL '21' DAY THEN 1 ELSE 0 END AS active_last_4_weeks
	,CASE WHEN date(max_order_time)>=date(week_of) THEN 1 ELSE 0 END AS active_last_1_week
	,CASE WHEN date(min_order_time)>=date(week_of) - INTERVAL '28' DAY AND date(min_order_time) < date(week_of) THEN 1 ELSE 0 END AS active_first_4_weeks
	
	  FROM
	
	  (
		  SELECT
		  passenger_id,
		  evaluation_week.week_of,
		  date_local,
		  city,
		  country,
		  row_number() OVER (PARTITION BY passenger_id,week_of ORDER BY date_local desc) AS latest_rank,
		  max(date_local) OVER (PARTITION BY passenger_id,week_of) AS max_order_time,
		  min(date_local) OVER (PARTITION BY passenger_id,week_of) AS min_order_time
		
		  FROM latest_booking_past_5weeks ride_hist
		
		  RIGHT JOIN
		
		  (SELECT 
			   DISTINCT date_trunc('week',date(l_date)) AS week_of 
		   from public.date_dim
		   WHERE
			   date(l_date)>=DATE_TRUNC('week', date([[inc_start_date]]))
			   AND l_date<= [[inc_end_date]]
		  ) evaluation_week
		
		  ON date(ride_hist.date_local) >= date(evaluation_week.week_of - INTERVAL '28' DAY)
		  AND date(ride_hist.date_local) < date(evaluation_week.week_of + INTERVAL '7' DAY)
	
	  )
	
	WHERE latest_rank=1
) churn_reactivated_user


LEFT JOIN

(
	SELECT 
	  passenger_id
	  ,min(date_trunc('week',first_bookings_on_taxi_type_simple)) AS week_of
	  FROM 
	  slide.gf_dash_pax_first_booking_order 	
	 GROUP BY 1 
	 HAVING min(date_trunc('week',first_bookings_on_taxi_type_simple)) IS NOT NULL
) new_users

ON churn_reactivated_user.passenger_id = new_users.passenger_id
AND churn_reactivated_user.week_of = new_users.week_of


GROUP BY 1,2 
) churn_reactivation_pax


ON base.week_of = churn_reactivation_pax.week_of
AND base.country = churn_reactivation_pax.country



LEFT JOIN

(
SELECT
churn_reactivated_driver.country,
churn_reactivated_driver.week_of,
SUM(CASE WHEN active_first_1_week = 1 AND (active_last_4_weeks = 0 OR active_last_4_weeks IS NULL) THEN 1 ELSE 0 end) AS churn_drivers,
SUM(CASE WHEN active_last_1_week = 1 AND (active_first_4_weeks = 0 OR active_first_4_weeks IS NULL) AND new_driver.driver_id IS NULL THEN 1 ELSE 0 end) AS reactivated_drivers
FROM

(
SELECT
DISTINCT
week_of
,driver_id
,city
,country
,min_bid_time
,max_bid_time
,CASE WHEN date(min_bid_time)>=date(week_of) - INTERVAL '28' DAY AND date(min_bid_time)<week_of - INTERVAL '21' DAY THEN 1 ELSE 0 END AS active_first_1_week
,CASE WHEN date(max_bid_time)>=date(week_of) - INTERVAL '21' DAY THEN 1 ELSE 0 END AS active_last_4_weeks
,CASE WHEN date(max_bid_time)>=date(week_of) THEN 1 ELSE 0 END AS active_last_1_week
,CASE WHEN date(min_bid_time)>=date(week_of) - INTERVAL '28' DAY AND date(min_bid_time) < date(week_of) THEN 1 ELSE 0 END AS active_first_4_weeks

  FROM

  (
  SELECT
  driver_id,
  evaluation_week.week_of,
  date_local,
  city,
  country,
  row_number() OVER (PARTITION BY driver_id,week_of ORDER BY date_local desc) AS latest_rank,
  max(date_local) OVER (PARTITION BY driver_id,week_of) AS max_bid_time,
  min(date_local) OVER (PARTITION BY driver_id,week_of) AS min_bid_time

  FROM

  latest_bidding_past_5weeks ride_hist

  RIGHT JOIN

  (SELECT DISTINCT date_trunc('week',date(l_date)) AS week_of from public.date_dim WHERE
  date(l_date)>=DATE(DATE_TRUNC('week', date([[inc_start_date]])))
  AND l_date<= [[inc_end_date]]
  ) evaluation_week

  ON date(ride_hist.date_local) >= date(evaluation_week.week_of - INTERVAL '28' DAY)
  AND date(ride_hist.date_local) < date(evaluation_week.week_of + INTERVAL '7' DAY)

  )

  WHERE latest_rank=1) churn_reactivated_driver


LEFT JOIN
(SELECT
     driver_id
	 ,min(DATE_TRUNC('week', first_bid_date)) AS week_of	 
	 FROM dax_first_bid
  GROUP BY 1
) new_driver

ON churn_reactivated_driver.driver_id = new_driver.driver_id
AND churn_reactivated_driver.week_of = new_driver.week_of


GROUP BY 1,2 ) churn_reactivation_driver

ON base.week_of = churn_reactivation_driver.week_of
AND base.country = churn_reactivation_driver.country 
)