/*
    Name: slide.gf_dash_pax_metrics_agg
    Refresh Time: Daily, 21:00 UTC
    Aggregation Mode: Incremental, 30 days lookback
    Lighthouse Dependancy Tables:
        slide.datamart_raw_bookings 18:00
        slide.gf_dash_pax_first_booking_order 21:00
*/

SELECT
	DATE_TRUNC('week',DATE(bookings.date_local)) AS week_of
	,DATE_TRUNC('month',DATE(bookings.date_local)) AS month_of
	,bookings.city_name
	,bookings.country_name
	,bookings.passenger_id
	/** FOR GF every order id is an unique order/booking; For other verticals still use unique booking flag **/
	,sum(CASE 
	     WHEN 
	     	bookings.vertical NOT IN ('GrabFood')
	     THEN 
	     	bookings.is_unique_booking
	     ELSE 1
	     END) AS unique_bookings
	,sum(CASE WHEN bookings.booking_state_simple = 'COMPLETED' THEN 1 ELSE 0 END) AS rides
	,sum(CASE 
	     WHEN 
	     	bookings.vertical IN ('GrabFood') AND bookings.booking_state_simple = 'COMPLETED'  
	     THEN 
	     	cast(gross_merchandise_value AS double)/cast(fx_one_usd AS double) 
	     ELSE 0.0 END) as gmv_usd_gf
	 	/** FOR GF every order id is an unique order/booking**/
	,sum(CASE WHEN bookings.vertical IN ('GrabFood') THEN 1 ELSE 0 end) AS unique_bookings_gf
	,sum(CASE WHEN bookings.vertical IN ('GrabFood') AND booking_state_simple = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_orders_gf
	,sum(CASE WHEN bookings.vertical NOT IN ('GrabFood','GrabExpress') THEN bookings.is_unique_booking ELSE 0 end) AS unique_bookings_transport
	,sum(CASE WHEN bookings.vertical NOT IN ('GrabFood','GrabExpress') AND booking_state_simple = 'COMPLETED' THEN 1 ELSE 0 END) AS rides_transport
    ,MAX(CASE WHEN first_bk.first_bookings_on_taxi_type_simple = date(bookings.date_local) 
            THEN 1 ELSE 0 END) AS new_user
    ,MAX(CASE WHEN first_bk.first_ride_on_taxi_type_simple = date(bookings.date_local) 
            THEN 1 ELSE 0 END) AS new_eater
, DATE(bookings.date_local) AS date_local


  FROM datamart.base_bookings bookings

  LEFT JOIN 
  
  (SELECT 
  passenger_id
  ,'GrabFood' AS vertical
  ,min(date(first_bookings_on_taxi_type_simple)) AS first_bookings_on_taxi_type_simple
  ,min(date(first_ride_on_taxi_type_simple)) AS first_ride_on_taxi_type_simple
  FROM slide.gf_dash_pax_first_booking_order 
  GROUP BY 1,2
  HAVING 
(min(date(first_bookings_on_taxi_type_simple))>= date([[inc_start_date]]) 
  AND min(date(first_bookings_on_taxi_type_simple)) <= date([[inc_end_date]]) )
OR 
 (min(date(first_ride_on_taxi_type_simple))>=  date([[inc_start_date]]) 
  AND min(date(first_ride_on_taxi_type_simple))<= date([[inc_end_date]]) )
 
) first_bk
  
  ON first_bk.passenger_id = bookings.passenger_id
  AND bookings.vertical = first_bk.vertical
    
  WHERE 
	bookings.date_local >= [[inc_start_date]]
	AND bookings.date_local <= [[inc_end_date]]
	GROUP BY 1,2,3,4,5,DATE(bookings.date_local)
  HAVING sum(CASE 
	     WHEN 
	     	bookings.vertical NOT IN ('GrabFood')
	     THEN 
	     	bookings.is_unique_booking
	     ELSE 1
	     END) > 0