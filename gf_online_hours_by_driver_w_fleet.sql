/*
    Name: slide.gf_online_hours_by_driver_w_fleet
    Refresh Time: Daily, 21:00 UTC
    Aggregation Mode: Incremental, 45 days lookback
    Lighthouse Dependancy Tables:
        slide.agg_vertical_driver_online_hours 21:00
*/

SELECT 
bk_metrics.driver_id,
bk_metrics.dedicated_fleet,
bk_metrics.country_name,
bk_metrics.city_name,
bk_metrics.completed_orders_gf,
/**** Keep the drivers with NULL online seconds here in case needed for investigation ***/
bk_metrics.total_online_seconds,
bk_metrics.total_transit_seconds,
bk_metrics.incentives_payout_dm,
bk_metrics.spot_incentives_bonus,
bk_metrics.dax_delivery_fee,
bk_metrics.pax_delivery_fee,
/***** Keep job bid here so that we can calculate AR/TPH for active dax only ****/
COALESCE(job_metrics.jobs_bid_gf,0) AS jobs_bid_gf,
COALESCE(job_metrics.jobs_received_gf,0) AS jobs_received_gf,
bk_metrics.date_local

fROM

(	SELECT 
	jd.driver_id,
	jd.dedicated_fleet,
	jd.country_name,
	jd.city_name,
	jd.completed_orders_gf,
	
	/*** if driver's online hours is missing, exclude them from TPH caculation **/
	os.total_online_seconds,
	os.total_transit_seconds,
	
	/*** In case Incentives by dedicated/shared fleet is required in the future ***/
	jd.incentives_payout_dm,
	jd.spot_incentives_bonus,
	jd.dax_delivery_fee,
	jd.pax_delivery_fee,
			
	jd.date_local
	
	FROM 
	
	(
		SELECT 
		bookings.date_local,
		bookings.city_id,
		ct.city_name,
		ct.country_name,
		bookings.driver_id,
		CASE 
			WHEN (df_non_vn.driver_id IS NOT NULL 
				OR df_vn.driver_id IS NOT NULL 
				OR ct.country_name IN ('Singapore','Malaysia')  )
			THEN 1 ELSE 0 END AS dedicated_fleet,
		sum(CASE WHEN is_completed_state = TRUE THEN 1 ELSE 0 END) AS completed_orders_gf,
		sum(coalesce(payout,0)) AS incentives_payout_dm,
		sum(
			   CASE WHEN is_completed_state = TRUE THEN
			     COALESCE(spot_incentives_bonus,0)
			     ELSE 0 END
		     ) AS spot_incentives_bonus,
		sum(
			   CASE WHEN is_completed_state = TRUE THEN
                               (dax_fare_upper_bound+dax_fare_lower_bound)/2.0
                        ELSE 0 END ) AS dax_delivery_fee,
		sum(CASE WHEN is_completed_state = TRUE THEN
                             indicative_fare
                         ELSE 0 END) AS pax_delivery_fee
		
		FROM 
		(
			SELECT 
			*
			,COALESCE(TRY_CAST(json_extract_scalar(json_extract(candidate_metadata, '$.spotIncentive'), '$.SIAmount') AS DOUBLE), 0) AS spot_incentives_bonus
	        ,COALESCE(TRY_CAST(json_extract_scalar(booking_metadata, '$.daxDeliveryFee.upperBound') AS DOUBLE), indicative_fare) AS dax_fare_upper_bound
	        ,COALESCE(TRY_CAST(json_extract_scalar(booking_metadata, '$.daxDeliveryFee.lowerBound') AS DOUBLE), indicative_fare) AS dax_fare_lower_bound
	        
	        FROM
			public.prejoin_grabfood 
			WHERE
			is_test_booking = FALSE
			AND date_local >= date([[inc_start_date]])
			AND date_local <= DATE([[inc_end_date]])
			AND DATE(partition_date) >= date([[inc_start_date]]) - interval '1' day
			AND DATE(partition_date) <= DATE([[inc_end_date]]) + interval '1' DAY
		) bookings
		
		LEFT JOIN 
		(
			 SELECT booking_code, SUM(payout_per_txn) AS payout
			 FROM datamart.incentives_payout_per_txn
			 WHERE date_local >= [[inc_start_date]]
			 AND date_local <= [[inc_end_date]]
			 AND is_qualified = 1

			 GROUP BY booking_code
		) incentives 
		ON bookings.booking_code = incentives.booking_code
		AND bookings.is_completed_state = TRUE
		
		LEFT JOIN 
		(SELECT city_id,date_local,driver_id,count(1) as cnt From 
		 slide.gf_dedicated_fleet_with_incentives
		 WHERE  date_local >= [[inc_start_date]]
		      AND date_local <= [[inc_end_date]]
		     AND fleet_group = 'Dedicated Fleet' 
		 GROUP BY 1,2,3
		) df_non_vn
		 ON bookings.driver_id = df_non_vn.driver_id
		 and bookings.date_local = date(df_non_vn.date_local)
		 and bookings.city_id = df_non_vn.city_id
		
		 LEFT JOIN 
		(SELECT city_id,date_local,driver_id,count(1) as cnt From 
		 slide.vn_df_list_daily
		 WHERE  date_local >= [[inc_start_date]]
		      AND date_local <= [[inc_end_date]]
		     AND is_df=TRUE
		 GROUP BY 1,2,3
		 ) df_vn
		 ON bookings.driver_id = df_vn.driver_id
		 and bookings.date_local = date(df_vn.date_local)
		 and bookings.city_id= df_vn.city_id
		
		LEFT JOIN datamart.dim_cities_countries ct ON bookings.city_id = ct.city_id
		
		LEFT JOIN datamart.ref_exchange_rates exchange
        ON (ct.country_id = exchange.country_id
        AND DATE_TRUNC('month', bookings.date_local) = DATE_TRUNC('month', exchange.start_date))
		

		
		GROUP BY 1,2,3,4,5,6
	) jd
	
	LEFT JOIN 
	
	(
		SELECT 
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
		GROUP BY 1,2,3
	) os
	
	ON jd.driver_id = os.driver_id
	AND jd.city_id = os.city_id
	AND jd.date_local = os.date_local

) bk_metrics

LEFT JOIN 


(
	SELECT 
	driver_id,
	city_name,
	date_local,
	sum(jobs_bid) AS jobs_bid_gf,
	sum(jobs_received) AS jobs_received_gf
	from
	slide.datamart_agg_driver_bookings bk
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
	GROUP BY 1,2,3
) job_metrics 

ON bk_metrics.driver_id = job_metrics.driver_id 
AND bk_metrics.city_name = job_metrics.city_name
AND bk_metrics.date_local = date(job_metrics.date_local)