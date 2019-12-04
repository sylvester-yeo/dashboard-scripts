SELECT 
*
FROM
	
	(


	
			SELECT 
			CASE 
				WHEN country_name IS NULL AND city_name IS NULL THEN 'By Region'
				WHEN country_name IS NOT NULL AND city_name IS NULL THEN 'By Country'
				WHEN city_name IS NOT NULL THEN 'By City'
			END AS by_city_country
			
			,CASE 
				WHEN date_local IS NOT NULL THEN 'By Day'
				WHEN week_of IS NOT NULL THEN 'By Week'
				WHEN month_of IS NOT NULL THEN 'By Month'
			END AS by_day_week_month
		
			
			,CASE 
				WHEN date_local IS NOT NULL THEN date_local
				WHEN week_of IS NOT NULL THEN week_of
				WHEN month_of IS NOT NULL THEN month_of
			END AS time_period
			
			
			,CASE 
				WHEN country_name IS NULL AND city_name IS NULL THEN 'All'
				ELSE country_name END AS country
				
			,CASE 
				WHEN city_name IS NULL THEN 'All'
				ELSE city_name END AS city
			
			,CASE 
				WHEN business_model IS NULL THEN 'All'
				ELSE business_model
			END AS business_model	
			
			,CASE 
				WHEN cashless_status IS NULL THEN 'All'
				ELSE cashless_status
			END AS cashless_status	
			
			,sum(gmv_usd_gf) as gmv_gf
			,sum(CASE WHEN restaurant_partner_status = 'partner' THEN gmv_usd_gf ELSE 0 END) as partner_gmv_usd
			,SUM(gmv_local) AS gmv_gf_local
			,SUM(CASE WHEN restaurant_partner_status = 'partner' THEN gmv_local ELSE 0 END) AS partner_gmv_local
			,sum(completed_orders_gf) as completed_orders_gf
			,sum(CASE WHEN restaurant_partner_status = 'partner' THEN completed_orders_gf ELSE 0 END) as partner_completed_orders_gf
			,sum(sub_total) as sub_total
			,sum(CASE WHEN restaurant_partner_status = 'partner' THEN sub_total ELSE 0 END) as partner_sub_total
			,SUM(sub_total_local) AS sub_total_local
			,SUM(CASE WHEN restaurant_partner_status = 'partner' THEN sub_total_local ELSE 0 END) AS partner_sub_total_local
			,sum(basket_size) as basket_size
			,sum(CASE WHEN restaurant_partner_status = 'partner' THEN basket_size ELSE 0 END) as partner_basket_size
			,SUM(basket_size_local) AS basket_size_local
			,SUM(CASE WHEN restaurant_partner_status = 'partner' THEN basket_size_local ELSE 0 END) AS partner_basket_size_local
			,sum(mex_commission) as partner_commission
			,SUM(mex_commission_local) AS partner_commission_local
            ,SUM(base_for_mex_commission) as base_for_mex_commission
            ,SUM(base_for_mex_commission_local) as base_for_mex_commission_local
			,sum(driver_commission) as driver_commission
			,SUM(driver_commission_local) AS driver_commission_local
			,sum(delivery_fare_gf) as delivery_fare_gf
			,sum(CASE WHEN restaurant_partner_status = 'partner' THEN delivery_fare_gf ELSE 0 END) as partner_delivery_fare_gf
			,SUM(delivery_fare_gf_local) AS delivery_fare_gf_local
			,SUM(CASE WHEN restaurant_partner_status = 'partner' THEN delivery_fare_gf_local ELSE 0 END) AS partner_delivery_fare_gf_local
			,SUM(dax_delivery_fare) AS dax_delivery_fare
			,SUM(CASE WHEN restaurant_partner_status = 'partner' THEN dax_delivery_fare ELSE 0 END) AS partner_dax_delivery_fare
			,SUM(dax_delivery_fare_local) AS dax_delivery_fare_local
			,SUM(CASE WHEN restaurant_partner_status = 'partner' THEN dax_delivery_fare_local ELSE 0 END) AS partner_dax_delivery_fare_local
			
			,sum(time_from_order_create_to_completed) as total_time_order_create_to_completed
			,sum(completed_orders_without_null_time_2) as completed_orders_without_null_time_2
			,sum(delivery_distance_gf) as delivery_distance_gf
			,sum(driver_distance_gf) as driver_distance_gf
			
			,sum(cancellations) as total_cancellations
			,sum(cancellations_passenger) as pax_cancellations
			,sum(cancellations_driver) as dax_cancellations
			,sum(cancellations_operator) as operator_cancellations
			,sum(cancellations_merchant) AS mex_cancellations
			
			,sum(pre_accept_cancellations) AS pre_accept_cancellations
			,sum(pre_accept_cancellations_pax) AS pre_accept_cancellations_pax
			,sum(pre_accept_cancellations_operator) AS pre_accept_cancellations_operator
			,sum(pre_accept_expired_orders) AS pre_accept_expired_orders
			,sum(pre_allocation_cancellations) AS pre_allocation_cancellations
			,sum(pre_allocation_cancellations_pax) AS pre_allocation_cancellations_pax
			,sum(pre_allocation_cancellations_operator) AS pre_allocation_cancellations_operator
						
--			,NULL AS paid_cancellation
--			,NULL AS non_paid_cancellation
			
			,sum(all_incoming_orders_gf) as all_incoming_orders_gf
			,sum(mex_accepted_orders_gf) AS mex_accepted_orders_gf
			,sum(allocated_orders) as allocated_orders
			,sum(unallocated_orders) as unallocated_orders
			,sum(first_allocated_orders) as first_allocated_orders
			,sum(effective_first_allocated_orders) as effective_first_allocated_orders
			
			,sum(promo_expense) as promo_expense
			,sum(promo_expense_local) as promo_expense_local
			,sum(promo_code_expense) as promo_code_expense
			,sum(promo_code_expense_local) as promo_code_expense_local
			,sum(promo_incoming_orders) as promo_incoming_orders
			,sum(promo_completed_orders) as promo_completed_orders
			
			,sum(num_of_total_items) as num_of_total_items
			,sum(total_item_price_usd) as total_item_price_usd
			,sum(completed_orders_gf_item) as completed_orders_gf_item
			,sum(jobs_accepted) as jobs_bid
			,sum(jobs_received) as jobs_received
			,sum(jobs_unread) as jobs_unread
			,sum(COALESCE(incentives_usd,0)+COALESCE(spot_incentive_bonus_usd,0)) as incentive_payout_usd
			,SUM(COALESCE(incentives_local,0)+COALESCE(spot_incentive_bonus_local,0)) AS incentive_payout_local
			,sum(COALESCE(incentives_usd,0)+COALESCE(spot_incentive_bonus_usd,0)+coalesce(dax_delivery_fare,0)-coalesce(delivery_fare_gf,0)) as incentive_payout_usd_w_tsp
			,SUM(COALESCE(incentives_local,0)+COALESCE(spot_incentive_bonus_local,0)+coalesce(dax_delivery_fare_local,0)-coalesce(delivery_fare_gf_local,0)) AS incentive_payout_local_w_tsp		

            --takeaway metrics
            ,sum(total_takeaway_orders) as total_takeaway_orders
            ,sum(total_takeaway_completed_orders) as total_takeaway_completed_orders
            ,sum(takeaway_gmv_local) as takeaway_gmv_local
            ,sum(takeaway_gmv_usd) as takeaway_gmv_usd
            ,sum(takeaway_mex_commission_local) as takeaway_mex_commission_local
            ,sum(takeaway_mex_commission_usd) as takeaway_mex_commission_usd
            ,sum(takeaway_base_for_mex_commission_local) as takeaway_base_for_mex_commission_local
            ,sum(takeaway_base_for_mex_commission) as takeaway_base_for_mex_commission
            ,sum(takeaway_basket_size_usd) as takeaway_basket_size_usd
            ,sum(takeaway_basket_size_local) as takeaway_basket_size_local
            ,sum(takeaway_sub_total_usd) as takeaway_sub_total_usd
            ,sum(takeaway_sub_total_local) as takeaway_sub_total_local
            ,sum(takeaway_time_from_order_create_to_completed) as takeaway_time_from_order_create_to_completed

			FROM 
			(
				SELECT 
				date_trunc('week',date_local) AS week_of,
				date_trunc('month',date_local) AS month_of,	
				*
				FROM
				slide.gf_mex_level_daily_metrics_v2 --to change once original table is rebuild			
				WHERE partition_date_local >= date_trunc('month',date([[inc_start_date]])) - INTERVAL '1' MONTH
				AND partition_date_local <= date([[inc_end_date]])
			)
			

			GROUP BY GROUPING SETS 
			(
				(date_local,country_name),(week_of,country_name),(month_of,country_name),
				(date_local,country_name,business_model),(week_of,country_name,business_model),(month_of,country_name,business_model),
				(date_local,country_name,cashless_status),(week_of,country_name,cashless_status),(month_of,country_name,cashless_status),
				(date_local,country_name,business_model,cashless_status),(week_of,country_name,business_model,cashless_status),(month_of,country_name,business_model,cashless_status),
				
				
				(date_local,country_name,city_name),(week_of,country_name,city_name),(month_of,country_name,city_name),
				(date_local,country_name,city_name,business_model),(week_of,country_name,city_name,business_model),(month_of,country_name,city_name,business_model),
				(date_local,country_name,city_name,cashless_status),(week_of,country_name,city_name,cashless_status),(month_of,country_name,city_name,cashless_status),
				(date_local,country_name,city_name,business_model,cashless_status),(week_of,country_name,city_name,business_model,cashless_status),(month_of,country_name,city_name,business_model,cashless_status),
				
				
				(date_local),(week_of),(month_of),
				(date_local,business_model),(week_of,business_model),(month_of,business_model),
				(date_local,cashless_status),(week_of,cashless_status),(month_of,cashless_status),	
				(date_local,business_model,cashless_status),(week_of,business_model,cashless_status),(month_of,business_model,cashless_status)				
			)
		 ) 
	
WHERE 
time_period >= (CASE 
	WHEN by_day_week_month = 'By Day' THEN date([[inc_start_date]])
	WHEN by_day_week_month = 'By Week' THEN date_trunc('week',date([[inc_start_date]]))
	WHEN by_day_week_month = 'By Month' THEN date_trunc('month',date([[inc_start_date]]))
 END)