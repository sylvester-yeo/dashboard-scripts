/*
    Name: slide.gf_dash_active_mex_all_dim
    Refresh Time: Daily, 21:00 UTC
    Aggregation Mode: Incremental, lookback window 45 days
    Lighthouse Dependancy Tables:
        slide.gf_dash_mex_join_date 21:00

*/

WITH new_mex_by_model AS 
(
	 SELECT 
	 country_name,
	 city_name,
	 merchant_id,
	 CASE WHEN model_type = 'IM' THEN 'Integrated' ELSE 'Concierge' END AS model_type,
	 join_date AS join_date,
	 date_trunc('week',join_date) AS join_week,
	 date_trunc('month',join_date) AS join_month
	 from slide.gf_dash_mex_join_date
	 WHERE 
	 join_date >= date_trunc('month',date([[inc_start_date]])) - INTERVAL '1' MONTH
	 AND join_date <= date([[inc_end_date]])

 )

,new_partner_by_model AS 
(
	SELECT 
	 country_name,
	 city_name,
	 merchant_id,
	 CASE WHEN model_type = 'IM' THEN 'Integrated' ELSE 'Concierge' END AS model_type,
	 partner_join_date as upgrade_date,
	 date_trunc('week',partner_join_date) AS upgrade_week,
	 date_trunc('month',partner_join_date) AS upgrade_month,
	 case when partner_join_date=join_date then 1 else 0 end as same_day,
	 case when date_trunc('week',partner_join_date)=date_trunc('week',join_date) then 1 else 0 end as same_week,
	 case when date_trunc('month',partner_join_date)=date_trunc('month',join_date) then 1 else 0 end as same_month
	 from slide.gf_dash_mex_join_date
	 WHERE 
	 partner_join_date >= date_trunc('month',date([[inc_start_date]])) - INTERVAL '1' MONTH 
	 AND partner_join_date <= date([[inc_end_date]])
 )
 
 ,new_mex_all AS 
(
	 SELECT 
	 country_name,
	 city_name,
	 merchant_id,
	 min(join_date) AS join_date,
	 min(date_trunc('week',join_date)) AS join_week,
	 min(date_trunc('month',join_date)) AS join_month
	 from slide.gf_dash_mex_join_date
	 GROUP BY 1,2,3 
        HAVING min(join_date)>=date_trunc('month',date([[inc_start_date]])) - INTERVAL '1' MONTH 
       AND min(join_date)<=date([[inc_end_date]])
 )

,new_partner_all AS 
(
	SELECT 
	*,
	case when upgrade_date=join_date then 1 else 0 end as same_day,
	case when date_trunc('week',upgrade_date)=date_trunc('week',join_date) then 1 else 0 end as same_week,
	case when date_trunc('month',upgrade_date)=date_trunc('month',join_date) then 1 else 0 end as same_month
	FROM
		(SELECT 
		 country_name,
		 city_name,
		 merchant_id,
	     min(join_date) AS join_date,
		 min(partner_join_date) as upgrade_date,
		 min(date_trunc('week',partner_join_date)) AS upgrade_week,
		 min(date_trunc('month',partner_join_date)) AS upgrade_month
		 from slide.gf_dash_mex_join_date
		 GROUP BY 1,2,3)
	 WHERE 
	 upgrade_date >= date_trunc('month',date([[inc_start_date]])) - INTERVAL '1' MONTH
	 AND upgrade_date <= date([[inc_end_date]])
 )

SELECT 
by_city_country
,by_day_week_month
,time_period
,country_name
,city_name
,business_model
,cashless_status
,COUNT(CASE WHEN completed_orders>0 THEN merchant_id ELSE NULL END) AS active_restaurant
,COUNT(CASE WHEN partner_completed_orders>0 THEN merchant_id ELSE NULL END) AS active_partner_restaurant
,COUNT(CASE WHEN new_merchant_completed_orders>0 THEN merchant_id ELSE NULL END) AS active_new_restaurant
,COUNT(CASE WHEN new_partner_completed_orders>0 THEN merchant_id ELSE NULL END) AS active_new_partner
,COUNT(CASE WHEN prev_new_merchant_completed_orders>0 THEN merchant_id ELSE NULL END) AS prev_active_new_restaurant
,COUNT(CASE WHEN prev_new_partner_completed_orders>0 THEN merchant_id ELSE NULL END) AS prev_active_new_partner
,SUM(completed_orders) AS completed_orders
,SUM(partner_completed_orders) AS partner_completed_orders
,SUM(new_merchant_completed_orders) AS new_merchant_completed_orders
,SUM(new_partner_completed_orders) AS new_partner_completed_orders
,SUM(prev_new_merchant_completed_orders) AS prev_new_merchant_completed_orders
,SUM(prev_new_partner_completed_orders) AS prev_new_partner_completed_orders
,SUM(gmv_usd) AS gmv_usd
,SUM(partner_gmv_usd) AS partner_gmv_usd
,SUM(new_merchant_gmv_usd) AS new_merchant_gmv_usd
,SUM(new_partner_gmv_usd) AS new_partner_gmv_usd
,SUM(prev_new_merchant_gmv_usd) AS prev_new_merchant_gmv_usd
,SUM(prev_new_partner_gmv_usd) AS prev_new_partner_gmv_usd
,SUM(new_merchant_gmv_local) AS new_merchant_gmv_local
,SUM(new_partner_gmv_local) AS new_partner_gmv_local
,SUM(prev_new_merchant_gmv_local) AS prev_new_merchant_gmv_local
,SUM(prev_new_partner_gmv_local) AS prev_new_partner_gmv_local

FROM 

(
	SELECT 
	by_city_country
	,by_day_week_month
	,time_period
	,country_name
	,city_name
	,business_model
	,'All' AS cashless_status	
	,merchant_id
	 
	/**** completed orders ****/
	,completed_orders
	,partner_completed_orders
	,CASE 
		WHEN business_model = 'All'
		THEN
			CASE
			 	WHEN by_day_week_month = 'By Day' THEN new_merchant_completed_orders_day
			 	WHEN by_day_week_month = 'By Week' THEN new_merchant_completed_orders_week
			 	WHEN by_day_week_month = 'By Month' THEN new_merchant_completed_orders_month
			 END
		ELSE
			CASE
			 	WHEN by_day_week_month = 'By Day' THEN new_merchant_completed_orders_day_model
			 	WHEN by_day_week_month = 'By Week' THEN new_merchant_completed_orders_week_model
			 	WHEN by_day_week_month = 'By Month' THEN new_merchant_completed_orders_month_model
			 END
	 END AS new_merchant_completed_orders
	 
	 
	,CASE
		WHEN business_model = 'All'
		THEN
			CASE
			 	WHEN by_day_week_month = 'By Day' THEN new_partner_completed_orders_day
			 	WHEN by_day_week_month = 'By Week' THEN new_partner_completed_orders_week
			 	WHEN by_day_week_month = 'By Month' THEN new_partner_completed_orders_month
			END
		ELSE
			CASE
			 	WHEN by_day_week_month = 'By Day' THEN new_partner_completed_orders_day_model
			 	WHEN by_day_week_month = 'By Week' THEN new_partner_completed_orders_week_model
			 	WHEN by_day_week_month = 'By Month' THEN new_partner_completed_orders_month_model
			END
	END AS new_partner_completed_orders
	
	
	
	,CASE
		WHEN business_model = 'All'
		THEN
			CASE
			 	WHEN by_day_week_month = 'By Day' THEN prev_new_merchant_completed_orders_day
			 	WHEN by_day_week_month = 'By Week' THEN prev_new_merchant_completed_orders_week
			 	WHEN by_day_week_month = 'By Month' THEN prev_new_merchant_completed_orders_month
			END
		ELSE
			CASE
			 	WHEN by_day_week_month = 'By Day' THEN prev_new_merchant_completed_orders_day_model
			 	WHEN by_day_week_month = 'By Week' THEN prev_new_merchant_completed_orders_week_model
			 	WHEN by_day_week_month = 'By Month' THEN prev_new_merchant_completed_orders_month_model
			END
	END AS prev_new_merchant_completed_orders
	
	
	,CASE
		WHEN business_model = 'All'
		THEN 
			CASE 
			 	WHEN by_day_week_month = 'By Day' THEN prev_new_partner_completed_orders_day
			 	WHEN by_day_week_month = 'By Week' THEN prev_new_partner_completed_orders_week
			 	WHEN by_day_week_month = 'By Month' THEN prev_new_partner_completed_orders_month
			END
		ELSE
			CASE 
			 	WHEN by_day_week_month = 'By Day' THEN prev_new_partner_completed_orders_day_model
			 	WHEN by_day_week_month = 'By Week' THEN prev_new_partner_completed_orders_week_model
			 	WHEN by_day_week_month = 'By Month' THEN prev_new_partner_completed_orders_month_model
			END
	END AS prev_new_partner_completed_orders	
	
	
	/***** gmv usd *****/
	,gmv_usd
	,partner_gmv_usd
	,CASE 
		WHEN business_model = 'All'
		THEN
			CASE
			 	WHEN by_day_week_month = 'By Day' THEN new_merchant_gmv_usd_day
			 	WHEN by_day_week_month = 'By Week' THEN new_merchant_gmv_usd_week
			 	WHEN by_day_week_month = 'By Month' THEN new_merchant_gmv_usd_month
			 END
		ELSE
			CASE
			 	WHEN by_day_week_month = 'By Day' THEN new_merchant_gmv_usd_day_model
			 	WHEN by_day_week_month = 'By Week' THEN new_merchant_gmv_usd_week_model
			 	WHEN by_day_week_month = 'By Month' THEN new_merchant_gmv_usd_month_model
			 END
	 END AS new_merchant_gmv_usd
	 
	 
	,CASE
		WHEN business_model = 'All'
		THEN
			CASE
			 	WHEN by_day_week_month = 'By Day' THEN new_partner_gmv_usd_day
			 	WHEN by_day_week_month = 'By Week' THEN new_partner_gmv_usd_week
			 	WHEN by_day_week_month = 'By Month' THEN new_partner_gmv_usd_month
			END
		ELSE
			CASE
			 	WHEN by_day_week_month = 'By Day' THEN new_partner_gmv_usd_day_model
			 	WHEN by_day_week_month = 'By Week' THEN new_partner_gmv_usd_week_model
			 	WHEN by_day_week_month = 'By Month' THEN new_partner_gmv_usd_month_model
			END
	END AS new_partner_gmv_usd
	
	
	
	,CASE
		WHEN business_model = 'All'
		THEN
			CASE
			 	WHEN by_day_week_month = 'By Day' THEN prev_new_merchant_gmv_usd_day
			 	WHEN by_day_week_month = 'By Week' THEN prev_new_merchant_gmv_usd_week
			 	WHEN by_day_week_month = 'By Month' THEN prev_new_merchant_gmv_usd_month
			END
		ELSE
			CASE
			 	WHEN by_day_week_month = 'By Day' THEN prev_new_merchant_gmv_usd_day_model
			 	WHEN by_day_week_month = 'By Week' THEN prev_new_merchant_gmv_usd_week_model
			 	WHEN by_day_week_month = 'By Month' THEN prev_new_merchant_gmv_usd_month_model
			END
	END AS prev_new_merchant_gmv_usd
	
	
	,CASE
		WHEN business_model = 'All'
		THEN 
			CASE 
			 	WHEN by_day_week_month = 'By Day' THEN prev_new_partner_gmv_usd_day
			 	WHEN by_day_week_month = 'By Week' THEN prev_new_partner_gmv_usd_week
			 	WHEN by_day_week_month = 'By Month' THEN prev_new_partner_gmv_usd_month
			END
		ELSE
			CASE 
			 	WHEN by_day_week_month = 'By Day' THEN prev_new_partner_gmv_usd_day_model
			 	WHEN by_day_week_month = 'By Week' THEN prev_new_partner_gmv_usd_week_model
			 	WHEN by_day_week_month = 'By Month' THEN prev_new_partner_gmv_usd_month_model
			END
	END AS prev_new_partner_gmv_usd
	
	/***** gmv local *****/
	,CASE
		WHEN business_model = 'All'
		THEN 
			CASE 
			 	WHEN by_day_week_month = 'By Day' THEN new_merchant_gmv_local_day
			 	WHEN by_day_week_month = 'By Week' THEN new_merchant_gmv_local_week
			 	WHEN by_day_week_month = 'By Month' THEN new_merchant_gmv_local_month
			END
		ELSE
			CASE 
			 	WHEN by_day_week_month = 'By Day' THEN new_merchant_gmv_local_day_model
			 	WHEN by_day_week_month = 'By Week' THEN new_merchant_gmv_local_week_model
			 	WHEN by_day_week_month = 'By Month' THEN new_merchant_gmv_local_month_model
			END
	 END AS new_merchant_gmv_local
	 
	 
	,CASE
		WHEN business_model = 'All'
		THEN
			CASE 
			 	WHEN by_day_week_month = 'By Day' THEN new_partner_gmv_local_day
			 	WHEN by_day_week_month = 'By Week' THEN new_partner_gmv_local_week
			 	WHEN by_day_week_month = 'By Month' THEN new_partner_gmv_local_month
			END
		ELSE
			CASE 
			 	WHEN by_day_week_month = 'By Day' THEN new_partner_gmv_local_day_model
			 	WHEN by_day_week_month = 'By Week' THEN new_partner_gmv_local_week_model
			 	WHEN by_day_week_month = 'By Month' THEN new_partner_gmv_local_month_model
			END
	END AS new_partner_gmv_local
	
	
	
	,CASE
		WHEN business_model = 'All'
		THEN
			CASE 
			 	WHEN by_day_week_month = 'By Day' THEN prev_new_merchant_gmv_local_day
			 	WHEN by_day_week_month = 'By Week' THEN prev_new_merchant_gmv_local_week
			 	WHEN by_day_week_month = 'By Month' THEN prev_new_merchant_gmv_local_month
			END		 	
		ELSE
			CASE 
			 	WHEN by_day_week_month = 'By Day' THEN prev_new_merchant_gmv_local_day_model
			 	WHEN by_day_week_month = 'By Week' THEN prev_new_merchant_gmv_local_week_model
			 	WHEN by_day_week_month = 'By Month' THEN prev_new_merchant_gmv_local_month_model
			END		
	END AS prev_new_merchant_gmv_local
	
	,CASE
		WHEN business_model = 'All'
		THEN 
			CASE 
			 	WHEN by_day_week_month = 'By Day' THEN prev_new_partner_gmv_local_day
			 	WHEN by_day_week_month = 'By Week' THEN prev_new_partner_gmv_local_week
			 	WHEN by_day_week_month = 'By Month' THEN prev_new_partner_gmv_local_month
			END		
		ELSE
			CASE 
			 	WHEN by_day_week_month = 'By Day' THEN prev_new_partner_gmv_local_day_model
			 	WHEN by_day_week_month = 'By Week' THEN prev_new_partner_gmv_local_week_model
			 	WHEN by_day_week_month = 'By Month' THEN prev_new_partner_gmv_local_month_model
			END		
	END AS prev_new_partner_gmv_local
	
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
			ELSE country_name END AS country_name
			
		,CASE 
			WHEN city_name IS NULL THEN 'All'
			ELSE city_name END AS city_name
		
		,CASE 
			WHEN business_model IS NULL THEN 'All'
			ELSE business_model
		END AS business_model	
		,merchant_id
	
		
	/**** COMPLETED ORDERS *******/
		/** all merchant completed orders ***/	
		,SUM(completed_orders_gf) AS completed_orders
	
		/** all partner merchant completed orders ***/	
		,SUM(CASE WHEN restaurant_partner_status='partner' THEN completed_orders_gf ELSE 0 END) AS partner_completed_orders	
		
		/** new merchant completed orders **/
		,SUM(CASE WHEN new_mex_by_day=1 THEN completed_orders_gf ELSE NULL END) AS new_merchant_completed_orders_day
		,SUM(CASE WHEN new_mex_by_week=1 THEN completed_orders_gf ELSE NULL END) AS new_merchant_completed_orders_week
		,SUM(CASE WHEN new_mex_by_month=1 THEN completed_orders_gf ELSE NULL END) AS new_merchant_completed_orders_month
	
		/** new merchant completed orders by model**/
		,SUM(CASE WHEN new_mex_by_model_day=1 THEN completed_orders_gf ELSE NULL END) AS new_merchant_completed_orders_day_model
		,SUM(CASE WHEN new_mex_by_model_week=1 THEN completed_orders_gf ELSE NULL END) AS new_merchant_completed_orders_week_model
		,SUM(CASE WHEN new_mex_by_model_month=1 THEN completed_orders_gf ELSE NULL END) AS new_merchant_completed_orders_month_model
	
		/** new partner completed orders **/
		,SUM(CASE WHEN new_partner_by_day=1 AND restaurant_partner_status='partner' THEN completed_orders_gf ELSE NULL END) AS new_partner_completed_orders_day
		,SUM(CASE WHEN new_partner_by_week=1 AND restaurant_partner_status='partner' THEN completed_orders_gf ELSE NULL END) AS new_partner_completed_orders_week
		,SUM(CASE WHEN new_partner_by_month=1 AND restaurant_partner_status='partner' THEN completed_orders_gf ELSE NULL END) AS new_partner_completed_orders_month
	
		/** new partner completed orders by model **/
		,SUM(CASE WHEN new_partner_by_model_day=1 AND restaurant_partner_status='partner' THEN completed_orders_gf ELSE NULL END) AS new_partner_completed_orders_day_model
		,SUM(CASE WHEN new_partner_by_model_week=1 AND restaurant_partner_status='partner' THEN completed_orders_gf ELSE NULL END) AS new_partner_completed_orders_week_model
		,SUM(CASE WHEN new_partner_by_model_month=1 AND restaurant_partner_status='partner' THEN completed_orders_gf ELSE NULL END) AS new_partner_completed_orders_month_model	
			
		
		/** prev new merchant completed orders **/
		,SUM(CASE WHEN prev_new_mex_by_day=1 THEN completed_orders_gf ELSE NULL END) AS prev_new_merchant_completed_orders_day
		,SUM(CASE WHEN prev_new_mex_by_week=1 THEN completed_orders_gf ELSE NULL END) AS prev_new_merchant_completed_orders_week
		,SUM(CASE WHEN prev_new_mex_by_month=1 THEN completed_orders_gf ELSE NULL END) AS prev_new_merchant_completed_orders_month
	
		/** prev new merchant completed orders by model**/
		,SUM(CASE WHEN prev_new_mex_by_model_day=1 THEN completed_orders_gf ELSE NULL END) AS prev_new_merchant_completed_orders_day_model
		,SUM(CASE WHEN prev_new_mex_by_model_week=1 THEN completed_orders_gf ELSE NULL END) AS prev_new_merchant_completed_orders_week_model
		,SUM(CASE WHEN prev_new_mex_by_model_month=1 THEN completed_orders_gf ELSE NULL END) AS prev_new_merchant_completed_orders_month_model	
		
		/** prev new partner completed orders **/
		,SUM(CASE WHEN prev_new_partner_by_day=1 AND restaurant_partner_status='partner' THEN completed_orders_gf ELSE NULL END) AS prev_new_partner_completed_orders_day
		,SUM(CASE WHEN prev_new_partner_by_week=1 AND restaurant_partner_status='partner' THEN completed_orders_gf ELSE NULL END) AS prev_new_partner_completed_orders_week
		,SUM(CASE WHEN prev_new_partner_by_month=1 AND restaurant_partner_status='partner' THEN completed_orders_gf ELSE NULL END) AS prev_new_partner_completed_orders_month
	
		/** prev new partner completed orders by model **/
		,SUM(CASE WHEN prev_new_partner_by_model_day=1 AND restaurant_partner_status='partner' THEN completed_orders_gf ELSE NULL END) AS prev_new_partner_completed_orders_day_model
		,SUM(CASE WHEN prev_new_partner_by_model_week=1 AND restaurant_partner_status='partner' THEN completed_orders_gf ELSE NULL END) AS prev_new_partner_completed_orders_week_model
		,SUM(CASE WHEN prev_new_partner_by_model_month=1 AND restaurant_partner_status='partner' THEN completed_orders_gf ELSE NULL END) AS prev_new_partner_completed_orders_month_model	
			
		
		
	/**** GMV *******/	
		
		/** all merchant gmv ***/	
		,SUM(gmv_usd_gf) AS gmv_usd
	
		/** all partner merchant gmv ***/	
		,SUM(CASE WHEN restaurant_partner_status='partner' THEN gmv_usd_gf ELSE 0 END) AS partner_gmv_usd	
		
		/** new merchant gmv usd **/
		,SUM(CASE WHEN new_mex_by_day=1 THEN gmv_usd_gf ELSE NULL END) AS new_merchant_gmv_usd_day
		,SUM(CASE WHEN new_mex_by_week=1 THEN gmv_usd_gf ELSE NULL END) AS new_merchant_gmv_usd_week
		,SUM(CASE WHEN new_mex_by_month=1 THEN gmv_usd_gf ELSE NULL END) AS new_merchant_gmv_usd_month
	
		/** new merchant gmv usd by model**/
		,SUM(CASE WHEN new_mex_by_model_day=1 THEN gmv_usd_gf ELSE NULL END) AS new_merchant_gmv_usd_day_model
		,SUM(CASE WHEN new_mex_by_model_week=1 THEN gmv_usd_gf ELSE NULL END) AS new_merchant_gmv_usd_week_model
		,SUM(CASE WHEN new_mex_by_model_month=1 THEN gmv_usd_gf ELSE NULL END) AS new_merchant_gmv_usd_month_model	
		
		/** new partner gmv usd **/
		,SUM(CASE WHEN new_partner_by_day=1 AND restaurant_partner_status='partner' THEN gmv_usd_gf ELSE NULL END) AS new_partner_gmv_usd_day
		,SUM(CASE WHEN new_partner_by_week=1 AND restaurant_partner_status='partner' THEN gmv_usd_gf ELSE NULL END) AS new_partner_gmv_usd_week
		,SUM(CASE WHEN new_partner_by_month=1 AND restaurant_partner_status='partner' THEN gmv_usd_gf ELSE NULL END) AS new_partner_gmv_usd_month
	
		/** new partner gmv usd by model **/
		,SUM(CASE WHEN new_partner_by_model_day=1 AND restaurant_partner_status='partner' THEN gmv_usd_gf ELSE NULL END) AS new_partner_gmv_usd_day_model
		,SUM(CASE WHEN new_partner_by_model_week=1 AND restaurant_partner_status='partner' THEN gmv_usd_gf ELSE NULL END) AS new_partner_gmv_usd_week_model
		,SUM(CASE WHEN new_partner_by_model_month=1 AND restaurant_partner_status='partner' THEN gmv_usd_gf ELSE NULL END) AS new_partner_gmv_usd_month_model	
			
		
		/** prev new merchant gmv usd **/
		,SUM(CASE WHEN prev_new_mex_by_day=1 THEN gmv_usd_gf ELSE NULL END) AS prev_new_merchant_gmv_usd_day
		,SUM(CASE WHEN prev_new_mex_by_week=1 THEN gmv_usd_gf ELSE NULL END) AS prev_new_merchant_gmv_usd_week
		,SUM(CASE WHEN prev_new_mex_by_month=1 THEN gmv_usd_gf ELSE NULL END) AS prev_new_merchant_gmv_usd_month
	
		/** prev new merchant gmv usd by model**/
		,SUM(CASE WHEN prev_new_mex_by_model_day=1 THEN gmv_usd_gf ELSE NULL END) AS prev_new_merchant_gmv_usd_day_model
		,SUM(CASE WHEN prev_new_mex_by_model_week=1 THEN gmv_usd_gf ELSE NULL END) AS prev_new_merchant_gmv_usd_week_model
		,SUM(CASE WHEN prev_new_mex_by_model_month=1 THEN gmv_usd_gf ELSE NULL END) AS prev_new_merchant_gmv_usd_month_model	
		
		/** prev new partner gmv usd **/
		,SUM(CASE WHEN prev_new_partner_by_day=1 AND restaurant_partner_status='partner' THEN gmv_usd_gf ELSE NULL END) AS prev_new_partner_gmv_usd_day
		,SUM(CASE WHEN prev_new_partner_by_week=1 AND restaurant_partner_status='partner' THEN gmv_usd_gf ELSE NULL END) AS prev_new_partner_gmv_usd_week
		,SUM(CASE WHEN prev_new_partner_by_month=1 AND restaurant_partner_status='partner' THEN gmv_usd_gf ELSE NULL END) AS prev_new_partner_gmv_usd_month
	
		/** prev new partner gmv usd by model **/
		,SUM(CASE WHEN prev_new_partner_by_model_day=1 AND restaurant_partner_status='partner' THEN gmv_usd_gf ELSE NULL END) AS prev_new_partner_gmv_usd_day_model
		,SUM(CASE WHEN prev_new_partner_by_model_week=1 AND restaurant_partner_status='partner' THEN gmv_usd_gf ELSE NULL END) AS prev_new_partner_gmv_usd_week_model
		,SUM(CASE WHEN prev_new_partner_by_model_month=1 AND restaurant_partner_status='partner' THEN gmv_usd_gf ELSE NULL END) AS prev_new_partner_gmv_usd_month_model	
			
		
		/** new merchant gmv local **/
		,SUM(CASE WHEN new_mex_by_day=1 THEN gmv_local ELSE NULL END) AS new_merchant_gmv_local_day
		,SUM(CASE WHEN new_mex_by_week=1 THEN gmv_local ELSE NULL END) AS new_merchant_gmv_local_week
		,SUM(CASE WHEN new_mex_by_month=1 THEN gmv_local ELSE NULL END) AS new_merchant_gmv_local_month
	
		/** new merchant gmv local by model**/
		,SUM(CASE WHEN new_mex_by_model_day=1 THEN gmv_local ELSE NULL END) AS new_merchant_gmv_local_day_model
		,SUM(CASE WHEN new_mex_by_model_week=1 THEN gmv_local ELSE NULL END) AS new_merchant_gmv_local_week_model
		,SUM(CASE WHEN new_mex_by_model_month=1 THEN gmv_local ELSE NULL END) AS new_merchant_gmv_local_month_model	
		
		/** new partner gmv local **/
		,SUM(CASE WHEN new_partner_by_day=1 AND restaurant_partner_status='partner' THEN gmv_local ELSE NULL END) AS new_partner_gmv_local_day
		,SUM(CASE WHEN new_partner_by_week=1 AND restaurant_partner_status='partner' THEN gmv_local ELSE NULL END) AS new_partner_gmv_local_week
		,SUM(CASE WHEN new_partner_by_month=1 AND restaurant_partner_status='partner' THEN gmv_local ELSE NULL END) AS new_partner_gmv_local_month
	
		/** new partner gmv local by model **/
		,SUM(CASE WHEN new_partner_by_model_day=1 AND restaurant_partner_status='partner' THEN gmv_local ELSE NULL END) AS new_partner_gmv_local_day_model
		,SUM(CASE WHEN new_partner_by_model_week=1 AND restaurant_partner_status='partner' THEN gmv_local ELSE NULL END) AS new_partner_gmv_local_week_model
		,SUM(CASE WHEN new_partner_by_model_month=1 AND restaurant_partner_status='partner' THEN gmv_local ELSE NULL END) AS new_partner_gmv_local_month_model	
		
		
		/** prev new merchant gmv local **/
		,SUM(CASE WHEN prev_new_mex_by_day=1 THEN gmv_local ELSE NULL END) AS prev_new_merchant_gmv_local_day
		,SUM(CASE WHEN prev_new_mex_by_week=1 THEN gmv_local ELSE NULL END) AS prev_new_merchant_gmv_local_week
		,SUM(CASE WHEN prev_new_mex_by_month=1 THEN gmv_local ELSE NULL END) AS prev_new_merchant_gmv_local_month
	
		/** prev new merchant gmv local by model**/
		,SUM(CASE WHEN prev_new_mex_by_model_day=1 THEN gmv_local ELSE NULL END) AS prev_new_merchant_gmv_local_day_model
		,SUM(CASE WHEN prev_new_mex_by_model_week=1 THEN gmv_local ELSE NULL END) AS prev_new_merchant_gmv_local_week_model
		,SUM(CASE WHEN prev_new_mex_by_model_month=1 THEN gmv_local ELSE NULL END) AS prev_new_merchant_gmv_local_month_model	
		
		/** prev new partner gmv local **/
		,SUM(CASE WHEN prev_new_partner_by_day=1 AND restaurant_partner_status='partner' THEN gmv_local ELSE NULL END) AS prev_new_partner_gmv_local_day
		,SUM(CASE WHEN prev_new_partner_by_week=1 AND restaurant_partner_status='partner' THEN gmv_local ELSE NULL END) AS prev_new_partner_gmv_local_week
		,SUM(CASE WHEN prev_new_partner_by_month=1 AND restaurant_partner_status='partner' THEN gmv_local ELSE NULL END) AS prev_new_partner_gmv_local_month
	
		/** prev new partner gmv local by model **/
		,SUM(CASE WHEN prev_new_partner_by_model_day=1 AND restaurant_partner_status='partner' THEN gmv_local ELSE NULL END) AS prev_new_partner_gmv_local_day_model
		,SUM(CASE WHEN prev_new_partner_by_model_week=1 AND restaurant_partner_status='partner' THEN gmv_local ELSE NULL END) AS prev_new_partner_gmv_local_week_model
		,SUM(CASE WHEN prev_new_partner_by_model_month=1 AND restaurant_partner_status='partner' THEN gmv_local ELSE NULL END) AS prev_new_partner_gmv_local_month_model	
			
		
		
		
		FROM
		(
		
		SELECT 
			bk.*,
			CASE WHEN new_mex_all.join_date = bk.date_local THEN 1 ELSE 0 END AS new_mex_by_day,
			CASE WHEN new_mex_all.join_week = bk.week_of THEN 1 ELSE 0 END AS new_mex_by_week,
			CASE WHEN new_mex_all.join_month = bk.month_of THEN 1 ELSE 0 END AS new_mex_by_month,
	
			CASE WHEN new_mex_by_model.join_date = bk.date_local  THEN 1 ELSE 0 END AS new_mex_by_model_day,
			CASE WHEN new_mex_by_model.join_week = bk.week_of THEN 1 ELSE 0 END AS new_mex_by_model_week,
			CASE WHEN new_mex_by_model.join_month = bk.month_of THEN 1 ELSE 0 END AS new_mex_by_model_month,		
			
			CASE WHEN date_add('day',1,new_mex_all.join_date) = bk.date_local THEN 1 ELSE 0 END AS prev_new_mex_by_day,
			CASE WHEN date_add('week',1,new_mex_all.join_week)= bk.week_of THEN 1 ELSE 0 END AS prev_new_mex_by_week,
			CASE WHEN date_add('month',1,new_mex_all.join_month) = bk.month_of THEN 1 ELSE 0 END AS prev_new_mex_by_month,
	
			CASE WHEN date_add('day',1,new_mex_by_model.join_date) = bk.date_local THEN 1 ELSE 0 END AS prev_new_mex_by_model_day,
			CASE WHEN date_add('week',1,new_mex_by_model.join_week)= bk.week_of THEN 1 ELSE 0 END AS prev_new_mex_by_model_week,
			CASE WHEN date_add('month',1,new_mex_by_model.join_month) = bk.month_of THEN 1 ELSE 0 END AS prev_new_mex_by_model_month,		
			
			CASE WHEN new_partner_all.upgrade_date = bk.date_local AND restaurant_partner_status='partner' THEN 1 ELSE 0 END AS new_partner_by_day,
			CASE WHEN new_partner_all.upgrade_week = bk.week_of AND restaurant_partner_status='partner' THEN 1 ELSE 0 END AS new_partner_by_week,
			CASE WHEN new_partner_all.upgrade_month = bk.month_of AND restaurant_partner_status='partner' THEN 1 ELSE 0 END AS new_partner_by_month,
	
			CASE WHEN new_partner_by_model.upgrade_date = bk.date_local AND restaurant_partner_status='partner' THEN 1 ELSE 0 END AS new_partner_by_model_day,
			CASE WHEN new_partner_by_model.upgrade_week = bk.week_of AND restaurant_partner_status='partner' THEN 1 ELSE 0 END AS new_partner_by_model_week,
			CASE WHEN new_partner_by_model.upgrade_month = bk.month_of AND restaurant_partner_status='partner' THEN 1 ELSE 0 END AS new_partner_by_model_month,				
			
			CASE WHEN date_add('day',1,new_partner_all.upgrade_date) = bk.date_local AND restaurant_partner_status='partner' THEN 1 ELSE 0 END AS prev_new_partner_by_day,
			CASE WHEN date_add('week',1,new_partner_all.upgrade_week) = bk.week_of AND restaurant_partner_status='partner' THEN 1 ELSE 0 END AS prev_new_partner_by_week,
			CASE WHEN date_add('month',1,new_partner_all.upgrade_month) = bk.month_of AND restaurant_partner_status='partner' THEN 1 ELSE 0 END AS prev_new_partner_by_month,
	
			CASE WHEN date_add('day',1,new_partner_by_model.upgrade_date) = bk.date_local AND restaurant_partner_status='partner' THEN 1 ELSE 0 END AS prev_new_partner_by_model_day,
			CASE WHEN date_add('week',1,new_partner_by_model.upgrade_week) = bk.week_of AND restaurant_partner_status='partner' THEN 1 ELSE 0 END AS prev_new_partner_by_model_week,
			CASE WHEN date_add('month',1,new_partner_by_model.upgrade_month) = bk.month_of AND restaurant_partner_status='partner' THEN 1 ELSE 0 END AS prev_new_partner_by_model_month		
			
			FROM 
			(
				SELECT 
				 date_local,
				 merchant_id,
				 date_trunc('week',date_local) AS week_of,
				 date_trunc('month',date_local) AS month_of,
				 country_name,
				 city_name,
				 gmv_usd_gf,
				 gmv_local,
				 restaurant_partner_status,
				 business_model,
				 completed_orders_gf
		
				FROM slide.gf_mex_level_daily_metrics_temp
				WHERE 
				completed_orders_gf>0
				AND date_local >= date_trunc('month',date([[inc_start_date]])) - INTERVAL '10' DAY  /**-10 days in case week start < month start **/
				AND date_local <= date([[inc_end_date]])
			) bk
			
			LEFT JOIN new_mex_by_model
			
			ON bk.merchant_id = new_mex_by_model.merchant_id
			AND bk.country_name = new_mex_by_model.country_name
			AND bk.city_name = new_mex_by_model.city_name
			AND bk.business_model = new_mex_by_model.model_type
			
			LEFT JOIN new_partner_by_model 
			
			ON bk.merchant_id = new_partner_by_model.merchant_id
			AND bk.country_name = new_partner_by_model.country_name
			AND bk.city_name = new_partner_by_model.city_name
		    AND bk.business_model = new_partner_by_model.model_type
		    
			LEFT JOIN new_mex_all
			
			ON bk.merchant_id = new_mex_all.merchant_id
			AND bk.country_name = new_mex_all.country_name
			AND bk.city_name = new_mex_all.city_name
			
			LEFT JOIN new_partner_all 
			
			ON bk.merchant_id = new_partner_all.merchant_id
			AND bk.country_name = new_partner_all.country_name
			AND bk.city_name = new_partner_all.city_name
		
		)
		
		GROUP BY GROUPING SETS 
		(
			(date_local,country_name,merchant_id),(week_of,country_name,merchant_id),(month_of,country_name,merchant_id),
			(date_local,country_name,business_model,merchant_id),(week_of,country_name,business_model,merchant_id),(month_of,country_name,business_model,merchant_id),
	
			
			
			(date_local,country_name,city_name,merchant_id),(week_of,country_name,city_name,merchant_id),(month_of,country_name,city_name,merchant_id),
			(date_local,country_name,city_name,business_model,merchant_id),(week_of,country_name,city_name,business_model,merchant_id),(month_of,country_name,city_name,business_model,merchant_id),
	
			
			
			(date_local,merchant_id),(week_of,merchant_id),(month_of,merchant_id),
			(date_local,business_model,merchant_id),(week_of,business_model,merchant_id),(month_of,business_model,merchant_id)			
		)
	
	)
	
	WHERE 
	time_period >= (CASE 
		WHEN by_day_week_month = 'By Day' THEN date([[inc_start_date]])
		WHEN by_day_week_month = 'By Week' THEN date_trunc('week',date([[inc_start_date]]))
		WHEN by_day_week_month = 'By Month' THEN date_trunc('month',date([[inc_start_date]]))
	 END)
	 
)

GROUP BY 1,2,3,4,5,6,7