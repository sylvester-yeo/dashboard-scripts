create view slide.gf_dash_metrics_consolidation_after_2019 as 

/********************Weekly*******************/
/***** MAU, MTU, Churn, Reactivation are based on Rolling 4 weeks*****/

(

SELECT 
base.*
,tph.d_gf_completed_orders
,tph.d_gf_online_seconds/3600.0 AS d_gf_online_hours
,tph.s_gf_completed_orders
,tph.s_gf_online_seconds/3600.0 AS s_gf_online_hours
,tph.all_gf_online_seconds/3600.0 AS active_dax_online_hours
,tph.all_gf_transit_seconds/3600.0 AS active_dax_transit_hours
,mex_dim.active_restaurant
,mex_dim.active_partner_restaurant
,mex_dim.active_new_partner
,mex_dim.active_new_restaurant
,mex_dim.prev_active_new_restaurant
,mex_dim.prev_active_new_partner
,mex_dim.new_merchant_gmv_usd
,mex_dim.new_partner_gmv_usd
,mex_dim.prev_new_merchant_gmv_usd
,mex_dim.prev_new_partner_gmv_usd
,mex_dim.new_merchant_gmv_local
,mex_dim.new_partner_gmv_local
,mex_dim.prev_new_merchant_gmv_local
,mex_dim.prev_new_partner_gmv_local
,new_mex.new_merchants
,new_mex.new_partner_merchants
,new_mex.new_partner_new_merchants

,dax_dim.active_drivers
,dax_dim.active_2w_drivers
,dax_dim.active_new_bidding_driver_gf AS new_drivers

,pax_dim.active_users_gf AS active_users
,pax_dim.active_eaters_gf AS active_eaters
,pax_dim.new_users

,rolling_metrics.gf_mau_past4week
,rolling_metrics.gf_mtu_past4week
,rolling_metrics.gf_orders_past4week
,rolling_metrics.gf_gmv_past4week
,rolling_metrics.gf_mau_past4week_new
,rolling_metrics.gf_mau_past4week_new_mtu
,rolling_metrics.gf_orders_past4week_new
,rolling_metrics.gf_orders_past4week_new_mtu
,rolling_metrics.gf_gmv_past4week_new
,rolling_metrics.gf_gmv_past4week_new_mtu
,rolling_metrics.transport_mau_past4week
,rolling_metrics.transport_mtu_past4week
,rolling_metrics.transport_gf_mau_past4week
,rolling_metrics.transport_gf_mtu_past4week
,rolling_metrics.transport_mau_who_used_gf
,rolling_metrics.transport_mtu_who_ordered_gf
,rolling_metrics.churn_users
,rolling_metrics.reactivated_users
,rolling_metrics.churn_drivers
,rolling_metrics.reactivated_drivers

/*Country-level Specific Metrics*/
,ce.total_contact
,ce.avg_csat


FROM 
(
	 SELECT * FROM 
	 slide.gf_daily_city_base_metrics_agg
	 WHERE 
	 by_day_week_month = 'By Week'
	--  AND time_period >= date_trunc('week',date([[inc_start_date]]))
	--  AND time_period <= date([[inc_end_date]])
 ) base
 

LEFT JOIN (SELECT *,'All' as partner_status FROM slide.gf_dash_active_mex_all_dim) mex_dim
ON base.country = mex_dim.country_name
AND base.city = mex_dim.city_name
AND base.time_period = mex_dim.time_period
AND base.By_day_week_month = mex_dim.By_day_week_month
AND base.By_city_country = mex_dim.By_city_country
AND base.business_model = mex_dim.business_model
AND base.cashless_status = mex_dim.cashless_status
and base.partner_status = mex_dim.partner_status


LEFT JOIN (SELECT *,'All' as partner_status FROM slide.gf_dash_new_mex_count_all_dim) new_mex
ON base.country = new_mex.country_name
AND base.city = new_mex.city_name
AND base.time_period = new_mex.time_period
AND base.By_day_week_month = new_mex.By_day_week_month
AND base.By_city_country = new_mex.By_city_country
AND base.business_model = new_mex.business_model
AND base.cashless_status = new_mex.cashless_status
and base.partner_status = new_mex.partner_status


LEFT JOIN 
(SELECT *,'All' AS business_model,'All' AS cashless_status, 'All' as partner_status FROM 
  slide.gf_dash_active_pax_all_dim)  pax_dim
ON base.country = pax_dim.country_name
AND base.city = pax_dim.city_name
AND base.time_period = pax_dim.time_period
AND base.By_day_week_month = pax_dim.By_day_week_month
AND base.By_city_country = pax_dim.By_city_country 
AND base.business_model = pax_dim.business_model
AND base.cashless_status = pax_dim.cashless_status
and base.partner_status = pax_dim.partner_status


LEFT JOIN 
(SELECT *,'All' AS business_model,'All' AS cashless_status, 'All' as partner_status FROM
 slide.gf_active_dax_all_dim_agg) dax_dim
ON base.country = dax_dim.country_name
AND base.city = dax_dim.city_name
AND base.time_period = dax_dim.time_period
AND base.By_day_week_month = dax_dim.By_day_week_month
AND base.By_city_country = dax_dim.By_city_country 
AND base.business_model = dax_dim.business_model
AND base.cashless_status = dax_dim.cashless_status
and base.partner_status = dax_dim.partner_status

LEFT JOIN
(SELECT *, 'All' AS business_model,'All' AS cashless_status, 'All' as partner_status FROM
 slide.gf_dash_tph_agg ) tph
ON base.country = tph.country
AND base.city = tph.city
AND base.time_period = tph.time_period
AND base.By_day_week_month = tph.By_day_week_month
AND base.By_city_country = tph.By_city_country 
AND base.business_model = tph.business_model
AND base.cashless_status = tph.cashless_status
and base.partner_status = tph.partner_status

LEFT JOIN 
(SELECT *,'All' AS business_model,'All' AS cashless_status, 'All' as partner_status FROM
 slide.gf_dash_weekly_rolling_metrics)  rolling_metrics

ON base.country = rolling_metrics.country_name
AND base.city = rolling_metrics.city_name
AND base.time_period = rolling_metrics.time_period
AND base.By_day_week_month = rolling_metrics.By_day_week_month
AND base.By_city_country = rolling_metrics.By_city_country 
AND base.business_model = rolling_metrics.business_model
AND base.cashless_status = rolling_metrics.cashless_status
and base.partner_status = rolling_metrics.partner_status

LEFT JOIN 
(
	(SELECT 
	 'All' AS country
	 ,'All' AS city
	 ,'By Region' AS by_city_country
	 ,'By Week' AS by_day_week_month
	 ,'All' AS business_model
	 ,'All' AS cashless_status
     ,'All' as partner_status
	 ,date_trunc('week',date_local) AS time_period
	 ,CAST(SUM(csat_sum) AS DOUBLE)/CAST(SUM(csat_count) AS DOUBLE) AS avg_csat
	 ,sum(total_contact) AS total_contact
	 FROM slide.gf_dash_ce_metrics
	--  WHERE date_local>=DATE_TRUNC('week',date([[inc_start_date]]))
	 	-- and date_local <= date([[inc_end_date]]) + interval '7' day
	 GROUP BY 1,2,3,4,5,6,7,8 )
	 
	 UNION ALL 
	 
	 (SELECT 
	 country
	 ,'All' AS city
	 ,'By Country' AS by_city_country
	 ,'By Week' AS by_day_week_month
	 ,'All' AS business_model
	 ,'All' AS cashless_status
     ,'All' as partner_status
	 ,date_trunc('week',date_local) AS time_period
	 ,CAST(SUM(csat_sum) AS DOUBLE)/CAST(SUM(csat_count) AS DOUBLE) AS avg_csat
	 ,sum(total_contact) AS total_contact
	 FROM slide.gf_dash_ce_metrics
	--  WHERE date_local>=DATE_TRUNC('week',date([[inc_start_date]]))
	 	-- and date_local <= date([[inc_end_date]]) + interval '7' day
	 GROUP BY 1,2,3,4,5,6,7,8)
 
 ) ce

 ON base.country = ce.country
AND base.city = ce.city
AND base.time_period = ce.time_period
AND base.By_day_week_month = ce.By_day_week_month
AND base.By_city_country = ce.By_city_country 
AND base.business_model = ce.business_model
AND base.cashless_status = ce.cashless_status
and base.partner_status = ce.partner_status


)

UNION ALL

/*******************Monthly*******************/
/***** MAU, MTU are based on Calendar Month*****/
/***** Chun, Reactivation are not available*****/

(
SELECT 
base.*
,tph.d_gf_completed_orders
,tph.d_gf_online_seconds/3600.0 AS d_gf_online_hours
,tph.s_gf_completed_orders
,tph.s_gf_online_seconds/3600.0 AS s_gf_online_hours
,tph.all_gf_online_seconds/3600.0 AS active_dax_online_hours
,tph.all_gf_transit_seconds/3600.0 AS active_dax_transit_hours
,mex_dim.active_restaurant
,mex_dim.active_partner_restaurant
,mex_dim.active_new_partner
,mex_dim.active_new_restaurant
,mex_dim.prev_active_new_restaurant
,mex_dim.prev_active_new_partner
,mex_dim.new_merchant_gmv_usd
,mex_dim.new_partner_gmv_usd
,mex_dim.prev_new_merchant_gmv_usd
,mex_dim.prev_new_partner_gmv_usd
,mex_dim.new_merchant_gmv_local
,mex_dim.new_partner_gmv_local
,mex_dim.prev_new_merchant_gmv_local
,mex_dim.prev_new_partner_gmv_local
,new_mex.new_merchants
,new_mex.new_partner_merchants
,new_mex.new_partner_new_merchants


,dax_dim.active_drivers
,dax_dim.active_2w_drivers
,dax_dim.active_new_bidding_driver_gf AS new_drivers

,pax_dim.active_users_gf AS active_users
,pax_dim.active_eaters_gf AS active_eaters
,pax_dim.new_users

,pax_dim.active_users_gf AS gf_mau_past4week
,pax_dim.active_eaters_gf AS gf_mtu_past4week
,pax_dim.completed_orders_gf AS gf_orders_past4week
,gmv_usd_gf AS gf_gmv_past4week
,new_users AS gf_mau_past4week_new
,pax_dim.new_eaters AS gf_mtu_past4week_new
,completed_orders_gf_new AS gf_orders_past4week_new
,completed_orders_gf_new_mtu AS gf_orders_past4week_new_mtu
,gmv_usd_gf_new AS gf_gmv_past4week_new
,gmv_usd_gf_new_mtu AS gf_gmv_past4week_new_mtu
,active_users_transport AS transport_mau_past4week
,active_riders_transport AS transport_mtu_past4week
,active_users_transport_gf AS transport_gf_mau_past4week
,active_rider_transport_gf AS transport_gf_mtu_past4week
,active_users_transport_gf_once AS transport_mau_who_used_gf
,active_rider_transport_gf_once AS transport_mtu_who_ordered_gf
,NULL AS churn_users
,NULL AS reactivated_users
,NULL AS churn_drivers
,NULL AS reactivated_drivers

/*Country-level Specific Metrics*/
,ce.total_contact
,ce.avg_csat

FROM

(
	 SELECT * FROM slide.gf_daily_city_base_metrics_agg
	 WHERE 
	 by_day_week_month = 'By Month'
	--  AND time_period >= date_trunc('month',date([[inc_start_date]]))
	--  AND time_period <= date([[inc_end_date]])
 ) base

LEFT JOIN (SELECT *, 'All' as partner_status FROM slide.gf_dash_active_mex_all_dim) mex_dim
ON base.country = mex_dim.country_name
AND base.city = mex_dim.city_name
AND base.time_period = mex_dim.time_period
AND base.By_day_week_month = mex_dim.By_day_week_month
AND base.By_city_country = mex_dim.By_city_country
AND base.business_model = mex_dim.business_model
AND base.cashless_status = mex_dim.cashless_status
and base.partner_status = mex_dim.partner_status


LEFT JOIN (SELECT *,'All' as partner_status FROM slide.gf_dash_new_mex_count_all_dim) new_mex
ON base.country = new_mex.country_name
AND base.city = new_mex.city_name
AND base.time_period = new_mex.time_period
AND base.By_day_week_month = new_mex.By_day_week_month
AND base.By_city_country = new_mex.By_city_country
AND base.business_model = new_mex.business_model
AND base.cashless_status = new_mex.cashless_status
and base.partner_status = new_mex.partner_status


LEFT JOIN 
(SELECT *,'All' AS business_model,'All' AS cashless_status, 'All' as partner_status FROM 
  slide.gf_dash_active_pax_all_dim)  pax_dim
ON base.country = pax_dim.country_name
AND base.city = pax_dim.city_name
AND base.time_period = pax_dim.time_period
AND base.By_day_week_month = pax_dim.By_day_week_month
AND base.By_city_country = pax_dim.By_city_country 
AND base.business_model = pax_dim.business_model
AND base.cashless_status = pax_dim.cashless_status
and base.partner_status = pax_dim.partner_status


LEFT JOIN 
(SELECT *,'All' AS business_model,'All' AS cashless_status, 'All' as partner_status FROM
 slide.gf_active_dax_all_dim_agg) dax_dim
ON base.country = dax_dim.country_name
AND base.city = dax_dim.city_name
AND base.time_period = dax_dim.time_period
AND base.By_day_week_month = dax_dim.By_day_week_month
AND base.By_city_country = dax_dim.By_city_country 
AND base.business_model = dax_dim.business_model
AND base.cashless_status = dax_dim.cashless_status
and base.partner_status = dax_dim.partner_status

LEFT JOIN
(SELECT *, 'All' AS business_model,'All' AS cashless_status, 'All' as partner_status FROM
 slide.gf_dash_tph_agg ) tph
ON base.country = tph.country
AND base.city = tph.city
AND base.time_period = tph.time_period
AND base.By_day_week_month = tph.By_day_week_month
AND base.By_city_country = tph.By_city_country 
AND base.business_model = tph.business_model
AND base.cashless_status = tph.cashless_status
and base.partner_status = tph.partner_status

LEFT JOIN 
(SELECT *,'All' AS business_model,'All' AS cashless_status, 'All' as partner_status FROM
 slide.gf_dash_weekly_rolling_metrics)  rolling_metrics

ON base.country = rolling_metrics.country_name
AND base.city = rolling_metrics.city_name
AND base.time_period = rolling_metrics.time_period
AND base.By_day_week_month = rolling_metrics.By_day_week_month
AND base.By_city_country = rolling_metrics.By_city_country 
AND base.business_model = rolling_metrics.business_model
AND base.cashless_status = rolling_metrics.cashless_status
and base.partner_status = rolling_metrics.partner_status

LEFT JOIN 
(
	(SELECT 
	 'All' AS country
	 ,'All' AS city
	 ,'By Region' AS by_city_country
	 ,'By Month' AS by_day_week_month
	 ,'All' AS business_model
	 ,'All' AS cashless_status
     ,'All' as partner_status
	 ,date_trunc('month',date_local) AS time_period
	 ,CAST(SUM(csat_sum) AS DOUBLE)/CAST(SUM(csat_count) AS DOUBLE) AS avg_csat
	 ,sum(total_contact) AS total_contact
	 FROM slide.gf_dash_ce_metrics
	--  WHERE date_local>=DATE_TRUNC('month',date([[inc_start_date]]))
	 	-- and date_local <= date([[inc_end_date]]) + interval '1' month
	 GROUP BY 1,2,3,4,5,6,7,8 )
	 
	 UNION ALL 
	 
	 (SELECT 
	 country
	 ,'All' AS city
	 ,'By Country' AS by_city_country
	 ,'By Month' AS by_day_week_month
	 ,'All' AS business_model
	 ,'All' AS cashless_status
     ,'All' as partner_status
	 ,date_trunc('month',date_local) AS time_period
	 ,CAST(SUM(csat_sum) AS DOUBLE)/CAST(SUM(csat_count) AS DOUBLE) AS avg_csat
	 ,sum(total_contact) AS total_contact
	 FROM slide.gf_dash_ce_metrics
	--  WHERE date_local>=DATE_TRUNC('month',date([[inc_start_date]]))
	 	-- and date_local <= date([[inc_end_date]])  + interval '1' month
	 GROUP BY 1,2,3,4,5,6,7,8)
 ) ce

 ON base.country = ce.country
AND base.city = ce.city
AND base.time_period = ce.time_period
AND base.By_day_week_month = ce.By_day_week_month
AND base.By_city_country = ce.By_city_country 
AND base.business_model = ce.business_model
AND base.cashless_status = ce.cashless_status
and base.partner_status = ce.partner_status
)


UNION ALL

/********************Daily*******************/
/***** MAU, MTU, Churn, Reactivation are not available*****/
(

SELECT 
base.*
,tph.d_gf_completed_orders
,tph.d_gf_online_seconds/3600.0 AS d_gf_online_hours
,tph.s_gf_completed_orders
,tph.s_gf_online_seconds/3600.0 AS s_gf_online_hours
,tph.all_gf_online_seconds/3600.0 AS active_dax_online_hours
,tph.all_gf_transit_seconds/3600.0 AS active_dax_transit_hours
,mex_dim.active_restaurant
,mex_dim.active_partner_restaurant
,mex_dim.active_new_partner
,mex_dim.active_new_restaurant
,mex_dim.prev_active_new_restaurant
,mex_dim.prev_active_new_partner
,mex_dim.new_merchant_gmv_usd
,mex_dim.new_partner_gmv_usd
,mex_dim.prev_new_merchant_gmv_usd
,mex_dim.prev_new_partner_gmv_usd
,mex_dim.new_merchant_gmv_local
,mex_dim.new_partner_gmv_local
,mex_dim.prev_new_merchant_gmv_local
,mex_dim.prev_new_partner_gmv_local
,new_mex.new_merchants
,new_mex.new_partner_merchants
,new_mex.new_partner_new_merchants


,dax_dim.active_drivers
,dax_dim.active_2w_drivers
,dax_dim.active_new_bidding_driver_gf AS new_drivers

,pax_dim.active_users_gf AS active_users
,pax_dim.active_eaters_gf AS active_eaters
,pax_dim.new_users

,pax_dim.active_users_gf AS gf_mau_past4week
,pax_dim.active_eaters_gf AS gf_mtu_past4week
,pax_dim.completed_orders_gf AS gf_orders_past4week
,gmv_usd_gf AS gf_gmv_past4week
,new_users AS gf_mau_past4week_new
,new_eaters AS gf_mau_past4week_new_mtu
,completed_orders_gf_new AS gf_orders_past4week_new
,completed_orders_gf_new_mtu AS gf_orders_past4week_new_mtu
,gmv_usd_gf_new AS gf_gmv_past4week_new
,gmv_usd_gf_new_mtu AS gf_gmv_past4week_new_mtu
,NULL AS transport_mau_past4week
,NULL AS transport_mtu_past4week
,NULL AS transport_gf_mau_past4week
,NULL AS transport_gf_mtu_past4week
,NULL AS transport_mau_who_used_gf
,NULL AS transport_mtu_who_ordered_gf
,NULL AS churn_users
,NULL AS reactivated_users
,NULL AS churn_drivers
,NULL AS reactivated_drivers

/*Country-level Specific Metrics*/
,ce.total_contact
,ce.avg_csat

FROM

(
	 SELECT * FROM 
	  slide.gf_daily_city_base_metrics_agg
	 WHERE 
	 by_day_week_month = 'By Day'
	--  AND time_period >= date([[inc_start_date]])
	--  AND time_period <= date([[inc_end_date]])
 ) base

LEFT JOIN (SELECT *, 'All' as partner_status FROM slide.gf_dash_active_mex_all_dim) mex_dim
ON base.country = mex_dim.country_name
AND base.city = mex_dim.city_name
AND base.time_period = mex_dim.time_period
AND base.By_day_week_month = mex_dim.By_day_week_month
AND base.By_city_country = mex_dim.By_city_country
AND base.business_model = mex_dim.business_model
AND base.cashless_status = mex_dim.cashless_status
and base.partner_status = mex_dim.partner_status


LEFT JOIN (SELECT *, 'All' as partner_status FROM slide.gf_dash_new_mex_count_all_dim) new_mex
ON base.country = new_mex.country_name
AND base.city = new_mex.city_name
AND base.time_period = new_mex.time_period
AND base.By_day_week_month = new_mex.By_day_week_month
AND base.By_city_country = new_mex.By_city_country
AND base.business_model = new_mex.business_model
AND base.cashless_status = new_mex.cashless_status
and base.partner_status = new_mex.partner_status


LEFT JOIN 
(SELECT *,'All' AS business_model,'All' AS cashless_status, 'All' as partner_status FROM 
  slide.gf_dash_active_pax_all_dim)  pax_dim
ON base.country = pax_dim.country_name
AND base.city = pax_dim.city_name
AND base.time_period = pax_dim.time_period
AND base.By_day_week_month = pax_dim.By_day_week_month
AND base.By_city_country = pax_dim.By_city_country 
AND base.business_model = pax_dim.business_model
AND base.cashless_status = pax_dim.cashless_status
and base.partner_status = pax_dim.partner_status


LEFT JOIN 
(SELECT *,'All' AS business_model,'All' AS cashless_status, 'All' as partner_status FROM
 slide.gf_active_dax_all_dim_agg) dax_dim
ON base.country = dax_dim.country_name
AND base.city = dax_dim.city_name
AND base.time_period = dax_dim.time_period
AND base.By_day_week_month = dax_dim.By_day_week_month
AND base.By_city_country = dax_dim.By_city_country 
AND base.business_model = dax_dim.business_model
AND base.cashless_status = dax_dim.cashless_status
and base.partner_status = dax_dim.partner_status

LEFT JOIN
(SELECT *, 'All' AS business_model,'All' AS cashless_status, 'All' as partner_status FROM
 slide.gf_dash_tph_agg ) tph
ON base.country = tph.country
AND base.city = tph.city
AND base.time_period = tph.time_period
AND base.By_day_week_month = tph.By_day_week_month
AND base.By_city_country = tph.By_city_country 
AND base.business_model = tph.business_model
AND base.cashless_status = tph.cashless_status
and base.partner_status = tph.partner_status

LEFT JOIN 
(SELECT *,'All' AS business_model,'All' AS cashless_status, 'All' as partner_status FROM
 slide.gf_dash_weekly_rolling_metrics)  rolling_metrics

ON base.country = rolling_metrics.country_name
AND base.city = rolling_metrics.city_name
AND base.time_period = rolling_metrics.time_period
AND base.By_day_week_month = rolling_metrics.By_day_week_month
AND base.By_city_country = rolling_metrics.By_city_country 
AND base.business_model = rolling_metrics.business_model
AND base.cashless_status = rolling_metrics.cashless_status
and base.partner_status = rolling_metrics.partner_status

LEFT JOIN 
(	(SELECT 
	 'All' AS country
	 ,'All' AS city
	 ,'By Region' AS by_city_country
	 ,'By Day' AS by_day_week_month
	 ,'All' AS business_model
	 ,'All' AS cashless_status
     ,'All' as partner_status
	 ,date_local AS time_period
	 ,CAST(SUM(csat_sum) AS DOUBLE)/CAST(SUM(csat_count) AS DOUBLE) AS avg_csat
	 ,sum(total_contact) AS total_contact
	 FROM slide.gf_dash_ce_metrics
	--  WHERE date_local>=DATE([[inc_start_date]])
	 	-- and date_local <= date([[inc_end_date]])
	 GROUP BY 1,2,3,4,5,6,7,8 )
	 
	 UNION ALL 
	 
	 (SELECT 
	 country
	 ,'All' AS city
	 ,'By Country' AS by_city_country
	 ,'By Day' AS by_day_week_month
	 ,'All' AS business_model
	 ,'All' AS cashless_status
     ,'All' as partner_status
	 ,date_local AS time_period
	 ,CAST(SUM(csat_sum) AS DOUBLE)/CAST(SUM(csat_count) AS DOUBLE) AS avg_csat
	 ,sum(total_contact) AS total_contact
	 FROM slide.gf_dash_ce_metrics
	--  WHERE date_local>=DATE([[inc_start_date]])
	 	-- and date_local <= date([[inc_end_date]])
	 GROUP BY 1,2,3,4,5,6,7,8)
 ) ce

 ON base.country = ce.country
AND base.city = ce.city
AND base.time_period = ce.time_period
AND base.By_day_week_month = ce.By_day_week_month
AND base.By_city_country = ce.By_city_country 
AND base.business_model = ce.business_model
AND base.cashless_status = ce.cashless_status
and base.partner_status = ce.partner_status

)