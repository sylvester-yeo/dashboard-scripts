select 
a.time_period as week_of 
,a.*  
,d.activated_merchants
,d.activated_partner_merchants
,d.activated_im_merchants
/* promo items  */

,b.completed_orders_promo_item
,b.gf_promo_spend_usd
,b.gf_promo_spend_local
,b.mex_promo_spend_usd
,b.mex_promo_spend_local
,b.mex_promo_spend_n_usd
,b.mex_promo_spend_n_local
,b.grab_promo_spend_usd
,b.grab_promo_spend_local
,b.grab_promo_spend_n_usd
,b.grab_promo_spend_n_local
,b.promo_item_normal_price_usd - b.promo_item_promo_price_usd as general_promo_item_price_diff_usd 
,b.promo_item_normal_price_local - b.promo_item_promo_price_local as general_promo_item_price_diff_local
,b.promo_item_n_normal_price_usd - b.promo_item_n_promo_price_usd as general_promo_item_price_diff_n_usd
,b.promo_item_n_normal_price_local - b.promo_item_n_promo_price_local as general_promo_item_price_diff_n_local

,b.gf_promo_spend_usd_non_mfc
,b.gf_promo_spend_local_non_mfc
,b.mex_promo_spend_usd_non_mfc
,b.mex_promo_spend_local_non_mfc
,b.mex_promo_spend_n_usd_non_mfc
,b.mex_promo_spend_n_local_non_mfc
,b.grab_promo_spend_usd_non_mfc
,b.grab_promo_spend_local_non_mfc
,b.grab_promo_spend_n_usd_non_mfc
,b.grab_promo_spend_n_local_non_mfc
,b.promo_item_normal_price_usd_non_mfc - b.promo_item_promo_price_usd_non_mfc as general_promo_item_price_diff_usd_non_mfc
,b.promo_item_normal_price_local_non_mfc - b.promo_item_promo_price_local_non_mfc as general_promo_item_price_diff_local_non_mfc
,b.promo_item_n_normal_price_usd_non_mfc - b.promo_item_n_promo_price_usd_non_mfc as general_promo_item_price_diff_n_usd_non_mfc
,b.promo_item_n_normal_price_local_non_mfc - b.promo_item_n_promo_price_local_non_mfc as general_promo_item_price_diff_n_local_non_mfc

,b.partner_gf_promo_spend_usd
,b.partner_gf_promo_spend_local
,b.partner_mex_promo_spend_usd
,b.partner_mex_promo_spend_local
,b.partner_mex_promo_spend_n_usd
,b.partner_mex_promo_spend_n_local
,b.partner_grab_promo_spend_usd
,b.partner_grab_promo_spend_local
,b.partner_grab_promo_spend_n_usd
,b.partner_grab_promo_spend_n_local
,b.partner_promo_item_normal_price_usd - b.partner_promo_item_promo_price_usd as partner_promo_item_price_diff_usd 
,b.partner_promo_item_normal_price_local - b.partner_promo_item_promo_price_local as partner_promo_item_price_diff_local 
,b.partner_promo_item_n_normal_price_usd - b.partner_promo_item_n_promo_price_usd as partner_promo_item_price_diff_n_usd
,b.partner_promo_item_n_normal_price_local - b.partner_promo_item_n_promo_price_local as partner_promo_item_price_diff_n_local 

,b.partner_gf_promo_spend_usd_non_mfc 
,b.partner_gf_promo_spend_local_non_mfc
,b.partner_mex_promo_spend_usd_non_mfc
,b.partner_mex_promo_spend_local_non_mfc
,b.partner_mex_promo_spend_n_usd_non_mfc
,b.partner_mex_promo_spend_n_local_non_mfc
,b.partner_grab_promo_spend_usd_non_mfc
,b.partner_grab_promo_spend_local_non_mfc
,b.partner_grab_promo_spend_n_usd_non_mfc
,b.partner_grab_promo_spend_n_local_non_mfc
,b.partner_promo_item_normal_price_usd_non_mfc - b.partner_promo_item_promo_price_usd_non_mfc as partner_promo_item_price_diff_usd_non_mfc
,b.partner_promo_item_normal_price_local_non_mfc - b.partner_promo_item_promo_price_local_non_mfc as partner_promo_item_price_diff_local_non_mfc
,b.partner_promo_item_n_normal_price_usd_non_mfc - b.partner_promo_item_n_promo_price_usd_non_mfc as partner_promo_item_price_diff_n_usd_non_mfc
,b.partner_promo_item_n_normal_price_local_non_mfc - b.partner_promo_item_n_promo_price_local_non_mfc as partner_promo_item_price_diff_n_local_non_mfc

,mex_funding_amount_perday_local as mex_funded_promocode_perday_local
,mex_funding_amount_perday_usd as mex_funded_promocode_perday_usd
,mbp_paid_by_mex_local
,mbp_paid_by_pax_local
,tsp_paid_by_us_local
,mbp_paid_by_mex_usd
,mbp_paid_by_pax_usd
,tsp_paid_by_us_usd


/*for Profitability tree
    sample: ,lag(gmv_gf) OVER (PARTITION BY by_day_week_month,country,city,business_model ORDER BY time_period) AS gmv_gf_L1
*/
,lag(a.completed_orders_gf) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS completed_orders_gf_lag
,lag(a.gmv_gf) over (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) as gmv_gf_lag
,lag(a.gmv_gf_local) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS gmv_gf_local_lag
,lag(a.basket_size) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS basket_size_lag
,lag(a.basket_size_local) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS basket_size_local_lag
,lag(a.partner_commission) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS partner_commission_lag
,lag(a.partner_commission_local) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS partner_commission_local_lag
,lag(a.driver_commission) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS driver_commission_lag
,lag(a.driver_commission_local) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS driver_commission_local_lag
,lag(a.delivery_fare_gf) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS delivery_fare_gf_lag
,lag(a.delivery_fare_gf_local) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS delivery_fare_gf_local_lag
,lag(a.dax_delivery_fare) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS dax_delivery_fare_lag
,lag(a.dax_delivery_fare_local) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS dax_delivery_fare_local_lag
,lag(a.promo_expense) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS promo_expense_lag
,lag(a.promo_expense_local) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS promo_expense_local_lag
,lag(a.incentive_payout_local) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS incentive_payout_local_lag
,lag(a.incentive_payout_usd) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS incentive_payout_usd_lag
,lag(a.incentive_payout_usd_w_tsp) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS incentive_payout_usd_w_tsp_lag
,lag(a.incentive_payout_local_w_tsp) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS incentive_payout_local_w_tsp_lag
,lag(b.mex_promo_spend_n_usd) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS mex_promo_spend_n_usd_lag
,lag(b.mex_promo_spend_n_local) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS mex_promo_spend_n_local_lag
,lag(b.grab_promo_spend_n_usd) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS grab_promo_spend_n_usd_lag
,lag(b.grab_promo_spend_n_local) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS grab_promo_spend_n_local_lag
,lag(b.mex_promo_spend_n_usd_non_mfc) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS mex_promo_spend_n_usd_non_mfc_lag
,lag(b.mex_promo_spend_n_local_non_mfc) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS mex_promo_spend_n_local_non_mfc_lag
,lag(b.grab_promo_spend_n_usd_non_mfc) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS grab_promo_spend_n_usd_non_mfc_lag
,lag(b.grab_promo_spend_n_local_non_mfc) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS grab_promo_spend_n_local_non_mfc_lag
,lag(c.mex_funding_amount_perday_local) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS mex_funding_amount_perday_local_lag
,lag(c.mex_funding_amount_perday_usd) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS mex_funding_amount_perday_usd_lag
,lag(e.mbp_paid_by_mex_usd) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS mbp_paid_by_mex_usd_lag
,lag(e.mbp_paid_by_mex_local) OVER (PARTITION BY a.by_day_week_month,a.country,a.city,a.business_model,a.by_city_country, a.cashless_status ORDER BY a.time_period) AS mbp_paid_by_mex_local_lag

from slide.gf_dash_metrics_consolidation_after_2019_v2 a

left join slide.gf_mfc_all_dim b 
ON a.country = b.country
and a.city = b.city
and a.time_period = b.date_local
and a.by_city_country = b.by_city_country
and a.by_day_week_month = b.by_day_week_month
and a.business_model = b.business_model
and a.cashless_status = b.cashless_status

left join slide.gf_mfp_redemption_aggregated c
ON a.country = c.country
and a.city = c.city
and a.time_period = c.time_period
and a.by_city_country = c.by_city_country
and a.by_day_week_month = c.by_day_week_month
and a.business_model = c.business_model
and a.cashless_status = c.cashless_status

left join 
(SELECT *,'All' AS business_model,'All' AS cashless_status FROM
           slide.grabfood_activated_mex_cnt_cg) d
on a.country = d.country_name
and a.city = d.city_name
and a.by_day_week_month = d.by_day_week_month
and a.by_city_country = d.by_city_country
and a.time_period = d.time_period
and a.business_model = d.business_model
and a.cashless_status = d.cashless_status

left join slide.gf_dash_mbp_tsp e 
on a.country = e.country
and a.city = e.city
and a.by_day_week_month = e.by_day_week_month
and a.by_city_country = e.by_city_country
and a.time_period = e.time_period
and a.business_model = e.business_model
and a.cashless_status = e.cashless_status