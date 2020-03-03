/*
    Name: slide.gf_mfc_all_dim
    Refresh Time: Daily, 21:00 UTC
    Aggregation Mode: Incremental, lookback window 60 days
    Lighthouse Dependancy Tables:
        slide.gf_mfc_brand_daily 21:00
*/

/*
    Reading from table: slide.gf_mfc_brand
    Structure of preceding table: merchant level, mfc/non-mfc, daily
    Resulting table: structured for leadership dashboard - daily/weekly/monthly, city/coutnry, im/cm, cashless/cash
*/

with base as (
    SELECT
        date_local
        ,date_trunc('week',date(date_local)) as week_of
        ,date_trunc('month',date(date_local)) as month_of
        ,city
        ,country
        ,business_model 
        ,cashless_status
        ,sum(completed_orders_promo_item) as completed_orders_promo_item
        ,sum(gf_promo_spend_usd) as gf_promo_spend_usd
        ,sum(gf_promo_spend_local) as gf_promo_spend_local
        ,sum(mex_promo_spend_usd) as mex_promo_spend_usd
        ,sum(mex_promo_spend_local) as mex_promo_spend_local
        ,sum(mex_promo_spend_n_usd) as mex_promo_spend_n_usd
        ,sum(mex_promo_spend_n_local) as mex_promo_spend_n_local
        ,sum(grab_promo_spend_usd) as grab_promo_spend_usd
        ,sum(grab_promo_spend_local) as grab_promo_spend_local
        ,sum(grab_promo_spend_n_usd) as grab_promo_spend_n_usd
        ,sum(grab_promo_spend_n_local) as grab_promo_spend_n_local
        ,sum(promo_item_promo_price_usd) as promo_item_promo_price_usd
        ,sum(promo_item_promo_price_local) as promo_item_promo_price_local
        ,sum(promo_item_normal_price_usd) as promo_item_normal_price_usd
        ,sum(promo_item_normal_price_local) as promo_item_normal_price_local
        ,sum(promo_item_n_promo_price_usd) as promo_item_n_promo_price_usd
        ,sum(promo_item_n_promo_price_local) as promo_item_n_promo_price_local
        ,sum(promo_item_n_normal_price_usd) as promo_item_n_normal_price_usd
        ,sum(promo_item_n_normal_price_local) as promo_item_n_normal_price_local

        ,sum(gf_promo_spend_usd_non_mfc) as gf_promo_spend_usd_non_mfc
        ,sum(gf_promo_spend_local_non_mfc) as gf_promo_spend_local_non_mfc
        ,sum(mex_promo_spend_usd_non_mfc) as mex_promo_spend_usd_non_mfc
        ,sum(mex_promo_spend_local_non_mfc) as mex_promo_spend_local_non_mfc
        ,sum(mex_promo_spend_n_usd_non_mfc) as mex_promo_spend_n_usd_non_mfc
        ,sum(mex_promo_spend_n_local_non_mfc) as mex_promo_spend_n_local_non_mfc
        ,sum(grab_promo_spend_usd_non_mfc) as grab_promo_spend_usd_non_mfc
        ,sum(grab_promo_spend_local_non_mfc) as grab_promo_spend_local_non_mfc
        ,sum(grab_promo_spend_n_usd_non_mfc) as grab_promo_spend_n_usd_non_mfc
        ,sum(grab_promo_spend_n_local_non_mfc) as grab_promo_spend_n_local_non_mfc
        ,sum(promo_item_promo_price_usd_non_mfc) as promo_item_promo_price_usd_non_mfc
        ,sum(promo_item_promo_price_local_non_mfc) as promo_item_promo_price_local_non_mfc
        ,sum(promo_item_normal_price_usd_non_mfc) as promo_item_normal_price_usd_non_mfc
        ,sum(promo_item_normal_price_local_non_mfc) as promo_item_normal_price_local_non_mfc
        ,sum(promo_item_n_promo_price_usd_non_mfc) as promo_item_n_promo_price_usd_non_mfc
        ,sum(promo_item_n_promo_price_local_non_mfc) as promo_item_n_promo_price_local_non_mfc
        ,sum(promo_item_n_normal_price_usd_non_mfc) as promo_item_n_normal_price_usd_non_mfc
        ,sum(promo_item_n_normal_price_local_non_mfc) as promo_item_n_normal_price_local_non_mfc

        ,sum(partner_gf_promo_spend_usd) as partner_gf_promo_spend_usd
        ,sum(partner_gf_promo_spend_local) as partner_gf_promo_spend_local
        ,sum(partner_mex_promo_spend_usd) as partner_mex_promo_spend_usd
        ,sum(partner_mex_promo_spend_local) as partner_mex_promo_spend_local
        ,sum(partner_mex_promo_spend_n_usd) as partner_mex_promo_spend_n_usd
        ,sum(partner_mex_promo_spend_n_local) as partner_mex_promo_spend_n_local
        ,sum(partner_grab_promo_spend_usd) as partner_grab_promo_spend_usd
        ,sum(partner_grab_promo_spend_local) as partner_grab_promo_spend_local
        ,sum(partner_grab_promo_spend_n_usd) as partner_grab_promo_spend_n_usd
        ,sum(partner_grab_promo_spend_n_local) as partner_grab_promo_spend_n_local
        ,sum(partner_promo_item_promo_price_usd) as partner_promo_item_promo_price_usd
        ,sum(partner_promo_item_promo_price_local) as partner_promo_item_promo_price_local
        ,sum(partner_promo_item_normal_price_usd) as partner_promo_item_normal_price_usd
        ,sum(partner_promo_item_normal_price_local) as partner_promo_item_normal_price_local
        ,sum(partner_promo_item_n_promo_price_usd) as partner_promo_item_n_promo_price_usd
        ,sum(partner_promo_item_n_promo_price_local) as partner_promo_item_n_promo_price_local
        ,sum(partner_promo_item_n_normal_price_usd) as partner_promo_item_n_normal_price_usd
        ,sum(partner_promo_item_n_normal_price_local) as partner_promo_item_n_normal_price_local

        ,sum(partner_gf_promo_spend_usd_non_mfc) as partner_gf_promo_spend_usd_non_mfc
        ,sum(partner_gf_promo_spend_local_non_mfc) as partner_gf_promo_spend_local_non_mfc
        ,sum(partner_mex_promo_spend_usd_non_mfc) as partner_mex_promo_spend_usd_non_mfc
        ,sum(partner_mex_promo_spend_local_non_mfc) as partner_mex_promo_spend_local_non_mfc
        ,sum(partner_mex_promo_spend_n_usd_non_mfc) as partner_mex_promo_spend_n_usd_non_mfc
        ,sum(partner_mex_promo_spend_n_local_non_mfc) as partner_mex_promo_spend_n_local_non_mfc
        ,sum(partner_grab_promo_spend_usd_non_mfc) as partner_grab_promo_spend_usd_non_mfc
        ,sum(partner_grab_promo_spend_local_non_mfc) as partner_grab_promo_spend_local_non_mfc
        ,sum(partner_grab_promo_spend_n_usd_non_mfc) as partner_grab_promo_spend_n_usd_non_mfc
        ,sum(partner_grab_promo_spend_n_local_non_mfc) as partner_grab_promo_spend_n_local_non_mfc
        ,sum(partner_promo_item_promo_price_usd_non_mfc) as partner_promo_item_promo_price_usd_non_mfc
        ,sum(partner_promo_item_promo_price_local_non_mfc) as partner_promo_item_promo_price_local_non_mfc
        ,sum(partner_promo_item_normal_price_usd_non_mfc) as partner_promo_item_normal_price_usd_non_mfc
        ,sum(partner_promo_item_normal_price_local_non_mfc) as partner_promo_item_normal_price_local_non_mfc
        ,sum(partner_promo_item_n_promo_price_usd_non_mfc) as partner_promo_item_n_promo_price_usd_non_mfc
        ,sum(partner_promo_item_n_promo_price_local_non_mfc) as partner_promo_item_n_promo_price_local_non_mfc
        ,sum(partner_promo_item_n_normal_price_usd_non_mfc) as partner_promo_item_n_normal_price_usd_non_mfc
        ,sum(partner_promo_item_n_normal_price_local_non_mfc) as partner_promo_item_n_normal_price_local_non_mfc
        
    FROM slide.gf_mfc_brand_daily
    where
        date_trunc('month', date(date_local)) >= date_add('month', -1, date_trunc('month',date([[inc_start_date]]))) -- date_trunc('month',date([[inc_start_date]]))
        and date_trunc('month', date(date_local)) < date_add('month', 1, date_trunc('month',date([[inc_end_date]])))
    group by 1,2,3,4,5,6,7)
select * from (
select
    CASE 
        WHEN country IS NULL AND city IS NULL THEN 'By Region'
        WHEN country IS NOT NULL AND city IS NULL THEN 'By Country'
        WHEN city IS NOT NULL THEN 'By City'
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
    END AS date_local


    ,CASE 
        WHEN country IS NULL AND city IS NULL THEN 'All'
        ELSE country END AS country
        
    ,CASE 
        WHEN city IS NULL THEN 'All'
        ELSE city END AS city

    ,CASE 
        WHEN business_model IS NULL THEN 'All'
        ELSE business_model
    END AS business_model	

    ,CASE 
        WHEN cashless_status IS NULL THEN 'All'
        ELSE cashless_status
    END AS cashless_status	

    ,sum(completed_orders_promo_item) as completed_orders_promo_item
    ,sum(gf_promo_spend_usd) as gf_promo_spend_usd
    ,sum(gf_promo_spend_local) as gf_promo_spend_local
    ,sum(mex_promo_spend_usd) as mex_promo_spend_usd
    ,sum(mex_promo_spend_local) as mex_promo_spend_local
    ,sum(mex_promo_spend_usd) as mex_promo_spend_n_usd
    ,sum(mex_promo_spend_n_local) as mex_promo_spend_n_local
    ,sum(grab_promo_spend_usd) as grab_promo_spend_usd
    ,sum(grab_promo_spend_local) as grab_promo_spend_local
    ,sum(grab_promo_spend_n_usd) as grab_promo_spend_n_usd
    ,sum(grab_promo_spend_n_local) as grab_promo_spend_n_local
    ,sum(promo_item_promo_price_usd) as promo_item_promo_price_usd
    ,sum(promo_item_promo_price_local) as promo_item_promo_price_local
    ,sum(promo_item_normal_price_usd) as promo_item_normal_price_usd
    ,sum(promo_item_normal_price_local) as promo_item_normal_price_local
    ,sum(promo_item_n_promo_price_usd) as promo_item_n_promo_price_usd
    ,sum(promo_item_n_promo_price_local) as promo_item_n_promo_price_local
    ,sum(promo_item_n_normal_price_usd) as promo_item_n_normal_price_usd
    ,sum(promo_item_n_normal_price_local) as promo_item_n_normal_price_local

    ,sum(gf_promo_spend_usd_non_mfc) as gf_promo_spend_usd_non_mfc
    ,sum(gf_promo_spend_local_non_mfc) as gf_promo_spend_local_non_mfc
    ,sum(mex_promo_spend_usd_non_mfc) as mex_promo_spend_usd_non_mfc
    ,sum(mex_promo_spend_local_non_mfc) as mex_promo_spend_local_non_mfc
    ,sum(mex_promo_spend_n_usd_non_mfc) as mex_promo_spend_n_usd_non_mfc
    ,sum(mex_promo_spend_n_local_non_mfc) as mex_promo_spend_n_local_non_mfc
    ,sum(grab_promo_spend_usd_non_mfc) as grab_promo_spend_usd_non_mfc
    ,sum(grab_promo_spend_local_non_mfc) as grab_promo_spend_local_non_mfc
    ,sum(grab_promo_spend_n_usd_non_mfc) as grab_promo_spend_n_usd_non_mfc
    ,sum(grab_promo_spend_n_local_non_mfc) as grab_promo_spend_n_local_non_mfc
    ,sum(promo_item_promo_price_usd_non_mfc) as promo_item_promo_price_usd_non_mfc
    ,sum(promo_item_promo_price_local_non_mfc) as promo_item_promo_price_local_non_mfc
    ,sum(promo_item_normal_price_usd_non_mfc) as promo_item_normal_price_usd_non_mfc
    ,sum(promo_item_normal_price_local_non_mfc) as promo_item_normal_price_local_non_mfc
    ,sum(promo_item_n_promo_price_usd_non_mfc) as promo_item_n_promo_price_usd_non_mfc
    ,sum(promo_item_n_promo_price_local_non_mfc) as promo_item_n_promo_price_local_non_mfc
    ,sum(promo_item_n_normal_price_usd_non_mfc) as promo_item_n_normal_price_usd_non_mfc
    ,sum(promo_item_n_normal_price_local_non_mfc) as promo_item_n_normal_price_local_non_mfc

    ,sum(partner_gf_promo_spend_usd) as partner_gf_promo_spend_usd
    ,sum(partner_gf_promo_spend_local) as partner_gf_promo_spend_local
    ,sum(partner_mex_promo_spend_usd) as partner_mex_promo_spend_usd
    ,sum(partner_mex_promo_spend_local) as partner_mex_promo_spend_local
    ,sum(partner_mex_promo_spend_n_usd) as partner_mex_promo_spend_n_usd
    ,sum(partner_mex_promo_spend_n_local) as partner_mex_promo_spend_n_local
    ,sum(partner_grab_promo_spend_usd) as partner_grab_promo_spend_usd
    ,sum(partner_grab_promo_spend_local) as partner_grab_promo_spend_local
    ,sum(partner_grab_promo_spend_n_usd) as partner_grab_promo_spend_n_usd
    ,sum(partner_grab_promo_spend_n_local) as partner_grab_promo_spend_n_local
    ,sum(partner_promo_item_promo_price_usd) as partner_promo_item_promo_price_usd
    ,sum(partner_promo_item_promo_price_local) as partner_promo_item_promo_price_local
    ,sum(partner_promo_item_normal_price_usd) as partner_promo_item_normal_price_usd
    ,sum(partner_promo_item_normal_price_local) as partner_promo_item_normal_price_local
    ,sum(partner_promo_item_n_promo_price_usd) as partner_promo_item_n_promo_price_usd
    ,sum(partner_promo_item_n_promo_price_local) as partner_promo_item_n_promo_price_local
    ,sum(partner_promo_item_n_normal_price_usd) as partner_promo_item_n_normal_price_usd
    ,sum(partner_promo_item_n_normal_price_local) as partner_promo_item_n_normal_price_local

    ,sum(partner_gf_promo_spend_usd_non_mfc) as partner_gf_promo_spend_usd_non_mfc
    ,sum(partner_gf_promo_spend_local_non_mfc) as partner_gf_promo_spend_local_non_mfc
    ,sum(partner_mex_promo_spend_usd_non_mfc) as partner_mex_promo_spend_usd_non_mfc
    ,sum(partner_mex_promo_spend_local_non_mfc) as partner_mex_promo_spend_local_non_mfc
    ,sum(partner_mex_promo_spend_n_usd_non_mfc) as partner_mex_promo_spend_n_usd_non_mfc
    ,sum(partner_mex_promo_spend_n_local_non_mfc) as partner_mex_promo_spend_n_local_non_mfc
    ,sum(partner_grab_promo_spend_usd_non_mfc) as partner_grab_promo_spend_usd_non_mfc
    ,sum(partner_grab_promo_spend_local_non_mfc) as partner_grab_promo_spend_local_non_mfc
    ,sum(partner_grab_promo_spend_n_usd_non_mfc) as partner_grab_promo_spend_n_usd_non_mfc
    ,sum(partner_grab_promo_spend_n_local_non_mfc) as partner_grab_promo_spend_n_local_non_mfc
    ,sum(partner_promo_item_promo_price_usd_non_mfc) as partner_promo_item_promo_price_usd_non_mfc
    ,sum(partner_promo_item_promo_price_local_non_mfc) as partner_promo_item_promo_price_local_non_mfc
    ,sum(partner_promo_item_normal_price_usd_non_mfc) as partner_promo_item_normal_price_usd_non_mfc
    ,sum(partner_promo_item_normal_price_local_non_mfc) as partner_promo_item_normal_price_local_non_mfc
    ,sum(partner_promo_item_n_promo_price_usd_non_mfc) as partner_promo_item_n_promo_price_usd_non_mfc
    ,sum(partner_promo_item_n_promo_price_local_non_mfc) as partner_promo_item_n_promo_price_local_non_mfc
    ,sum(partner_promo_item_n_normal_price_usd_non_mfc) as partner_promo_item_n_normal_price_usd_non_mfc
    ,sum(partner_promo_item_n_normal_price_local_non_mfc) as partner_promo_item_n_normal_price_local_non_mfc

from 
    base

GROUP BY GROUPING SETS
  (
	(date_local,country),(week_of,country),(month_of,country),
	(date_local,country,business_model),(week_of,country,business_model),(month_of,country,business_model),
	(date_local,country,cashless_status),(week_of,country,cashless_status),(month_of,country,cashless_status),
	(date_local,country,business_model,cashless_status),(week_of,country,business_model,cashless_status),(month_of,country,business_model,cashless_status),
	
	
	(date_local,country,city),(week_of,country,city),(month_of,country,city),
	(date_local,country,city,business_model),(week_of,country,city,business_model),(month_of,country,city,business_model),
	(date_local,country,city,cashless_status),(week_of,country,city,cashless_status),(month_of,country,city,cashless_status),
	(date_local,country,city,business_model,cashless_status),(week_of,country,city,business_model,cashless_status),(month_of,country,city,business_model,cashless_status),
	
	
	(date_local),(week_of),(month_of),
	(date_local,business_model),(week_of,business_model),(month_of,business_model),
	(date_local,cashless_status),(week_of,cashless_status),(month_of,cashless_status),	
	(date_local,business_model,cashless_status),(week_of,business_model,cashless_status),(month_of,business_model,cashless_status)				
  )
)
where 
    date_local >= (CASE 
	WHEN by_day_week_month = 'By Day' THEN date([[inc_start_date]])
	WHEN by_day_week_month = 'By Week' THEN date_trunc('week',date([[inc_start_date]]))
	WHEN by_day_week_month = 'By Month' THEN date_trunc('month',date([[inc_start_date]]))
 END)