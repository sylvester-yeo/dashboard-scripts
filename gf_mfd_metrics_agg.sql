with base as (
    select 
        date(date_local) as date_local
        ,date_trunc('week',date(date_local)) as week_of
        ,date_trunc('month',date(date_local)) as month_of
        ,country_name
        ,city_name
        ,restaurant_partner_status
        ,business_model
        ,cashless_status
        ,sum(commission_total_local) as commission_total_local
        ,sum(commission_total_usd) as commission_total_usd
        ,sum(total_incentive_inc_tsp_local) as total_incentive_inc_tsp_local
        ,sum(total_incentive_inc_tsp_usd) as total_incentive_inc_tsp_usd
        ,sum(total_promo_spend_local) as total_promo_spend_local
        ,sum(total_promo_spend_usd) as total_promo_spend_usd
        ,sum(total_grab_promo_spend_local) as total_grab_promo_spend_local
        ,sum(total_grab_promo_spend_usd) as total_grab_promo_spend_usd
        ,sum(total_mfd_local) as total_mfd_local
        ,sum(total_mfd_usd) as total_mfd_usd
        ,sum(mfc_orders) as mfc_orders
        ,sum(mfp_orders) as mfp_orders
        ,sum(double_dipped_orders) as double_dipped_orders
        ,sum(no_of_mfc_campaign) as no_of_mfc_campaign
        ,sum(total_mfc_mex_promo_spend_local) as total_mfc_mex_promo_spend_local
        ,sum(total_mfc_mex_promo_spend_usd) as total_mfc_mex_promo_spend_usd
        ,sum(mfc_prod_mex_promo_spend_local) as mfc_prod_mex_promo_spend_local
        ,sum(mfc_prod_mex_promo_spend_usd) as mfc_prod_mex_promo_spend_usd
        ,sum(mfc_gsheet_mex_promo_spend_local) as mfc_gsheet_mex_promo_spend_local
        ,sum(mfc_gsheet_mex_promo_spend_usd) as mfc_gsheet_mex_promo_spend_usd
        ,sum(mfc_prod_grab_promo_spend_local) as mfc_prod_grab_promo_spend_local
        ,sum(mfc_prod_grab_promo_spend_usd) as mfc_prod_grab_promo_spend_usd
        ,sum(mfc_gsheet_grab_promo_spend_local) as mfc_gsheet_grab_promo_spend_local
        ,sum(mfc_gsheet_grab_promo_spend_usd) as mfc_gsheet_grab_promo_spend_usd
        ,sum(total_mfp_promo_code_expense_local) as total_mfp_promo_code_expense_local
        ,sum(total_mfp_promo_code_expense_usd) as total_mfp_promo_code_expense_usd
        ,sum(mfp_mex_promo_spend_local) as mfp_mex_promo_spend_local
        ,sum(mfp_mex_promo_spend_usd) as mfp_mex_promo_spend_usd
        ,sum(mfp_prod_mex_promo_spend_local) as mfp_prod_mex_promo_spend_local
        ,sum(mfp_prod_mex_promo_spend_usd) as mfp_prod_mex_promo_spend_usd
        ,sum(mfp_gsheet_mex_promo_spend_local) as mfp_gsheet_mex_promo_spend_local
        ,sum(mfp_gsheet_mex_promo_spend_usd) as mfp_gsheet_mex_promo_spend_usd
        ,sum(mfp_grab_promo_code_spend_local) as mfp_grab_promo_code_spend_local
        ,sum(mfp_grab_promo_code_spend_usd) as mfp_grab_promo_code_spend_usd
        ,sum(grab_promo_code_spend_local) as grab_promo_code_spend_local
        ,sum(grab_promo_code_spend_usd) as grab_promo_code_spend_usd
        ,sum(grab_mfc_prod_spend_local) as grab_mfc_prod_spend_local
        ,sum(grab_mfc_prod_spend_usd) as grab_mfc_prod_spend_usd
        ,sum(pax_fare_local) as pax_fare_local
        ,sum(pax_fare_usd) as pax_fare_usd
        ,sum(small_order_fee_local) as small_order_fee_local
        ,sum(small_order_fee_usd) as small_order_fee_usd
        ,sum(convenience_fee_local) as convenience_fee_local
        ,sum(convenience_fee_usd) as convenience_fee_usd
        ,sum(pax_platform_fee_local) as pax_platform_fee_local
        ,sum(pax_platform_fee_usd) as pax_platform_fee_usd
        ,sum(dax_fare_local) as dax_fare_local
        ,sum(dax_fare_usd) as dax_fare_usd
        ,sum(subsidy_local) as subsidy_local
        ,sum(subsidy_usd) as subsidy_usd
        ,sum(incentives_local) as incentives_local
        ,sum(incentives_usd) as incentives_usd
        ,sum(spot_incentive_bonus_local) as spot_incentive_bonus_local
        ,sum(spot_incentive_bonus_usd) as spot_incentive_bonus_usd
        ,sum(sub_total_local) as sub_total_local
        ,sum(sub_total_usd) as sub_total_usd
        ,sum(basket_size_local) as basket_size_local
        ,sum(basket_size_usd) as basket_size_usd
        ,sum(gmv_local) as gmv_local
        ,sum(gmv_usd) as gmv_usd
        ,sum(commission_from_driver_local) as commission_from_driver_local
        ,sum(commission_from_driver_usd) as commission_from_driver_usd
        ,sum(commission_from_merchant_local) as commission_from_merchant_local
        ,sum(commission_from_merchant_usd) as commission_from_merchant_usd
        ,sum(partner_total_mfc_prod_promo_usd) as partner_total_mfc_prod_promo_usd
        ,sum(partner_total_mfc_prod_promo_local) as partner_total_mfc_prod_promo_local
    from slide.gf_mfd_mex_agg
    where
        date_trunc('month', date(partition_date_local)) >= date_add('month', -1, date_trunc('month',date([[inc_start_date]]))) 
        and date_trunc('month', date(partition_date_local)) <= date([[inc_end_date]])
    group by 1,2,3,4,5,6,7,8
)
select * from (
select
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
    END AS date_local


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
    
    ,case 
        when restaurant_partner_status is null then 'All'
        else restaurant_partner_status
    end as partner_status

    ,sum(commission_total_local) as commission_total_local
        ,sum(commission_total_usd) as commission_total_usd
        ,sum(total_incentive_inc_tsp_local) as total_incentive_inc_tsp_local
        ,sum(total_incentive_inc_tsp_usd) as total_incentive_inc_tsp_usd
        ,sum(total_promo_spend_local) as total_promo_spend_local
        ,sum(total_promo_spend_usd) as total_promo_spend_usd
        ,sum(total_grab_promo_spend_local) as total_grab_promo_spend_local
        ,sum(total_grab_promo_spend_usd) as total_grab_promo_spend_usd
        ,sum(total_mfd_local) as total_mfd_local
        ,sum(total_mfd_usd) as total_mfd_usd
        ,sum(mfc_orders) as mfc_orders
        ,sum(mfp_orders) as mfp_orders
        ,sum(double_dipped_orders) as double_dipped_orders
        ,sum(no_of_mfc_campaign) as no_of_mfc_campaign
        ,sum(total_mfc_mex_promo_spend_local) as total_mfc_mex_promo_spend_local
        ,sum(total_mfc_mex_promo_spend_usd) as total_mfc_mex_promo_spend_usd
        ,sum(mfc_prod_mex_promo_spend_local) as mfc_prod_mex_promo_spend_local
        ,sum(mfc_prod_mex_promo_spend_usd) as mfc_prod_mex_promo_spend_usd
        ,sum(mfc_gsheet_mex_promo_spend_local) as mfc_gsheet_mex_promo_spend_local
        ,sum(mfc_gsheet_mex_promo_spend_usd) as mfc_gsheet_mex_promo_spend_usd
        ,sum(mfc_prod_grab_promo_spend_local) as mfc_prod_grab_promo_spend_local
        ,sum(mfc_prod_grab_promo_spend_usd) as mfc_prod_grab_promo_spend_usd
        ,sum(mfc_gsheet_grab_promo_spend_local) as mfc_gsheet_grab_promo_spend_local
        ,sum(mfc_gsheet_grab_promo_spend_usd) as mfc_gsheet_grab_promo_spend_usd
        ,sum(total_mfp_promo_code_expense_local) as total_mfp_promo_code_expense_local
        ,sum(total_mfp_promo_code_expense_usd) as total_mfp_promo_code_expense_usd
        ,sum(mfp_mex_promo_spend_local) as mfp_mex_promo_spend_local
        ,sum(mfp_mex_promo_spend_usd) as mfp_mex_promo_spend_usd
        ,sum(mfp_prod_mex_promo_spend_local) as mfp_prod_mex_promo_spend_local
        ,sum(mfp_prod_mex_promo_spend_usd) as mfp_prod_mex_promo_spend_usd
        ,sum(mfp_gsheet_mex_promo_spend_local) as mfp_gsheet_mex_promo_spend_local
        ,sum(mfp_gsheet_mex_promo_spend_usd) as mfp_gsheet_mex_promo_spend_usd
        ,sum(mfp_grab_promo_code_spend_local) as mfp_grab_promo_code_spend_local
        ,sum(mfp_grab_promo_code_spend_usd) as mfp_grab_promo_code_spend_usd
        ,sum(grab_promo_code_spend_local) as grab_promo_code_spend_local
        ,sum(grab_promo_code_spend_usd) as grab_promo_code_spend_usd
        ,sum(grab_mfc_prod_spend_local) as grab_mfc_prod_spend_local
        ,sum(grab_mfc_prod_spend_usd) as grab_mfc_prod_spend_usd
        ,sum(pax_fare_local) as pax_fare_local
        ,sum(pax_fare_usd) as pax_fare_usd
        ,sum(small_order_fee_local) as small_order_fee_local
        ,sum(small_order_fee_usd) as small_order_fee_usd
        ,sum(convenience_fee_local) as convenience_fee_local
        ,sum(convenience_fee_usd) as convenience_fee_usd
        ,sum(pax_platform_fee_local) as pax_platform_fee_local
        ,sum(pax_platform_fee_usd) as pax_platform_fee_usd
        ,sum(dax_fare_local) as dax_fare_local
        ,sum(dax_fare_usd) as dax_fare_usd
        ,sum(subsidy_local) as subsidy_local
        ,sum(subsidy_usd) as subsidy_usd
        ,sum(incentives_local) as incentives_local
        ,sum(incentives_usd) as incentives_usd
        ,sum(spot_incentive_bonus_local) as spot_incentive_bonus_local
        ,sum(spot_incentive_bonus_usd) as spot_incentive_bonus_usd
        ,sum(sub_total_local) as sub_total_local
        ,sum(sub_total_usd) as sub_total_usd
        ,sum(basket_size_local) as basket_size_local
        ,sum(basket_size_usd) as basket_size_usd
        ,sum(gmv_local) as gmv_local
        ,sum(gmv_usd) as gmv_usd
        ,sum(commission_from_driver_local) as commission_from_driver_local
        ,sum(commission_from_driver_usd) as commission_from_driver_usd
        ,sum(commission_from_merchant_local) as commission_from_merchant_local
        ,sum(commission_from_merchant_usd) as commission_from_merchant_usd
        ,sum(partner_total_mfc_prod_promo_usd) as partner_total_mfc_prod_promo_usd
        ,sum(partner_total_mfc_prod_promo_local) as partner_total_mfc_prod_promo_local

from 
    base


GROUP BY GROUPING SETS
  (
	(date_local,country_name),(week_of,country_name),(month_of,country_name),
				(date_local,country_name,business_model),(week_of,country_name,business_model),(month_of,country_name,business_model),
				(date_local,country_name,restaurant_partner_status),(week_of,country_name,restaurant_partner_status),(month_of,country_name,restaurant_partner_status),
				(date_local,country_name,business_model,restaurant_partner_status),(week_of,country_name,business_model,restaurant_partner_status),(month_of,country_name,business_model,restaurant_partner_status),
				(date_local,country_name,cashless_status),(week_of,country_name,cashless_status),(month_of,country_name,cashless_status),
				(date_local,country_name,business_model,cashless_status),(week_of,country_name,business_model,cashless_status),(month_of,country_name,business_model,cashless_status),
				(date_local,country_name,business_model,cashless_status,restaurant_partner_status),(week_of,country_name,business_model,cashless_status,restaurant_partner_status),(month_of,country_name,business_model,cashless_status,restaurant_partner_status),
				
				
				(date_local,country_name,city_name),(week_of,country_name,city_name),(month_of,country_name,city_name),
				(date_local,country_name,city_name,business_model),(week_of,country_name,city_name,business_model),(month_of,country_name,city_name,business_model),
				(date_local,country_name,city_name,restaurant_partner_status),(week_of,country_name,city_name,restaurant_partner_status),(month_of,country_name,city_name,restaurant_partner_status),
				(date_local,country_name,city_name,cashless_status),(week_of,country_name,city_name,cashless_status),(month_of,country_name,city_name,cashless_status),
				(date_local,country_name,city_name,business_model,cashless_status),(week_of,country_name,city_name,business_model,cashless_status),(month_of,country_name,city_name,business_model,cashless_status),
				(date_local,country_name,city_name,business_model,cashless_status,restaurant_partner_status),(week_of,country_name,city_name,business_model,cashless_status,restaurant_partner_status),(month_of,country_name,city_name,business_model,cashless_status,restaurant_partner_status),
				(date_local,country_name,city_name,business_model,restaurant_partner_status),(week_of,country_name,city_name,business_model,restaurant_partner_status),(month_of,country_name,city_name,business_model,restaurant_partner_status),
				
				
				(date_local),(week_of),(month_of),
				(date_local,business_model),(week_of,business_model),(month_of,business_model),
				(date_local,restaurant_partner_status),(week_of,restaurant_partner_status),(month_of,restaurant_partner_status),
				(date_local,cashless_status),(week_of,cashless_status),(month_of,cashless_status),	
				(date_local,business_model,cashless_status),(week_of,business_model,cashless_status),(month_of,business_model,cashless_status),				
				(date_local,business_model,cashless_status,restaurant_partner_status),(week_of,business_model,cashless_status,restaurant_partner_status),(month_of,business_model,cashless_status,restaurant_partner_status),				
				(date_local,business_model,restaurant_partner_status),(week_of,business_model,restaurant_partner_status),(month_of,business_model,restaurant_partner_status)		
  )
)
where 
    date_local >= (CASE 
	WHEN by_day_week_month = 'By Day' THEN date([[inc_start_date]])
	WHEN by_day_week_month = 'By Week' THEN date_trunc('week',date([[inc_start_date]]))
	WHEN by_day_week_month = 'By Month' THEN date_trunc('month',date([[inc_start_date]]))
 END)