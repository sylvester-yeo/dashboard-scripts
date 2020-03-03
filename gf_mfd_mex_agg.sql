select *, date_local as partition_date_local from (
    select 
        merchant_id
        ,date_local
        ,country_name
        ,city_name
        ,restaurant_partner_status
        ,business_model
        ,cashless_status
        ,sum(commission_total) as commission_total_local
        ,sum(commission_total/fx_one_usd) as commission_total_usd
        ,sum(total_incentive_inc_tsp) as total_incentive_inc_tsp_local
        ,sum(total_incentive_inc_tsp/fx_one_usd) as total_incentive_inc_tsp_usd
        ,sum(total_promo_spend) as total_promo_spend_local
        ,sum(total_promo_spend/fx_one_usd) as total_promo_spend_usd
        ,sum(total_grab_promo_spend) as total_grab_promo_spend_local
        ,sum(total_grab_promo_spend/fx_one_usd) as total_grab_promo_spend_usd
        ,sum(total_mfd) as total_mfd_local
        ,sum(total_mfd/fx_one_usd) as total_mfd_usd
        ,sum(net_revenue) as net_revenue_local
        ,sum(net_revenue/fx_one_usd) as net_revenue_usd

        ,sum(case when mfc_indicator = 'MFC order' then 1 else 0 end) as mfc_orders
        ,sum(case when mfp_indicator = 'MFP order' then 1 else 0 end) as mfp_orders
        ,sum(case when total_mfd > 0 and total_grab_promo_spend > 0 then 1 else 0 end) double_dipped_orders
        ,sum(no_of_mfc_campaign) as no_of_mfc_campaign

        ,sum(total_mfc_mex_promo_spend) as total_mfc_mex_promo_spend_local
        ,sum(total_mfc_mex_promo_spend/fx_one_usd) as total_mfc_mex_promo_spend_usd

        ,sum(mfc_prod_mex_promo_spend) as mfc_prod_mex_promo_spend_local
        ,sum(mfc_prod_mex_promo_spend/fx_one_usd) as mfc_prod_mex_promo_spend_usd

        ,sum(mfc_gsheet_mex_promo_spend) as mfc_gsheet_mex_promo_spend_local
        ,sum(mfc_gsheet_mex_promo_spend/fx_one_usd) as mfc_gsheet_mex_promo_spend_usd
        ,sum(mfc_prod_grab_promo_spend) as mfc_prod_grab_promo_spend_local
        ,sum(mfc_prod_grab_promo_spend/fx_one_usd) as mfc_prod_grab_promo_spend_usd
        ,sum(mfc_gsheet_grab_promo_spend) as mfc_gsheet_grab_promo_spend_local
        ,sum(mfc_gsheet_grab_promo_spend/fx_one_usd) as mfc_gsheet_grab_promo_spend_usd
        ,sum(total_mfp_promo_code_expense) as total_mfp_promo_code_expense_local
        ,sum(total_mfp_promo_code_expense/fx_one_usd) as total_mfp_promo_code_expense_usd
        ,sum(mfp_mex_promo_spend) as mfp_mex_promo_spend_local
        ,sum(mfp_mex_promo_spend/fx_one_usd) as mfp_mex_promo_spend_usd
        ,sum(mfp_prod_mex_promo_spend) as mfp_prod_mex_promo_spend_local
        ,sum(mfp_prod_mex_promo_spend/fx_one_usd) as mfp_prod_mex_promo_spend_usd
        ,sum(mfp_gsheet_mex_promo_spend) as mfp_gsheet_mex_promo_spend_local
        ,sum(mfp_gsheet_mex_promo_spend/fx_one_usd) as mfp_gsheet_mex_promo_spend_usd
        ,sum(mfp_grab_promo_code_spend) as mfp_grab_promo_code_spend_local
        ,sum(mfp_grab_promo_code_spend/fx_one_usd) as mfp_grab_promo_code_spend_usd
        ,sum(grab_promo_code_spend) as grab_promo_code_spend_local
        ,sum(grab_promo_code_spend/fx_one_usd) as grab_promo_code_spend_usd
        ,sum(grab_mfc_prod_spend) as grab_mfc_prod_spend_local
        ,sum(grab_mfc_prod_spend/fx_one_usd) as grab_mfc_prod_spend_usd
        ,sum(pax_fare) as pax_fare_local
        ,sum(pax_fare/fx_one_usd) as pax_fare_usd
        ,sum(small_order_fee) as small_order_fee_local
        ,sum(small_order_fee/fx_one_usd) as small_order_fee_usd
        ,sum(dax_fare) as dax_fare_local
        ,sum(dax_fare/fx_one_usd) as dax_fare_usd
        ,sum(subsidy) as subsidy_local
        ,sum(subsidy/fx_one_usd) as subsidy_usd
        ,sum(incentives) as incentives_local
        ,sum(incentives/fx_one_usd) as incentives_usd
        ,sum(spot_incentive_bonus) as spot_incentive_bonus_local
        ,sum(spot_incentive_bonus/fx_one_usd) as spot_incentive_bonus_usd
        ,sum(sub_total) as sub_total_local
        ,sum(sub_total/fx_one_usd) as sub_total_usd
        ,sum(basket_size) as basket_size_local
        ,sum(basket_size/fx_one_usd) as basket_size_usd
        ,sum(gmv) as gmv_local
        ,sum(gmv/fx_one_usd) as gmv_usd
        ,sum(commission_from_driver) as commission_from_driver_local
        ,sum(commission_from_driver/fx_one_usd) as commission_from_driver_usd
        ,sum(commission_from_merchant) as commission_from_merchant_local
        ,sum(commission_from_merchant/fx_one_usd) as commission_from_merchant_usd

        ,sum(case when restaurant_partner_status = 'partner' then coalesce(mfc_prod_grab_promo_spend/fx_one_usd,0) + coalesce(mfc_prod_mex_promo_spend/fx_one_usd,0) else 0 end) as partner_total_mfc_prod_promo_usd
        ,sum(case when restaurant_partner_status = 'partner' then coalesce(mfc_prod_grab_promo_spend,0) + coalesce(mfc_prod_mex_promo_spend,0) else 0 end) as partner_total_mfc_prod_promo_local
    from slide.gf_mfd --change to the updated table
    where date_local >= [[inc_start_date]]
        and date_local <= [[inc_end_date]]
    group by 1,2,3,4,5,6,7
)