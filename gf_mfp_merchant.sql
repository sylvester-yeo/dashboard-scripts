/*
    Name: slide.gf_mfp_merchant
    Refresh Time: Daily, 21:00 UTC
    Aggregation Mode: Overwrite
    Lighthouse Dependancy Tables:
        slide.mfp_orders 19:00
*/

/*starting from q3 onwards*/
select 
    country_name as country
    ,city_name as city
    ,merchant_id 
    ,merchant_name
    ,case when is_integrated_model = 1 then 'Integrated' else 'Concierge' end as business_model
    ,case when is_grabpay = 1 then 'Cashless' else 'Cash' end as cashless_status
    ,case when is_partner_merchant = 1 then 'Partner' else 'Non-Partner' end as partner_status
    ,count(1) as no_of_mfp_orders
    ,sum(total_promo_spend) as total_mex_grab_spend_local
    ,sum(total_promo_spend/exchange_one_usd) as total_mex_grab_spend_usd
    ,sum(mex_promo_spend) as mex_mfp_spend_local
    ,sum(mex_promo_spend/exchange_one_usd) as mex_mfp_spend_usd
    ,sum(case when product_flag = 'Non MFP' then total_promo_spend else 0 end) as total_mex_grab_promo_spend_non_mfp_local
    ,sum(case when product_flag = 'Non MFP' then total_promo_spend/exchange_one_usd else 0 end) as total_mex_grab_promo_spend_non_mfp_usd
    ,sum(case when product_flag = 'Non MFP' then mex_promo_spend else 0 end) as mex_promo_spend_non_mfp_local
    ,sum(case when product_flag = 'Non MFP' then mex_promo_spend/exchange_one_usd else 0 end) as mex_promo_spend_non_mfp_usd
   ,date_local
from slide.mfp_orders
where date(date_local) >= date([[inc_start_date]])
    and date(date_local) <= date([[inc_end_date]])
group by 1,2,3,4,5,6,7,date_local