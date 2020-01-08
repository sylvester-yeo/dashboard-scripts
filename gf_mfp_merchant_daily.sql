/*
    Name: slide.gf_mfp_merchant_daily
    Refresh Time: Daily, 21:00 UTC
    Aggregation Mode: Overwrite
    Lighthouse Dependancy Tables:
        slide.mfp_orders 21:00
*/

/*starting from q3 onwards*/
select 
    country
    ,city
    ,merchant_id
    ,merchant_name 
    ,date_local
    ,case when order_type = 1 then 'Integrated' else 'Concierge' end as business_model
    ,case when is_cashless_booking then 'Cashless' else 'Cash' end as cashless_status
    ,count(1) as no_of_mfp_orders
    ,sum(total_promo_spend) as total_mfp_mex_grab_spend_local
    ,sum(total_promo_spend/exchange_one_usd) as total_mfp_mex_grab_spend_usd
    ,sum(mex_promo_spend) as mex_mfp_spend_local
    ,sum(mex_promo_spend/exchange_one_usd) as mex_mfp_spend_usd
    ,date_local as partition_date_local
from slide.mfp_orders
/*where date(partition_date) >= date([[inc_start_date]]) - interval '1' day
    and date(partition_date) <= date([[inc_end_date]]) + interval '1' day
    and date(date_local) >= date([[inc_start_date]])
    and date(date_local) <= date([[inc_end_date]])*/
group by 1,2,3,4,5,6,7,13