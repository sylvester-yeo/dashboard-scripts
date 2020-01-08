/*
    to check with zian:
        ad_id = 18971825, mex_id = 6-CY3KJCMYT4MAL7 --> no mapping to dim_merchants
*/


with indiv_mex as (
    select 
        ad_id
        ,serve_date_local
        ,indiv_mex
        ,mex_ids
        ,coalesce(ad_spend,0) as total_ad_spend
        ,coalesce(ad_spend,0) / cardinality(split(mex_ids, ',')) as avg_ad_spend
    from data_analytics.ads_in_app_food_mex_ad_id_view
    cross join unnest (cast(split(mex_ids, ',') as array<varchar>)) as x(indiv_mex)
    where serve_date_local >= date('2019-12-01')
        -- and serve_date_local >= [end_date]
)
select 
    dim_merchants.country_id
    ,countries.name
    ,dim_merchants.business_name 
    ,ad_mex.*
    ,rer.exchange_one_usd
from indiv_mex ad_mex
left join datamart.dim_merchants on dim_merchants.merchant_id = ad_mex.indiv_mex
left join datamart.ref_exchange_rates rer on dim_merchants.country_id  = rer.country_id and (ad_mex.serve_date_local between rer.start_date and rer.end_date)
left join public.countries on dim_merchants.country_id = countries.id
where ad_mex.total_ad_spend > 0
    -- and countries.id is null


