with indiv_mex as (
    select 
        ad_id
        ,serve_date_local as date_local
        ,indiv_mex
        ,mex_ids
        ,coalesce(ad_spend,0) as total_ad_spend_usd
        ,coalesce(ad_spend,0) / cardinality(split(mex_ids, ',')) as avg_ad_spend_usd
    from data_analytics.ads_in_app_food_mex_ad_id_view
    cross join unnest (cast(split(mex_ids, ',') as array<varchar>)) as x(indiv_mex)
    where serve_date_local >= date([[inc_start_date]])
        and serve_date_local <= date([[inc_end_date]])
)
select 
    dim_merchants.country_id
    ,dim_merchants.city_id
    ,countries.name as country_name
    ,cities.name as city_name
    ,ad_mex.*
    ,total_ad_spend_usd*rer.exchange_one_usd as total_ad_spend_local
    ,avg_ad_spend_usd*rer.exchange_one_usd as avg_ad_spend_local
    ,rer.exchange_one_usd
    ,date_local as partition_date_local
from indiv_mex ad_mex
left join datamart.dim_merchants on dim_merchants.merchant_id = ad_mex.indiv_mex
left join datamart.ref_exchange_rates rer on dim_merchants.country_id  = rer.country_id and (ad_mex.date_local between rer.start_date and rer.end_date)
left join public.countries on dim_merchants.country_id = countries.id
left join public.cities on dim_merchants.city_id = cities.id
where ad_mex.total_ad_spend_usd > 0